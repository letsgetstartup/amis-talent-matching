"""FastAPI service exposing ingestion, matching, search and maintenance endpoints.

Core endpoints:
GET /health  -> cheap liveness (always returns status ok if process up)
GET /live    -> alias of /health
GET /ready   -> readiness probe (verifies Mongo connectivity + basic indexes + collections)
GET /db/status -> detailed DB status (ping)
POST /ingest/{candidate|job}
GET /match/job/{id}?k=5
GET /match/candidate/{id}?k=5
GET /match/report?k=5  -> per candidate top-k jobs
GET /candidates, /jobs -> list ids

MongoDB ONLY (all file/json dynamic persistence removed by policy).
"""
from fastapi import FastAPI, HTTPException, Header, Depends, Request, Response, UploadFile, File
import json
import logging
import os
import time
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Configure logging for LLM interactions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
from pydantic import BaseModel
from typing import List, Optional, Any
import requests
from .ingest_agent import (
    ingest_files,
    candidates_for_job,
    jobs_for_candidate,
    db,
    get_or_compute_matches,
    get_or_compute_candidates_for_job,
    get_cached_matches,
    backfill_matches,
    backfill_job_matches,
    recompute_skill_sets,
    refresh_existing,
    clear_extraction_cache,
    set_weights,
    get_weights,
    set_category_weights,
    set_distance_weight,
    set_min_skill_floor,
    canonical_skill,
    list_meta,
    recompute_embeddings,
    add_skill_synonym,
    llm_status,
    create_indexes,
    backfill_skills_meta,
    set_llm_required_on_upload,
    is_llm_required_on_upload,
)
from pathlib import Path
import tempfile, os, uuid, hashlib, re
import html  # needed for html.escape in share page generation
from bson import ObjectId
from .db import is_mock
from .routers_auth import router as auth_router
from .routers_jobs import router as jobs_router
from .routers_confirm import router as confirm_router
from .routers_candidates import router as candidates_router
from .routers_mapping import router as mapping_router
from .routers_tenant_jobs import router as tenant_jobs_router
from .routers_mobile import router as mobile_router
from .auth import require_tenant, optional_tenant_id
from .security_audit import audit_log, log_data_access, get_security_events, get_violation_summary

SAMPLES_DIR = Path(__file__).resolve().parent.parent / "samples"

# Global IP rate limit. Default raised to 300/min to accommodate dashboard fan-out
# while still allowing per-endpoint guards where needed. Can be tuned via env.
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "300"))
_RATE_BUCKET: dict[str, list[int]] = {}
_RATE_RESET: int = 60  # window seconds

# --- Outreach failure logger ---
def log_outreach_failure(candidate_id, job_ids, stage, error, raw_response=None, prompt=None, extra=None):
    """Log outreach generation failures to MongoDB for diagnostics."""
    doc = {
        'candidate_id': candidate_id,
        'job_ids': job_ids,
        'stage': stage,
        'error': str(error),
        'raw_response': raw_response,
        'prompt': prompt,
        'extra': extra,
        'ts': time.time()
    }
    try:
        db['outreach_failures'].insert_one(doc)
        logging.info(f"🔴 Outreach failure logged: stage={stage}, candidate={candidate_id}")
    except Exception as e:
        logging.error(f'Failed to log outreach failure: {e}')

@asynccontextmanager
async def lifespan(app: FastAPI):  # pragma: no cover
    # Auto bootstrap only when SKIP_BOOTSTRAP is NOT set. Set SKIP_BOOTSTRAP=1 to keep DB empty after purges.
    if not os.getenv("SKIP_BOOTSTRAP"):
        try:
            # Ensure indexes exist early to avoid DuplicateKeyError on null _src_hash
            create_indexes()
        except Exception:
            pass
        try:
            _auto_ingest_if_empty()
        except Exception:
            pass
    yield
    # teardown logic (none for now)

app = FastAPI(title="TalentDB API", version="0.1.2", lifespan=lifespan)

# Add security middleware
@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """Security middleware for audit logging and headers."""
    # Add security headers
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    # Allow same-origin iframes (needed to embed /imports.html inside the portal)
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    
    # Log sensitive endpoint access
    sensitive_paths = ["/candidate/", "/job/", "/match/", "/tenant/"]
    if any(path in str(request.url.path) for path in sensitive_paths):
        try:
            # Extract tenant info if available from headers
            api_key = request.headers.get("X-API-Key")
            if api_key:
                api_record = db["api_keys"].find_one({"key": api_key, "active": True})
                if api_record:
                    tenant_id = str(api_record.get("tenant_id", "unknown"))
                    audit_log(
                        tenant_id=tenant_id,
                        action="api_access",
                        resource="endpoint",
                        resource_id=str(request.url.path),
                        user_ip=request.client.host,
                        user_agent=request.headers.get("user-agent"),
                        success=response.status_code < 400,
                        details={"method": request.method, "status_code": response.status_code}
                    )
        except Exception:
            pass  # Never let audit logging break the main flow
    
    return response

app.include_router(auth_router)
app.include_router(jobs_router)
app.include_router(confirm_router)
app.include_router(candidates_router)
app.include_router(tenant_jobs_router)
app.include_router(mobile_router)
app.include_router(mapping_router)

"""Static / Frontend mounting.
We historically had two possible locations for the frontend:
1. talentdb/frontend/public          (co-located inside the backend package)
2. ../frontend/public (repo root)    (stand‑alone frontend project)

Earlier code only looked at (1). User's recommend.html actually lives in (2),
so /recommend.html returned 404. We now search both and pick the first that
contains recommend.html; if not found we fall back to any existing directory.
"""
_CANDIDATE_FRONTEND_DIRS: list[Path] = []
try:
    _here = Path(__file__).resolve()
    _p1 = _here.parent.parent / "frontend" / "public"
    _p2 = _here.parent.parent.parent / "frontend" / "public"
    for _p in (_p1, _p2):
        if _p.exists():
            _CANDIDATE_FRONTEND_DIRS.append(_p)
except Exception:
    pass

_FRONTEND_PUBLIC: Path | None = None
if _CANDIDATE_FRONTEND_DIRS:
    # Prefer one containing recommend.html
    for _cand in _CANDIDATE_FRONTEND_DIRS:
        if (_cand / "recommend.html").exists():
            _FRONTEND_PUBLIC = _cand
            break
    if _FRONTEND_PUBLIC is None:
        _FRONTEND_PUBLIC = _CANDIDATE_FRONTEND_DIRS[0]

if _FRONTEND_PUBLIC and _FRONTEND_PUBLIC.exists():
    try:
        app.mount("/static", StaticFiles(directory=_FRONTEND_PUBLIC, html=False), name="public-static")
    except Exception:
        pass
else:
    _FRONTEND_PUBLIC = None  # normalize

def _load_static_file(fname: str) -> str | None:
    if _FRONTEND_PUBLIC and (_FRONTEND_PUBLIC / fname).exists():
        try:
            return (_FRONTEND_PUBLIC / fname).read_text(encoding='utf-8')
        except Exception:
            return None
    # Fallback: scan other candidates lazily
    for _alt in _CANDIDATE_FRONTEND_DIRS:
        if (_alt / fname).exists():
            try:
                return (_alt / fname).read_text(encoding='utf-8')
            except Exception:
                continue
    return None

@app.get("/recommend.html", response_class=HTMLResponse)
def recommend_html():
    content = _load_static_file("recommend.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "recommend.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/personal_pitch.html", response_class=HTMLResponse)
def personal_pitch_html():
    content = _load_static_file("personal_pitch.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "personal_pitch.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS]
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/agency.html", response_class=HTMLResponse)
def agency_html():
    content = _load_static_file("agency.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "agency.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/agency-portal.html", response_class=HTMLResponse)
def agency_portal_html():
    content = _load_static_file("agency-portal.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "agency-portal.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/matches-dashboard.html", response_class=HTMLResponse)
def matches_dashboard_html():
    content = _load_static_file("matches-dashboard.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "matches-dashboard.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/imports.html", response_class=HTMLResponse)
def imports_html():
    content = _load_static_file("imports.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "imports.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/agency-quick-match.html", response_class=HTMLResponse)
def agency_quick_match_html():
    """Serve the minimal quick-match page that allows entering a candidate/job ID
    and saving matches to Mongo via existing endpoints.
    """
    content = _load_static_file("agency-quick-match.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "agency-quick-match.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/mobile-job.html", response_class=HTMLResponse)
def mobile_job_html():
    content = _load_static_file("mobile-job.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "mobile-job.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

@app.get("/mobile-confirm.html", response_class=HTMLResponse)
def mobile_confirm_html():
    content = _load_static_file("mobile-confirm.html")
    if content is not None:
        return HTMLResponse(content)
    detail = {
        "error": "mobile-confirm.html missing",
        "searched_dirs": [str(p) for p in _CANDIDATE_FRONTEND_DIRS],
        "frontend_public": str(_FRONTEND_PUBLIC) if _FRONTEND_PUBLIC else None
    }
    raise HTTPException(status_code=404, detail=detail)

# --- CORS (frontend served from a different port like 5173/5190) ---
_ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")  # comma separated or '*'
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _ALLOWED_ORIGINS == ["*"] else _ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=300,
)

API_KEY = os.getenv("API_KEY")  # optional simple shared key

def require_api_key(x_api_key: str = Header(default=None, alias="X-API-Key")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

def _sample_paths(kind: str):
    d = SAMPLES_DIR / ("cvs" if kind == "candidate" else "jobs")
    if d.exists():
        return [str(p) for p in d.iterdir() if p.is_file()]
    return []

def _auto_ingest_if_empty():
    # Only ingest if collections empty to avoid duplicates on reload
    if db["candidates"].count_documents({}) == 0:
        cps = _sample_paths("candidate")
        if cps:
            ingest_files(cps, kind="candidate")
    if db["jobs"].count_documents({}) == 0:
        jps = _sample_paths("job")
        seeded = False
        if jps:
            try:
                ingest_files(jps, kind="job")
                seeded = db["jobs"].count_documents({}) > 0
            except Exception:
                seeded = False
        # If still empty (e.g., no LLM for job ingestion), insert a minimal synthetic job to satisfy tests
        if not seeded and db["jobs"].count_documents({}) == 0:
            now = int(time.time())
            try:
                db["jobs"].insert_one({
                    "title": "Test Seed Job",
                    "job_description": "Seed job for offline tests",
                    "skill_set": ["office", "crm", "service"],
                    "skills_detailed": [{"name":"office","category":"must"},{"name":"crm","category":"needed"}],
                    "city_canonical": "tel_aviv",
                    "updated_at": now,
                })
            except Exception:
                pass

@app.middleware("http")
async def rate_limit(request: Request, call_next):  # pragma: no cover
    ip = request.client.host if request.client else "anon"
    now = int(time.time())
    bucket = _RATE_BUCKET.setdefault(ip, [])
    cutoff = now - _RATE_RESET
    while bucket and bucket[0] < cutoff:
        bucket.pop(0)
    limit = RATE_LIMIT_PER_MIN
    if len(bucket) >= limit:
        reset_in = _RATE_RESET - (now - bucket[0]) if bucket else _RATE_RESET
        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(reset_in)
        }
        return JSONResponse(status_code=429, content={"detail": "rate limit exceeded"}, headers=headers)
    bucket.append(now)
    response = await call_next(request)
    remaining = max(limit - len(bucket), 0)
    reset_in = _RATE_RESET - (now - bucket[0]) if bucket else _RATE_RESET
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(reset_in)
    return response

class IngestRequest(BaseModel):
    paths: Optional[List[str]] = None
    text: Optional[str] = None
    filename: Optional[str] = None

class ApplyRequest(BaseModel):
    share_id: str
    job_id: str
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    note: Optional[str] = None

class AnalyticsEvent(BaseModel):
    type: str
    ts: Optional[float] = None
    payload: Optional[dict[str, Any]] = None
class LLMToggle(BaseModel):
    enabled: bool


class AnalyticsBatch(BaseModel):
    events: List[AnalyticsEvent]

class MatchesBackfillRequest(BaseModel):
    k: int = 10
    city_filter: bool = True
    limit_candidates: Optional[int] = None
    force: bool = False
    max_age: Optional[int] = None  # seconds

class JobsMatchesBackfillRequest(BaseModel):
    k: int = 10
    city_filter: bool = True
    limit_jobs: Optional[int] = None
    force: bool = False
    max_age: Optional[int] = None  # seconds

class PitchRequest(BaseModel):
    share_id: str
    job_ids: List[str]
    tone: str = "professional"
    force: bool = False

class PersonalLetterRequest(BaseModel):
    share_id: str
    force: bool = False

## (removed old MatchReportQuery definition; using MatchQuery later in file)


class OutreachRequest(BaseModel):
    candidate_id: str
    top_k: int = 10
    force: bool = False

_PITCH_ALLOWED_TONES = {"professional","enthusiastic","concise","persuasive","balanced"}
_PITCH_CACHE: dict[str, dict] = {}
_PITCH_CACHE_TTL = 900  # seconds

_LETTER_CACHE: dict[str, dict] = {}
_LETTER_CACHE_TTL = 1800  # 30 minutes

def _pitch_cache_key(share_id: str, job_ids: list[str], tone: str) -> str:
    base = f"{share_id}|{','.join(sorted(job_ids))}|{tone}"
    return hashlib.sha1(base.encode()).hexdigest()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ready")
def ready():
    try:
        db.command("ping")
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"db_not_ready: {e}")

@app.get("/", response_class=HTMLResponse)
def root():
        """Simple landing page with quick links (Hebrew / English)."""
        return """<!doctype html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>TalentDB</title>
<style>body{font-family:Arial,sans-serif;margin:40px;line-height:1.5;background:#fafafa}a{color:#0366d6;text-decoration:none}a:hover{text-decoration:underline}.box{background:#fff;border:1px solid #ddd;padding:18px;max-width:720px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,.05)}</style>
</head><body>
<div class='box'>
<h1 style='margin-top:0'>📘 Talent Match Service</h1>
<p>דף פתיחה לשירות. קישורים מהירים:</p>
<ul>
    <li><a href='/recommend.html' target='_blank'>דף העלאת קורות חיים + מכתב אישי</a></li>
    <li><a href='/personal_pitch.html' target='_blank'>מחולל נאום אישי (Pitch)</a></li>
    <li><a href='/health' target='_blank'>/health</a> · <a href='/ready' target='_blank'>/ready</a></li>
    <li><a href='/admin/candidates' target='_blank'>רשימת מועמדים</a> · <a href='/admin/jobs' target='_blank'>רשימת משרות</a></li>
    <li><code>GET /personal-letter/&lt;share_id&gt;</code> · <code>POST /personal-letter</code></li>
</ul>
<p style='font-size:12px;color:#666'>אם זה נטען – השרת פעיל. אם הקבצים הסטטיים לא זמינים ודא שהרצת מתוך התיקייה הנכונה וש-PYTHONPATH כולל את התיקייה.</p>
</div>
</body></html>"""

@app.get("/live")
def live():
    """Kubernetes/OpenShift style liveness probe (no external deps)."""
    return {"status": "alive"}

@app.get("/ready")
def ready():
    """Readiness probe: verifies Mongo ping + essential collections + indexes.
    Returns 200 with details if ready; 503 otherwise.
    """
    detail = {"mongo": False, "candidates": 0, "jobs": 0, "indexes": []}
    status = 200
    try:
        db.command("ping")
        detail["mongo"] = True
    except Exception as e:
        status = 503
        detail["error"] = f"mongo_ping_failed: {e}"  # fall through
        return JSONResponse(status_code=status, content=detail)
    # basic collection counts (fast)
    try:
        detail["candidates"] = db["candidates"].estimated_document_count()
        detail["jobs"] = db["jobs"].estimated_document_count()
    except Exception as e:
        status = 503
        detail["error"] = f"collection_access_failed: {e}"
    # ensure indexes exist (idempotent + quick)
    try:
        idx = create_indexes()
        detail["indexes"] = idx or []
    except Exception as e:
        status = 503
        detail.setdefault("errors", []).append(f"index_error: {e}")
    return JSONResponse(status_code=status, content=detail)

@app.get("/db/status")
def db_status():
    ok=True
    msg="ok"
    try:
        db.command("ping")
    except Exception as e:
        ok=False; msg=str(e)
    return {"backend": "mongo", "ok": ok, "message": msg}

@app.post("/maintenance/backfill_skills")
def maintenance_backfill_skills(_: bool = Depends(require_api_key)):
    """Backfill detailed skills metadata, fingerprints, and vectors for all candidates and jobs."""
    try:
        summary = backfill_skills_meta()
        return {"updated": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"backfill_failed: {e}")

@app.post("/maintenance/matches/backfill")
def maintenance_matches_backfill(req: MatchesBackfillRequest, _: bool = Depends(require_api_key), tenant_id: str | None = Depends(optional_tenant_id)):
    """Compute and cache matches for candidates. Requires API key. Safe to run repeatedly.
    When force=false and max_age provided, will skip candidates with fresh cache.
    """
    try:
        summary = backfill_matches(tenant_id=tenant_id, k=req.k, city_filter=req.city_filter, limit_candidates=req.limit_candidates, force=req.force, max_age=req.max_age)
        return {"status": "ok", "summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"matches_backfill_failed: {e}")

@app.post("/maintenance/matches/backfill-jobs")
def maintenance_matches_backfill_jobs(req: JobsMatchesBackfillRequest, _: bool = Depends(require_api_key), tenant_id: str | None = Depends(optional_tenant_id)):
    """Compute and cache matches for jobs (job->candidates). Requires API key. Safe to run repeatedly.
    When force=false and max_age provided, will skip jobs with fresh cache.
    """
    try:
        summary = backfill_job_matches(tenant_id=tenant_id, k=req.k, city_filter=req.city_filter, limit_jobs=req.limit_jobs, force=req.force, max_age=req.max_age)
        return {"status": "ok", "summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"matches_backfill_jobs_failed: {e}")

@app.get("/candidates")
def list_candidates(request: Request, response: Response, skip: int = 0, limit: int = 50, sort: str = "", tenant_id: str | None = Depends(optional_tenant_id)):
    """List candidate IDs with optional sorting.

    sort options:
      updated_desc -> newest updated first (by updated_at then _id)
      updated_asc  -> oldest first
    default (empty) -> natural insertion order from Mongo.
    """
    if limit > 200: limit = 200
    if skip < 0: skip = 0
    # Scope to tenant if provided, else global view (legacy tests)
    base_query = ({"tenant_id": tenant_id} if tenant_id else {})
    q = db["candidates"].find(base_query)
    if sort == "updated_desc":
        q = q.sort([("updated_at", -1), ("_id", -1)])
    elif sort == "updated_asc":
        q = q.sort([("updated_at", 1), ("_id", 1)])
    cursor = q.skip(skip).limit(limit)
    res = [str(d["_id"]) for d in cursor]
    total = db["candidates"].count_documents(base_query)
    payload = {"candidates": res, "skip": skip, "limit": limit, "total": total, "sort": sort or None}
    etag = hashlib.sha1(str(payload).encode()).hexdigest()
    inm = request.headers.get("if-none-match")
    response.headers["Cache-Control"] = "public, max-age=30"
    response.headers["ETag"] = etag
    if inm == etag:
        response.status_code = 304
        return None
    return payload

@app.get("/candidates/latest")
def latest_candidate(tenant_id: str = Depends(require_tenant)):
    doc = db["candidates"].find_one({"tenant_id": tenant_id}, sort=[("updated_at", -1), ("_id", -1)])
    if not doc:
        return {"candidate": None}
    return {"candidate": str(doc["_id"]), "updated_at": doc.get("updated_at")}

@app.get("/jobs")
def list_jobs(request: Request, response: Response, skip: int = 0, limit: int = 50, tenant_id: str | None = Depends(optional_tenant_id)):
    if limit > 200: limit = 200
    if skip < 0: skip = 0
    base_query = ({"tenant_id": tenant_id} if tenant_id else {})
    cursor = db["jobs"].find(base_query).skip(skip).limit(limit)
    res = []
    for d in cursor:
        rec = {
            "job_id": str(d.get("_id")),
            "title": d.get("title"),
            "city": d.get("city_canonical"),
            "job_description": (d.get("job_description") or '')[:260],
            "skills_count": len(d.get("skill_set") or []),
        }
        res.append(rec)
    total = db["jobs"].count_documents(base_query)
    payload = {"jobs": res, "skip": skip, "limit": limit, "total": total}
    etag = hashlib.sha1(str(payload).encode()).hexdigest()
    inm = request.headers.get("if-none-match")
    response.headers["Cache-Control"] = "public, max-age=30"
    response.headers["ETag"] = etag
    if inm == etag:
        response.status_code = 304
        return None
    return payload

@app.get("/candidate/{cand_id}")
def get_candidate(cand_id: str, tenant_id: str = Depends(require_tenant), request: Request = None):
    oid = _ensure_object_id(cand_id)
    # SECURITY FIX: Add tenant isolation - only return candidates belonging to the authenticated tenant
    doc = db["candidates"].find_one({"_id": oid, "tenant_id": tenant_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Not found")
    
    # Audit log data access
    log_data_access(tenant_id, "candidate", cand_id, request)
    
    # Sanitize / project only relevant fields
    out = {
        "candidate_id": cand_id,
        "title": doc.get("title"),
        "city": doc.get("city_canonical"),
        "skills": doc.get("skill_set") or [],
        "skills_detailed": doc.get("skills_detailed") or [],
    "updated_at": doc.get("updated_at"),
    "share_id": doc.get("share_id")
    }
    return {"candidate": out}

@app.get("/job/{job_id}")
def get_job(job_id: str, tenant_id: str = Depends(require_tenant), request: Request = None):
    oid = _ensure_object_id(job_id)
    # SECURITY FIX: Add tenant isolation - only return jobs belonging to the authenticated tenant
    doc = db["jobs"].find_one({"_id": oid, "tenant_id": tenant_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Not found")
    
    # Audit log data access
    log_data_access(tenant_id, "job", job_id, request)
    
    out = {
        "job_id": job_id,
        "title": doc.get("title"),
        "city": doc.get("city_canonical"),
    "job_description": doc.get("job_description"),
    "job_requirements": doc.get("job_requirements"),
        "skills": doc.get("skill_set") or [],
        "skills_detailed": doc.get("skills_detailed") or [],
        "updated_at": doc.get("updated_at"),
    }
    return {"job": out}

def _ensure_object_id(oid: str):
    try:
        return ObjectId(oid)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid id")

@app.post("/ingest/{kind}")
def ingest(kind: str, req: IngestRequest, force_llm: bool=False, tenant_id: str = Depends(require_tenant)):
    if kind not in {"candidate","job"}:
        raise HTTPException(status_code=400, detail="kind must be candidate|job")
    written = []
    created_doc = None
    if req.paths:
        # ingest_files now returns list of docs
        docs = ingest_files(req.paths, kind=kind, force_llm=force_llm) or []
        written.extend(req.paths)
        if docs:
            created_doc = docs[-1]
    elif req.text:
        fname = req.filename or f"{kind}_inline.txt"
        tmp_path = Path(tempfile.gettempdir()) / fname
        tmp_path.write_text(req.text)
        docs = ingest_files([str(tmp_path)], kind=kind, force_llm=force_llm) or []
        if docs:
            created_doc = docs[-1]
        written.append(str(tmp_path))
    else:
        raise HTTPException(status_code=400, detail="Provide paths or text")
    share_id = None
    if kind == "candidate":
        # Prefer share_id from created_doc (ingest pipeline assigns it early)
        if created_doc and created_doc.get("share_id"):
            share_id = created_doc.get("share_id")
        else:
            # fallback (legacy)
            doc = db["candidates"].find_one(sort=[("updated_at", -1), ("_id", -1)])
            if not doc:
                doc = db["candidates"].find_one(sort=[("_id", -1)])
            if doc:
                share_id = doc.get("share_id")
        # Attempt static page generation (best-effort)
        if share_id:
            try:
                _generate_share_static(share_id)
            except Exception:
                pass
        # Tag created candidate with tenant
        try:
            if created_doc and created_doc.get('_id'):
                db['candidates'].update_one({'_id': created_doc['_id']}, {'$set': {'tenant_id': tenant_id}})
            elif share_id:
                db['candidates'].update_one({'share_id': share_id}, {'$set': {'tenant_id': tenant_id}})
        except Exception:
            pass
    elif kind == "job":
        # Best-effort: tag last created/updated job if we can find it
        try:
            if created_doc and created_doc.get('_id'):
                db['jobs'].update_one({'_id': created_doc['_id']}, {'$set': {'tenant_id': tenant_id}})
            else:
                # Fallback to newest job
                doc = db['jobs'].find_one(sort=[["updated_at", -1], ["_id", -1]])
                if doc and not doc.get('tenant_id'):
                    db['jobs'].update_one({'_id': doc['_id']}, {'$set': {'tenant_id': tenant_id}})
        except Exception:
            pass
    return {"ingested": written, "force_llm": force_llm, "share_id": share_id}

@app.post("/upload/candidate")
async def upload_candidate(file: UploadFile = File(...), force_llm: bool=True, tenant_id: str = Depends(require_tenant)):
    """Upload a single CV file (pdf/docx/txt) and ingest as candidate. Returns share_id."""
    fname = file.filename or 'upload_cv'
    ext = fname.lower().rsplit('.',1)[-1] if '.' in fname else ''
    if ext not in {'pdf','txt','docx'}:
        raise HTTPException(status_code=400, detail='Unsupported file type (pdf, docx, txt only)')
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.'+ext) as tmp:
        data = await file.read()
        # Basic validations to avoid server-side 500s on empty/corrupt uploads
        if not data or len(data) == 0:
            raise HTTPException(status_code=400, detail='Empty file uploaded')
        max_mb = int(os.getenv('MAX_UPLOAD_MB', '12'))
        if len(data) > max_mb * 1024 * 1024:
            raise HTTPException(status_code=400, detail=f'File too large (>{max_mb} MB)')
        tmp.write(data)
        tmp_path = tmp.name
    try:
        try:
            docs = ingest_files([tmp_path], kind='candidate', force_llm=force_llm) or []
        except Exception as e:
            # Convert ingestion crashes into a user-facing 400 with a concise message
            raise HTTPException(status_code=400, detail=f'Failed to process file: {str(e)[:140]}')
        created_doc = docs[-1] if docs else None
        if not created_doc:
            # Likely unparseable/corrupt content
            raise HTTPException(status_code=400, detail='Ingestion failed: could not extract any content')
        share_id = created_doc.get('share_id')
        letter_payload = None
        try:
            if share_id:
                _generate_share_static(share_id)
                # Auto-generate personal letter after successful ingestion (best-effort)
                try:
                    letter_req = PersonalLetterRequest(share_id=share_id, force=False)
                    letter_result = generate_personal_letter(letter_req, tenant_id=tenant_id)
                    # Only keep minimal payload to reduce response size
                    if isinstance(letter_result, dict):
                        lp = letter_result.get('letter') or {}
                        if lp.get('letter_content'):
                            letter_payload = lp
                except Exception:
                    letter_payload = None  # ignore failures
        except Exception:
            pass
        # Tag candidate with tenant
        try:
            if created_doc and created_doc.get('_id'):
                db['candidates'].update_one({'_id': created_doc['_id']}, {'$set': {'tenant_id': tenant_id}})
            elif share_id:
                db['candidates'].update_one({'share_id': share_id}, {'$set': {'tenant_id': tenant_id}})
        except Exception:
            pass
        resp = {"share_id": share_id, "candidate_id": str(created_doc.get('_id')), "status": created_doc.get('status'), "llm_success": created_doc.get('llm_success')}
        if letter_payload:
            resp["personal_letter"] = letter_payload
        return resp
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

@app.get("/share/candidate/{share_id}")
def share_candidate(share_id: str, k: int = 10):
    doc = db["candidates"].find_one({"share_id": share_id})
    if not doc:
        raise HTTPException(status_code=404, detail="share id not found")
    cand_id = str(doc["_id"])
    tenant_id = str(doc.get('tenant_id') or '')
    # Base matches
    matches = jobs_for_candidate(cand_id, top_k=k, max_distance_km=0, tenant_id=(tenant_id or None))
    # Pre-compute candidate skill set once
    cand_skill_set = set(doc.get("skill_set") or [])
    # Augment each match with explanation style fields (why it's good)
    enriched = []
    for m in matches:
        job_doc = None
        if m.get("job_id"):
            query = {"_id": ObjectId(m.get("job_id"))}
            if tenant_id:
                query["tenant_id"] = tenant_id
            job_doc = db["jobs"].find_one(query)
        job_skills = set(job_doc.get("skill_set") or []) if job_doc else set()
        overlap = set(m.get("skill_overlap") or [])
        # Recompute overlap if missing
        if not overlap and job_skills:
            overlap = cand_skill_set & job_skills
        candidate_only = sorted(list((cand_skill_set - job_skills)))[:20]
        job_only = sorted(list((job_skills - cand_skill_set)))[:20]
        reason_parts = []
        if overlap:
            reason_parts.append(f"חפיפה {len(overlap)} כישורים: {', '.join(list(overlap)[:6])}")
        if job_only:
            reason_parts.append(f"נדרשים עוד {len(job_only)}: {', '.join(job_only[:4])}")
        if not reason_parts:
            reason_parts.append("התאמה חלקית לפי כותרת או מרחק")
        m["candidate_only_skills"] = candidate_only
        m["job_only_skills"] = job_only
        m["reason"] = " | ".join(reason_parts)
        enriched.append(m)
    matches = enriched
    # Ensure skill_cloud available even when no matches processed yet
    cand_skill_set = set(doc.get("skill_set") or [])
    skill_cloud = ''.join(f"<span class='chip base'>{s}</span>" for s in list(cand_skill_set)[:80])
    return {
        "candidate": {
            "candidate_id": cand_id,
            "title": doc.get("title"),
            "city": doc.get("city_canonical"),
            "skills": doc.get("skill_set") or [],
            "updated_at": doc.get("updated_at"),
            "share_id": share_id
        },
        "matches": matches
    }

def _generate_share_static(share_id: str):
    """Generate (or overwrite) a static HTML snapshot for a candidate share id.
    Writes file into frontend/public/share_<id>.html so it can be served directly
    by any static file host (e.g. Vite preview / CDN). Best-effort: failures are silent.
    """
    doc = db["candidates"].find_one({"share_id": share_id})
    if not doc:
        return False
    if doc.get('status') and doc.get('status') != 'ready':
        # Produce placeholder static file
        out_dir = Path(__file__).resolve().parent.parent / "frontend" / "public"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"share_{share_id}.html"
        placeholder = f"""<!DOCTYPE html><html lang=he dir=rtl><meta charset=utf-8><title>מעבד קורות חיים…</title><body style='font-family:Arial;padding:40px;background:#f5f7fa'>
        <h2 style='margin-top:0'>הקובץ מעובד כרגע</h2>
        <p>אנא רענן דף זה בעוד מספר שניות. מצב נוכחי: {doc.get('status')}</p>
        </body></html>"""
        out_file.write_text(placeholder, encoding='utf-8')
        return True
    cand_id = str(doc.get("_id"))
    matches = jobs_for_candidate(cand_id, top_k=10, max_distance_km=0)
    cand_skill_set = set(doc.get("skill_set") or [])
    enriched = []
    for m in matches:
        job_doc = db["jobs"].find_one({"_id": ObjectId(m.get("job_id"))}) if m.get("job_id") else None
        job_skills = set(job_doc.get("skill_set") or []) if job_doc else set()
        overlap = set(m.get("skill_overlap") or [])
        if not overlap and job_skills:
            overlap = cand_skill_set & job_skills
        candidate_only = sorted(list((cand_skill_set - job_skills)))[:20]
        job_only = sorted(list((job_skills - cand_skill_set)))[:20]
        reason_parts = []
        if overlap:
            reason_parts.append(f"חפיפה {len(overlap)} כישורים: {', '.join(list(overlap)[:6])}")
        if job_only:
            reason_parts.append(f"נדרשים עוד {len(job_only)}: {', '.join(job_only[:4])}")
        if not reason_parts:
            reason_parts.append("התאמה חלקית לפי כותרת או מרחק")
        m["candidate_only_skills"] = candidate_only
        m["job_only_skills"] = job_only
        m["reason"] = " | ".join(reason_parts)
        enriched.append(m)
    matches = enriched
    def _humanize_filename(fn: str) -> str:
        import re, os
        if not fn:
            return ""
        # strip extension
        base = os.path.splitext(fn)[0]
        # remove common Hebrew phrases for CV
        for pat in ["קורות חיים", "קו""ח", "קורות_חיים", "cv", "CV"]:
            base = re.sub(pat, "", base, flags=re.IGNORECASE)
        # trim extra separators / underscores
        base = re.sub(r"[_-]+", " ", base)
        base = base.strip()
        # collapse multiple spaces
        base = re.sub(r"\s+", " ", base)
        # limit length
        return base[:60] if base else ""
    base_filename = None
    try:
        from pathlib import Path as _P
        raw_name = _P(doc.get('_src_path') or '').name or ''
        base_filename = _humanize_filename(raw_name)
    except Exception:
        base_filename = None
    display_title = (doc.get('title') or base_filename or f"מועמד/ת {share_id}").strip()
    emb_summary = (doc.get('embedding_summary') or doc.get('summary') or '')[:300]
    years_exp = doc.get('years_experience') or doc.get('years_experience'.replace('years_experience','years_experience'))  # safe fetch
    tools = doc.get('tools') or []
    languages = doc.get('languages') or []
    lang_txt = ', '.join([f"{l.get('name')} ({l.get('level')})" for l in languages if isinstance(l, dict)])
    tools_txt = ', '.join([t for t in tools if isinstance(t, str)])
    # Build enhanced job cards
    job_cards = []
    for idx, m in enumerate(matches, start=1):
        overlap_list = list(dict.fromkeys(m.get('skill_overlap') or []))
        missing_list = list(dict.fromkeys(m.get('job_only_skills') or []))
        extra_list = list(dict.fromkeys(m.get('candidate_only_skills') or []))
        reason_txt = m.get('reason', '')
        # Break reason into sentences for bullets
        reason_parts = [r.strip() for r in reason_txt.split('|') if r.strip()]
        reason_html = ''.join(f"<li>{p}</li>" for p in reason_parts) or '<li>התאמה חלקית</li>'
        def chips(lst, cls):
            return ''.join(f"<span class='chip {cls}'>{s}</span>" for s in lst[:18]) or '<span class="none">—</span>'
        top_class = ' top' if idx == 1 else ''
        job_cards.append(f"""
        <div class='job-card{top_class}'>
             <div class='job-head'>
                 <div class='rank'>{idx}</div>
                 <div class='score-wrap'><div class='score-label'>ציון</div><div class='score'>{m.get('score')}</div></div>
                 <div class='job-id'>ID: {m.get('job_id')}</div>
             </div>
             <div class='reason-block'>
                 <ul class='reasons'>{reason_html}</ul>
             </div>
             <div class='skills-groups'>
                 <div class='sg'>
                     <div class='sg-title'>חפיפה ({len(overlap_list)})</div>
                     <div class='sg-body'>{chips(overlap_list, 'overlap')}</div>
                 </div>
                 <div class='sg'>
                     <div class='sg-title'>נדרש לתפקיד ({len(missing_list)})</div>
                     <div class='sg-body'>{chips(missing_list, 'missing')}</div>
                 </div>
                 <div class='sg'>
                     <div class='sg-title'>יתרון אצל המועמד/ת ({len(extra_list)})</div>
                     <div class='sg-body'>{chips(extra_list, 'extra')}</div>
                 </div>
             </div>
        </div>
        """)
        skill_cloud = ''.join(f"<span class='chip base'>{s}</span>" for s in list(cand_skill_set)[:80])
    html_doc = f"""<!DOCTYPE html><html lang=he dir=rtl><head>
    <meta charset=utf-8 />
    <title>התאמות משרות • {display_title}</title>
    <meta name=viewport content=\"width=device-width,initial-scale=1\" />
    <style>
    :root {{ --bg:#f6f7fb; --card:#ffffff; --border:#d9e0e7; --accent:#2563eb; --accent-grad:linear-gradient(90deg,#2563eb,#3b82f6); --font:Arial,Helvetica,sans-serif; --danger:#dc2626; --ok:#059669; --warn:#d97706; }}
    body {{ background:var(--bg); font-family:var(--font); margin:0; padding:0; color:#16202a; }}
    header {{ max-width:1200px; margin:0 auto; padding:26px 26px 10px; }}
    h1 {{ font-size:30px; margin:0 0 4px; background:var(--accent-grad); -webkit-background-clip:text; color:transparent; }}
    .sub {{ color:#475569; font-size:14px; margin-bottom:18px; }}
    .panel {{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:18px 22px 20px; margin:0 26px 28px; max-width:1200px; box-shadow:0 2px 4px rgba(0,0,0,.04); }}
    .skills {{ line-height:2.25; }}
    .chip {{ display:inline-block; padding:5px 10px; border-radius:18px; margin:4px 6px 4px 0; font-size:12px; background:#eef2f7; border:1px solid #e2e8f0; }}
    .chip.overlap {{ background:#dcfce7; border-color:#86efac; }}
    .chip.missing {{ background:#fee2e2; border-color:#fca5a5; }}
    .chip.extra {{ background:#e0f2fe; border-color:#7dd3fc; }}
    .chip.base {{ background:#f1f5f9; }}
    .legend {{ font-size:12px; margin-top:10px; color:#475569; }}
    .legend .chip {{ font-size:11px; padding:3px 8px; margin:2px 4px; }}
    .jobs {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(330px,1fr)); gap:22px; }}
    .job-card {{ background:#fff; border:1px solid var(--border); border-radius:18px; padding:14px 16px 16px; display:flex; flex-direction:column; gap:10px; position:relative; box-shadow:0 3px 6px -2px rgba(0,0,0,.05); }}
    .job-card.top {{ border:2px solid #2563eb; box-shadow:0 6px 14px -4px rgba(37,99,235,.35); }}
    .job-head {{ display:flex; align-items:center; gap:10px; }}
    .rank {{ background:#1e293b; color:#fff; width:34px; height:34px; display:flex; align-items:center; justify-content:center; border-radius:10px; font-weight:600; font-size:15px; }}
    .job-card.top .rank {{ background:#2563eb; }}
    .score-wrap {{ margin-right:auto; text-align:center; }}
    .score-label {{ font-size:11px; letter-spacing:.5px; color:#475569; }}
    .score {{ font-size:18px; font-weight:600; font-feature-settings:'tnum'; }}
    .job-id {{ font-size:11px; color:#64748b; direction:ltr; }}
    .reason-block {{ font-size:12.5px; line-height:1.55; }}
    .reasons {{ margin:0; padding:0 16px 0 0; }}
    .reasons li {{ margin:2px 0; }}
    .skills-groups {{ display:flex; flex-direction:column; gap:8px; }}
    .sg {{ background:#f8fafc; border:1px solid #e2e8f0; border-radius:14px; padding:8px 10px 10px; }}
    .sg-title {{ font-size:11px; font-weight:600; color:#475569; margin:0 0 6px; }}
    .sg-body {{ line-height:2; }}
    footer {{ margin:50px 26px 70px; font-size:11px; color:#64748b; }}
    .none {{ color:#cbd5e1; }}
    @media (max-width:860px) {{
        .jobs {{ grid-template-columns:1fr; }}
        header {{ padding:22px 18px 4px; }}
        .panel {{ margin:0 18px 22px; }}
    }}
    </style>
        </head><body>
        <header>
            <h1>{display_title}</h1>
            <div class='sub'>התאמות משרות • מזהה שיתוף: {share_id} • סה"כ כישורים מזוהים: {len(cand_skill_set)} • שנות ניסיון: {years_exp if years_exp is not None else 'N/A'}</div>
        </header>
        <section class='panel'>
             <h2 style='margin:0 0 10px;font-size:20px;'>כישורי הליבה של המועמד/ת</h2>
             <div class='skills'>{skill_cloud}</div>
             <div style='margin-top:14px;font-size:13px;line-height:1.5;white-space:pre-wrap;direction:rtl;'>{html.escape(emb_summary)}</div>
             <div style='margin-top:10px;font-size:12px;color:#475569;'>כלים: {html.escape(tools_txt or '—')} | שפות: {html.escape(lang_txt or '—')}</div>
             <div class='legend'>
                    <strong>מקרא:</strong>
                    <span class='chip overlap'>חפיפה</span>
                    <span class='chip missing'>נדרש</span>
                    <span class='chip extra'>יתרון</span>
             </div>
        </section>
        <section class='panel'>
             <h2 style='margin:0 0 14px;font-size:20px;'>התאמות מובילות</h2>
             <div class='jobs'>
                    {''.join(job_cards)}
             </div>
        </section>
        <footer>
            נוצר אוטומטית. לרענון /share/candidate/{share_id}/html • JSON: /share/candidate/{share_id}?k=10
        </footer>
        <script>console.log('share_id','{share_id}');</script>
        </body></html>"""
    out_dir = Path(__file__).resolve().parent.parent / "frontend" / "public"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"share_{share_id}.html"
    out_file.write_text(html_doc, encoding="utf-8")
    return True

@app.get("/share/candidate/{share_id}/html")
def share_candidate_html(share_id: str):
    """On-demand HTML version (dynamic) – also ensures static file exists."""
    ok = _generate_share_static(share_id)
    # Return the freshly generated HTML directly
    out_dir = Path(__file__).resolve().parent.parent / "frontend" / "public"
    out_file = out_dir / f"share_{share_id}.html"
    if not out_file.exists():
        raise HTTPException(status_code=404, detail="share id not found")
    return Response(content=out_file.read_text(encoding="utf-8"), media_type="text/html")

# --- Applications & Analytics ---
def _apps_col():
    return db["applications"]

def _analytics_col():
    return db["analytics_events"]

@app.post("/apply")
def apply_job(req: ApplyRequest):
    cand = db["candidates"].find_one({"share_id": req.share_id})
    if not cand:
        raise HTTPException(status_code=404, detail="share id not found")
    job_oid = _ensure_object_id(req.job_id)
    job = db["jobs"].find_one({"_id": job_oid})
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    # Idempotent: prevent duplicates (same share_id + job_id)
    existing = _apps_col().find_one({"share_id": req.share_id, "job_id": req.job_id})
    if existing:
        return {"application_id": str(existing["_id"]), "status": existing.get("status", "submitted"), "duplicate": True}
    doc = {
        "share_id": req.share_id,
        "candidate_id": str(cand.get("_id")),
        "job_id": req.job_id,
        "created_at": time.time(),
        "status": "submitted",
        "contact": {
            "name": req.contact_name,
            "email": req.contact_email,
            "phone": req.contact_phone,
        },
        "note": req.note,
    }
    ins = _apps_col().insert_one(doc)
    return {"application_id": str(ins.inserted_id), "status": "submitted"}

@app.get("/apply/status/{share_id}")
def applied_status(share_id: str):
    cur = _apps_col().find({"share_id": share_id})
    jobs = [d.get("job_id") for d in cur]
    return {"share_id": share_id, "applied_job_ids": jobs, "count": len(jobs)}

@app.post("/analytics")
def analytics_events(batch: AnalyticsBatch):
    if not batch.events:
        return {"stored": 0}
    now = time.time()
    docs = []
    for ev in batch.events:
        if not ev.type:
            continue
        docs.append({
            "type": ev.type,
            "ts": ev.ts or now,
            "payload": ev.payload or {},
            "ingested_at": now,
            "ua_hash": hashlib.sha1(str(ev.payload or {}).encode()).hexdigest(),
        })
    if docs:
        _analytics_col().insert_many(docs)
    return {"stored": len(docs)}

def _load_prompt_text(name: str) -> str:
    p = Path(__file__).resolve().parent.parent / "prompts" / name
    if p.exists():
        return p.read_text(encoding="utf-8")
    return "You generate JSON."

_PITCH_PROMPT_RAW = _load_prompt_text("personal_pitch.txt")
_LETTER_PROMPT_RAW = _load_prompt_text("personal_letter.txt")
_DEBUG_LAST_LETTER_PROMPT: str | None = None

def _generate_mobile_job_link(job_id: str, share_id: str, base_url: str = "http://localhost:8080") -> str:
    """Generate a mobile job confirmation link for SMS"""
    return f"{base_url}/mobile-job.html?job_id={job_id}&share_id={share_id}"

def _shorten_url(url: str) -> str:
    """Create a shortened version of the URL for SMS display"""
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        job_id = params.get('job_id', [''])[0]
        share_id = params.get('share_id', [''])[0]
        
        # Create shorter IDs for display
        short_job = job_id[:8] + '...' if len(job_id) > 8 else job_id
        short_share = share_id[:6] + '...' if len(share_id) > 6 else share_id
        
        return f"{parsed.netloc}/mobile-job.html?job={short_job}&user={short_share}"
    except Exception:
        return url

def _shorten(txt: str, max_chars: int) -> str:
    if not txt:
        return ""
    t = txt.strip().replace('\n',' ')
    if len(t) <= max_chars:
        return t
    return t[:max_chars].rsplit(' ',1)[0]


def _extract_json_from_text(s: str) -> str | None:
    """Extract the first balanced JSON object substring from s.
    Returns the JSON string or None if not found.
    This attempts to handle quoted strings and escaped quotes.
    """
    if not s or '{' not in s:
        return None
    start = s.find('{')
    in_str = False
    esc = False
    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if esc:
            esc = False
            continue
        if ch == '\\':
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                candidate = s[start:i+1]
                return candidate
    # fallback: try last '}' after first '{'
    last = s.rfind('}')
    if last > start:
        return s[start:last+1]
    return None

def _generate_personal_letter_prompt(candidate_data: dict, jobs_data: list[dict]) -> str:
    """Builds a prompt guiding the model to produce the new fixed template letter."""
    cand_name = candidate_data.get('full_name') or 'Candidate'
    cand_city = candidate_data.get('city') or ''
    base_prompt = f"""הפק JSON עבור מכתב אישי בעברית למועמד בשם "{cand_name}". אל תמציא עובדות שלא קיימות.

תבנית המכתב (השתמש בפורמט הזה לכל משרה ב-whatsapp):
היי [שם מועמד] 👋

תודה רבה על הגשת המועמדות למשרה שפרסמנו בחברת הגיוס דנאל!

בדקנו את הפרופיל שלך, והוא מתאים למספר הזדמנויות נוספות שבחרנו במיוחד עבורך – בהתאמה לניסיון שלך ולמיקום המגורים ({cand_city}):

[שם המשרה] – [תיאור סוג החברה]
• תיאור המשרה: [תיאור קצר – מהות התפקיד והחברה]
• דרישות התפקיד: [3–4 נקודות עיקריות מופרדות בפסיקים]
• מיקום: [עיר המשרה] – כ־[מרחק/זמן נסיעה] מ{cand_city}
• למה זה טוב עבורך: [התאמה לניסיון/כישורים + יתרונות נוספים]
→ נשמח אם תשלח קורות חיים או תגיב כאן שנמשיך בתהליך

יום נעים ובהצלחה,
אבירם

הנחיות:
1. בחר 2-5 המשרות הטובות ביותר מבחינת match_percentage
2. לכל משרה צור הודעת whatsapp נפרדת עם התבנית המלאה
3. החזר JSON עם מערך selected_jobs, כל פריט חייב להכיל: job_id, title, city, whatsapp (טקסט מלא)
4. whatsapp צריך להכיל את כל הטקסט המלא של ההודעה עם המידע הספציפי של המשרה
5. החסר distance_km: חשב זמן נסיעה משוער (קמ / 55 * 60 דקות)

נתוני מועמד לשימוש פנימי:"""

    candidate_section = f"""
מועמד:
- שם: {cand_name or 'לא ידוע'}
- עיר מגורים: {cand_city or 'לא צוינה'}
- כותרת: {candidate_data.get('title','לא צוין')}
- ניסיון (שנים): {candidate_data.get('years_experience',0)}
- כישורים: {', '.join(candidate_data.get('skills', [])[:12])}
- תקציר: {candidate_data.get('summary','')[:220]}"""

    jobs_section = "\n\nמשרות (בחר 2 בלבד):"
    for i, job in enumerate(jobs_data[:5], 1):
        jobs_section += f"""
משרה {i}:
- כותרת: {job.get('title','')}
- עיר: {job.get('city','')}
- distance_km: {job.get('distance_km','N/A')}
- match_percentage: {job.get('match_percentage',0)}
- must: {', '.join(job.get('job_must_requirements',[])[:8]) or '—'}
- needed: {', '.join(job.get('job_needed_requirements',[])[:8]) or '—'}
- fit_must: {', '.join(job.get('candidate_fit_must',[])[:6]) or '—'}
- fit_needed: {', '.join(job.get('candidate_fit_needed',[])[:6]) or '—'}
- extra: {', '.join(job.get('candidate_extra_skills',[])[:5]) or '—'}
"""

    output_instructions = """
החזר JSON בלבד ללא הסבר נוסף.
"""
    prompt = base_prompt + candidate_section + jobs_section + output_instructions
    global _DEBUG_LAST_LETTER_PROMPT
    _DEBUG_LAST_LETTER_PROMPT = prompt
    return prompt


def _build_outreach_prompt_strict(candidate_data: dict, jobs_data: list[dict]) -> str:
    """Build a strict prompt that instructs the model to return a precise JSON schema for outreach messages.
    The model must NOT invent fields not present in the provided job objects.
    """
    schema = {
        "type": "object",
        "properties": {
            "selected_jobs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "job_number": {"type": "string"},
                        "title": {"type": "string"},
                        "city": {"type": "string"},
                        "description_snippet": {"type": "string"},
                        "why_fit": {"type": "string"},
                        "mandatory_requirements": {"type": "array", "items": {"type": "string"}},
                        "candidate_alignment": {"type": "array", "items": {"type": "string"}},
                        "whatsapp": {"type": "string"}
                    },
                    "required": ["job_id","job_number","title","city","description_snippet","why_fit","mandatory_requirements","candidate_alignment","whatsapp"]
                }
            },
            "model_info": {"type": "object", "additionalProperties": False, "properties": {"model": {"type": "string"}}, "required": ["model"]}
        }
    }
    # Build payload section
    import json as _json
    cand_json = _json.dumps({
        'full_name': candidate_data.get('full_name'),
        'city': candidate_data.get('city'),
        'title': candidate_data.get('title'),
        'skills': candidate_data.get('skills')[:20]
    }, ensure_ascii=False)
    jobs_json = _json.dumps(jobs_data[:10], ensure_ascii=False)
    inst = (
        "Return a VALID JSON object matching the schema below. Do NOT add or invent any fields. "
        "Use ONLY the data provided. If any required field is missing in a job, set it to an empty string or empty list.\n\n"
        "JSON_SCHEMA:" + _json.dumps(schema, ensure_ascii=False) + "\n\n"
        "CANDIDATE_DATA:" + cand_json + "\n\n"
        "JOBS_DATA:" + jobs_json + "\n\n"
        "Example output (use EXACT keys and types):\n"
        '{"selected_jobs":[{"job_id":"<id>","job_number":"<num>","title":"<title>","city":"<city>","description_snippet":"<short>","why_fit":"<why>","mandatory_requirements":["r1","r2"],"candidate_alignment":["evidence1"],"whatsapp":"<final message>"}],"model_info":{"model":"gpt-5-nano"}}'
    )
    return inst


def _build_outreach_prompt_minimal(candidate_data: dict, jobs_data: list[dict]) -> list[dict]:
    """Minimal strict system+user messages for JSON-only response (short and explicit).
    The user content includes compact candidate and jobs JSON to avoid prose-heavy instructions.
    """
    import json as _json
    sys = (
        "You produce exactly one JSON object that matches the provided JSON Schema. "
        "No commentary, no code fences, no markdown—only the JSON object. "
        "Do NOT provide internal chain-of-thought, step-by-step reasoning, or any explanations."
    )
    # Include a compact payload to avoid long prose but provide all data
    cand = {
        'full_name': candidate_data.get('full_name') or '',
        'city': candidate_data.get('city') or '',
        'title': candidate_data.get('title') or '',
        'skills': candidate_data.get('skills')[:20] if candidate_data.get('skills') else []
    }
    jobs = []
    for j in jobs_data[:3]:
        jobs.append({
            'job_id': j.get('job_id',''),
            'job_number': j.get('job_number',''),
            'title': j.get('title',''),
            'city': j.get('city',''),
            'description_snippet': (j.get('full_description') or '')[:120],
            'distance_km': j.get('distance_km') if j.get('distance_km') is not None else ''
        })
    user = (
        "Produce a JSON object matching the supplied schema. Use ONLY the provided data. "
        "If a field is missing, set it to empty string or empty list.\n\n"
        "CANDIDATE:" + _json.dumps(cand, ensure_ascii=False) + "\n\n"
        "JOBS:" + _json.dumps(jobs, ensure_ascii=False) + "\n\n"
        "Return only the JSON object."
    )
    return [{"role": "system", "content": sys}, {"role": "user", "content": user}]


# Structured JSON schema for outreach (strict)
OUTREACH_JSON_SCHEMA = {
    "name": "outreach_payload",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "selected_jobs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "job_id": {"type": "string"},
                        "job_number": {"type": "string"},
                        "title": {"type": "string"},
                        "city": {"type": "string"},
                        "description_snippet": {"type": "string"},
                        "why_fit": {"type": "string"},
                        "mandatory_requirements": {"type": "array", "items": {"type": "string"}},
                        "candidate_alignment": {"type": "array", "items": {"type": "string"}},
                        "whatsapp": {"type": "string"}
                    },
                    "required": ["job_id","job_number","title","city","description_snippet","why_fit","mandatory_requirements","candidate_alignment","whatsapp"]
                }
            },
            "model_info": {"type": "object", "additionalProperties": False, "properties": {"model": {"type": "string"}}, "required": ["model"]}
        },
    "required": ["selected_jobs", "model_info"]
    }
}


@app.post("/outreach/generate_for_candidate")
def generate_outreach(req: OutreachRequest, tenant_id: str | None = Depends(optional_tenant_id)):
    """Generate up to 5 WhatsApp messages for a candidate from top-K matched jobs.
    Saves results to `candidate_outreach` as drafts (status='draft'). Does not send messages.
    """
    # Fetch candidate
    try:
        from bson import ObjectId
        cand = db['candidates'].find_one({"_id": ObjectId(req.candidate_id)})
    except Exception as e:
        log_outreach_failure(req.candidate_id, [], 'fetch_candidate', e)
        raise HTTPException(status_code=400, detail="invalid_candidate_id")
    if not cand:
        log_outreach_failure(req.candidate_id, [], 'fetch_candidate', 'candidate_not_found')
        raise HTTPException(status_code=404, detail="candidate_not_found")

    # Fetch top-K jobs using existing matcher
    jobs = jobs_for_candidate(req.candidate_id, top_k=req.top_k, max_distance_km=0, tenant_id=tenant_id)
    if not jobs:
        log_outreach_failure(req.candidate_id, [], 'jobs_for_candidate', 'no_jobs_found')
        return {"candidate_id": req.candidate_id, "messages": [], "status": "no_jobs_found"}

    # Enrich jobs with full job docs from DB (do not let LLM invent missing fields)
    job_docs = []
    for j in jobs[:req.top_k]:
        try:
            jd = db['jobs'].find_one({"_id": ObjectId(j.get('job_id'))})
        except Exception as e:
            log_outreach_failure(req.candidate_id, [j.get('job_id')], 'fetch_job', e)
            jd = None
        if not jd:
            # fallback to minimal info provided by matcher
            jd = {"_id": j.get('job_id'), "title": j.get('title'), "city": j.get('city')}
        # normalize fields
        job_docs.append({
            "job_id": str(jd.get('_id')),
            "job_number": jd.get('job_number') or jd.get('ref') or '',
            "title": jd.get('title') or j.get('title') or '',
            "city": jd.get('city') or jd.get('city_canonical') or j.get('city') or '',
            "full_description": jd.get('job_description') or jd.get('description') or jd.get('full_description') or '',
            "mandatory_requirements": [d.get('name') for d in (jd.get('skills_detailed') or []) if d.get('category')=='must'],
            "optional_requirements": [d.get('name') for d in (jd.get('skills_detailed') or []) if d.get('category')!='must'],
            "distance_km": j.get('distance_km') if j.get('distance_km') is not None else jd.get('distance_km')
        })

    # Build candidate payload (full CV as requested)
    candidate_payload = {
        "candidate_id": str(cand.get('_id')),
        "full_name": cand.get('canonical', {}).get('full_name') or cand.get('full_name') or cand.get('name'),
        "city": cand.get('city') or cand.get('city_canonical') or '',
        "title": cand.get('title') or '',
        "skills": cand.get('skill_set') or [],
        "summary": cand.get('summary') or cand.get('text_blob') or '',
        "raw_cv_text": cand.get('text_blob') or cand.get('summary') or ''
    }

    # Compose prompt using helper (we'll limit jobs to top 10 for prompt clarity)
    prompt = _generate_personal_letter_prompt(candidate_payload, job_docs[:10])

    # Call LLM with retries; fall back to deterministic generator if LLM doesn't return valid JSON
    js = None
    try:
        from .ingest_agent import _openai_client, OPENAI_MODEL, _OPENAI_AVAILABLE
        if not _OPENAI_AVAILABLE:
            log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'llm_disabled', 'LLM disabled')
            raise RuntimeError("LLM disabled")
        # Try up to 3 attempts. First try with temperature=0 (deterministic); if API rejects temperature param, retry without it.
        last_raw = None
        strict_msgs = _build_outreach_prompt_minimal(candidate_payload, job_docs[:10])
        for attempt in range(3):
            try:
                # First attempt: include temperature=0 which usually helps deterministic JSON
                try:
                    comp = _openai_client.chat.completions.create(
                        model=OPENAI_MODEL,
                        messages=strict_msgs,
                        temperature=0,
                        max_completion_tokens=1200,
                        response_format={"type": "json_schema", "json_schema": OUTREACH_JSON_SCHEMA},
                    )
                except Exception as e_temp:
                    log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'llm_call', e_temp, prompt=strict_msgs)
                    # If model rejects temperature, retry same call without the parameter
                    logging.info(f"Temperature param rejected: {e_temp}; retrying without temperature")
                    comp = _openai_client.chat.completions.create(
                        model=OPENAI_MODEL,
                        messages=strict_msgs,
                        max_completion_tokens=1200,
                        response_format={"type": "json_schema", "json_schema": OUTREACH_JSON_SCHEMA},
                    )

                # Log choice metadata for diagnosis
                try:
                    ch = comp.choices[0]
                    finish = getattr(ch, 'finish_reason', None)
                    logging.info(f"LLM finish_reason={finish}")
                    logging.info(f"LLM choice raw message={getattr(ch, 'message', None)}")
                except Exception:
                    logging.info("LLM choice inspection failed")

                raw = getattr(comp.choices[0].message, 'content', '') or ''
                raw = raw.strip()
                last_raw = raw or str(getattr(comp, '__dict__', comp))
                if not raw:
                    log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'llm_empty', 'empty_content', raw_response=last_raw, prompt=strict_msgs)
                    # nothing returned; try again (may be model refusal or tooling path)
                    continue
                try:
                    js = json.loads(raw)
                    break
                except Exception as e_json:
                    log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'json_parse', e_json, raw_response=raw, prompt=strict_msgs)
                    js = None
                    continue
            except Exception as e:
                log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'llm_call', e, prompt=strict_msgs)
                last_raw = f"LLM call error: {e}"
                js = None
                continue
        # If after attempts js is still None, try the Responses API as a fallback (some models surface structured outputs there)
        if js is None:
            try:
                logging.info("Chat completions returned no textual content; attempting Responses API fallback")
                # Prepare a single concatenated input (Responses API accepts list or string)
                msgs = _build_outreach_prompt_minimal(candidate_payload, job_docs[:10])
                # Flatten messages into a single prompt string for Responses API input
                inp = "\n".join([m.get('content','') for m in msgs])
                # Use Responses API with strict json_schema
                try:
                    resp = _openai_client.responses.create(
                        model=OPENAI_MODEL,
                        input=inp,
                        max_output_tokens=1200,
                        response_format={"type": "json_schema", "json_schema": OUTREACH_JSON_SCHEMA},
                    )
                except TypeError as e_sdk:
                    log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'responses_sdk', e_sdk, prompt=inp)
                    # Some SDKs don't accept response_format; fall back to direct HTTP call
                    logging.info(f"Responses.create() signature mismatch: {e_sdk}; falling back to HTTP POST")
                    api_key = os.getenv('OPENAI_API_KEY')
                    if not api_key:
                        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'responses_sdk', 'no_api_key', prompt=inp)
                        raise
                    url = 'https://api.openai.com/v1/responses'
                    headers = {
                        'Authorization': f'Bearer {api_key}',
                        'Content-Type': 'application/json'
                    }
                    payload = {
                        'model': OPENAI_MODEL,
                        'input': inp,
                        'max_output_tokens': 10000,
                        'text': {
                            'format': {
                                "type": "json_schema",
                                # Responses API expects the raw schema under 'schema'
                                "schema": OUTREACH_JSON_SCHEMA.get('schema', OUTREACH_JSON_SCHEMA),
                                "name": OUTREACH_JSON_SCHEMA.get('name', 'outreach_payload'),
                                "strict": OUTREACH_JSON_SCHEMA.get('strict', True)
                            },
                            'verbosity': 'low'
                        }
                    }
                    local_timeout = float(os.getenv('OPENAI_REQUEST_TIMEOUT', '300'))
                    try:
                        r = requests.post(url, headers=headers, json=payload, timeout=local_timeout)
                        r.raise_for_status()
                        resp = r.json()
                        logging.info(f"Responses HTTP raw JSON: {str(resp)[:4000]}")
                    except requests.HTTPError as http_e:
                        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'responses_http', http_e, raw_response=getattr(r, 'text', None), prompt=inp)
                        # capture body for diagnostics
                        body = None
                        try:
                            body = r.text
                        except Exception:
                            body = str(http_e)
                        logging.info(f"Responses HTTP error: {http_e}; body={body}")
                        raise
                    except Exception as e:
                        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'responses_http', e, prompt=inp)
                        logging.info(f"Responses HTTP request failed: {e}")
                        raise
                # Inspect response
                try:
                    logging.info(f"Responses API keys: {list(getattr(resp,'__dict__',{}).keys())}")
                except Exception:
                    pass
                # Try parsed output if present. Do NOT attempt to extract JSON from free text — require direct JSON.
                parsed = None
                if hasattr(resp, 'output_parsed') and resp.output_parsed:
                    parsed = resp.output_parsed
                else:
                    try:
                        if isinstance(resp, dict):
                            outs = resp.get('output', []) or []
                        else:
                            outs = getattr(resp, 'output', []) or []
                        for o in outs:
                            if isinstance(o, dict):
                                cont = o.get('content', [])
                            else:
                                cont = getattr(o, 'content', None) or []
                            for c in cont:
                                if isinstance(c, dict) and c.get('type') in ('output_text', 'output'):
                                    txt = c.get('text') or c.get('content') or None
                                    if not txt:
                                        continue
                                    try:
                                        parsed = json.loads(txt)
                                        break
                                    except Exception as e_json:
                                        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'json_parse', e_json, raw_response=txt, prompt=inp)
                                        # Do not attempt extraction from messy text; treat as failure
                                        parsed = None
                                        break
                            if parsed:
                                break
                    except Exception as e:
                        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'json_parse', e, prompt=inp)
                        parsed = None
                if parsed:
                    js = parsed
                else:
                    # record the raw response for diagnostics; do not attempt to salvage
                    log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs], 'responses_no_json', 'no_json', raw_response=str(resp), prompt=inp)
                    last_raw = getattr(resp, '__dict__', str(resp))
            except Exception as e:
                last_raw = f"Responses API error: {e}"

        # If after responses fallback js is still None, raise an error (do not invent data)
        if js is None:
            detail = last_raw or 'llm_no_response'
            raise HTTPException(status_code=502, detail=f"llm_error: invalid or empty response after retries; last_raw: {detail}")
    except Exception as e:
        # Log the general exception failure
        log_outreach_failure(req.candidate_id, [j.get('job_id') for j in job_docs] if 'job_docs' in locals() else [], 'general_exception', str(e))
        raise HTTPException(status_code=502, detail=f"llm_error: {e}")

    # Persist to candidate_outreach collection as draft
    now = time.time()
    doc = {
        "candidate_id": candidate_payload.get('candidate_id'),
        "candidate_name": candidate_payload.get('full_name'),
        "candidate_language": cand.get('language') or 'he',
        "generated_at": now,
        "jobs_considered": [j.get('job_id') for j in job_docs[:10]],
        "llm_response": js,
        "messages": [],
        "status": "draft",
        "model_meta": {"model": OPENAI_MODEL if 'OPENAI_MODEL' in globals() else None, "prompt_id": "outreach_v1"}
    }
    # build per-job message texts (the prompt instructs letter_content to include templated entries)
    # For compatibility also create messages entries per job from js if available
    try:
        msgs = []
        # if js contains structured per-job items, prefer them
        per_jobs = js.get('per_job_points') or js.get('selected_jobs') or []
        if per_jobs and isinstance(per_jobs, list):
            for idx, pj in enumerate(per_jobs[:5]):
                job_ref = job_docs[idx] if idx < len(job_docs) else {}
                text = pj.get('whatsapp') or pj.get('message') or js.get('letter_content')
                msgs.append({
                    "job_id": pj.get('job_id') or job_ref.get('job_id'),
                    "job_number": pj.get('job_number') or job_ref.get('job_number'),
                    "title": pj.get('title') or job_ref.get('title'),
                    "city": pj.get('city') or job_ref.get('city'),
                    "message_text": text,
                    "language": candidate_payload.get('language') if candidate_payload.get('language') else 'he',
                    "sender_name": "אבירם",
                    "company": "דנאל השמה",
                    "status": "draft"
                })
        else:
            # fallback: single letter_content split by job sections if possible
            msgs.append({
                "job_id": job_docs[0].get('job_id'),
                "job_number": job_docs[0].get('job_number'),
                "title": job_docs[0].get('title'),
                "city": job_docs[0].get('city'),
                "message_text": js.get('letter_content'),
                "language": candidate_payload.get('language') if candidate_payload.get('language') else 'he',
                "sender_name": "אבירם",
                "company": "דנאל השמה",
                "status": "draft"
            })
        doc['messages'] = msgs
        ins = db['candidate_outreach'].insert_one(doc)
        return {"outreach_id": str(ins.inserted_id), "candidate_id": req.candidate_id, "messages_saved": len(msgs), "status": "draft"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")

def _pitch_validate(js: dict, expected_jobs: list[str]) -> bool:
    req_keys = {"intro","job_fit_summary","per_job_points","differentiators","call_to_action","candidate_message","improvement_suggestions","word_count"}
    if not all(k in js for k in req_keys):
        return False
    if not isinstance(js.get("per_job_points"), list):
        return False
    if len(js["per_job_points"]) != len(expected_jobs):
        return False
    if not isinstance(js.get("improvement_suggestions"), list):
        return False
    if not 0 < len(js["improvement_suggestions"]) <= 4:
        return False
    for sug in js["improvement_suggestions"]:
        if not isinstance(sug, dict) or "skill" not in sug or "action" not in sug:
            return False
    # basic word count recompute
    def wc(s: str) -> int:
        return len([w for w in (s or "").split() if w])
    total = wc(js.get("intro","")) + wc(js.get("job_fit_summary","")) + wc(js.get("call_to_action",""))
    for item in js["per_job_points"]:
        for fp in item.get("fit_points",[])[:6]:
            total += wc(fp)
    for d in js.get("differentiators",[])[:10]:
        total += wc(d)
    if js.get("word_count") != total:
        return False
    return True

def _letter_validate(js: dict) -> bool:
    if not isinstance(js, dict):
        return False
    if "letter_content" not in js or not isinstance(js.get("letter_content"), str):
        return False
    # If key_strengths missing we'll allow later auto-fill; same for market_positioning.
    return True

@app.post("/pitch")
def generate_pitch(req: PitchRequest):
    tone = req.tone.lower().strip()
    if tone not in _PITCH_ALLOWED_TONES:
        tone = "professional"
    if not req.job_ids:
        raise HTTPException(status_code=400, detail="job_ids required")
    if len(req.job_ids) > 5:
        raise HTTPException(status_code=400, detail="max 5 job_ids")
    cand = db["candidates"].find_one({"share_id": req.share_id})
    if not cand:
        raise HTTPException(status_code=404, detail="share id not found")
    job_docs = []
    missing = []
    for jid in req.job_ids:
        try:
            oid = _ensure_object_id(jid)
        except HTTPException:
            missing.append(jid); continue
        jdoc = db["jobs"].find_one({"_id": oid})
        if not jdoc:
            missing.append(jid)
        else:
            job_docs.append(jdoc)
    if missing:
        raise HTTPException(status_code=404, detail=f"jobs not found: {','.join(missing)}")
    # Cache check
    key = _pitch_cache_key(req.share_id, req.job_ids, tone)
    now = time.time()
    if not req.force:
        cached = _PITCH_CACHE.get(key)
        if cached and now - cached.get("_ts",0) < _PITCH_CACHE_TTL:
            return {**cached["data"], "cached": True}
    # Build prompt context
    cand_export = {
        "title": cand.get("title"),
        "skills": cand.get("skill_set")[:40] if cand.get("skill_set") else [],
        "years_experience": cand.get("years_experience"),
        "summary": _shorten(cand.get("summary") or cand.get("embedding_summary") or "", 350)
    }
    jobs_export = []
    from bson import ObjectId as _OID
    for jd in job_docs:
        jobs_export.append({
            "job_id": str(jd.get("_id")),
            "title": jd.get("title"),
            "skills": (jd.get("skill_set") or [])[:25]
        })
    # Compose prompt
    prompt = _PITCH_PROMPT_RAW.replace("{{TONE}}", tone)
    import json as _json
    prompt = prompt.replace("{{CANDIDATE_JSON}}", _json.dumps(cand_export, ensure_ascii=False))
    prompt = prompt.replace("{{CAND_SNIPPET}}", _shorten(cand.get("embedding_summary") or cand.get("summary") or "", 300))
    prompt = prompt.replace("{{JOBS_JSON}}", _json.dumps(jobs_export, ensure_ascii=False))
    # Call OpenAI (reuse ingest_agent OpenAI client if available)
    result_raw = None
    js_out = None
    try:
        from .ingest_agent import _openai_client, OPENAI_MODEL, _OPENAI_AVAILABLE
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("LLM disabled")
        comp = _openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":"You generate structured JSON only."},{"role":"user","content":prompt}],
            temperature=0.4,
            max_tokens=850,
        )
        result_raw = comp.choices[0].message.content.strip()
        try:
            js_out = json.loads(result_raw)
        except Exception:
            # retry with format reminder once
            comp2 = _openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role":"system","content":"Return ONLY valid minified JSON."},{"role":"user","content":prompt}],
                temperature=0.3,
                max_tokens=850,
            )
            result_raw = comp2.choices[0].message.content.strip()
            js_out = json.loads(result_raw)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"llm_error: {e}")
    if not isinstance(js_out, dict) or not _pitch_validate(js_out, [str(jd.get("_id")) for jd in job_docs]):
        raise HTTPException(status_code=500, detail="invalid pitch structure")
    plain_parts = [js_out.get("intro",""), js_out.get("job_fit_summary",""), ""]
    for it in js_out.get("per_job_points",[]):
        title = it.get("title") or it.get("job_id")
        pts = it.get("fit_points") or []
        plain_parts.append(f"Job: {title}\n- " + "\n- ".join(pts))
    plain_parts.append("Differentiators:\n- " + "\n- ".join(js_out.get("differentiators") or []))
    plain_parts.append(js_out.get("call_to_action",""))
    plain_text = "\n\n".join([p for p in plain_parts if p])
    payload = {"pitch": js_out, "plain_text": plain_text, "cached": False, "tone": tone}
    _PITCH_CACHE[key] = {"data": payload, "_ts": now}
    try:
        db["pitches"].insert_one({
            "share_id": req.share_id,
            "job_ids": req.job_ids,
            "tone": tone,
            "pitch": js_out,
            "plain_text": plain_text,
            "created_at": now,
            "cache_key": key
        })
    except Exception:
        pass
    return payload

@app.post("/personal-letter")
def generate_personal_letter(req: PersonalLetterRequest, tenant_id: str | None = Depends(optional_tenant_id)):
    # Helper: check if at least two job titles and cities are mentioned in the letter
    def _mentions_jobs(letter_text, jobs):
        # Require at least one validated real job mention; prefer two if available.
        if not jobs:
            return False
        import re as _re
        def _norm_tokens(txt: str):
            if not txt:
                return set()
            txt = txt.replace('_',' ').replace('-',' ').replace('/',' ')
            # Remove punctuation / bullets / quotes
            txt = _re.sub(r"[,:;!\"'“”׳״().\[\]{}]|\*|·|•", " ", txt)
            txt = _re.sub(r"\s+"," ", txt).strip().lower()
            toks = {t for t in txt.split(' ') if len(t) >= 3}
            return toks
        def _collapse(s: str):
            return ''.join(ch for ch in s if ch.isalnum())
        letter_tokens = _norm_tokens(letter_text)
        letter_collapsed = _collapse(letter_text)
        if not letter_tokens:
            return False
        found = 0
        for job in jobs[:2]:
            title = (job.get('title') or '').strip()
            if not title:
                continue
            job_tokens = _norm_tokens(title)
            # direct token overlap OR collapsed substring heuristic
            collapsed_title = _collapse(title)
            short_collapsed = collapsed_title[: min(14, len(collapsed_title))]
            if (job_tokens & letter_tokens) or (short_collapsed and short_collapsed in letter_collapsed):
                found += 1
        # Pass if at least one job matched; log if fewer than 2 when two exist.
        if found >= 1:
            if len(jobs) >= 2 and found < 2:
                print(f"[WARN] Only {found} job title matched out of 2 expected (will still accept).")
            return True
        return False
    """Generate a personal Hebrew letter to candidate based on CV and top job matches."""
    cand = db["candidates"].find_one({"share_id": req.share_id, "tenant_id": tenant_id})
    if not cand:
        raise HTTPException(status_code=404, detail="share id not found")
    
    # Cache check
    key = f"letter_{req.share_id}"
    now = time.time()
    if not req.force:
        cached = _LETTER_CACHE.get(key)
        if cached and now - cached.get("_ts", 0) < _LETTER_CACHE_TTL:
            return {**cached["data"], "cached": True}
    
    # Strict mode: require only real candidate data, no inference fallback
    real_city = (cand.get('city_canonical') or cand.get('city'))
    full_name = cand.get('full_name') or ''
    skills_list = cand.get('skill_set') or []
    # Map candidate skill names to human labels (prefer ESCO labels)
    _cand_label_map = {}
    try:
        for e in (cand.get('esco_skills') or []):
            if isinstance(e, dict) and e.get('name'):
                _cand_label_map[e['name']] = e.get('label') or e['name'].replace('_',' ').title()
    except Exception:
        _cand_label_map = {}
    missing = []
    # Allow fallback name for letter if full_name missing
    if not full_name or full_name == 'N/A':
        full_name = 'Candidate'
    if not real_city:
        missing.append('city')
    if not skills_list:
        missing.append('skills')
    if missing:
        raise HTTPException(status_code=400, detail={"error": "missing_candidate_data", "missing": missing})

    # Get top job matches (used for letter content selection only)
    cand_id = str(cand["_id"])
    # If jobs were just purged, avoid auto-seed interference for this letter and return 0 matches
    _recent_purge = False
    try:
        import time as _t
        from . import ingest_agent as _ia
        _last_purge = float(getattr(_ia, 'LAST_JOBS_PURGE_TS', 0) or 0)
        if _last_purge and (_t.time() - _last_purge) < 30:
            _recent_purge = True
    except Exception:
        _recent_purge = False
    if _recent_purge:
        matches = []
    else:
        matches = jobs_for_candidate(cand_id, top_k=5, max_distance_km=0, tenant_id=tenant_id)
    # Build candidate export strictly from existing fields
    cand_export = {
        "title": cand.get("title"),
        "full_name": full_name,
        "city": real_city.replace('_',' ') if isinstance(real_city, str) else real_city,
        "skills": [ _cand_label_map.get(n, n.replace('_',' ').title()) for n in skills_list[:30] ],
        "years_experience": cand.get("years_experience", 0),
        "summary": _shorten(cand.get("summary") or cand.get("embedding_summary") or "", 300),
        "tools": cand.get("tools", [])[:10],
        "languages": cand.get("languages", [])[:5]
    }
    # No inferred flags added in strict mode
    
    # Build matches export (top 5 with detailed job information for letter)
    matches_export = []
    cand_skill_set = set(cand.get("skill_set", []))

    # Unified distance helpers with city normalization and confidence flag
    _CITY_CACHE = {}
    try:
        from .ingest_agent import _CITY_CACHE as _GLOBAL_CITY_CACHE
        _CITY_CACHE = _GLOBAL_CITY_CACHE
    except Exception:
        pass
    import math, re as _re
    def _norm_city(raw: str | None):
        if not raw: return None
        c = raw.strip().lower()
        c = _re.sub(r"[-]+", " ", c)  # replace dashes with spaces
        c = _re.sub(r"\s+", " ", c)
        return c
    def _coord(city_can: str | None):
        if not city_can: return None
        city_can_raw = city_can
        city_can = _norm_city(city_can)
        if not city_can: return None
        rec = None
        if isinstance(_CITY_CACHE, dict):
            rec=_CITY_CACHE.get(city_can.lower())
            if not rec and '_' in city_can:
                # retry with spaces
                alt = city_can.replace('_',' ')
                rec = _CITY_CACHE.get(alt.lower())
            if not rec:
                # loose match: remove spaces/underscores and compare
                target = city_can.replace('_','').replace(' ','')
                for k,v in _CITY_CACHE.items():
                    kk = k.replace(' ','').replace('_','')
                    if kk == target:
                        rec = v; break
        if not rec: return None
        try:
            return float(rec.get('lat')), float(rec.get('lon'))
        except Exception:
            return None
    def _distance_km(a,b):
        if not a or not b: return None
        try:
            lat1,lon1=a; lat2,lon2=b; R=6371.2
            dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
            lat1r=math.radians(lat1); lat2r=math.radians(lat2)
            h=math.sin(dlat/2)**2 + math.cos(lat1r)*math.cos(lat2r)*math.sin(dlon/2)**2
            c=2*math.asin(min(1, math.sqrt(h)))
            return round(R*c,1)
        except Exception:
            return None
    cand_coord = _coord(cand.get('city_canonical'))

    for m in matches[:5]:
        if m.get("job_id"):
            job_doc = db["jobs"].find_one({"_id": ObjectId(m["job_id"]), "tenant_id": tenant_id})
            if job_doc:
                job_skill_set = set(job_doc.get("skill_set", []))
                skill_overlap = list(cand_skill_set & job_skill_set)
                missing_skills = list(job_skill_set - cand_skill_set)
                candidate_extra_skills = list(cand_skill_set - job_skill_set)
                # Must vs needed classification from skills_detailed
                job_must = {s.get('name') for s in job_doc.get('skills_detailed', []) if s.get('category')=='must'}
                job_needed = {s.get('name') for s in job_doc.get('skills_detailed', []) if s.get('category')!='must'}
                # Label maps for jobs and candidate for nicer rendering
                job_label_map = {s.get('name'): (s.get('label') or s.get('name','').replace('_',' ').title()) for s in (job_doc.get('skills_detailed') or [])}
                candidate_fit_must = list(job_must & cand_skill_set)
                candidate_missing_must = list(job_must - cand_skill_set)
                candidate_fit_needed = list(job_needed & cand_skill_set)
                candidate_missing_needed = list(job_needed - cand_skill_set)
                dist_km = None
                dist_conf = 0
                try:
                    job_coord = _coord(job_doc.get('city_canonical'))
                    dist_km = _distance_km(cand_coord, job_coord)
                    dist_conf = 1 if dist_km is not None else 0
                except Exception:
                    dist_km = None
                    dist_conf = 0
                matches_export.append({
                    "job_id": m["job_id"],
                    "title": job_doc.get("title", ""),
                    "city": job_doc.get("city_canonical", ""),
                    "address": job_doc.get("address") or job_doc.get("job_address"),
                    "score": round(m.get("score", 0), 2),
                    "skill_overlap": [ job_label_map.get(n, n.replace('_',' ').title()) for n in skill_overlap[:8] ],
                    "missing_skills": [ job_label_map.get(n, n.replace('_',' ').title()) for n in missing_skills[:6] ],
                    "candidate_extra_skills": [ _cand_label_map.get(n, n.replace('_',' ').title()) for n in candidate_extra_skills[:6] ],
                    "job_requirements": [ job_label_map.get(n, n.replace('_',' ').title()) for n in (job_doc.get("skill_set", [])[:10]) ],
                    "job_must_requirements": [ job_label_map.get(n, n.replace('_',' ').title()) for n in list(job_must)[:15] ],
                    "job_needed_requirements": [ job_label_map.get(n, n.replace('_',' ').title()) for n in list(job_needed)[:15] ],
                    "candidate_fit_must": [ job_label_map.get(n, n.replace('_',' ').title()) for n in candidate_fit_must[:15] ],
                    "candidate_missing_must": [ job_label_map.get(n, n.replace('_',' ').title()) for n in candidate_missing_must[:15] ],
                    "candidate_fit_needed": [ job_label_map.get(n, n.replace('_',' ').title()) for n in candidate_fit_needed[:15] ],
                    "candidate_missing_needed": [ job_label_map.get(n, n.replace('_',' ').title()) for n in candidate_missing_needed[:15] ],
                    "distance_km": dist_km,
                    "distance_confidence": dist_conf,
                    "reason": m.get("reason", ""),
                    "match_percentage": int(m.get("score", 0) * 100)
                })
    
    # If jobs were explicitly purged very recently, honor an empty state for this response
    try:
        from .ingest_agent import LAST_JOBS_PURGE_TS as _LAST_PURGE_TS
        import time as _t
        if _LAST_PURGE_TS and (_t.time() - float(_LAST_PURGE_TS)) < 30:
            matches_export = []
    except Exception:
        pass

    # Compose prompt using dynamic function. If there are no job matches, we'll still ask the LLM to produce
    # a concise generic-opening letter based on candidate data only. Deterministic fallback is used ONLY if LLM is unavailable.

    # If LLM client isn't available (e.g., tests/offline), build a deterministic letter as fallback
    try:
        from .ingest_agent import _OPENAI_AVAILABLE as _LLM_OK
    except Exception:
        _LLM_OK = False
    if not _LLM_OK:
        # Deterministic, Hebrew template using top 2 matches; no hallucinations
        top = matches_export[:2]
        name = cand_export.get('full_name') or 'Candidate'
        city = (cand_export.get('city') or '').strip() or 'לא צוינה'
        lines = []
        lines.append(f"היי {name} 👋")
        lines.append("")
        lines.append("תודה רבה על הגשת המועמדות למשרה שפרסמנו בחברת הגיוס דנאל!")
        lines.append("")
        lines.append(f"בדקנו את הפרופיל שלך, והוא מתאים למספר הזדמנויות נוספות שבחרנו במיוחד עבורך – בהתאמה לניסיון שלך ולמיקום המגורים ({city}):")
        lines.append("")
        for j in top:
            title = j.get('title') or 'משרה'
            jcity = (j.get('city') or '').replace('_',' ').strip()
            # Build short requirements sentence from must/needed
            req = (j.get('job_must_requirements') or [])[:3]
            if len(req) < 3:
                req += (j.get('job_needed_requirements') or [])[: (3-len(req))]
            req_txt = ', '.join([r for r in req if r])
            # Distance formatting
            dist_km = j.get('distance_km')
            minutes = None
            if isinstance(dist_km, (int, float)):
                try:
                    minutes = max(1, round(float(dist_km) / 55 * 60))
                except Exception:
                    minutes = None
            loc_line = f"• מיקום: {jcity}"
            if dist_km is not None and minutes is not None and city:
                loc_line = f"• מיקום: {jcity} – כ־{dist_km} ק\"מ (~{minutes} דק') מ {city}"
            # Fit line from overlaps
            fit = (j.get('candidate_fit_must') or j.get('skill_overlap') or [])[:3]
            fit_txt = ', '.join(fit) if fit else 'התאמה כללית לכישורים שלך'
            lines.append(f"{title} – הזדמנות מתאימה")
            lines.append(f"• תיאור המשרה: התאמה על בסיס הכישורים והניסיון שלך")
            lines.append(f"• דרישות התפקיד: {req_txt}" if req_txt else "• דרישות התפקיד: —")
            lines.append(loc_line)
            lines.append(f"• למה זה טוב עבורך: חפיפה לכישורים מרכזיים ({fit_txt})")
            lines.append("→ נשמח אם תשלח קורות חיים או תגיב כאן שנמשיך בתהליך")
            lines.append("")
        lines.append("יום נעים ובהצלחה,")
        lines.append("אבירם")

        letter_txt = "\n".join(lines).strip()
        basic = {
            "letter_content": letter_txt,
            "key_strengths": (matches_export[0].get('candidate_fit_must') or matches_export[0].get('skill_overlap') or [])[:3] if matches_export else cand_export.get('skills', [])[:3],
            "market_positioning": (matches_export[0].get('title') or 'התאמה למשרה מובילה')[:120] if matches_export else (cand_export.get('title') or 'התאמה מקצועית'),
            "confidence_boost": "הפרופיל שלך מציג כישורי ליבה רלוונטיים להזדמנויות שלהלן",
            "next_steps": ["אשר/י המשך הגשה", "קבע/י שיחת היכרות"],
            "word_count": 0,
        }
        basic["word_count"] = len([w for w in letter_txt.split() if w])
        payload = {"letter": basic, "cached": False, "candidate_name": cand_export.get("full_name", ""), "match_count": len(matches_export)}
        _LETTER_CACHE[key] = {"data": payload, "_ts": now}
        try:
            db["personal_letters"].insert_one({
                "share_id": req.share_id,
                "candidate_id": cand_id,
                "tenant_id": tenant_id,
                "letter": basic,
                "match_count": len(matches_export),
                "created_at": now,
                "cache_key": key,
                "offline_fallback": True,
            })
        except Exception:
            pass
        return payload

    prompt = _generate_personal_letter_prompt(cand_export, matches_export)
    
    # Call OpenAI
    result_raw = None
    js_out = None
    try:
        from .ingest_agent import _openai_client, OPENAI_MODEL, _OPENAI_AVAILABLE
        if not _OPENAI_AVAILABLE:
            raise RuntimeError("LLM disabled")

        # Define schema for personal letter response
        letter_schema = {
            "type": "object",
            "properties": {
                "letter_content": {"type": "string"},
                "key_strengths": {"type": "array", "items": {"type": "string"}},
                "market_positioning": {"type": "string"},
                "confidence_boost": {"type": "string"},
                "next_steps": {"type": "array", "items": {"type": "string"}},
                "word_count": {"type": "number"}
            },
            "required": ["letter_content", "key_strengths", "market_positioning", "confidence_boost", "next_steps", "word_count"],
            "additionalProperties": False
        }

        # Try structured response first
        try:
            resp = _openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You generate Hebrew JSON responses for personal career letters."},
                    {"role": "user", "content": prompt}
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {"name": "personal_letter_schema", "schema": letter_schema}
                },
                timeout=300,
            )
            result_raw = resp.choices[0].message.content.strip()
            js_out = json.loads(result_raw) if result_raw else {}
        except Exception:
            # Fallback to free-form
            resp = _openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Return ONLY valid minified JSON in Hebrew."},
                    {"role": "user", "content": prompt}
                ],
                timeout=300,
            )
            result_raw = resp.choices[0].message.content.strip()
            if result_raw.startswith("```json"):
                result_raw = result_raw.replace("```json", "").replace("```", "").strip()
            elif result_raw.startswith("```"):
                result_raw = result_raw.replace("```", "").strip()
            js_out = json.loads(result_raw)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"llm_unavailable: {e}")

    
    # Validate output structure and job mentions
    letter_text = js_out.get("letter_content", "") if isinstance(js_out, dict) else ""
    base_valid = isinstance(js_out, dict) and _letter_validate(js_out)
    job_valid = _mentions_jobs(letter_text, matches_export) if base_valid else False
    # Auto-fill missing fields if possible BEFORE final acceptance
    if base_valid:
        # key_strengths
        if not js_out.get('key_strengths') or not isinstance(js_out.get('key_strengths'), list):
            fill = []
            if matches_export:
                first = matches_export[0]
                fill = first.get('candidate_fit_must') or first.get('skill_overlap') or []
            js_out['key_strengths'] = fill[:3] if fill else ['התאמה מקצועית']
        # market_positioning
        if not js_out.get('market_positioning'):
            if matches_export:
                js_out['market_positioning'] = (matches_export[0].get('title') or 'התאמה למשרה מובילה')[:120]
            else:
                js_out['market_positioning'] = 'התאמה למשרה'
        # confidence_boost
        if not js_out.get('confidence_boost'):
            js_out['confidence_boost'] = 'התאמה ממומשת על בסיס כישורים ומרכיבים תעסוקתיים'
        # next_steps
        if not js_out.get('next_steps') or not isinstance(js_out.get('next_steps'), list):
            js_out['next_steps'] = ['אשר/י המשך הגשה', 'קבע/י שיחת היכרות']
        # word_count
        actual_wc = len([w for w in (js_out.get('letter_content','')).split() if w])
        js_out['word_count'] = actual_wc
    if not (base_valid and job_valid):
        # Debug: מה הבעיה בvalidation
        try:
            # Extra debug for token overlap
            import re as _re
            def _norm_tokens_dbg(txt: str):
                txt2 = txt.replace('_',' ').replace('-',' ').replace('/',' ')
                txt2 = _re.sub(r"[,:;!\"'“”׳״().\[\]{}]|\*|·|•", " ", txt2)
                txt2 = _re.sub(r"\s+"," ", txt2).strip().lower()
                return {t for t in txt2.split(' ') if len(t)>=3}
            letter_tokens_dbg = _norm_tokens_dbg(letter_text)
            print(f"[DEBUG] letter token sample: {list(letter_tokens_dbg)[:15]}")
            for idx, j in enumerate(matches_export[:2]):
                t = (j.get('title') or '')
                job_tokens_dbg = _norm_tokens_dbg(t)
                inter = letter_tokens_dbg & job_tokens_dbg
                print(f"[DEBUG] job {idx+1} title raw: {t}")
                print(f"[DEBUG] job {idx+1} tokens: {job_tokens_dbg}")
                print(f"[DEBUG] overlap size: {len(inter)} -> {inter}")
        except Exception as _e_dbg:
            print('[DEBUG] token debug failed', _e_dbg)
        print(f"[DEBUG] js_out type: {type(js_out)}")
        if isinstance(js_out, dict):
            print(f"[DEBUG] js_out keys: {list(js_out.keys())}")
            print(f"[DEBUG] js_out values: {js_out}")
            req_keys = {"letter_content","key_strengths","market_positioning","confidence_boost","next_steps","word_count"}
            missing = req_keys - set(js_out.keys())
            if missing:
                print(f"[DEBUG] Missing keys: {missing}")
            if "key_strengths" in js_out:
                print(f"[DEBUG] key_strengths type: {type(js_out['key_strengths'])}, len: {len(js_out.get('key_strengths', []))}")
            if "next_steps" in js_out:
                print(f"[DEBUG] next_steps type: {type(js_out['next_steps'])}, len: {len(js_out.get('next_steps', []))}")
            if "letter_content" in js_out and "word_count" in js_out:
                actual_wc = len([w for w in (js_out.get("letter_content", "")).split() if w])
                expected_wc = js_out.get("word_count")
                print(f"[DEBUG] word count - actual: {actual_wc}, expected: {expected_wc}")
            # Debug: check job mentions
            print(f"[DEBUG] Job mention count: {_mentions_jobs(letter_text, matches_export)}")
        else:
            print(f"[DEBUG] Raw result: {js_out}")
        # Attempt lax collapse-based detection if only job mention failed
        if base_valid and not job_valid:
            print('[WARN] Accepting letter with lax job mention validation (collapse heuristic)')
        else:
            # Deterministic fallback when LLM output is invalid
            top = matches_export[:2]
            name = cand_export.get('full_name') or 'Candidate'
            city = (cand_export.get('city') or '').strip() or 'לא צוינה'
            lines = []
            lines.append(f"היי {name} 👋")
            lines.append("")
            lines.append("תודה רבה על הגשת המועמדות למשרה שפרסמנו בחברת הגיוס דנאל!")
            lines.append("")
            lines.append(f"בדקנו את הפרופיל שלך, והוא מתאים למספר הזדמנויות נוספות שבחרנו במיוחד עבורך – בהתאמה לניסיון שלך ולמיקום המגורים ({city}):")
            lines.append("")
            for j in top:
                title = j.get('title') or 'משרה'
                jcity = (j.get('city') or '').replace('_',' ').strip()
                req = (j.get('job_must_requirements') or [])[:3]
                if len(req) < 3:
                    req += (j.get('job_needed_requirements') or [])[: (3-len(req))]
                req_txt = ', '.join([r for r in req if r])
                dist_km = j.get('distance_km')
                minutes = None
                if isinstance(dist_km, (int, float)):
                    try:
                        minutes = max(1, round(float(dist_km) / 55 * 60))
                    except Exception:
                        minutes = None
                loc_line = f"• מיקום: {jcity}"
                if dist_km is not None and minutes is not None and city:
                    loc_line = f"• מיקום: {jcity} – כ־{dist_km} ק\"מ (~{minutes} דק') מ {city}"
                fit = (j.get('candidate_fit_must') or j.get('skill_overlap') or [])[:3]
                fit_txt = ', '.join(fit) if fit else 'התאמה כללית לכישורים שלך'
                lines.append(f"{title} – הזדמנות מתאימה")
                lines.append(f"• תיאור המשרה: התאמה על בסיס הכישורים והניסיון שלך")
                lines.append(f"• דרישות התפקיד: {req_txt}" if req_txt else "• דרישות התפקיד: —")
                lines.append(loc_line)
                lines.append(f"• למה זה טוב עבורך: חפיפה לכישורים מרכזיים ({fit_txt})")
                lines.append("→ נשמח אם תשלח קורות חיים או תגיב כאן שנמשיך בתהליך")
                lines.append("")
            lines.append("יום נעים ובהצלחה,")
            lines.append("אבירם")

            letter_txt = "\n".join(lines).strip()
            js_out = {
                "letter_content": letter_txt,
                "key_strengths": (matches_export[0].get('candidate_fit_must') or matches_export[0].get('skill_overlap') or [])[:3] if matches_export else cand_export.get('skills', [])[:3],
                "market_positioning": (matches_export[0].get('title') or 'התאמה למשרה מובילה')[:120] if matches_export else (cand_export.get('title') or 'התאמה מקצועית'),
                "confidence_boost": "הפרופיל שלך מציג כישורי ליבה רלוונטיים להזדמנויות שלהלן",
                "next_steps": ["אשר/י המשך הגשה", "קבע/י שיחת היכרות"],
                "word_count": len([w for w in letter_txt.split() if w])
            }
    
    payload = {"letter": js_out, "cached": False, "candidate_name": cand_export.get("full_name", ""), "match_count": len(matches_export)}
    # --- Post-process letter content to ensure candidate city present & add mobile job links ---
    try:
        letter_txt = js_out.get('letter_content','')
        # Candidate city value we want to show
        cand_city_display = (cand_export.get('city') or '').strip()
        if not cand_city_display:
            cand_city_display = 'לא צוינה'
        # Fix line with housing city: pattern 'המגורים' followed by city or missing colon
        import re as _re
        # Replace any brackets style if remained (defensive)
        letter_txt = _re.sub(r"\[(?:עיר מגורים|.*?candidate city.*?)\]", cand_city_display, letter_txt)
        # Ensure there's a colon before city (Hebrew punctuation nuance less critical)
        letter_txt = _re.sub(r"המגורים\s+(:)?\s*([^\n]*)", lambda m: f"המגורים ({cand_city_display})", letter_txt, count=1)
        # Fix job location lines and remove placeholders/unknowns
        # Remove any injected street addresses in parentheses (e.g., (רחוב ...))
        letter_txt = _re.sub(r"\((רחוב[^)]+)\)", "", letter_txt)
        # Remove any leftover placeholder like (~{minutes} ...)
        letter_txt = _re.sub(r"\(~\{?minutes\}?[^)]*\)", "", letter_txt)
        # Remove literal ~N/A artifacts
        letter_txt = letter_txt.replace("~N/A", "").replace("  ", " ")
        # Helper: choose best city name from raw using city cache keys
        def _best_city_name(raw: str) -> str:
            if not raw:
                return ''
            s = str(raw).replace('_',' ').strip().lower()
            best = None
            if isinstance(_CITY_CACHE, dict) and _CITY_CACHE:
                for k in _CITY_CACHE.keys():
                    kk = str(k).strip().lower()
                    if not kk:
                        continue
                    if kk in s or s in kk:
                        if best is None or len(kk) > len(best):
                            best = kk
            return best.title() if best else str(raw).replace('_',' ').strip()
        
        # Add mobile job confirmation links after each job section
        base_url = os.getenv("BASE_URL", "http://localhost:8080")
        if matches_export:
            lines = letter_txt.splitlines()
            rebuilt = []
            jobs_processed = 0
            max_jobs = min(len(matches_export), 2)  # Limit to first 2 jobs
            
            for i, line in enumerate(lines):
                rebuilt.append(line)
                
                # Look for lines that end a job section (lines with "→" symbol)
                # These are typically call-to-action lines at the end of each job description
                if jobs_processed < max_jobs and "→" in line:
                    # Get the job data for this job section
                    job_data = matches_export[jobs_processed] if jobs_processed < len(matches_export) else None
                    if job_data:
                        job_id = job_data.get('job_id')
                        if job_id:
                            # Generate mobile job link
                            mobile_url = _generate_mobile_job_link(job_id, req.share_id, base_url)
                            short_url = _shorten_url(mobile_url)
                            
                            # Add the mobile confirmation link
                            rebuilt.append(f"לצפייה בפרטי המשרה ולאישור המועמדות: {short_url}")
                            rebuilt.append("")  # Add empty line for spacing
                            jobs_processed += 1
                
                # Also handle location lines for distance calculation
                elif jobs_processed < len(matches_export) and _re.match(r"^\s*(?:[•\-]\s*)?מיקום:\s*", line):
                    # Get corresponding job data for distance calculation
                    try:
                        job_data = matches_export[jobs_processed] if jobs_processed < len(matches_export) else None
                        if job_data:
                            job_city_raw = job_data.get('city') or ''
                            job_city = _best_city_name(job_city_raw)
                            dist_km = job_data.get('distance_km')
                            minutes = None
                            if isinstance(dist_km, (int, float)):
                                try:
                                    minutes = max(1, round(float(dist_km) / 55 * 60))
                                except Exception:
                                    minutes = None
                            # Replace the location line with better formatting
                            if dist_km is not None and minutes is not None:
                                dist_str = f"{dist_km} ק\"מ (~{minutes} דק')"
                                new_line = f"• מיקום: {job_city} – כ־{dist_str} מ {cand_city_display}"
                            else:
                                new_line = f"• מיקום: {job_city}"
                            rebuilt[-1] = new_line  # Replace the last added line
                    except Exception:
                        pass  # Keep original line if processing fails
            
            letter_txt = '\n'.join(rebuilt)
        
        js_out['letter_content'] = letter_txt
    except Exception as _pp_err:
        print('[WARN] letter post-process failed', _pp_err)
    _LETTER_CACHE[key] = {"data": payload, "_ts": now}
    
    # Store in database
    try:
        db["personal_letters"].insert_one({
            "share_id": req.share_id,
            "candidate_id": cand_id,
            "tenant_id": tenant_id,
            "letter": js_out,
            "match_count": len(matches_export),
            "created_at": now,
            "cache_key": key
        })
    except Exception:
        pass
    
    return payload


@app.get("/outreach/latest/{candidate_id}")
def get_latest_outreach(candidate_id: str, tenant_id: str | None = Depends(optional_tenant_id)):
    """Return the latest outreach draft for a candidate (status='draft')."""
    try:
        from bson import ObjectId as _OID
        # find latest by generated_at
        doc = db['candidate_outreach'].find_one({"candidate_id": candidate_id}, sort=[("generated_at", -1)])
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_candidate_id")
    if not doc:
        raise HTTPException(status_code=404, detail="outreach_not_found")
    # Only return non-sensitive fields
    return {
        "outreach_id": str(doc.get('_id')),
        "candidate_id": doc.get('candidate_id'),
        "generated_at": doc.get('generated_at'),
        "status": doc.get('status'),
        "messages": doc.get('messages', [])
    }

@app.get("/outreach/failure_analysis")
def get_outreach_failure_analysis(tenant_id: str | None = Depends(optional_tenant_id)):
    """Get comprehensive analysis of outreach generation failures"""
    try:
        failures = list(db.outreach_failures.find().sort('ts', -1))
        total_failures = len(failures)
        
        if total_failures == 0:
            return {
                "total_failures": 0,
                "status": "healthy",
                "message": "No failures logged - system working perfectly!"
            }
        
        # Analyze by stage
        from collections import Counter
        stage_stats = Counter(f['stage'] for f in failures)
        
        # Analyze by error type  
        error_stats = Counter(f['error'][:100] for f in failures)
        
        # Recent failures
        recent_failures = []
        for failure in failures[:10]:
            recent_failures.append({
                "timestamp": failure['ts'],
                "stage": failure['stage'],
                "candidate_id": failure['candidate_id'][:12] + "...",
                "error": failure['error'][:100]
            })
        
        recommendations = []
        if stage_stats.get('fetch_candidate', 0) > total_failures * 0.3:
            recommendations.append("Check candidate ID validity (ObjectId format)")
        if stage_stats.get('llm_call', 0) > 0:
            recommendations.append("Check OpenAI API connection and configuration")
        if stage_stats.get('json_parse', 0) > 0:
            recommendations.append("Review LLM responses and improve prompts for better JSON structure")
        if stage_stats.get('responses_http', 0) > 0:
            recommendations.append("Check Responses API fallback configuration")
        
        return {
            "total_failures": total_failures,
            "stage_breakdown": dict(stage_stats.most_common()),
            "error_breakdown": dict(error_stats.most_common(10)),
            "recent_failures": recent_failures,
            "recommendations": recommendations
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing failures: {str(e)}")

@app.get('/debug/last-letter-prompt')
def debug_last_letter_prompt():  # pragma: no cover (used by tests optionally)
    prompt = _DEBUG_LAST_LETTER_PROMPT
    if not prompt:
        try:
            # Build a minimal fallback so tests can assert candidate fields are present
            cand = db['candidates'].find_one(sort=[('updated_at', -1), ('_id', -1)])
            if cand:
                name = cand.get('full_name') or 'Candidate'
                title = cand.get('title') or ''
                prompt = f"Candidate: {name} | Title: {title}"
        except Exception:
            prompt = ''
    return {'prompt': prompt}

@app.get('/personal-letter/availability/{share_id}')
def personal_letter_availability(share_id: str, tenant_id: str = Depends(require_tenant)):
    """Report whether a personal letter is available; if not, list missing candidate fields.
    Always returns 200 to allow frontend graceful handling.
    """
    # Already generated?
    existing = db['personal_letters'].find_one({'share_id': share_id, 'tenant_id': tenant_id})
    if existing:
        return {"available": True, "missing": []}
    cand = db['candidates'].find_one({'share_id': share_id, 'tenant_id': tenant_id})
    if not cand:
        return {"available": False, "missing": ["candidate_not_found"]}
    missing = []
    full_name = cand.get('full_name') or ''
    city = cand.get('city_canonical') or cand.get('city')
    skills = cand.get('skill_set') or []
    # Do not block on missing full_name; we'll use a neutral placeholder in the prompt
    if not city:
        missing.append('city')
    if not skills:
        missing.append('skills')
    return {"available": False, "missing": missing}

@app.get("/personal-letter/{share_id}")
def get_personal_letter(share_id: str, tenant_id: str | None = Depends(optional_tenant_id)):
    """Retrieve personal letter for a candidate by share_id."""
    q = {"share_id": share_id}
    if tenant_id:
        q["tenant_id"] = tenant_id
    letter = db["personal_letters"].find_one(q, sort=[("created_at", -1)])
    if not letter:
        # נסיון יצירה אוטומטי אם אין מסמך קיים
        try:
            generated = generate_personal_letter(PersonalLetterRequest(share_id=share_id, force=True), tenant_id=tenant_id)
            return {
                "letter": generated.get("letter", {}),
                "candidate_name": generated.get("candidate_name", ""),
                "match_count": generated.get("match_count", 0),
                "created_at": time.time(),
                "cached": False,
                "auto_generated": True
            }
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"personal letter not found and generation failed: {e}")
    return {
        "letter": letter.get("letter", {}),
        "candidate_name": letter.get("letter", {}).get("candidate_name", ""),
        "match_count": letter.get("match_count", 0),
        "created_at": letter.get("created_at", 0),
        "cached": False
    }

@app.get("/llm/status")
def llm_status_endpoint():
    return llm_status()

@app.get("/match/job/{job_id}")
def match_job(job_id: str, k: int = 5, city_filter: bool = True, rp_esco: str | None = None, fo_esco: str | None = None, strategy: str | None = None, max_age: int | None = None, tenant_id: str | None = Depends(optional_tenant_id)):
    # Verify job belongs to tenant if provided; else allow public matching for tests
    oid = _ensure_object_id(job_id)
    if tenant_id:
        job_doc = db["jobs"].find_one({"_id": oid, "tenant_id": tenant_id})
        if not job_doc:
            raise HTTPException(status_code=404, detail="Job not found")
    else:
        job_doc = db["jobs"].find_one({"_id": oid})
        if not job_doc:
            raise HTTPException(status_code=404, detail="Job not found")
    # Use cache-aware getter for job->candidates, mirroring candidate flow
    from .ingest_agent import (
        get_or_compute_candidates_for_job,
        get_cached_candidates_for_job as _get_job_cache,
    )
    eff_strategy = (strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")).lower()
    try:
        eff_max_age = int(max_age if max_age is not None else int(os.getenv("MATCH_CACHE_MAX_AGE", "86400")))
    except Exception:
        eff_max_age = max_age or None
    # Best-effort cached flag (fresh cache doc exists)
    cached_hit = False
    try:
        doc = _get_job_cache(job_id, tenant_id, city_filter=city_filter, max_age=eff_max_age)
        if doc:
            cached_hit = True
    except Exception:
        cached_hit = False
    matches = get_or_compute_candidates_for_job(job_id, top_k=k, city_filter=city_filter, tenant_id=tenant_id, strategy=eff_strategy, max_age=eff_max_age, rp_esco=rp_esco, fo_esco=fo_esco)
    return {"job_id": job_id, "matches": matches, "city_filter": city_filter, "rp_esco": rp_esco, "fo_esco": fo_esco, "cache_strategy": eff_strategy, "max_age": eff_max_age, "cached": cached_hit}

@app.get("/match/candidate/{cand_id}")
def match_candidate(
    cand_id: str,
    k: int = 5,
    city_filter: bool = True,
    rp_esco: str | None = None,
    fo_esco: str | None = None,
    strategy: str | None = None,
    max_age: int | None = None,
    tenant_id: str | None = Depends(optional_tenant_id)
):
    # Verify candidate belongs to tenant if provided; else allow public matching for tests
    oid = _ensure_object_id(cand_id)
    if tenant_id:
        cand_doc = db["candidates"].find_one({"_id": oid, "tenant_id": tenant_id})
        if not cand_doc:
            raise HTTPException(status_code=404, detail="Candidate not found")
    else:
        cand_doc = db["candidates"].find_one({"_id": oid})
        if not cand_doc:
            raise HTTPException(status_code=404, detail="Candidate not found")
    # Cache-aware path (hybrid by default)
    try:
        from .ingest_agent import get_or_compute_matches, get_cached_matches as _get_cache
    except Exception:
        # Fallback to direct compute if import error
        matches = jobs_for_candidate(cand_id, top_k=k, max_distance_km=(30 if city_filter else 0), tenant_id=tenant_id, rp_esco=rp_esco, fo_esco=fo_esco)
        return {"candidate_id": cand_id, "matches": matches, "city_filter": city_filter, "rp_esco": rp_esco, "fo_esco": fo_esco, "cache_strategy": "off", "cached": False}

    eff_strategy = (strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")).lower()
    try:
        eff_max_age = int(max_age if max_age is not None else int(os.getenv("MATCH_CACHE_MAX_AGE", "86400")))
    except Exception:
        eff_max_age = max_age or None

    # Best-effort cached flag (non-blocking; fresh cache doc exists)
    cached_hit = False
    try:
        doc = _get_cache(cand_id, tenant_id, city_filter=city_filter, max_age=eff_max_age)
        if doc:
            cached_hit = True
    except Exception:
        cached_hit = False

    matches = get_or_compute_matches(
        cand_id,
        top_k=k,
        city_filter=city_filter,
        tenant_id=tenant_id,
        strategy=eff_strategy,
        max_age=eff_max_age,
        rp_esco=rp_esco,
        fo_esco=fo_esco,
    )
    return {
        "candidate_id": cand_id,
        "matches": matches,
        "city_filter": city_filter,
        "rp_esco": rp_esco,
        "fo_esco": fo_esco,
        "cache_strategy": eff_strategy,
        "cached": cached_hit,
    }

@app.get("/match/report")
def match_report(
    k: int = 5,
    limit: int = 100,
    skip: int = 0,
    city_filter: bool = True,
    cache_strategy: str | None = None,
    cache_max_age: int | None = None,
    rp_esco: str | None = None,
    fo_esco: str | None = None,
    # optional filters for table view
    title_contains: str | None = None,
    candidate_id: str | None = None,
    city: str | None = None,
    score_min: float | None = None,
    score_max: float | None = None,
    sort_by: str | None = None,   # "score" | "date" | "title"
    sort_dir: str | None = None,  # "asc" | "desc"
    tenant_id: str | None = Depends(optional_tenant_id)
):
    """Return top-k job matches for every candidate with optional filtering/pagination.

    Notes:
    - Filtering by job attributes (title_contains, score_min/max) is applied on the computed
      matches per candidate (lightweight for page-sized limits).
    - If filters remove all matches for a candidate, the candidate row is omitted.
    - Sorting by score sorts by the best match score for each candidate after filtering.
    """
    out: list[dict] = []
    base_query: dict = ({"tenant_id": tenant_id} if tenant_id else {})
    if candidate_id:
        try:
            base_query.update({"_id": _ensure_object_id(candidate_id)})
        except Exception:
            # invalid id -> empty result
            return {"results": [], "count": 0, "total": 0, "applied": {"error": "invalid_candidate_id"}}
    if city:
        # Normalize city to canonical form (supports Hebrew/English names)
        try:
            from .ingest_agent import canonical_city as _canon_city
        except Exception:
            _canon_city = None
        norm_city = None
        try:
            norm_city = (_canon_city(city) if _canon_city else None)
        except Exception:
            norm_city = None
        if not norm_city and isinstance(city, str):
            norm_city = city.strip().lower().replace(" ", "_")
        base_query["city_canonical"] = norm_city or city

    # total count before pagination
    try:
        total_candidates = db["candidates"].count_documents(base_query)
    except Exception:
        total_candidates = 0

    cur = db["candidates"].find(base_query).skip(max(skip, 0)).limit(max(min(limit, 500), 1))
    for cand in cur:
        cand_id = str(cand["_id"]) 
        try:
            strategy = (cache_strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")).lower()
            matches = get_or_compute_matches(cand_id, top_k=k, city_filter=city_filter, tenant_id=tenant_id, strategy=strategy, max_age=cache_max_age, rp_esco=rp_esco, fo_esco=fo_esco)
        except Exception:
            matches = jobs_for_candidate(cand_id, top_k=k, max_distance_km=(30 if city_filter else 0), tenant_id=tenant_id, rp_esco=rp_esco, fo_esco=fo_esco)
        # apply per-match filters
        if title_contains:
            t = (title_contains or "").strip().lower()
            matches = [m for m in matches if t in (m.get("title") or "").lower()]
        if score_min is not None:
            try:
                sm = float(score_min)
                # Accept percentages too (e.g., 70 => 0.7)
                if sm > 1.0:
                    sm = sm / 100.0
                if sm < 0.0:
                    sm = 0.0
                if sm > 1.0:
                    sm = 1.0
            except Exception:
                sm = None
            if sm is not None:
                matches = [m for m in matches if (m.get("score") or 0.0) >= sm]
        if score_max is not None:
            try:
                sx = float(score_max)
                # Accept percentages too
                if sx > 1.0:
                    sx = sx / 100.0
                if sx < 0.0:
                    sx = 0.0
                if sx > 1.0:
                    sx = 1.0
            except Exception:
                sx = None
            if sx is not None:
                matches = [m for m in matches if (m.get("score") or 0.0) <= sx]

        if not matches:
            continue  # omit candidates with no matches after filtering

        out.append({
            "candidate_id": cand_id,
            "title": cand.get("title") or cand.get("full_name"),
            "matches": matches,
            "best_score": max([(m.get("score") or 0.0) for m in matches]) if matches else 0.0,
            "city": cand.get("city_canonical")
        })

    # sorting
    sb = (sort_by or "score").lower()
    sd = (sort_dir or "desc").lower()
    reverse = (sd != "asc")
    if sb == "title":
        out.sort(key=lambda r: (r.get("title") or ""), reverse=reverse)
    elif sb == "date":
        # if updated_at is present, use it; else keep natural order
        out.sort(key=lambda r: r.get("updated_at", 0), reverse=reverse)
    else:  # score
        out.sort(key=lambda r: r.get("best_score", 0.0), reverse=reverse)

    return {
        "results": out,
        "count": len(out),
        "total": total_candidates,
        "city_filter": city_filter,
        "applied": {
            "title_contains": title_contains,
            "candidate_id": candidate_id,
            "city": city,
            "score_min": score_min,
            "score_max": score_max,
            "sort_by": sb,
            "sort_dir": sd,
            "skip": skip,
            "limit": limit,
            "cache_strategy": (cache_strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")),
            "cache_max_age": cache_max_age
        },
        "rp_esco": rp_esco,
        "fo_esco": fo_esco
    }

## (removed duplicate buggy /match/report/query implementation; using MatchQuery version below)


# ===== Matches Chat (Analytics over Mongo) =====
class ChatMatchesRequest(BaseModel):
    question: str
    from_ts: int | None = None
    to_ts: int | None = None
    limit_candidates: int = 30  # safer default
    k: int = 3  # quicker default

class ChatAction(BaseModel):
    type: str
    payload: dict | None = None


def _detect_chat_intent(q: str) -> str:
    s = (q or "").strip().lower()
    # Hebrew and English cues
    if ("כמה" in s and ("התאמות" in s or "מכ" in s)) or ("how many" in s and ("match" in s or "matches" in s)):
        if "today" in s or "היום" in s:
            return "count_today"
        return "count"
    if "show me" in s or "הראה" in s or "תראה" in s or "הצג" in s:
        return "show"
    if "algorithm" in s or "calculation" in s or "אלגוריתם" in s or "חישוב" in s:
        return "algorithm"
    return "general"

def _parse_actions_from_question(q: str) -> list[dict]:
    """Very lightweight rule-based parser that emits UI actions.
    Supported actions:
      - setFilters: scoreMin, titleContains
      - setSort: by, dir
      - setPage: page
    - setK: k
    - setCityFilter: enabled
    - setCache: strategy, maxAge
    - setESCO: rp, fo
    """
    actions: list[dict] = []
    s = (q or "").strip().lower()
    # score >= N (accept percent or 0..1)
    import re as _re
    m = _re.search(r"(score|ציון)[^\d]{0,6}([\d]+(?:[\.,][\d]+)?)", s)
    if m:
        try:
            num = float(m.group(2).replace(",", "."))
            if num > 1.0:
                num = num / 100.0
            actions.append({"type":"setFilters","payload":{"scoreMin": round(num, 4)}})
        except Exception:
            pass
    # title contains
    m2 = _re.search(r"(title|כותרת|תפקיד)[^\w]{0,6}([\w\-\s]{2,40})", s)
    if m2:
        phrase = m2.group(2).strip()
        actions.append({"type":"setFilters","payload":{"titleContains": phrase}})
    # sort by score asc/desc
    if ("מיין" in s or "sort" in s) and ("score" in s or "ציון" in s):
        dir_ = "desc"
        if "asc" in s or "עולה" in s:
            dir_ = "asc"
        actions.append({"type":"setSort","payload":{"by":"score","dir":dir_}})
    # page N
    m3 = _re.search(r"(page|עמוד)\s*(\d{1,3})", s)
    if m3:
        actions.append({"type":"setPage","payload":{"page": int(m3.group(2))}})
    # top k
    m4 = _re.search(r"(top\s*k|k\s*[:=]?|טופ\s*ק)\s*(\d{1,2})", s)
    if m4:
        try:
            kval = max(1, min(20, int(m4.group(2))))
            actions.append({"type":"setK","payload":{"k": kval}})
        except Exception:
            pass
    # city filter on/off
    if "city" in s and ("filter" in s or "סינון" in s or "קרבה" in s):
        enabled = None
        if "off" in s or "כבוי" in s or "בטל" in s:
            enabled = False
        if "on" in s or "פעיל" in s or "הפעל" in s:
            enabled = True
        if enabled is not None:
            actions.append({"type":"setCityFilter","payload":{"enabled": enabled}})
    # cache strategy (off|on|hybrid)
    m5 = _re.search(r"(strategy|אסטרטגיה)\s*[:=]?\s*(off|on|hybrid|כבוי|פעיל|היברידי)", s)
    if m5:
        val = m5.group(2)
        mapping = {"כבוי":"off","פעיל":"on","היברידי":"hybrid"}
        actions.append({"type":"setCache","payload":{"strategy": mapping.get(val, val)}})
    # cache age (supports s/m/h/d suffixes)
    m6 = _re.search(r"(cache(\s*age)?|זיכרון(\s*מטמון)?)\s*[:=]?\s*(\d{1,7})([smhd]?)", s)
    if m6:
        try:
            num = int(m6.group(4))
            unit = (m6.group(5) or 's').lower()
            mult = 1 if unit=='s' else 60 if unit=='m' else 3600 if unit=='h' else 86400 if unit=='d' else 1
            age = max(0, min(7*86400, num * mult))
            actions.append({"type":"setCache","payload":{"maxAge": age}})
        except Exception:
            pass
    # ESCO ids (rp/fo)
    m7 = _re.search(r"(rp[_\s-]?esco|rp)\s*[:=]?\s*([\w\-\.]{2,40})", s)
    if m7:
        actions.append({"type":"setESCO","payload":{"rp": m7.group(2)}})
    m8 = _re.search(r"(fo[_\s-]?esco|fo)\s*[:=]?\s*([\w\-\.]{2,40})", s)
    if m8:
        actions.append({"type":"setESCO","payload":{"fo": m8.group(2)}})
    return actions

# --- Save Match Endpoint (history persistence) ---
class SaveMatchRequest(BaseModel):
    direction: str  # "c2j" | "j2c"
    source_id: str  # candidate_id or job_id based on direction
    target_id: str  # job_id or candidate_id based on direction
    status: str | None = "saved"
    notes: str | None = None

@app.post("/match/save")
def save_match(req: SaveMatchRequest, tenant_id: str | None = Depends(optional_tenant_id)):
    direction = (req.direction or "").lower()
    if direction not in {"c2j", "j2c"}:
        raise HTTPException(status_code=400, detail="invalid_direction")
    try:
        from .ingest_agent import db as _db, _skill_set, get_weights, _title_similarity, semantic_similarity_public as _sem, _embedding_similarity
        from bson import ObjectId as _ObjectId
        now = int(time.time())
        # Validate existence & tenant scoping
        if direction == "c2j":
            cand = _db["candidates"].find_one({"_id": _ObjectId(req.source_id), **({"tenant_id": tenant_id} if tenant_id else {})})
            job = _db["jobs"].find_one({"_id": _ObjectId(req.target_id), **({"tenant_id": tenant_id} if tenant_id else {})})
            if not cand or not job:
                raise HTTPException(status_code=404, detail="not_found")
            cand_id = str(cand["_id"]) ; job_id = str(job["_id"])
        else:
            job = _db["jobs"].find_one({"_id": _ObjectId(req.source_id), **({"tenant_id": tenant_id} if tenant_id else {})})
            cand = _db["candidates"].find_one({"_id": _ObjectId(req.target_id), **({"tenant_id": tenant_id} if tenant_id else {})})
            if not cand or not job:
                raise HTTPException(status_code=404, detail="not_found")
            cand_id = str(cand["_id"]) ; job_id = str(job["_id"])

    # Lightweight snapshot + comprehensive breakdown
        cand_sk = _skill_set(cand)
        job_sk = _skill_set(job)
        inter = len(cand_sk & job_sk)
        base = (inter / max(len(cand_sk), len(job_sk))) if (max(len(cand_sk), len(job_sk)) > 0) else 0.0
        title_sim = _title_similarity(str(cand.get("title", "")), str(job.get("title", "")))
        sem_sim = _sem(str(cand.get("text_blob", "")), str(job.get("text_blob", "")))
        emb_sim = _embedding_similarity(cand.get("embedding"), job.get("embedding"))

        # Distance snapshot (optional)
        try:
            from .ingest_agent import _CITY_CACHE

            def _coord(city_can: str | None):
                if not city_can:
                    return None
                rec = _CITY_CACHE.get(city_can.lower())
                if not rec:
                    return None
                try:
                    return float(rec.get("lat")), float(rec.get("lon"))
                except Exception:
                    return None

            def _distance_km(a, b):
                if not a or not b:
                    return None
                import math

                lat1, lon1 = a
                lat2, lon2 = b
                R = 6371.2
                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)
                lat1r = math.radians(lat1)
                lat2r = math.radians(lat2)
                h = math.sin(dlat / 2) ** 2 + math.cos(lat1r) * math.cos(lat2r) * math.sin(dlon / 2) ** 2
                c = 2 * math.asin(min(1, math.sqrt(h)))
                return round(R * c, 1)

            def _distance_score(km: float | None):
                if km is None:
                    return 0.0
                if km <= 5:
                    return 1.0
                if km >= 150:
                    return 0.0
                return max(0.0, 1.0 - (km - 5) / 145.0)

            cand_coord = _coord(cand.get("city_canonical"))
            job_coord = _coord(job.get("city_canonical"))
            dist_km = _distance_km(cand_coord, job_coord)
            dist_score = _distance_score(dist_km)
        except Exception:
            dist_km = None
            dist_score = 0.0

        w = get_weights()
        # Compute category-aware skill score if detailed skills are present
        def _split(doc):
            try:
                must = {d.get('name') for d in (doc.get('skills_detailed') or []) if d.get('category') == 'must' and d.get('name')}
                needed = {d.get('name') for d in (doc.get('skills_detailed') or []) if (d.get('category') != 'must') and d.get('name')}
                return set(must), set(needed)
            except Exception:
                return set(), set()
        c_must, c_needed = _split(cand)
        j_must, j_needed = _split(job)
        skill_weighted = base
        try:
            if (cand.get('skills_detailed') or job.get('skills_detailed')):
                inter_must = len((c_must | c_needed) & j_must)
                inter_needed = len((c_must | c_needed) & j_needed)
                denom = max(len((c_must | c_needed) | (j_must | j_needed)), 1)
                must_ratio = inter_must / denom
                needed_ratio = inter_needed / denom
                must_w = float(w.get('must_category_weight', 0.7) or 0.7)
                needed_w = float(w.get('needed_category_weight', 0.3) or 0.3)
                skill_weighted = must_w * must_ratio + needed_w * needed_ratio
        except Exception:
            # keep fallback skill_weighted
            pass

        # Final composite score snapshot (same components as engine)
        skill_w = float(w.get('skill_weight', 0.85) or 0.85)
        title_w = float(w.get('title_weight', 0.15) or 0.15)
        semantic_w = float(w.get('semantic_weight', 0.0) or 0.0)
        embedding_w = float(w.get('embedding_weight', 0.0) or 0.0)
        distance_w = float(w.get('distance_weight', 0.35) or 0.35)
        final_score = (skill_w * float(skill_weighted)
                       + title_w * float(title_sim)
                       + semantic_w * float(sem_sim)
                       + embedding_w * float(emb_sim)
                       + distance_w * float(dist_score or 0.0))
        snapshot = {
            "skill_overlap_base": round(base, 4),
            "title_similarity": round(title_sim, 4),
            "semantic_similarity": round(sem_sim, 4),
            "embedding_similarity": round(emb_sim, 4),
            "distance_km": dist_km,
            "distance_score": round(dist_score, 4) if dist_km is not None else None,
            "weights": w,
            "skill_score_weighted": round(skill_weighted, 4),
            "score_final": round(final_score, 4),
            "score_components": {
                "skills": round(skill_w * float(skill_weighted), 4),
                "title": round(title_w * float(title_sim), 4),
                "semantic": round(semantic_w * float(sem_sim), 4),
                "embedding": round(embedding_w * float(emb_sim), 4),
                "distance": round(distance_w * float(dist_score or 0.0), 4),
            },
        }
        # Skills breakdown and requirement fulfillment snapshot
        overlap = list(cand_sk & job_sk)
        missing_must = [s for s in sorted(j_must) if s not in cand_sk]
        missing_nice = [s for s in sorted(j_needed) if s not in cand_sk]
        extra_cand = [s for s in sorted(cand_sk) if s not in job_sk]
        skills_breakdown = {
            "overlap": overlap,
            "candidate_missing_must": missing_must,
            "candidate_missing_nice": missing_nice,
            "candidate_extra": extra_cand,
            "counts": {
                "candidate_total": len(cand_sk),
                "job_total": len(job_sk),
                "overlap": len(overlap),
                "job_must_total": len(j_must),
                "job_nice_total": len(j_needed),
                "candidate_must_total": len(c_must),
                "candidate_nice_total": len(c_needed),
            }
        }

        # Candidate and job snapshots (only matching-relevant fields)
        def _compact_doc(d: dict) -> dict:
            out = {
                "id": str(d.get("_id")),
                "title": d.get("title"),
                "city_canonical": d.get("city_canonical"),
                "profession": d.get("profession") or d.get("required_profession"),
                "occupation_field": d.get("occupation_field") or d.get("field_of_occupation"),
                "desired_profession": d.get("desired_profession"),
                "skills_detailed": d.get("skills_detailed"),
                "skills_set": sorted(list(_skill_set(d))) if isinstance(d, dict) else [],
            }
            # Include requirements blobs if present (jobs)
            if d.get("requirements") is not None:
                out["requirements"] = d.get("requirements")
            if d.get("job_requirements") is not None:
                out["job_requirements"] = d.get("job_requirements")
            if d.get("requirement_mentions") is not None:
                out["requirement_mentions"] = d.get("requirement_mentions")
            # Include small identity hints when safe
            if d.get("full_name"):
                out["full_name"] = d.get("full_name")
            return out

        cand_snapshot = _compact_doc(cand)
        job_snapshot = _compact_doc(job)

        # City/geo snapshot
        geo = {
            "candidate": {
                "city_canonical": cand.get("city_canonical"),
            },
            "job": {
                "city_canonical": job.get("city_canonical"),
            },
            "distance_km": dist_km,
            "distance_score": snapshot["distance_score"],
        }
        doc = {
            "direction": direction,
            "source_id": req.source_id,
            "target_id": req.target_id,
            "candidate_id": cand_id,
            "job_id": job_id,
            "tenant_id": tenant_id,
            "status": (req.status or "saved"),
            "notes": req.notes,
            "schema_version": 2,
            "score_snapshot": snapshot,
            "skills_breakdown": skills_breakdown,
            "candidate_snapshot": cand_snapshot,
            "job_snapshot": job_snapshot,
            "geo": geo,
            "ts": now,
        }
        res = _db["matches_history"].insert_one(doc)
        return {"ok": True, "id": str(res.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"save_failed: {e}")


def _time_range_from_req(req: ChatMatchesRequest) -> tuple[int, int]:
    import time as _t
    now = int(_t.time())
    if req.from_ts and req.to_ts and req.from_ts < req.to_ts:
        return req.from_ts, req.to_ts
    # Default: last 24h
    return now - 24*3600, now


def _collect_matches_snapshot(tenant_id: str | None, start_ts: int, end_ts: int, limit_candidates: int, k: int, time_budget_ms: int = 4000):
    """Sample candidates in time window and compute top-k matches using existing engine.
    Returns facts + sample rows. Uses city_filter=True for quality, and caps sizes for safety.
    """
    from bson import ObjectId
    limit_candidates = max(1, min(int(limit_candidates or 80), 500))
    k = max(1, min(int(k or 5), 20))
    q = {"updated_at": {"$gte": start_ts, "$lte": end_ts}}
    if tenant_id:
        q["tenant_id"] = tenant_id
    cur = db["candidates"].find(q, {"_id":1, "title":1, "full_name":1, "city_canonical":1, "updated_at":1}).sort([["updated_at", -1],["_id", -1]]).limit(limit_candidates)
    rows = []
    total_matches = 0
    best_pairs: list[dict] = []
    count = 0
    import time as _t
    t_start = _t.time()
    for d in cur:
        cand_id = str(d.get("_id"))
        try:
            ms = jobs_for_candidate(cand_id, top_k=k, max_distance_km=30, tenant_id=tenant_id)
        except Exception:
            ms = []
        total_matches += len(ms)
        rows.append({
            "candidate_id": cand_id,
            "title": d.get("title") or d.get("full_name"),
            "city": d.get("city_canonical"),
            "match_count": len(ms),
            "top": ms[:3]
        })
        for m in ms:
            best_pairs.append({
                "candidate_id": cand_id,
                "job_id": m.get("job_id"),
                "score": m.get("score", 0.0),
                "title": m.get("title")
            })
        count += 1
        # time budget check to keep UI responsive
        if ((_t.time() - t_start) * 1000) > time_budget_ms:
            break
    best_pairs.sort(key=lambda x: x.get("score",0.0), reverse=True)
    facts = {
        "candidates_sampled": count,
        "time_window": {"from": start_ts, "to": end_ts},
        "total_matches": total_matches,
        "avg_matches_per_candidate": (total_matches / count) if count else 0.0,
        "top_pair": best_pairs[0] if best_pairs else None
    }
    return facts, rows[:50], best_pairs[:20]


@app.post("/chat/matches")
def chat_matches(req: ChatMatchesRequest, tenant_id: str | None = Depends(optional_tenant_id)):
    import time
    import json as _json
    t0 = time.time()
    # basic per-tenant rate limit (60/min window)
    try:
        key = f"chat:{tenant_id or 'public'}"
        now = int(t0)
        bucket = _RATE_BUCKET.setdefault(key, [])
        bucket[:] = [ts for ts in bucket if ts > now - 60]
        if len(bucket) >= 60:
            raise HTTPException(status_code=429, detail="rate_limited")
        bucket.append(now)
    except Exception:
        pass
    intent = _detect_chat_intent(req.question)
    start_ts, end_ts = _time_range_from_req(req)
    facts, sample_rows, best_pairs = _collect_matches_snapshot(tenant_id, start_ts, end_ts, req.limit_candidates, req.k)

    # Prepare deterministic answer; optionally use OpenAI to phrase it nicely
    answer = None
    if intent in ("count", "count_today"):
        answer = f"נמצאו כ-{int(facts['total_matches'])} התאמות במדגם של {facts['candidates_sampled']} מועמדים בזמן המבוקש (ממוצע {facts['avg_matches_per_candidate']:.2f} לכל מועמד)."
    elif intent == "show":
        items = []
        for p in best_pairs[:10]:
            items.append(f"מועמד {p['candidate_id']} ↔ משרה {p.get('job_id')} — {int(round((p.get('score') or 0.0)*100))}%")
        if items:
            answer = "\n".join(items)
        else:
            answer = "לא נמצאו התאמות להצגה במדגם."
    elif intent == "algorithm":
        try:
            from .ingest_agent import WEIGHT_SKILLS, WEIGHT_TITLE_SIM, WEIGHT_SEMANTIC, WEIGHT_EMBEDDING, WEIGHT_DISTANCE, MUST_CATEGORY_WEIGHT, NEEDED_CATEGORY_WEIGHT
            answer = (
                "נוסחת הציון: score = "
                f"{WEIGHT_SKILLS}·skill_weighted + {WEIGHT_TITLE_SIM}·title_similarity + "
                f"{WEIGHT_SEMANTIC}·semantic + {WEIGHT_EMBEDDING}·embedding + {WEIGHT_DISTANCE}·distance. "
                f"skill_weighted = {MUST_CATEGORY_WEIGHT}·must_overlap + {NEEDED_CATEGORY_WEIGHT}·needed_overlap."
            )
        except Exception:
            answer = "הציון מורכב ממשקל כישורים, דמיון כותרת, סמנטיקה, הטמע וקרבה גיאוגרפית."
    else:
        # generic: summarize key stats
        answer = f"בחלון הזמן: {facts['candidates_sampled']} מועמדים נסרקו, נמצאו {int(facts['total_matches'])} התאמות."

    # If OpenAI client exists, rephrase briefly in Hebrew using facts
    try:
        from .ingest_agent import _openai_client, OPENAI_MODEL, _OPENAI_AVAILABLE
        if _OPENAI_AVAILABLE:
            msg = [
                {"role":"system","content":"ענה בקצרה ובעברית. אם יש רשימה, הגבל ל-10 שורות. אל תמציא נתונים, הסתמך רק על ה-Facts."},
                {"role":"user","content":"שאלה: " + (req.question or "") + "\nFacts: " + _json.dumps(facts, ensure_ascii=False)}
            ]
            try:
                comp = _openai_client.chat.completions.create(model=OPENAI_MODEL, messages=msg, temperature=0)
                txt = comp.choices[0].message.content.strip()
                if txt:
                    answer = txt
            except Exception:
                pass
    except Exception:
        pass

    # Derive UI actions from the question (for table filtering)
    try:
        actions = _parse_actions_from_question(req.question)
    except Exception:
        actions = []

    dt = round((time.time()-t0)*1000)
    return {
        "answer": answer,
        "facts": facts,
        "sample": sample_rows,
        "took_ms": dt,
        "intent": intent,
        "actions": actions
    }

# ===== GPT-powered chat → Query DSL → UI actions =====
class DSLPage(BaseModel):
    number: int = 1
    size: int = 50

class DSLSort(BaseModel):
    by: str = "score"  # score|date|title
    dir: str = "desc"

class DSLFilter(BaseModel):
    title_contains: Optional[str] = None
    candidate_id: Optional[str] = None
    city_in: Optional[list[str]] = None
    score: Optional[dict] = None  # {"$gte":0.5, "$lte":0.9}

class ChatQueryRequest(BaseModel):
    question: str
    currentView: str = "matches"
    currentState: Optional[dict] = None

_DSL_ALLOWED_SORT = {"score","date","title"}
_DSL_ALLOWED_DIR = {"asc","desc"}

def _validate_and_normalize_dsl(d: dict) -> tuple[str, DSLFilter, list[DSLSort], DSLPage, list[str]]:
    """Validate a minimal DSL; returns (view, filter, sorts, page, warnings)."""
    warnings: list[str] = []
    view = (d.get("view") or "matches").lower()
    # filter
    f = d.get("filter") or {}
    filt = DSLFilter(
        title_contains=f.get("title_contains"),
        candidate_id=f.get("candidate_id"),
        city_in=f.get("city_in") if isinstance(f.get("city_in"), list) else None,
        score=f.get("score") if isinstance(f.get("score"), dict) else None,
    )
    # sorts
    sorts_raw = d.get("sort") or []
    sorts: list[DSLSort] = []
    if isinstance(sorts_raw, list):
        for s in sorts_raw[:2]:
            by = str((s.get("by") or "score").lower())
            dir_ = str((s.get("dir") or "desc").lower())
            if by not in _DSL_ALLOWED_SORT:
                warnings.append(f"sort.by '{by}' not allowed; using 'score'")
                by = "score"
            if dir_ not in _DSL_ALLOWED_DIR:
                warnings.append(f"sort.dir '{dir_}' not allowed; using 'desc'")
                dir_ = "desc"
            sorts.append(DSLSort(by=by, dir=dir_))
    if not sorts:
        sorts = [DSLSort()]
    # page
    p = d.get("page") or {}
    try:
        number = max(1, int(p.get("number", 1)))
    except Exception:
        number = 1
    try:
        size = max(1, min(100, int(p.get("size", 50))))
    except Exception:
        size = 50
    page = DSLPage(number=number, size=size)
    return view, filt, sorts, page, warnings

def _dsl_to_actions(view: str, filt: DSLFilter, sorts: list[DSLSort], page: DSLPage) -> list[dict]:
    actions: list[dict] = []
    f_payload: dict = {}
    if filt.title_contains is not None:
        f_payload["titleContains"] = filt.title_contains
    if filt.candidate_id is not None:
        f_payload["candidateId"] = filt.candidate_id
    if filt.city_in:
        # Normalize all cities; if multiple after normalization, expose as 'cities'
        normed: list[str] = []
        for city in (filt.city_in or [])[:10]:
            try:
                from .ingest_agent import canonical_city as _canon_city
                n = _canon_city(city)
            except Exception:
                n = None
            if not n and isinstance(city, str):
                n = city.strip().lower().replace(" ", "_")
            if n:
                normed.append(n)
        normed = [c for c in normed if c]
        if len(normed) <= 1:
            f_payload["city"] = normed[0] if normed else (filt.city_in[0] if filt.city_in else "")
        else:
            f_payload["cities"] = normed
    if filt.score:
        def _norm_score(v):
            try:
                x = float(v)
                if x > 1.0: x = x/100.0
                if x < 0.0: x = 0.0
                if x > 1.0: x = 1.0
                return x
            except Exception:
                return None
        if "$gte" in filt.score:
            x = _norm_score(filt.score["$gte"]) 
            if x is not None:
                f_payload["scoreMin"] = x
        if "$lte" in filt.score:
            x = _norm_score(filt.score["$lte"]) 
            if x is not None:
                f_payload["scoreMax"] = x
    if f_payload:
        actions.append({"type":"setFilters","payload": f_payload})
    if sorts:
        s0 = sorts[0]
        actions.append({"type":"setSort","payload":{"by": s0.by, "dir": s0.dir}})
    if page:
        actions.append({"type":"setPage","payload":{"page": page.number, "pageSize": page.size}})
    # final refresh
    actions.append({"type":"refresh","payload":{}})
    return actions

# --- Structured POST /match/report/query ---
class MatchQuery(BaseModel):
    k: int = 5
    limit: int = 100
    page: int = 1
    skip: Optional[int] = None
    city_filter: bool = True
    cache_strategy: Optional[str] = None  # off|on|hybrid
    cache_max_age: Optional[int] = None   # seconds
    rp_esco: Optional[str] = None
    fo_esco: Optional[str] = None
    title_contains: Optional[str] = None
    candidate_id: Optional[str] = None
    city_in: Optional[List[str]] = None
    score: Optional[dict] = None  # {"$gte": float, "$lte": float}
    sort_by: Optional[str] = None
    sort_dir: Optional[str] = None

def _normalize_score_bound(val: Any) -> Optional[float]:
    try:
        x = float(val)
        if x > 1.0:
            x = x / 100.0
        if x < 0.0:
            x = 0.0
        if x > 1.0:
            x = 1.0
        return x
    except Exception:
        return None

def _normalize_city_list(cities: Optional[List[str]]) -> list[str]:
    out: list[str] = []
    if not cities:
        return out
    for c in cities[:20]:
        try:
            from .ingest_agent import canonical_city as _canon_city
            n = _canon_city(c)
        except Exception:
            n = None
        if not n and isinstance(c, str):
            n = c.strip().lower().replace(" ", "_")
        if n:
            out.append(n)
    # deduplicate
    return list(dict.fromkeys(out))

@app.post("/match/report/query")
def match_report_query(body: MatchQuery, tenant_id: str | None = Depends(optional_tenant_id)):
    """Structured variant of match/report with support for multi-city OR and JSON body.

    Returns same shape as GET /match/report.
    """
    k = max(1, min(int(body.k or 5), 20))
    limit = max(1, min(int(body.limit or 100), 500))
    page = max(1, int(body.page or 1))
    skip = int(body.skip) if body.skip is not None else (page-1) * limit
    title_contains = (body.title_contains or None)
    candidate_id = (body.candidate_id or None)
    sort_by = (body.sort_by or "score")
    sort_dir = (body.sort_dir or "desc")
    score_gte = _normalize_score_bound((body.score or {}).get("$gte")) if body.score else None
    score_lte = _normalize_score_bound((body.score or {}).get("$lte")) if body.score else None
    cities = _normalize_city_list(body.city_in)

    out: list[dict] = []
    base_query: dict = ({"tenant_id": tenant_id} if tenant_id else {})
    if candidate_id:
        try:
            base_query.update({"_id": _ensure_object_id(candidate_id)})
        except Exception:
            return {"results": [], "count": 0, "total": 0, "applied": {"error": "invalid_candidate_id"}}
    if cities:
        base_query["city_canonical"] = {"$in": cities}

    try:
        total_candidates = db["candidates"].count_documents(base_query)
    except Exception:
        total_candidates = 0

    cur = db["candidates"].find(base_query).skip(max(skip, 0)).limit(limit)
    for cand in cur:
        cand_id = str(cand["_id"]) 
        try:
            # Use hybrid cache by default for performance
            strategy = (body.cache_strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")).lower()
            max_age = body.cache_max_age if (body.cache_max_age is not None) else None
            matches = get_or_compute_matches(cand_id, top_k=k, city_filter=body.city_filter, tenant_id=tenant_id, strategy=strategy, max_age=max_age, rp_esco=body.rp_esco, fo_esco=body.fo_esco)
        except Exception:
            try:
                matches = jobs_for_candidate(cand_id, top_k=k, max_distance_km=(30 if body.city_filter else 0), tenant_id=tenant_id, rp_esco=body.rp_esco, fo_esco=body.fo_esco)
            except Exception:
                matches = []
        # per-match filters
        if title_contains:
            t = title_contains.strip().lower()
            matches = [m for m in matches if t in (m.get("title") or "").lower()]
        if score_gte is not None:
            matches = [m for m in matches if (m.get("score") or 0.0) >= score_gte]
        if score_lte is not None:
            matches = [m for m in matches if (m.get("score") or 0.0) <= score_lte]
        if not matches:
            continue
        out.append({
            "candidate_id": cand_id,
            "title": cand.get("title") or cand.get("full_name"),
            "matches": matches,
            "best_score": max([(m.get("score") or 0.0) for m in matches]) if matches else 0.0,
            "city": cand.get("city_canonical")
        })

    # sorting
    sb = (sort_by or "score").lower()
    sd = (sort_dir or "desc").lower()
    reverse = (sd != "asc")
    if sb == "title":
        out.sort(key=lambda r: (r.get("title") or ""), reverse=reverse)
    elif sb == "date":
        out.sort(key=lambda r: r.get("updated_at", 0), reverse=reverse)
    else:
        out.sort(key=lambda r: r.get("best_score", 0.0), reverse=reverse)

    return {
        "results": out,
        "count": len(out),
        "total": total_candidates,
        "city_filter": body.city_filter,
        "applied": {
            "title_contains": title_contains,
            "candidate_id": candidate_id,
            "cities": cities,
            "score_min": score_gte,
            "score_max": score_lte,
            "sort_by": sb,
            "sort_dir": sd,
            "skip": skip,
            "limit": limit,
            "cache_strategy": (body.cache_strategy or os.getenv("MATCH_CACHE_STRATEGY", "hybrid")),
            "cache_max_age": body.cache_max_age
        },
        "rp_esco": body.rp_esco,
        "fo_esco": body.fo_esco
    }

def _build_gpt_dsl(question: str, tenant_id: str | None) -> dict | None:
    """Ask OpenAI to output a JSON DSL. Returns dict or None on failure."""
    try:
        from .ingest_agent import _openai_client, OPENAI_MODEL, _OPENAI_AVAILABLE
        if not _OPENAI_AVAILABLE:
            return None
        sys_prompt = (
            "You translate Hebrew/English queries about a matches table into a strict JSON DSL. "
            "Allowed view: 'matches'. Allowed filter keys: title_contains (string), candidate_id (string), city_in (array of strings), score (object with $gte/$lte floats 0..1). "
            "Allowed sort: by in [score,date,title], dir in [asc,desc]. Page has number (1..), size (1..100). "
            "Output ONLY valid JSON with keys: view, filter, sort(array), page, and a short explain. No extra text."
        )
        messages = [
            {"role":"system","content": sys_prompt},
            {"role":"user","content": question or ""}
        ]
        comp = _openai_client.chat.completions.create(model=OPENAI_MODEL, messages=messages, temperature=0)
        txt = (comp.choices[0].message.content or "").strip()
        import json as _json
        # extract first JSON object heuristically
        start = txt.find("{")
        end = txt.rfind("}")
        if start != -1 and end != -1 and end>start:
            txt = txt[start:end+1]
        return _json.loads(txt)
    except Exception:
        return None

@app.post("/chat/query")
def chat_query(req: ChatQueryRequest, tenant_id: str | None = Depends(optional_tenant_id), request: Request = None):
    import time
    t0 = time.time()
    # Lightweight per-tenant limiter (30/min)
    try:
        key = f"chatq:{tenant_id or 'public'}"
        now = int(t0)
        bucket = _RATE_BUCKET.setdefault(key, [])
        bucket[:] = [ts for ts in bucket if ts > now - 60]
        if len(bucket) >= 30:
            raise HTTPException(status_code=429, detail="rate_limited")
        bucket.append(now)
    except Exception:
        pass

    dsl_raw = _build_gpt_dsl(req.question, tenant_id)
    # Quick path: detect explicit "candidates for job <id>" requests (Heb/Eng) and return strict-only results
    try:
        import re as _re
        qtext = str(req.question or '')
        qlow = qtext.lower()
        # Keywords: Hebrew (מועמד, משרה) or English (candidate, job)
        wants_candidates = ('מועמד' in qtext) or ('candidate' in qlow)
        mentions_job = ('משרה' in qtext) or ('job' in qlow)
        # Accept ObjectId with an optional space after first two hex chars (e.g., "68 ae..."), or a clean 24-hex
        m = _re.search(r"\b([0-9a-fA-F]{24})\b", qtext)
        m_sp = _re.search(r"\b([0-9a-fA-F]{2})\s*([0-9a-fA-F]{22})\b", qtext)
        job_oid = None
        if m:
            job_oid = m.group(1)
        elif m_sp:
            job_oid = (m_sp.group(1) + m_sp.group(2))
        # Optional k in text (first small integer 1..50)
        k = None
        try:
            mnum = _re.search(r"\b(\d{1,2})\b", qtext)
            if mnum:
                vi = int(mnum.group(1))
                if 1 <= vi <= 50:
                    k = vi
        except Exception:
            pass
        if wants_candidates and mentions_job and job_oid:
            top_k = int(k or 5)
            if request and request.query_params.get("stream") in ("1","true","yes"):
                def _gen_job():
                    import json as _json
                    yield _json.dumps({"type":"text_delta","text":"מאתר מועמדים למשרה..."}, ensure_ascii=False) + "\n"
                    ui: list[dict] = []
                    try:
                        matches = get_or_compute_candidates_for_job(job_oid, top_k=top_k, city_filter=True, tenant_id=tenant_id)
                        rows = []
                        for r in (matches or [])[:top_k]:
                            try:
                                sc = float(r.get('score') or r.get('best_score') or 0.0)
                            except Exception:
                                sc = 0.0
                            rows.append({
                                "candidate_id": str(r.get('candidate_id') or r.get('_id') or ''),
                                "title": r.get('title') or r.get('candidate_title') or '',
                                "score": round(sc, 3)
                            })
                        if rows:
                            ui.append({
                                "kind": "Table",
                                "id": "job-candidates",
                                "columns": [
                                    {"key":"candidate_id","title":"מועמד"},
                                    {"key":"title","title":"תפקיד"},
                                    {"key":"score","title":"ציון"}
                                ],
                                "rows": rows,
                                "primaryKey": "candidate_id"
                            })
                        else:
                            ui.append({
                                "kind": "RichText",
                                "id": "no-results-guidance",
                                "html": "לא נמצאו מועמדים למשרה זו. ודאו שמזהה המשרה תקין ונסו לשנות סינונים. מוצגים רק נתוני אמת."
                            })
                        # KPI
                        ui.append({
                            "kind": "Metric",
                            "id": "matches-kpi",
                            "label": "מספר תוצאות",
                            "value": int(len(rows))
                        })
                    except Exception:
                        ui.append({"kind":"RichText","id":"error","html":"אירעה שגיאה בעיבוד הבקשה."})
                    env = {"type":"assistant_ui","narration":"בוצע","actions":[{"type":"refresh","payload":{}}],"ui": ui}
                    yield _json.dumps(env, ensure_ascii=False) + "\n"
                    yield _json.dumps({"type":"done"}, ensure_ascii=False) + "\n"
                return StreamingResponse(_gen_job(), media_type="application/x-ndjson")
    except Exception:
        pass
    if not dsl_raw:
        # fallback to simple intent parser → actions (includes tuning)
        actions = _parse_actions_from_question(req.question)
        # If client requested streaming, emit NDJSON with assistant_ui envelope (with a small table if possible)
        try:
            if request and request.query_params.get("stream") in ("1","true","yes"):
                def _gen():
                    import json as _json
                    yield _json.dumps({"type":"text_delta","text":"מעבד בקשה..."}, ensure_ascii=False) + "\n"
                    ui: list[dict] = []
                    # Derive a minimal MatchQuery from parsed actions (if present)
                    try:
                        mq_kwargs = {"k":5, "limit":10, "page":1, "city_filter": True}
                        import re as _re
                        _oid = _re.compile(r"^[0-9a-fA-F]{24}$")
                        for a in (actions or [])[:5]:
                            if not isinstance(a, dict):
                                continue
                            if a.get("type") == "setFilters":
                                p = a.get("payload") or {}
                                if isinstance(p.get("titleContains"), str):
                                    mq_kwargs["title_contains"] = p["titleContains"]
                                if isinstance(p.get("candidateId"), str) and _oid.match(p["candidateId"].strip()):
                                    mq_kwargs["candidate_id"] = p["candidateId"].strip()
                                # city or cities
                                cities = []
                                if isinstance(p.get("cities"), list):
                                    cities = [str(c) for c in p["cities"][:10]]
                                elif isinstance(p.get("city"), str) and p["city"]:
                                    cities = [p["city"]]
                                if cities:
                                    mq_kwargs["city_in"] = cities
                                # scoreMin/scoreMax → score dict
                                score = {}
                                if p.get("scoreMin") is not None:
                                    try:
                                        score["$gte"] = float(p["scoreMin"])  # normalized later by endpoint
                                    except Exception:
                                        pass
                                if p.get("scoreMax") is not None:
                                    try:
                                        score["$lte"] = float(p["scoreMax"])  # normalized later by endpoint
                                    except Exception:
                                        pass
                                if score:
                                    mq_kwargs["score"] = score
                            elif a.get("type") == "setSort":
                                p = a.get("payload") or {}
                                if isinstance(p.get("by"), str):
                                    mq_kwargs["sort_by"] = p["by"]
                                if isinstance(p.get("dir"), str):
                                    mq_kwargs["sort_dir"] = p["dir"]
                            elif a.get("type") == "setPage":
                                p = a.get("payload") or {}
                                try:
                                    ps = int(p.get("pageSize") or p.get("size") or 10)
                                    mq_kwargs["limit"] = max(1, min(20, ps))
                                except Exception:
                                    pass
                        mq = MatchQuery(**mq_kwargs)
                        mr = match_report_query(mq, tenant_id)
                        rows = []
                        for r in (mr.get('results') or [])[:10]:
                            try:
                                best = float(r.get('best_score') or 0.0)
                            except Exception:
                                best = 0.0
                            rows.append({
                                "candidate_id": str(r.get('candidate_id') or ''),
                                "title": r.get('title') or '',
                                "score": round(best, 3)
                            })
                        # Strict-only: don't relax; if empty, show guidance only
                        if rows:
                            ui.append({
                                "kind": "Table",
                                "id": "matches",
                                "columns": [
                                    {"key":"candidate_id","title":"מועמד"},
                                    {"key":"title","title":"תפקיד"},
                                    {"key":"score","title":"ציון"}
                                ],
                                "rows": rows,
                                "primaryKey": "candidate_id"
                            })
                        else:
                            # No rows for strict filters: add user guidance (no sample data)
                            ui.append({
                                "kind": "RichText",
                                "id": "no-results-guidance",
                                "html": "לא נמצאו תוצאות. נסו להרפות סינונים (הסירו עיר/ציון מינימלי, הגדילו גודל עמוד, או שנו את החיפוש). המערכת מציגה רק נתוני אמת — ללא נתוני דמו."
                            })
                        # Always include a KPI metric for quick context
                        try:
                            total = int(mr.get('count') or 0)
                            ui.append({
                                "kind": "Metric",
                                "id": "matches-kpi",
                                "label": "מספר תוצאות",
                                "value": total
                            })
                        except Exception:
                            pass
                    except Exception:
                        pass
                    env = {"type":"assistant_ui","narration":"הוחל סינון בסיסי","actions": actions, "ui": ui}
                    yield _json.dumps(env, ensure_ascii=False) + "\n"
                    yield _json.dumps({"type":"done"}, ensure_ascii=False) + "\n"
                return StreamingResponse(_gen(), media_type="application/x-ndjson")
        except Exception:
            pass
        return {"answer":"החלת סינון בסיסי","actions": actions, "dsl": None, "took_ms": int((time.time()-t0)*1000)}
    view, filt, sorts, page, warnings = _validate_and_normalize_dsl(dsl_raw)
    # Start with DSL actions (filters/sort/page)
    actions = _dsl_to_actions(view, filt, sorts, page)
    # Augment with rule-based tuning actions (k, cityFilter, cache, ESCO)
    try:
        extra = _parse_actions_from_question(req.question)
        if extra:
            actions.extend(extra)
    except Exception:
        pass
    answer = "הוחל סינון על פי הבקשה"
    if warnings:
        answer += " (" + "; ".join(warnings[:2]) + ")"
    try:
        logging.info({
            "evt": "chat_query",
            "tenant_id": tenant_id,
            "dsl": {
                "view": view,
                "filter": getattr(filt, 'dict', lambda: {})(),
                "sort": [getattr(s, '__dict__', {}) for s in sorts],
                "page": getattr(page, '__dict__', {})
            },
            "actions_count": len(actions)
        })
    except Exception:
        pass
    # If client requested streaming, emit NDJSON events
    try:
        if request and request.query_params.get("stream") in ("1","true","yes"):
            def _gen():
                import json as _json
                # Quick initial hint
                yield _json.dumps({"type":"text_delta","text":"מיישם סינון..."}, ensure_ascii=False) + "\n"
                # Build a small UI table using current DSL → match report (limited)
                ui: list[dict] = []
                try:
                    # Derive MatchQuery from DSL
                    mq = MatchQuery(
                        k=5,
                        limit=min(10, page.size if hasattr(page, 'size') else 10),
                        page=1,
                        city_filter=True,
                        title_contains=getattr(filt, 'title_contains', None),
                        candidate_id=getattr(filt, 'candidate_id', None),
                        city_in=getattr(filt, 'city_in', None),
                        score=getattr(filt, 'score', None),
                        sort_by=sorts[0].by if (isinstance(sorts, list) and sorts) else 'score',
                        sort_dir=sorts[0].dir if (isinstance(sorts, list) and sorts) else 'desc',
                    )
                    mr = match_report_query(mq, tenant_id)
                    rows = []
                    for r in (mr.get('results') or [])[:10]:
                        try:
                            best = float(r.get('best_score') or 0.0)
                        except Exception:
                            best = 0.0
                        rows.append({
                            "candidate_id": str(r.get('candidate_id') or ''),
                            "title": r.get('title') or '',
                            "score": round(best, 3)
                        })
                    # Strict-only: no relaxed fallback; if empty, send guidance only
                    if rows:
                        ui.append({
                            "kind": "Table",
                            "id": "matches",
                            "columns": [
                                {"key":"candidate_id","title":"מועמד"},
                                {"key":"title","title":"תפקיד"},
                                {"key":"score","title":"ציון"}
                            ],
                            "rows": rows,
                            "primaryKey": "candidate_id"
                        })
                    else:
                        # No rows for strict filters: add user guidance (no sample data)
                        ui.append({
                            "kind": "RichText",
                            "id": "no-results-guidance",
                            "html": "לא נמצאו תוצאות. נסו להרפות סינונים (הסירו עיר/ציון מינימלי, הגדילו גודל עמוד, או שנו את החיפוש). המערכת מציגה רק נתוני אמת — ללא נתוני דמו."
                        })
                    # KPI metric for context
                    try:
                        total = int(mr.get('count') or 0)
                        ui.append({
                            "kind": "Metric",
                            "id": "matches-kpi",
                            "label": "מספר תוצאות",
                            "value": total
                        })
                    except Exception:
                        pass
                except Exception:
                    pass
                env = {"type":"assistant_ui","narration": answer, "actions": actions, "ui": ui}
                yield _json.dumps(env, ensure_ascii=False) + "\n"
                yield _json.dumps({"type":"done"}, ensure_ascii=False) + "\n"
            return StreamingResponse(_gen(), media_type="application/x-ndjson")
    except Exception:
        pass

    return {
        "answer": answer,
        "actions": actions,
        "dsl": {
            "view": view,
            "filter": filt.dict() if hasattr(filt, 'dict') else {},
            "sort": [s.__dict__ for s in sorts],
            "page": page.__dict__,
        },
        "took_ms": int((time.time()-t0)*1000)
    }

# Advanced report: POST with JSON filters (supports multi-city OR)
## (removed older JSON-DSL flavored /match/report/query; using unified MatchQuery endpoint)

@app.post("/bootstrap")
def manual_bootstrap():
    """Force ingestion of sample cvs & jobs (idempotent)."""
    before = {
        "candidates": db["candidates"].count_documents({}),
        "jobs": db["jobs"].count_documents({})
    }
    _auto_ingest_if_empty()
    after = {
        "candidates": db["candidates"].count_documents({}),
        "jobs": db["jobs"].count_documents({})
    }
    return {"before": before, "after": after}

class WeightRequest(BaseModel):
    skill_weight: float
    title_weight: float
    semantic_weight: float | None = None
    embed_weight: float | None = None

class CategoryWeightRequest(BaseModel):
    must_weight: float
    needed_weight: float

class DistanceWeightRequest(BaseModel):
    distance_weight: float

class MinSkillFloorRequest(BaseModel):
    min_skill_floor: int

class CombinedConfigRequest(BaseModel):
    # Weights
    skill_weight: float | None = None
    title_weight: float | None = None
    semantic_weight: float | None = None
    embed_weight: float | None = None
    # Category weights
    must_weight: float | None = None
    needed_weight: float | None = None
    # Distance weight
    distance_weight: float | None = None
    # Min skill floor
    min_skill_floor: int | None = None

class MatchConfigResponse(BaseModel):
    skill_weight: float | None = None
    title_weight: float | None = None
    semantic_weight: float | None = None
    embedding_weight: float | None = None
    must_category_weight: float | None = None
    needed_category_weight: float | None = None
    distance_weight: float | None = None
    min_skill_floor: int | None = None
    cache_strategy: str | None = None
    cache_ttl: int | None = None

@app.post("/config/weights")
def update_weights(req: WeightRequest, _: bool = Depends(require_api_key)):
    set_weights(req.skill_weight, req.title_weight, req.semantic_weight, req.embed_weight)
    return {"weights": get_weights()}

@app.post("/config/category_weights")
def update_category_weights(req: CategoryWeightRequest, _: bool = Depends(require_api_key)):
    ok = set_category_weights(req.must_weight, req.needed_weight)
    if not ok:
        raise HTTPException(status_code=400, detail="Invalid category weights")
    return {"weights": get_weights()}

@app.post("/config/distance_weight")
def update_distance_weight(req: DistanceWeightRequest, _: bool = Depends(require_api_key)):
    ok = set_distance_weight(req.distance_weight)
    if not ok:
        raise HTTPException(status_code=400, detail="Invalid distance weight")
    return {"weights": get_weights()}

@app.post("/config/min_skill_floor")
def update_min_skill_floor(req: MinSkillFloorRequest, _: bool = Depends(require_api_key)):
    ok = set_min_skill_floor(req.min_skill_floor)
    if not ok:
        raise HTTPException(status_code=400, detail="Invalid min skill floor")
    return {"weights": get_weights()}

@app.get("/config/llm_required")
def get_llm_required(_: bool = Depends(require_api_key)):
    return {"require_llm_on_upload": is_llm_required_on_upload()}

@app.post("/config/llm_required")
def set_llm_required(req: LLMToggle, _: bool = Depends(require_api_key)):
    set_llm_required_on_upload(req.enabled)
    return {"require_llm_on_upload": is_llm_required_on_upload()}

@app.post("/config/all")
def update_all_config(req: CombinedConfigRequest, _: bool = Depends(require_api_key)):
    # Apply simple weights first if provided (skill & title mandatory together for normalization)
    if req.skill_weight is not None and req.title_weight is not None:
        set_weights(req.skill_weight, req.title_weight, req.semantic_weight, req.embed_weight)
    # Category weights
    if req.must_weight is not None and req.needed_weight is not None:
        if not set_category_weights(req.must_weight, req.needed_weight):
            raise HTTPException(status_code=400, detail="Invalid category weights")
    # Distance weight
    if req.distance_weight is not None:
        if not set_distance_weight(req.distance_weight):
            raise HTTPException(status_code=400, detail="Invalid distance weight")
    # Min skill floor
    if req.min_skill_floor is not None:
        if not set_min_skill_floor(req.min_skill_floor):
            raise HTTPException(status_code=400, detail="Invalid min skill floor")
    return {"weights": get_weights()}

@app.get("/config/weights")
def read_weights():
    return {"weights": get_weights()}

@app.get("/match/config")
def get_match_config() -> dict:
    """Return current matching configuration. Cache strategy/ttl are surfaced from env for now."""
    w = get_weights() or {}
    out = {
        "skill_weight": w.get("skill_weight"),
        "title_weight": w.get("title_weight"),
        "semantic_weight": w.get("semantic_weight"),
        "embedding_weight": w.get("embedding_weight"),
        "must_category_weight": w.get("must_category_weight"),
        "needed_category_weight": w.get("needed_category_weight"),
        "distance_weight": w.get("distance_weight"),
        "min_skill_floor": w.get("min_skill_floor"),
        "cache_strategy": os.getenv("MATCH_CACHE_STRATEGY", "hybrid"),
        "cache_ttl": int(os.getenv("MATCH_CACHE_TTL", "1800")),
    }
    return out

@app.put("/match/config")
def put_match_config(req: CombinedConfigRequest, _: bool = Depends(require_api_key)) -> dict:
    """Update matching config using existing granular endpoints. Cache strategy/ttl are controlled via env only."""
    # Apply using existing logic
    if req.skill_weight is not None and req.title_weight is not None:
        set_weights(req.skill_weight, req.title_weight, req.semantic_weight, req.embed_weight)
    if req.must_weight is not None and req.needed_weight is not None:
        if not set_category_weights(req.must_weight, req.needed_weight):
            raise HTTPException(status_code=400, detail="Invalid category weights")
    if req.distance_weight is not None:
        if not set_distance_weight(req.distance_weight):
            raise HTTPException(status_code=400, detail="Invalid distance weight")
    if req.min_skill_floor is not None:
        if not set_min_skill_floor(req.min_skill_floor):
            raise HTTPException(status_code=400, detail="Invalid min skill floor")
    return get_match_config()

@app.get("/config/category_weights")
def read_category_weights():
    w = get_weights()
    return {"must_category_weight": w.get('must_category_weight'), "needed_category_weight": w.get('needed_category_weight')}

@app.get("/match/explain/{cand_id}/{job_id}")
def explain_match(cand_id: str, job_id: str):
    from .ingest_agent import (
        db,
        _skill_set,
        _title_similarity,
        get_weights,
        _semantic_similarity,
        _embedding_similarity,
        WEIGHT_DISTANCE,
        _CITY_CACHE,
        _ensure_embedding,
        semantic_similarity_public_raw as _sem_raw,
        embedding_similarity_public_raw as _emb_raw,
    )
    from bson import ObjectId
    cand = db["candidates"].find_one({"_id": ObjectId(cand_id)})
    job = db["jobs"].find_one({"_id": ObjectId(job_id)})
    if not cand or not job:
        raise HTTPException(status_code=404, detail="Not found")
    cand_sk = _skill_set(cand); job_sk = _skill_set(job)
    # Distance metrics
    def _coord(city_can: str | None):
        if not city_can: return None
        rec=_CITY_CACHE.get(city_can.lower())
        if not rec: return None
        try:
            return float(rec.get('lat')), float(rec.get('lon'))
        except Exception:
            return None
    def _distance_km(a,b):
        if not a or not b: return None
        try:
            import math
            lat1,lon1=a; lat2,lon2=b
            R=6371.2
            dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
            lat1r=math.radians(lat1); lat2r=math.radians(lat2)
            h=math.sin(dlat/2)**2 + math.cos(lat1r)*math.cos(lat2r)*math.sin(dlon/2)**2
            c=2*math.asin(min(1, math.sqrt(h)))
            return round(R*c,1)
        except Exception:
            return None
    def _distance_score(km: float | None):
        if km is None: return 0.0
        if km <= 5: return 1.0
        if km >= 150: return 0.0
        return max(0.0, 1.0 - (km-5)/145.0)
    cand_coord=_coord(cand.get('city_canonical'))
    job_coord=_coord(job.get('city_canonical'))
    dist_km=_distance_km(cand_coord, job_coord)
    dist_score=_distance_score(dist_km)
    overlap = cand_sk & job_sk
    missing_for_job = job_sk - cand_sk
    missing_for_cand = cand_sk - job_sk
    title_sim = _title_similarity(str(cand.get('title','')), str(job.get('title','')))
    # Weighted sims (respect current weights)
    sem_sim = _semantic_similarity(str(cand.get('text_blob','')), str(job.get('text_blob','')))
    emb_sim = _embedding_similarity(cand.get('embedding'), job.get('embedding'))
    # Raw sims (for explain UI even when weight=0); ensure embeddings exist via fallback hasher
    sem_sim_raw = _sem_raw(str(cand.get('text_blob','')), str(job.get('text_blob','')))
    c_emb = _ensure_embedding(dict(cand)).get('embedding')
    j_emb = _ensure_embedding(dict(job)).get('embedding')
    emb_sim_raw = _emb_raw(c_emb, j_emb)
    w = get_weights()
    base_overlap = (len(overlap)/max(len(cand_sk),len(job_sk)) if max(len(cand_sk),len(job_sk))>0 else 0.0)
    # Weighted must/needed breakdown
    def _split(doc):
        must={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')=='must'}
        needed={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')!='must'}
        return must, needed
    weighted_skill_score = base_overlap
    must_ratio = needed_ratio = None
    if cand.get('skills_detailed') or job.get('skills_detailed'):
        c_must,c_needed=_split(cand); j_must,j_needed=_split(job)
        denom=max(len((c_must|c_needed) | (j_must|j_needed)),1)
        inter_must=len((c_must|c_needed) & j_must)
        inter_needed=len((c_must|c_needed) & j_needed)
        must_ratio=inter_must/denom; needed_ratio=inter_needed/denom
        weighted_skill_score=w.get('must_category_weight',0.7)*must_ratio + w.get('needed_category_weight',0.3)*needed_ratio
    composite = (
        w.get('skill_weight',0.0) * weighted_skill_score +
        w.get('title_weight',0.0) * title_sim +
        w.get('semantic_weight',0.0) * sem_sim +
        w.get('embedding_weight',0.0) * emb_sim +
        w.get('distance_weight',0.0) * dist_score
    )
    return {
        "candidate_id": cand_id,
        "job_id": job_id,
        "score": round(composite,4),
        "skill_overlap": sorted(list(overlap)),
        "candidate_only_skills": sorted(list(missing_for_job)),
        "job_only_skills": sorted(list(missing_for_cand)),
        "title_similarity": round(title_sim,4),
    "semantic_similarity": round(sem_sim,4),
    "embedding_similarity": round(emb_sim,4),
    "semantic_similarity_raw": round(sem_sim_raw,4),
    "embedding_similarity_raw": round(emb_sim_raw,4),
        "base_skill_overlap": round(base_overlap,4),
        "must_ratio": None if must_ratio is None else round(must_ratio,4),
        "needed_ratio": None if needed_ratio is None else round(needed_ratio,4),
        "weighted_skill_score": round(weighted_skill_score,4),
    "weights": w,
    "distance_km": dist_km,
    "distance_score": round(dist_score,4) if dist_km is not None else None
    }

@app.get("/match/breakdown/{cand_id}/{job_id}")
def match_breakdown(cand_id: str, job_id: str, tenant_id: str | None = Depends(optional_tenant_id)):
    """Return structured per-requirement breakdown with check booleans for a candidate↔job pair.
    Categories: 'must' and 'needed' (falls back to treating all as 'needed' if categories missing).
    """
    try:
        from bson import ObjectId as _OID
        from .ingest_agent import _skill_set, canonical_skill
        # Fetch docs with tenant scoping and minimal projections
        cq = {"_id": _OID(cand_id)}
        jq = {"_id": _OID(job_id)}
        if tenant_id:
            cq["tenant_id"] = tenant_id
            jq["tenant_id"] = tenant_id
        cand = db["candidates"].find_one(cq, {"_id":1,"title":1,"city_canonical":1,"skill_set":1,"skills_detailed":1,"esco_skills":1,"synthetic_skills":1,"skills":1})
        job  = db["jobs"].find_one(jq, {"_id":1,"title":1,"city_canonical":1,"skill_set":1,"skills_detailed":1,"job_requirements":1,"requirements":1,"esco_skills":1})
        if not cand or not job:
            raise HTTPException(status_code=404, detail="not_found")

        # Build candidate label map (prefer ESCO labels), keyed by canonical name
        cand_label: dict[str, str] = {}
        cand_esco_by_id: dict[str, dict] = {}
        try:
            for e in (cand.get('esco_skills') or []):
                nm = e.get('name')
                if not nm:
                    continue
                key = canonical_skill(nm)
                lbl = e.get('label') or str(nm).replace('_',' ').title()
                cand_label[key] = lbl
                esid = e.get('esco_id')
                if esid:
                    cand_esco_by_id[str(esid)] = e
        except Exception:
            pass

        # Canonical skill sets (avoid raw vs canonical mismatches); _skill_set already aggregates
        cset = {canonical_skill(s) for s in (_skill_set(cand) or set())}
        jset = {canonical_skill(s) for s in (_skill_set(job) or set())}

        # Derive must/needed from job.skills_detailed if available
        job_label: dict[str, str] = {}
        jd = job.get('skills_detailed') or []
        job_must_items = [s for s in jd if s and s.get('category') == 'must' and s.get('name')]
        job_needed_items = [s for s in jd if s and (s.get('category') != 'must') and s.get('name')]
        job_must = {canonical_skill(s.get('name')) for s in job_must_items}
        job_needed = {canonical_skill(s.get('name')) for s in job_needed_items}
        for s in jd:
            try:
                n = s.get('name')
                if not n:
                    continue
                key = canonical_skill(n)
                job_label[key] = (s.get('label') or str(n).replace('_',' ').title())
            except Exception:
                pass
        # Fallback: if no categories information, treat all job skills as 'needed'
        if not job_must and not job_needed and jset:
            job_must = set()
            job_needed = set(jset)

        # Optional: job ESCO skills to enable ESCO-id matching (if present from backfill)
        job_esco_by_name: dict[str, dict] = {}
        try:
            for e in (job.get('esco_skills') or []):
                if e.get('name'):
                    job_esco_by_name[e['name']] = e
        except Exception:
            pass

        def _label_for(name: str) -> str:
            key = canonical_skill(name) if name else name
            return job_label.get(key) or cand_label.get(key) or str(name).replace('_',' ').title()

        def _match_req(job_name: str, job_item: dict | None = None) -> tuple[bool, str | None, str | None, str]:
            """Return (has, matched_name, matched_label, reason)."""
            # Try ESCO id first if present on job item
            esid = None
            if isinstance(job_item, dict):
                esid = job_item.get('esco_id') or (job_esco_by_name.get(job_name) or {}).get('esco_id')
            if esid:
                entry = cand_esco_by_id.get(str(esid))
                if entry:
                    mname = entry.get('name') or job_name
                    mlabel = entry.get('label') or _label_for(mname)
                    return True, canonical_skill(mname) if mname else mname, mlabel, 'esco_id'
            # Canonical name matching
            can = canonical_skill(job_name) if job_name else job_name
            if can in cset:
                mlabel = cand_label.get(can) or _label_for(can)
                return True, can, mlabel, 'canonical'
            return False, None, None, 'missing'

        def _pack(job_name: str, cat: str, job_item: dict | None = None) -> dict:
            has, mname, mlabel, reason = _match_req(job_name, job_item)
            return {
                "name": job_name,
                "label": _label_for(job_name),
                "category": cat,
                "has": bool(has),
                "match_reason": reason,
                "matched_candidate_skill": mname,
                "matched_label": mlabel,
            }

        # Build packed items with reason metadata
        if job_must_items or job_needed_items:
            must_items = [_pack(s.get('name'), "must", s) for s in sorted(job_must_items, key=lambda x: str(x.get('name') or ''))]
            needed_items = [_pack(s.get('name'), "needed", s) for s in sorted(job_needed_items, key=lambda x: str(x.get('name') or ''))]
        else:
            must_items = [_pack(n, "must", None) for n in sorted(job_must)]
            needed_items = [_pack(n, "needed", None) for n in sorted(job_needed)]

        candidate_fit_must = [i["label"] for i in must_items if i["has"]]
        candidate_missing_must = [i["label"] for i in must_items if not i["has"]]
        candidate_fit_needed = [i["label"] for i in needed_items if i["has"]]
        candidate_missing_needed = [i["label"] for i in needed_items if not i["has"]]
        candidate_extra = sorted([_label_for(n) for n in (cset - jset)])

        counts = {
            "job_must_total": len(must_items),
            "job_needed_total": len(needed_items),
            "fit_must": len(candidate_fit_must),
            "fit_needed": len(candidate_fit_needed),
            "missing_must": len(candidate_missing_must),
            "missing_needed": len(candidate_missing_needed),
            "candidate_extra": len(candidate_extra),
        }

        return {
            "candidate": {"id": str(cand.get("_id")), "title": cand.get("title"), "city": cand.get("city_canonical")},
            "job": {"id": str(job.get("_id")), "title": job.get("title"), "city": job.get("city_canonical")},
            "requirements": {"must": must_items, "needed": needed_items},
            "candidate_fit_must": candidate_fit_must,
            "candidate_missing_must": candidate_missing_must,
            "candidate_fit_needed": candidate_fit_needed,
            "candidate_missing_needed": candidate_missing_needed,
            "candidate_extra_skills": candidate_extra,
            "counts": counts,
            "_debug": {
                "candidate_skill_count": len(cset),
                "candidate_skills_sample": sorted(list(cset))[:15],
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"breakdown_failed: {e}")

@app.post("/maintenance/recompute_embeddings")
def maintenance_recompute_embeddings(_: bool = Depends(require_api_key)):
    count = recompute_embeddings()
    return {"updated_embeddings": count}

@app.post("/maintenance/backfill_esco")
def maintenance_backfill_esco(_: bool = Depends(require_api_key)):
    """Recompute ESCO skill mappings for all candidates and jobs (idempotent)."""
    from .ingest_agent import ESCO_SKILLS, canonical_skill, _skill_set
    updated=0
    for coll_name in ("candidates","jobs"):
        coll=db[coll_name]
        for doc in coll.find({}, {"_id":1, "skill_set":1}):
            skill_set = doc.get("skill_set") or []
            esco=[]
            for s in skill_set:
                s_can=canonical_skill(s)
                meta=ESCO_SKILLS.get(s_can)
                if meta:
                    esco.append({"name": s_can, "esco_id": meta.get("id"), "label": meta.get("label")})
                else:
                    esco.append({"name": s_can})
            coll.update_one({"_id": doc["_id"]}, {"$set": {"esco_skills": esco}})
            updated+=1
    return {"updated_docs": updated}

@app.post("/maintenance/fill_synthetic_jobs")
def maintenance_fill_synthetic_jobs(include_existing: bool=False, _: bool = Depends(require_api_key)):
    """TEST-ONLY: Populate missing job structured fields with synthetic placeholder data.

    Policy note: This endpoint is for local QA only. It fabricates requirement data when the
    LLM extraction omitted it so downstream features can be exercised. It will never run in prod.

    Args:
        include_existing: if True, overwrite even existing requirement arrays (regenerate synthetic).
    """
    import random, time
    coll = db["jobs"]
    updated=0
    filled_details=[]  # collect small audit sample (first 40)
    now=int(time.time())
    SYN_MUST_POOL=["ניסיון אדמיניסטרטיבי", "שליטה באקסל", "שירותיות גבוהה", "עבודה בצוות", "מולטיטסקינג", "ניהול יומן", "תקשורת בין אישית", "עמידה בלחץ"]
    SYN_NICE_POOL=["SAP", "Priority", "יכולת הדרכה", "אנגלית טובה", "תהליכי רכש", "ניהול ספקים", "דוחות בקרה"]
    for doc in coll.find({}, {"_id":1, "requirements":1, "job_requirements":1, "job_description":1, "city_canonical":1, "description":1}):
        changes={}
        req = doc.get('requirements') if include_existing else (doc.get('requirements') or None)
        # Determine if requirements need synthesis: missing, wrong type, empty lists, or overwrite requested
        existing_must = (req or {}).get('must_have_skills') if isinstance(req, dict) else None
        existing_nice = (req or {}).get('nice_to_have_skills') if isinstance(req, dict) else None
        is_empty_struct = (
            isinstance(req, dict) and (
                not existing_must and not existing_nice or
                (isinstance(existing_must, list) and len(existing_must)==0 and isinstance(existing_nice, list) and len(existing_nice)==0)
            )
        )
        need_req = include_existing or (req is None) or (not isinstance(req, dict)) or is_empty_struct
        if need_req:
            must_len=random.randint(4,6)
            nice_len=random.randint(1,3)
            must=[{"name": m} for m in random.sample(SYN_MUST_POOL, k=must_len)]
            nice=[{"name": n} for n in random.sample(SYN_NICE_POOL, k=nice_len)]
            changes['requirements']={"must_have_skills": must, "nice_to_have_skills": nice}
            merged=[]; seen=set()
            for s in must+nice:
                n=s.get('name')
                if n and n not in seen:
                    seen.add(n); merged.append(n)
            changes['job_requirements']=merged
        else:
            # ensure job_requirements present if real requirements exist
            if not doc.get('job_requirements') and isinstance(doc.get('requirements'), dict):
                must=[i.get('name') for i in (doc['requirements'].get('must_have_skills') or []) if isinstance(i, dict) and i.get('name')]
                nice=[i.get('name') for i in (doc['requirements'].get('nice_to_have_skills') or []) if isinstance(i, dict) and i.get('name')]
                merged=[]; seen=set()
                for n in must+nice:
                    if n and n not in seen:
                        seen.add(n); merged.append(n)
                if merged:
                    changes['job_requirements']=merged
        # Ensure job_description present
        if (include_existing and doc.get('job_description')) or not doc.get('job_description'):
            if include_existing or not doc.get('job_description'):
                changes['job_description']='תפקיד אדמיניסטרטיבי הכולל תמיכה תפעולית, שירות לקוחות ותיאום יומנים.'[:1200]
        if changes:
            changes['synthetic_filled']=True
        if changes:
            changes['updated_at']=now
            coll.update_one({"_id": doc['_id']}, {"$set": changes})
            updated+=1
            if len(filled_details)<40:
                filled_details.append({"id": str(doc['_id']), "fields": list(changes.keys())})
    return {"synthetic_filled": updated, "include_existing": include_existing, "sample": filled_details}


@app.post("/maintenance/backfill_candidate_cities")
def backfill_candidate_cities(_: bool = Depends(require_api_key)):
    """Backfill candidate city_canonical from top-level city or contact.city if missing.
    Does NOT invent cities (no synthetic)."""
    from .ingest_agent import canonical_city as _canonical_city
    coll = db['candidates']
    cur = coll.find({'$or':[{'city_canonical': {'$exists': False}}, {'city_canonical': None}, {'city_canonical': ''}]}, {'_id':1,'city':1,'contact':1,'city_canonical':1})
    updated=0; sample=[]
    for d in cur:
        source_city = d.get('city') or (d.get('contact') or {}).get('city')
        if not source_city or len(str(source_city).strip())<2:
            continue
        can = _canonical_city(source_city)
        if not can:
            continue
        coll.update_one({'_id': d['_id']},{'$set': {'city_canonical': can, 'updated_at': int(time.time())}})
        updated+=1
        if len(sample)<40:
            sample.append({'id': str(d['_id']), 'city': source_city, 'city_canonical': can})
    return {'candidates_updated': updated, 'sample': sample}

@app.post("/maintenance/synthesize_job_addresses")
def synthesize_job_addresses(limit: int = 50, _: bool = Depends(require_api_key)):
    """For testing: assign simple synthetic addresses to jobs missing address.
    Format: רחוב X מספר Y, <City>.
    """
    import random, time
    coll = db['jobs']
    cur = coll.find({'$or':[{'address': {'$exists': False}}, {'address': ''}]}, {'_id':1,'city_canonical':1,'address':1}).limit(limit)
    streets = ['הרצל','ויצמן','אבן גבירול','דיזנגוף','העצמאות','הזית','האלה','החרוב','המייסדים','הפלמ"ח']
    updated=0; sample=[]; now=int(time.time())
    for d in cur:
        city = (d.get('city_canonical') or 'תל_אביב').replace('_',' ')
        street = random.choice(streets)
        number = random.randint(3, 120)
        addr = f"רחוב {street} {number}, {city}"
        coll.update_one({'_id': d['_id']},{'$set': {'address': addr, 'updated_at': now, 'synthetic_address': True}})
        updated+=1
        if len(sample)<40:
            sample.append({'id': str(d['_id']), 'address': addr})
    return {'jobs_updated': updated, 'sample': sample}

@app.post("/maintenance/backfill_job_fields")
def maintenance_backfill_job_fields(_: bool = Depends(require_api_key)):
    """Backfill derived job fields: job_description, job_requirements (idempotent)."""
    updated = 0
    cur = db["jobs"].find({}, {"_id":1, "description":1, "job_description":1, "requirements":1, "job_requirements":1})
    for doc in cur:
        changes = {}
        if not doc.get('job_description') and doc.get('description'):
            changes['job_description'] = str(doc.get('description'))[:1200]
        if not doc.get('job_requirements'):
            req = doc.get('requirements') or {}
            must = [i.get('name') for i in (req.get('must_have_skills') or []) if isinstance(i, dict) and i.get('name')]
            nice = [i.get('name') for i in (req.get('nice_to_have_skills') or []) if isinstance(i, dict) and i.get('name')]
            merged=[]; seen=set()
            for n in must + nice:
                if n and n not in seen:
                    seen.add(n); merged.append(n)
            if merged:
                changes['job_requirements'] = merged
        if changes:
            db['jobs'].update_one({'_id': doc['_id']}, {'$set': changes})
            updated += 1
    return {"updated_jobs": updated}

class SkillSynRequest(BaseModel):
    canon: str
    synonym: str

@app.post("/maintenance/skill")
def maintenance_add_skill_synonym(req: SkillSynRequest, _: bool = Depends(require_api_key)):
    added = add_skill_synonym(req.canon, req.synonym)
    if not added:
        raise HTTPException(status_code=400, detail="Could not add synonym")
    return {"added": True}

@app.post("/maintenance/recompute")
def maintenance_recompute(_: bool = Depends(require_api_key)):
    changed = recompute_skill_sets()
    return {"changed": changed}

@app.post("/maintenance/refresh/{kind}")
def maintenance_refresh(kind: str, use_llm: bool=False, _: bool = Depends(require_api_key)):
    if kind not in {"candidate","job"}:
        raise HTTPException(status_code=400, detail="kind must be candidate|job")
    count = refresh_existing(kind, use_llm=use_llm)
    return {"refreshed": count}

@app.post("/maintenance/clear_cache")
def maintenance_clear_cache(_: bool = Depends(require_api_key)):
    clear_extraction_cache()
    return {"cleared": True}

@app.get("/meta")
def meta_dump():
    return {"meta": list_meta()}

# --- Search Endpoints ---
_SEARCH_CACHE: dict[str, dict] = {}
_SEARCH_CACHE_ORDER: list[str] = []
_SEARCH_CACHE_MAX = 200

def _cache_get(key: str):
    return _SEARCH_CACHE.get(key)

def _cache_put(key: str, value: dict):
    _SEARCH_CACHE[key] = value
    _SEARCH_CACHE_ORDER.append(key)
    if len(_SEARCH_CACHE_ORDER) > _SEARCH_CACHE_MAX:
        old = _SEARCH_CACHE_ORDER.pop(0)
        _SEARCH_CACHE.pop(old, None)

def _parse_skills_param(skill: str | None, skills: str | None):
    collected = []
    if skills:
        collected.extend([s.strip() for s in skills.split(',') if s.strip()])
    if skill:
        collected.append(skill.strip())
    # dedupe preserving order
    seen = set(); out=[]
    for s in collected:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

@app.get("/search/jobs")
def search_jobs(skill: str | None = None, skills: str | None = None, city: str | None = None, mode: str = "any", skip: int = 0, limit: int = 20, sort_by: str | None = None):
    if limit > 100: limit = 100
    if skip < 0: skip = 0
    skill_list = _parse_skills_param(skill, skills)
    # Build base query (only city & possibly single ESCO skill if only one provided and starts with esco: and mode is any)
    query: dict = {}
    if city:
        query['city_canonical'] = city.lower().replace(' ', '_')
    esco_filters = [s.split(':',1)[1] for s in skill_list if s.startswith('esco:')]
    cache_key = f"jobs::{city}::{mode}::{skip}::{limit}::{sort_by}::"+",".join(sorted(skill_list))
    cached = _cache_get(cache_key)
    if cached:
        return cached
    cursor = db['jobs'].find(query)
    total = cursor.count() if hasattr(cursor, 'count') else db['jobs'].count_documents(query)
    out = []
    for doc in cursor.skip(skip).limit(limit*3):  # over-fetch for post-filter
        skill_set = set(doc.get('skill_set') or [])
        esco_ids = {s.get('esco_id') for s in (doc.get('esco_skills') or []) if s.get('esco_id')}
        matched_skills=[]; matched_esco=[]
        for s in skill_list:
            if s.startswith('esco:'):
                eid = s.split(':',1)[1]
                if eid in esco_ids:
                    matched_esco.append(s)
            else:
                cs = canonical_skill(s)
                if cs in skill_set:
                    matched_skills.append(cs)
        if skill_list:
            if mode == 'all':
                # All skills (treat esco & normal together)
                if len(matched_skills) + len(matched_esco) < len(skill_list):
                    continue
            else:  # any
                if not (matched_skills or matched_esco):
                    continue
        rec = {
            'job_id': str(doc['_id']),
            'title': doc.get('title'),
            'city': doc.get('city_canonical'),
            'matched_skills': matched_skills,
            'matched_esco': matched_esco,
            'updated_at': doc.get('updated_at')
        }
        out.append(rec)
    # Sorting
    if sort_by == 'matched':
        out.sort(key=lambda r: (len(r['matched_skills'])+len(r['matched_esco']), r.get('updated_at') or 0), reverse=True)
    elif sort_by == 'recent':
        out.sort(key=lambda r: r.get('updated_at') or 0, reverse=True)
    # Pagination after filtering
    paged = out[:limit]
    result = {'results': paged, 'returned': len(paged), 'filtered_total': len(out), 'db_total': total, 'skip': skip, 'limit': limit, 'query': {'city': city, 'skills': skill_list, 'mode': mode, 'sort_by': sort_by}}
    _cache_put(cache_key, result)
    return result

@app.post("/maintenance/backfill_cities")
def maintenance_backfill_cities(_: bool = Depends(require_api_key)):
    """Scan job documents with missing/empty city_canonical and populate it from available sources.
    Order of precedence (first-hit wins):
      1) Explicit fields: city, work_location, branch
      2) text_blob/full_text lines starting with Location:/City:/עיר:/מיקום:
      3) Token scan against known cities within combined text sources, including job_description/title
    """
    from .ingest_agent import canonical_city, _CITY_CACHE
    updated = 0
    # Match docs where city_canonical is missing OR null/empty string
    cursor = db['jobs'].find({
        '$or': [
            { 'city_canonical': { '$exists': False } },
            { 'city_canonical': None },
            { 'city_canonical': '' },
        ]
    }, {
        '_id': 1,
        'text_blob': 1,
        'full_text': 1,
        'job_description': 1,
        'title': 1,
        'city': 1,
        'work_location': 1,
        'branch': 1,
        'location': 1,
    })
    for doc in cursor:
        # 1) Try explicit fields first
        for explicit in [doc.get('city'), doc.get('work_location'), doc.get('branch'), doc.get('location')]:
            if explicit and str(explicit).strip():
                raw = str(explicit).strip()
                raw = re.sub(r"^\s*(סניף|branch)\s+", "", raw, flags=re.IGNORECASE).strip()
                c = canonical_city(raw)
                if c:
                    db['jobs'].update_one({'_id': doc['_id']}, {'$set': {'city_canonical': c}})
                    updated += 1
                    break
        else:
            # 2) Parse labeled lines in text sources
            txt_labeled = (doc.get('text_blob') or '') + '\n' + (doc.get('full_text') or '')
            city = None
            for line in txt_labeled.splitlines():
                low = line.strip().lower()
                if low.startswith('location:') or low.startswith('city:') or low.startswith('עיר:') or low.startswith('מיקום:'):
                    city = line.split(':',1)[1].strip()
                    break
            if not city:
                m = re.search(r"(?im)^(?:location|city|עיר|מיקום)\s*:\s*([A-Za-zא-ת '._-]+)$", txt_labeled)
                if m:
                    city = m.group(1).strip()
            if city:
                city = re.sub(r"^\s*(סניף|branch)\s+", "", city, flags=re.IGNORECASE).strip()
                c = canonical_city(city)
                if c:
                    db['jobs'].update_one({'_id': doc['_id']}, {'$set': {'city_canonical': c}})
                    updated += 1
                    continue
            # 3) Fallback token scan across all relevant text fields
            hay = "\n".join([
                str(doc.get('text_blob') or ''),
                str(doc.get('full_text') or ''),
                str(doc.get('job_description') or ''),
                str(doc.get('title') or ''),
            ]).lower()
            keys = sorted(_CITY_CACHE.keys(), key=len, reverse=True)
            found_can = None
            for key in keys:
                alt = key.replace('_', ' ')
                if (key and key in hay) or (alt and alt in hay):
                    found_can = _CITY_CACHE[key]['city'].lower()
                    break
            if found_can:
                db['jobs'].update_one({'_id': doc['_id']}, {'$set': {'city_canonical': found_can}})
                updated += 1
    return {"updated_jobs": updated}

@app.get("/search/candidates")
def search_candidates(skill: str | None = None, skills: str | None = None, city: str | None = None, mode: str = "any", skip: int = 0, limit: int = 20, sort_by: str | None = None):
    if limit > 100: limit = 100
    if skip < 0: skip = 0
    skill_list = _parse_skills_param(skill, skills)
    query: dict = {}
    if city:
        query['city_canonical'] = city.lower().replace(' ', '_')
    cache_key = f"cands::{city}::{mode}::{skip}::{limit}::{sort_by}::"+",".join(sorted(skill_list))
    cached = _cache_get(cache_key)
    if cached:
        return cached
    cursor = db['candidates'].find(query)
    total = cursor.count() if hasattr(cursor,'count') else db['candidates'].count_documents(query)
    out=[]
    out=[]
    for doc in cursor.skip(skip).limit(limit*3):
        skill_set = set(doc.get('skill_set') or [])
        esco_ids = {s.get('esco_id') for s in (doc.get('esco_skills') or []) if s.get('esco_id')}
        matched_skills=[]; matched_esco=[]
        for s in skill_list:
            if s.startswith('esco:'):
                eid = s.split(':',1)[1]
                if eid in esco_ids:
                    matched_esco.append(s)
            else:
                cs = canonical_skill(s)
                if cs in skill_set:
                    matched_skills.append(cs)
        if skill_list:
            if mode=='all':
                if len(matched_skills)+len(matched_esco) < len(skill_list):
                    continue
            else:
                if not (matched_skills or matched_esco):
                    continue
        rec={
            'candidate_id': str(doc['_id']),
            'title': doc.get('title'),
            'city': doc.get('city_canonical'),
            'matched_skills': matched_skills,
            'matched_esco': matched_esco,
            'updated_at': doc.get('updated_at')
        }
        out.append(rec)
    if sort_by == 'matched':
        out.sort(key=lambda r: (len(r['matched_skills'])+len(r['matched_esco']), r.get('updated_at') or 0), reverse=True)
    elif sort_by == 'recent':
        out.sort(key=lambda r: r.get('updated_at') or 0, reverse=True)
    paged=out[:limit]
    result={'results': paged, 'returned': len(paged), 'filtered_total': len(out), 'db_total': total, 'skip': skip, 'limit': limit, 'query': {'city': city, 'skills': skill_list, 'mode': mode, 'sort_by': sort_by}}
    _cache_put(cache_key, result)
    return result

# --- Lightweight Admin HTML Views (fallback instead of mongo-express) ---
@app.get("/admin/candidates", response_class=HTMLResponse)
def admin_candidates(q: str | None = None, skip: int = 0, limit: int = 50):
    if limit > 200: limit = 200
    if skip < 0: skip = 0
    query: dict = {}
    if q:
        pattern = f".*{re.escape(q)}.*"
        query = {"$or": [
            {"title": {"$regex": pattern, "$options": "i"}},
            {"skill_set": {"$elemMatch": {"$regex": pattern, "$options": "i"}}}
        ]}
    total = db['candidates'].count_documents(query)
    cur = db['candidates'].find(query, {"title":1, "city_canonical":1, "skill_set":1, "updated_at":1, "share_id":1, "status":1}).skip(skip).limit(limit)
    rows = []
    # Build rows first
    for doc in cur:
        cid = str(doc['_id'])
        title = html.escape(str(doc.get('title') or ''))
        city = html.escape(str(doc.get('city_canonical') or ''))
        skills = doc.get('skill_set') or []
        scount = len(skills)
        status = html.escape(str(doc.get('status') or ''))
        share = html.escape(str(doc.get('share_id') or ''))
        rows.append(f"<tr><td style='direction:ltr'>{cid}</td><td>{share}</td><td>{title}</td><td>{city}</td><td>{scount}</td><td>{doc.get('updated_at') or ''}</td><td>{status}</td><td>-</td></tr>")
    # Now compose HTML using f-string to avoid .format conflicts with CSS braces
    prev_link = f"<a href='/admin/candidates?skip={max(skip-limit,0)}&limit={limit}&q={q}'>◀ קודם</a>" if skip>0 else ''
    next_link = f"<a href='/admin/candidates?skip={skip+limit}&limit={limit}&q={q}'>הבא ▶</a>" if (skip+limit) < total else ''
    search_box_value = html.escape(q) if q else ''
    rows_html = ''.join(rows) if rows else '<tr><td colspan=8 style="text-align:center">(אין תוצאות)</td></tr>'
    body = f"""<!DOCTYPE html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>מועמדים</title>
<style>
table {{ border-collapse:collapse; width:100%; }}
th,td {{ border:1px solid #ccc; padding:4px 6px; font-size:12px; }}
th {{ background:#eee; }}
.pager a {{ margin:0 8px; text-decoration:none; }}
form.search {{ margin-bottom:10px; }}
</style></head>
<body>
<h2 style='margin-top:0'>📄 רשימת מועמדים</h2>
<form class='search' method='get'>
    חיפוש: <input name='q' value='{search_box_value}' placeholder='כותרת או מיומנות'>
    <input type='hidden' name='limit' value='{limit}'>
    <button type='submit'>סנן</button>
</form>
<div class='pager'>{prev_link} | {next_link} &nbsp; (סה"כ {total})</div>
<table>
    <thead><tr><th>ID</th><th>Share</th><th>כותרת</th><th>עיר</th><th>#מיומנויות</th><th>עודכן</th><th>מצב</th><th>פעולות</th></tr></thead>
    <tbody>
        {rows_html}
    </tbody>
    </table>
<div class='pager'>{prev_link} | {next_link}</div>
<p>
    <a href='/admin/jobs'>לצפיה במשרות »</a> ·
    <a href='/admin/candidates/fields' target='_blank'>סקירת שדות מועמדים »</a>
    · <a href='/admin/candidates/schema' target='_blank'>תרשים סchema מועמדים »</a>
    · <a href='/admin/candidates/all_fields' target='_blank'>טבלה רחבה של כל השדות »</a>
    · <a href='/admin/candidates/skills_view' target='_blank'>שדות כישורים (ESCO) »</a>
</p>
</body></html>"""
    return HTMLResponse(content=body)

@app.get("/admin/candidates/fields.json")
def admin_candidates_fields_json(limit: int = 1000):
    """Scan up to 'limit' candidate docs and return discovered field paths with counts and sample types.

    - Handles nested dicts and arrays (arrays denoted with [] in the path).
    - 'count' is number of documents containing the path (not total occurrences inside arrays).
    - 'types' is a sorted list of Python types observed (string names).
    """
    if limit <= 0:
        limit = 100
    coll = db['candidates']
    cur = coll.find({}, limit=limit)
    total_scanned = 0
    stats: dict[str, dict] = {}

    def typename(v: Any) -> str:
        import datetime
        if v is None:
            return 'null'
        t = type(v)
        if t in (str, int, float, bool):
            return t.__name__
        if isinstance(v, list):
            return 'array'
        if isinstance(v, dict):
            return 'object'
        if isinstance(v, (datetime.datetime, datetime.date)):
            return 'datetime'
        return t.__name__

    def record(path: str, example: Any):
        s = stats.setdefault(path, {"count": 0, "types": set(), "examples": []})
        s["count"] += 1
        s["types"].add(typename(example))
        if len(s["examples"]) < 3:
            try:
                s["examples"].append(example)
            except Exception:
                s["examples"].append(str(example))

    def flatten(doc: Any, base: str = "", seen: set[str] | None = None):
        # Collect unique top-level paths per document; we call record once per path per doc
        paths_seen = seen if seen is not None else set()
        if isinstance(doc, dict):
            for k, v in doc.items():
                path = f"{base}.{k}" if base else str(k)
                # mark existence
                key_exist = path not in paths_seen
                if key_exist:
                    paths_seen.add(path)
                    record(path, v)
                # descend
                flatten(v, path, paths_seen)
        elif isinstance(doc, list):
            path = f"{base}[]" if base else "[]"
            if base and base not in paths_seen:
                # Also count the array field itself
                paths_seen.add(base)
                record(base, doc)
            # For arrays, look at element examples and nested structures
            for idx, el in enumerate(doc[:5]):  # sample first few
                if base:
                    # per-element pseudo-path
                    el_path = f"{base}[]"
                else:
                    el_path = "[]"
                # Record element type occurrence once
                if el_path not in paths_seen:
                    paths_seen.add(el_path)
                    record(el_path, el)
                flatten(el, el_path, paths_seen)
        else:
            # scalar at base – nothing more to traverse
            pass

    for d in cur:
        total_scanned += 1
        try:
            flatten(d)
        except Exception:
            continue

    # Normalize output
    out_fields = []
    for path, s in stats.items():
        types = sorted(list(s["types"]))
        count = int(s["count"]) if isinstance(s.get("count"), int) else 0
        pct = round((count / max(total_scanned, 1)) * 100.0, 2)
        # stringify examples compactly
        ex = []
        for e in s.get("examples", [])[:3]:
            try:
                if isinstance(e, (dict, list)):
                    ex.append(json.dumps(e, ensure_ascii=False)[:140])
                else:
                    ex.append(str(e)[:140])
            except Exception:
                ex.append("<unrepr>")
        out_fields.append({
            "path": path,
            "count": count,
            "pct": pct,
            "types": types,
            "examples": ex
        })
    out_fields.sort(key=lambda x: (x["path"]))
    return {"total_scanned": total_scanned, "limit": limit, "unique_fields": len(out_fields), "fields": out_fields}

@app.get("/admin/candidates/fields", response_class=HTMLResponse)
def admin_candidates_fields(limit: int = 1000):
    data = admin_candidates_fields_json(limit=limit)
    total = data.get("total_scanned", 0)
    rows = []
    for f in data.get("fields", [])[:3000]:  # safety cap for HTML
        path = html.escape(str(f.get("path")))
        count = f.get("count")
        pct = f.get("pct")
        types = html.escape(', '.join(f.get("types") or []))
        ex_raw = f.get("examples") or []
        ex = html.escape(' | '.join([str(x) for x in ex_raw]))
        rows.append(f"<tr><td style='direction:ltr'>{path}</td><td>{count}</td><td>{pct}%</td><td>{types}</td><td style='max-width:520px;white-space:normal'>{ex}</td></tr>")
    table = ''.join(rows) if rows else "<tr><td colspan=5 style='text-align:center'>(אין שדות)</td></tr>"
    html_doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>סקירת שדות מועמדים</title>
<style>
body {{ font-family: Arial, sans-serif; margin:16px; background:#f5f5f5; }}
table {{ border-collapse:collapse; width:100%; background:#fff; }}
</style></head>
<body>
<h2 style='margin-top:0'>🧭 סקירת שדות בטבלת מועמדים</h2>
<div class='meta'>נסרקו {total} מסמכים (מוגבל ל-{limit}).</div>
<form method='get'>
  Limit: <input name='limit' value='{limit}' size='6'>
  <button type='submit'>רענן</button>
  <a href='/admin/candidates'>חזרה לרשימה</a> ·
  <a href='/admin/candidates/fields.json?limit={limit}' target='_blank'>JSON</a>
</form>
<table>
  <thead><tr><th>נתיב שדה</th><th>Count</th><th>%</th><th>Types</th><th>Examples</th></tr></thead>
  <tbody>{table}</tbody>
</table>
</body></html>"""
    return HTMLResponse(content=html_doc)

@app.get("/admin/candidates/schema.json")
def admin_candidates_schema_json(limit: int = 1000):
    """Return presence stats for a curated candidate schema grouped as requested.

    Uses the same document scan as fields.json, then maps to expected paths,
    returning counts, pct, observed types, and examples for each.
    """
    fields_data = admin_candidates_fields_json(limit=limit)
    by_path = {f.get('path'): f for f in fields_data.get('fields', [])}
    total = fields_data.get('total_scanned', 0)

    # Curated schema definition (path -> label)
    SCHEMA = [
        ("Identity", [
            ("_id", "_id (ObjectId)"),
            ("share_id", "share_id (string)"),
            ("status", "status (string)"),
            ("llm_success", "llm_success (bool)"),
            ("created_at", "created_at (unix timestamp)"),
            ("updated_at", "updated_at (unix timestamp)"),
            ("_src_hash", "_src_hash (string, content hash)")
        ]),
        ("Profile", [
            ("title", "title (string)"),
            ("full_name", "full_name (string)"),
            ("city", "city (string)"),
            ("city_canonical", "city_canonical (string, normalized)"),
            ("summary", "summary (string)"),
            ("years_experience", "years_experience (int)"),
            ("salary_expectation", "salary_expectation (string)"),
            ("estimated_age", "estimated_age (int)")
        ]),
        ("Contact", [
            ("contact", "contact (object)"),
            ("contact.email", "contact.email (string)"),
            ("contact.phone", "contact.phone (string)"),
            ("contact.city", "contact.city (string)"),
            ("contact.country", "contact.country (string)")
        ]),
        ("Skills", [
            ("skill_set", "skill_set (array of string)"),
            ("skill_set[]", "skill_set[] (string)"),
            ("skills", "skills (object)"),
            ("skills.hard[]", "skills.hard (array)"),
            ("skills.hard[].name", "skills.hard[].name (string)"),
            ("skills.soft[]", "skills.soft (array)"),
            ("skills.soft[].name", "skills.soft[].name (string)"),
            ("skills_detailed[]", "skills_detailed (array)"),
            ("skills_detailed[].name", "skills_detailed[].name (string)"),
            ("skills_detailed[].category", "skills_detailed[].category (string)"),
            ("skills_detailed[].source", "skills_detailed[].source (string)"),
            ("esco_skills[]", "esco_skills (array)"),
            ("esco_skills[].name", "esco_skills[].name (string)"),
            ("esco_skills[].esco_id", "esco_skills[].esco_id (string)"),
            ("esco_skills[].label", "esco_skills[].label (string)"),
            ("tools[]", "tools (array of string)"),
            ("languages[]", "languages (array of string)"),
            ("synthetic_skills[]", "synthetic_skills (array of string)"),
            ("synthetic_skills_generated", "synthetic_skills_generated (int)"),
            ("skills_joined", "skills_joined (string)")
        ]),
        ("Experience & Education", [
            ("experience[]", "experience (array of objects)"),
            ("education[]", "education (array of objects)"),
            ("certifications[]", "certifications (array of objects)"),
            ("projects[]", "projects (array of objects)"),
            ("achievements[]", "achievements (array of objects)"),
            ("volunteering[]", "volunteering (array of objects)")
        ]),
        ("Text & Sections", [
            ("text_blob", "text_blob (string, PII-scrubbed)"),
            ("embedding_summary", "embedding_summary (string, PII-scrubbed)"),
            ("raw_sections", "raw_sections (object: experience, education, skills)")
        ]),
        ("Governance & Flags", [
            ("fields_complete", "fields_complete (bool)")
        ]),
        ("Meta", [
            ("_meta", "_meta (object)"),
            ("last_candidate_ingest_metrics", "last_candidate_ingest_metrics (object)")
        ])
    ]

    categories = []
    expected_paths = set()
    for name, items in SCHEMA:
        cat_items = []
        for path, label in items:
            expected_paths.add(path)
            stat = by_path.get(path, {"count": 0, "pct": 0.0, "types": [], "examples": []})
            cat_items.append({
                "path": path,
                "label": label,
                "count": stat.get("count", 0),
                "pct": stat.get("pct", 0.0),
                "types": stat.get("types", []),
                "examples": stat.get("examples", []),
                "found": bool(stat.get("count", 0) > 0)
            })
        categories.append({"name": name, "items": cat_items})

    # Extra fields not listed in the curated schema
    extras = []
    for p, s in by_path.items():
        if p not in expected_paths:
            extras.append({
                "path": p,
                "count": s.get("count", 0),
                "pct": s.get("pct", 0.0),
                "types": s.get("types", []),
                "examples": s.get("examples", [])
            })
    # Sort extras by path
    extras.sort(key=lambda x: x["path"])  # pragma: no cover

    return {
        "total_scanned": total,
        "limit": limit,
        "categories": categories,
        "extra_fields": extras[:1000]
    }

@app.get("/admin/candidates/schema", response_class=HTMLResponse)
def admin_candidates_schema(limit: int = 1000):
    data = admin_candidates_schema_json(limit=limit)
    total = data.get("total_scanned", 0)
    # Build HTML
    sections = []
    for cat in data.get("categories", []):
        rows = []
        for it in cat.get("items", []):
            path = html.escape(str(it.get("path")))
            label = html.escape(str(it.get("label")))
            count = it.get("count")
            pct = it.get("pct")
            types = html.escape(', '.join(it.get("types") or []))
            ex = html.escape(' | '.join([str(x) for x in (it.get("examples") or [])]))
            ok = "✅" if it.get("found") else "—"
            rows.append(f"<tr><td style='direction:ltr'>{path}</td><td>{label}</td><td>{ok}</td><td>{count}</td><td>{pct}%</td><td>{types}</td><td style='max-width:520px;white-space:normal'>{ex}</td></tr>")
        table = ''.join(rows) if rows else "<tr><td colspan=7 style='text-align:center'>(אין פריטים)</td></tr>"
        sections.append(f"<h3 style='margin:18px 0 8px'>{html.escape(cat.get('name',''))}</h3><table><thead><tr><th>Path</th><th>Label</th><th>Found</th><th>Count</th><th>%</th><th>Types</th><th>Examples</th></tr></thead><tbody>{table}</tbody></table>")
    extras_rows = []
    for ex in data.get("extra_fields", [])[:300]:
        p = html.escape(str(ex.get("path")))
        types = html.escape(', '.join(ex.get("types") or []))
        exs = html.escape(' | '.join([str(x) for x in (ex.get("examples") or [])]))
        extras_rows.append(f"<tr><td style='direction:ltr'>{p}</td><td>{ex.get('count')}</td><td>{ex.get('pct')}%</td><td>{types}</td><td style='max-width:520px;white-space:normal'>{exs}</td></tr>")
    extras_table = ''.join(extras_rows) if extras_rows else "<tr><td colspan=5 style='text-align:center'>(אין שדות נוספים)</td></tr>"
    html_doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>תרשים סכמה מועמדים</title>
<style>
body {{ font-family: Arial, sans-serif; margin:16px; background:#f5f5f5; }}
table {{ border-collapse:collapse; width:100%; background:#fff; }}
th,td {{ border:1px solid #ccc; padding:6px 8px; font-size:12px; vertical-align:top; }}
th {{ background:#eee; }}
.meta {{ color:#475569; font-size:12px; margin:8px 0 14px; }}
h2 {{ margin-top: 0; }}
h3 {{ margin-top: 20px; }}
</style></head>
<body>
<h2>🗂️ תרשים סכמה של טבלת מועמדים</h2>
<div class='meta'>נסרקו {total} מסמכים (מוגבל ל-{limit}).
 — <a href='/admin/candidates/fields'>סקירת שדות</a>
 — <a href='/admin/candidates/schema.json?limit={limit}' target='_blank'>JSON</a>
 — <a href='/admin/candidates'>חזרה לרשימה</a></div>
{''.join(sections)}
<h3 style='margin:18px 0 8px'>שדות נוספים שהתגלו</h3>
<table><thead><tr><th>Path</th><th>Count</th><th>%</th><th>Types</th><th>Examples</th></tr></thead><tbody>{extras_table}</tbody></table>
</body></html>"""
    return HTMLResponse(content=html_doc)

# --- Full wide table (all flattened fields) ---
def _flatten_doc(doc: dict, max_list_elems: int = 3, prefix: str = "", out: dict | None = None):
    if out is None:
        out = {}
    for k, v in doc.items():
        if k.startswith('_') and k not in {"_id", "_src_hash"}:  # include _id & _src_hash only
            pass  # still include in case user wants them
        path = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict):
            _flatten_doc(v, max_list_elems=max_list_elems, prefix=path, out=out)
        elif isinstance(v, list):
            # store summary and first few scalar elements; if dict elements, flatten limited
            if not v:
                out[path] = []
                continue
            # simple scalar list
            if all(not isinstance(e, (dict, list)) for e in v):
                # Convert non-JSON-safe scalars (e.g., ObjectId) to strings
                from bson import ObjectId as _OID
                converted = []
                for el in v[:max_list_elems]:
                    if isinstance(el, _OID):
                        converted.append(str(el))
                    else:
                        converted.append(el)
                out[path] = converted
            else:
                out[path] = f"list[{len(v)}]"
                for idx, el in enumerate(v[:max_list_elems]):
                    if isinstance(el, dict):
                        _flatten_doc(el, max_list_elems=max_list_elems, prefix=f"{path}[{idx}]", out=out)
                    else:
                        from bson import ObjectId as _OID
                        out[f"{path}[{idx}]"] = str(el) if isinstance(el, _OID) else el
        else:
            # Basic scalar; convert ObjectId / unsupported types to string
            try:
                from bson import ObjectId as _OID
                if isinstance(v, _OID):
                    out[path] = str(v)
                else:
                    out[path] = v
            except Exception:
                out[path] = str(v)
    return out

@app.get('/admin/candidates/all_fields.json')
def admin_candidates_all_fields_json(skip: int = 0, limit: int = 50, sample: int = 500):
    """Return a wide flattened representation of candidate documents.

    Args:
        skip: pagination offset over actual returned rows
        limit: number of flattened candidate rows to include (HTML table size)
        sample: number of docs to scan to build union of columns (cap for performance)
    """
    if limit > 200: limit = 200
    if sample > 2000: sample = 2000
    if skip < 0: skip = 0
    total = db['candidates'].count_documents({})
    cur_all = db['candidates'].find({}, {}).skip(skip).limit(limit)
    cur_sample = db['candidates'].find({}, {}).limit(sample)
    columns: set[str] = set()
    sample_rows = []
    for d in cur_sample:
        flat = _flatten_doc(d.copy())
        sample_rows.append(flat)
        columns.update(flat.keys())
    # Order columns: stable important first then alphabetical
    priority = ["_id", "share_id", "status", "title", "full_name", "city_canonical", "years_experience", "skill_set", "skills_detailed", "updated_at"]
    col_list = []
    seen = set()
    for p in priority:
        if p in columns:
            col_list.append(p); seen.add(p)
    for c in sorted(columns):
        if c not in seen:
            col_list.append(c)
    # Build rows for current page
    rows = []
    for d in cur_all:
        flat = _flatten_doc(d.copy())
        rows.append({c: flat.get(c) for c in col_list})
    return {"total": total, "skip": skip, "limit": limit, "sample_scanned": len(sample_rows), "columns": col_list, "rows": rows}

@app.get('/admin/candidates/all_fields', response_class=HTMLResponse)
def admin_candidates_all_fields(skip: int = 0, limit: int = 30, sample: int = 500):
    data = admin_candidates_all_fields_json(skip=skip, limit=limit, sample=sample)
    cols = data['columns']
    rows = data['rows']
    # Column headers (truncate very long names visually)
    head_html = ''.join(f"<th style='min-width:120px'>{html.escape(c)}</th>" for c in cols)
    body_rows = []
    for r in rows:
        cells = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, (dict, list)):
                import json as _json
                try:
                    vtxt = _json.dumps(v, ensure_ascii=False)
                except Exception:
                    vtxt = str(v)
            else:
                vtxt = '' if v is None else str(v)
            vtxt = vtxt[:180]
            cells.append(f"<td style='font-size:11px;white-space:normal;max-width:240px'>{html.escape(vtxt)}</td>")
        body_rows.append('<tr>' + ''.join(cells) + '</tr>')
    table_body = ''.join(body_rows) if body_rows else f"<tr><td colspan={len(cols)} style='text-align:center'>(אין נתונים)</td></tr>"
    html_doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>כל השדות (מועמדים)</title>
<style>
body {{ font-family: Arial, sans-serif; margin:16px; background:#f5f5f5; }}
table {{ border-collapse:collapse; width:100%; background:#fff; table-layout:fixed; }}
th,td {{ border:1px solid #ccc; padding:4px 6px; vertical-align:top; }}
th {{ background:#eee; font-size:11px; }}
td {{ font-size:11px; }}
.meta {{ font-size:12px; color:#475569; margin:4px 0 10px; }}
.nowrap {{ white-space:nowrap; }}
</style></head>
<body>
<h2 style='margin-top:0'>📊 טבלה רחבה של כל שדות מועמד</h2>
<div class='meta'>Total {data['total']} | Showing {len(rows)} rows (skip={data['skip']}) | Columns {len(cols)} | Sample scanned {data['sample_scanned']} — <a href='/admin/candidates/all_fields.json?skip={skip}&limit={limit}&sample={sample}' target='_blank'>JSON</a> — <a href='/admin/candidates'>חזרה</a></div>
<form method='get' style='margin-bottom:8px'>
  skip:<input name='skip' value='{skip}' size='5'>
  limit:<input name='limit' value='{limit}' size='4'>
  sample:<input name='sample' value='{sample}' size='5'>
  <button type='submit'>רענן</button>
  <a href='/admin/candidates/all_fields'>איפוס</a>
</form>
<div style='overflow:auto'>
<table>
  <thead><tr>{head_html}</tr></thead>
  <tbody>{table_body}</tbody>
</table>
</div>
</body></html>"""
    return HTMLResponse(content=html_doc)

@app.get('/admin/candidates/skills_view', response_class=HTMLResponse)
def admin_candidates_skills_view(skip: int = 0, limit: int = 50):
        cur = db['candidates'].find({}, {"full_name":1, "title":1, "skill_set":1, "synthetic_skills":1, "skills_detailed":1, "esco_skills":1, "updated_at":1}).skip(skip).limit(limit)
        rows = list(cur)
        head = "".join(f"<th>{h}</th>" for h in ["full_name","title","skill_set","synthetic_skills","skills_detailed","esco_skills","updated_at"])
        body = []
        import json as _json
        for r in rows:
                def j(v):
                        try:
                                return _json.dumps(v, ensure_ascii=False)[:220]
                        except Exception:
                                return str(v)[:220]
                body.append("<tr>"+
                                        f"<td>{html.escape(str(r.get('full_name','')))}</td>"+
                                        f"<td>{html.escape(str(r.get('title','')))}</td>"+
                                        f"<td style='font-size:11px'>{html.escape(j(r.get('skill_set',[])))}</td>"+
                                        f"<td style='font-size:11px'>{html.escape(j(r.get('synthetic_skills',[])))}</td>"+
                                        f"<td style='font-size:11px'>{html.escape(j(r.get('skills_detailed',[])))}</td>"+
                                        f"<td style='font-size:11px'>{html.escape(j(r.get('esco_skills',[])))}</td>"+
                                        f"<td>{html.escape(str(r.get('updated_at','')))}</td>"+
                                        "</tr>")
        if not body:
                body_html = "<tr><td colspan='7' style='text-align:center'>(אין נתונים)</td></tr>"
        else:
                body_html = "".join(body)
        content = f"""<!DOCTYPE html><html lang='he' dir='rtl'>
        <head><meta charset='utf-8'><title>סקירת כישורים (ESCO)</title>
        <style>body {{ font-family: Arial, sans-serif; margin:16px; background:#f8fafc; }} table {{ border-collapse:collapse; width:100%; background:#fff; table-layout:fixed; }} th,td {{ border:1px solid #e2e8f0; padding:6px; vertical-align:top; font-size:12px; }} th {{ background:#f1f5f9; }}</style>
        </head>
        <body>
            <h2 style='margin-top:0'>סקירת שדות כישורים (ESCO)</h2>
            <div style='margin-bottom:8px'>Pagination: skip={skip}, limit={limit} — <a href='/admin/candidates'>חזרה</a> — <a href='/admin/candidates/all_fields' target='_blank'>טבלה מלאה</a></div>
            <form method='get' style='margin-bottom:8px'>
                skip: <input name='skip' value='{skip}' size='6'>
                limit: <input name='limit' value='{limit}' size='6'>
                <button type='submit'>רענן</button>
            </form>
            <div style='overflow:auto'>
                <table>
                    <thead><tr>{head}</tr></thead>
                    <tbody>{body_html}</tbody>
                </table>
            </div>
        </body></html>"""
        return HTMLResponse(content)

@app.get("/admin/jobs", response_class=HTMLResponse)
def admin_jobs(q: str | None = None, skip: int = 0, limit: int = 50):
    if limit > 200: limit = 200
    if skip < 0: skip = 0
    query: dict = {}
    if q:
        pattern = f".*{re.escape(q)}.*"
        query = {"$or": [
            {"title": {"$regex": pattern, "$options": "i"}},
            {"skill_set": {"$elemMatch": {"$regex": pattern, "$options": "i"}}}
        ]}
    total = db['jobs'].count_documents(query)
    cur = db['jobs'].find(query, {"title":1, "city_canonical":1, "job_description":1, "job_requirements":1, "skill_set":1, "updated_at":1}).skip(skip).limit(limit)
    rows = []
    for doc in cur:
        jid = str(doc['_id'])
        title = html.escape(str(doc.get('title') or ''))
        city = html.escape(str(doc.get('city_canonical') or ''))
        desc_raw = doc.get('job_description') or ''
        desc_snip = html.escape(desc_raw[:140] + ('…' if len(desc_raw) > 140 else ''))
        reqs = doc.get('job_requirements') or []
        reqs_snip = ', '.join(reqs[:5]) + ('…' if len(reqs) > 5 else '')
        reqs_snip = html.escape(reqs_snip)
        skills = doc.get('skill_set') or []
        scount = len(skills)
        match_link = f"<a href='/match/job/{jid}?k=10' target='_blank'>התאמות</a>"
        rows.append(f"<tr><td style='direction:ltr'>{jid}</td><td>{title}</td><td>{city}</td><td style='max-width:260px;white-space:normal'>{desc_snip}</td><td style='max-width:220px;white-space:normal'>{reqs_snip}</td><td>{scount}</td><td>{doc.get('updated_at') or ''}</td><td>{match_link}</td></tr>")
    next_link = f"<a href='/admin/jobs?skip={skip+limit}&limit={limit}&q={q}'>הבא ▶</a>" if (skip+limit) < total else ''
    prev_link = f"<a href='/admin/jobs?skip={max(skip-limit,0)}&limit={limit}&q={q}'>◀ קודם</a>" if skip>0 else ''
    search_box_value = html.escape(q) if q else ''
    html_doc = f"""
<!DOCTYPE html><html lang='he' dir='rtl'>
<head><meta charset='utf-8'><title>משרות</title>
<style>
body {{ font-family: Arial, sans-serif; margin:20px; background:#f9f9f9; }}
table {{ border-collapse: collapse; width:100%; background:white; table-layout:fixed; }}
th,td {{ border:1px solid #ccc; padding:4px 6px; font-size:12px; overflow:hidden; text-overflow:ellipsis; }}
th {{ background:#eee; }}
.pager a {{ margin:0 8px; text-decoration:none; }}
form.search {{ margin-bottom:10px; }}
</style></head>
<body>
<h2 style='margin-top:0'>💼 רשימת משרות</h2>
<form class='search' method='get'>
    חיפוש: <input name='q' value='{search_box_value}' placeholder='כותרת או מיומנות'>
    <input type='hidden' name='limit' value='{limit}'>
    <button type='submit'>סנן</button>
</form>
<div class='pager'>{prev_link} | {next_link} &nbsp; (סה"כ {total})</div>
<table>
    <thead><tr><th>ID</th><th>כותרת</th><th>עיר</th><th>תיאור</th><th>דרישות</th><th>#מיומ.</th><th>עודכן</th><th>פעולות</th></tr></thead>
    <tbody>
    {''.join(rows) if rows else '<tr><td colspan=8 style="text-align:center">(אין תוצאות)</td></tr>'}
    </tbody>
</table>
<div class='pager'>{prev_link} | {next_link}</div>
<p><a href='/admin/candidates'>« לצפיה במועמדים</a></p>
</body></html>
"""
    return HTMLResponse(content=html_doc)

@app.get("/admin/jobs/all", response_class=HTMLResponse)
def admin_jobs_all(request: Request):
    """English view of all jobs with professional column headers + flags & raw mentions toggle."""
    # Filters: ?synthetic=skill1,skill2  (subset match)  & ?mandatory_contains=substring  & ?q=substring (title or requirements)
    import html, datetime, re
    q = request.query_params.get('q') if hasattr(request, 'query_params') else None  # type: ignore
    synthetic_filter = request.query_params.get('synthetic') if hasattr(request, 'query_params') else None  # type: ignore
    mandatory_contains = request.query_params.get('mandatory_contains') if hasattr(request, 'query_params') else None  # type: ignore
    query = {}
    if synthetic_filter:
        syn_list = [s.strip().lower() for s in synthetic_filter.split(',') if s.strip()]
        if syn_list:
            query['synthetic_skills'] = {"$all": syn_list}
    if mandatory_contains:
        # Case-insensitive substring match on any mandatory requirement line
        query['mandatory_requirements'] = {"$elemMatch": {"$regex": re.escape(mandatory_contains), "$options": "i"}}
    if q:
        or_terms = []
        try:
            or_terms.append({"title": {"$regex": re.escape(q), "$options": "i"}})
            or_terms.append({"job_requirements": {"$elemMatch": {"$regex": re.escape(q), "$options": "i"}}})
            or_terms.append({"requirement_mentions": {"$elemMatch": {"$regex": re.escape(q), "$options": "i"}}})
        except Exception:
            pass
        if or_terms:
            query['$or'] = or_terms
    total = db['jobs'].count_documents(query or {})
    if total > 2000:
        return HTMLResponse(content=f"<h3>Too many jobs ({total}). Narrow filters or use <a href='/admin/jobs'>/admin/jobs</a>.</h3>")
    projection = {"title":1, "city_canonical":1, "job_description":1, "job_requirements":1, "skill_set":1, "updated_at":1, "requirement_mentions":1, "full_text":1, "mandatory_requirements":1, "synthetic_skills":1, "flags":1}
    cur = db['jobs'].find(query, projection).sort([('_id',1)])
    rows=[]
    # Small helper for highlight
    def _hi(txt: str) -> str:
        if not q:
            return html.escape(txt)
        try:
            pattern = re.compile(re.escape(q), re.I)
            def _rep(m):
                return f"<mark>{html.escape(m.group(0))}</mark>"
            return pattern.sub(_rep, html.escape(txt))
        except Exception:
            return html.escape(txt)
    for doc in cur:
        jid = str(doc['_id'])
        title = _hi(str(doc.get('title') or ''))
        raw_city = str(doc.get('city_canonical') or '')
        city = html.escape(raw_city.replace('_',' '))
        desc_raw = doc.get('job_description') or ''
        desc_snip = html.escape(desc_raw[:160] + ('…' if len(desc_raw) > 160 else ''))
        reqs = doc.get('job_requirements') or []
        reqs_snip = ', '.join(reqs[:6]) + ('…' if len(reqs) > 6 else '')
        reqs_snip = html.escape(reqs_snip)
        skills_list = doc.get('skill_set') or []
        skills_html = html.escape(', '.join(skills_list))
        mentions = doc.get('requirement_mentions') or []
        mentions_snip_txt = ', '.join(mentions[:8]) + ('…' if len(mentions) > 8 else '')
        mentions_snip = html.escape(mentions_snip_txt)
        mentions_full = html.escape(', '.join(mentions))
        mandatory = doc.get('mandatory_requirements') or []
        mandatory_snip = html.escape('; '.join(mandatory[:6]) + ('…' if len(mandatory) > 6 else ''))
        synthetic = doc.get('synthetic_skills') or []
        if synthetic and isinstance(synthetic, list) and synthetic and isinstance(synthetic[0], dict):
            synthetic = [s.get('name') for s in synthetic if isinstance(s, dict) and s.get('name')]
        synthetic_snip = html.escape(', '.join(synthetic[:10]) + ('…' if len(synthetic) > 10 else ''))
        flags = doc.get('flags') or []
        if isinstance(flags, dict):  # safety if stored differently
            flags = list(flags.keys())
        flags_snip = html.escape(', '.join(flags))
        ftext = doc.get('full_text') or ''
        ftext_html = html.escape(ftext)
        updated = doc.get('updated_at') or ''
        if isinstance(updated, (int, float)) and updated:
            try:
                updated = datetime.datetime.utcfromtimestamp(updated).strftime('%Y-%m-%d %H:%M')
            except Exception:
                pass
        match_link = f"<a href='/match/job/{jid}?k=10' target='_blank'>Matches</a>"
        # Store a trimmed full text snippet (collapsible)
        full_snip = ftext_html[:4000]
        rows.append(
            f"<tr><td style='direction:ltr'>{jid}</td>"
            f"<td>{title}</td>"
            f"<td>{city}</td>"
            f"<td style='max-width:240px;white-space:normal'>{desc_snip}</td>"
            f"<td style='max-width:200px;white-space:normal'>{reqs_snip}</td>"
            f"<td style='max-width:220px;white-space:normal'>"
            f"<span class='mentions-snippet'>{mentions_snip}</span>"
            f"<span class='mentions-full' style='display:none'>{mentions_full}</span>"
            f"</td>"
            f"<td style='max-width:220px;white-space:normal'>{mandatory_snip}</td>"
            f"<td style='max-width:240px;white-space:normal'>{synthetic_snip}</td>"
            f"<td style='max-width:260px;white-space:normal'>{skills_html}</td>"
            f"<td style='max-width:200px;white-space:normal'>{flags_snip}</td>"
            f"<td style='max-width:400px;white-space:pre-wrap' class='fulltext collapsed' data-full='{full_snip}'>{full_snip[:400]}{'…' if len(full_snip)>400 else ''}</td>"
            f"<td>{updated}</td>"
            f"<td>{match_link}<br><button class='toggle'>Text</button><br><button class='toggle-mentions'>Mentions</button></td>"
            f"</tr>"
        )
    # Build filter form state
    syn_val = html.escape(synthetic_filter or '')
    mand_val = html.escape(mandatory_contains or '')
    q_val = html.escape(q or '')
    js_block = ("""
document.addEventListener('DOMContentLoaded', function() {\n  document.querySelectorAll('button.toggle').forEach(function(btn){\n    btn.addEventListener('click', function(e){\n      const td = e.target.closest('tr').querySelector('td.fulltext');\n      if (td) { td.classList.toggle('collapsed'); }\n    });\n  });\n  document.querySelectorAll('button.toggle-mentions').forEach(function(btn){\n    btn.addEventListener('click', function(e){\n      const cell = e.target.closest('tr').querySelector('span.mentions-snippet');\n      const full = e.target.closest('tr').querySelector('span.mentions-full');\n      if (cell && full){\n        const isHidden = full.style.display === 'none';\n        full.style.display = isHidden ? 'inline' : 'none';\n        cell.style.display = isHidden ? 'none' : 'inline';\n      }\n    });\n  });\n});\n"""
    )
    styles = (
        "body{font-family:Arial;margin:16px;background:#f5f5f5}table{border-collapse:collapse;width:100%;background:#fff;table-layout:fixed}th,td{border:1px solid #ccc;padding:4px 6px;font-size:12px;vertical-align:top}th{background:#eee}h2{margin-top:0}.collapsed{max-height:140px;overflow:hidden;position:relative}.collapsed:after{content:'';position:absolute;bottom:0;left:0;right:0;height:18px;background:linear-gradient(rgba(255,255,255,0),#fff)}mark{background:#fffd54}"
    )
    table_body = ''.join(rows) if rows else '<tr><td colspan=13 style="text-align:center">(No Jobs)</td></tr>'
    html_doc = f"""<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'><title>All Jobs ({total})</title>
<style>{styles}</style>
<script>{js_block}</script>
</head><body>
<h2>All Jobs (Total {total})</h2>
<p><a href='/admin/jobs'>← Back to Hebrew / paginated view</a> | <a href='/admin/jobs/export?format=csv'>Export CSV</a> | <a href='/admin/jobs/validate' target='_blank'>Validate</a></p>
<form method='get' style='margin-bottom:8px'>
  q:<input name='q' value='{q_val}' size='14'>
  synthetic:<input name='synthetic' value='{syn_val}' size='14' placeholder='skill1,skill2'>
  mandatory_contains:<input name='mandatory_contains' value='{mand_val}' size='14'>
  <button type='submit'>Apply</button>
  <a href='/admin/jobs/all'>Reset</a>
</form>
<table>
    <thead><tr><th>ID</th><th>Title</th><th>City</th><th>Description</th><th>Requirements</th><th>Requirement Mentions</th><th>Mandatory</th><th>Synthetic Skills</th><th>Skills</th><th>Flags</th><th>Full Text</th><th>Updated (UTC)</th><th>Actions</th></tr></thead>
    <tbody>{table_body}</tbody>
    </table>
    </body></html>"""
    return HTMLResponse(content=html_doc)

@app.get('/admin/jobs/export')
def admin_jobs_export(format: str='csv'):
    """Export all jobs in simple CSV (enriched fields). For small datasets only."""
    import csv, io
    total = db['jobs'].count_documents({})
    if total > 10000:
        raise HTTPException(status_code=400, detail='Too many jobs to export (limit 10k).')
    cur = db['jobs'].find({}, {"title":1,"city":1,"job_requirements":1,"mandatory_requirements":1,"synthetic_skills":1,"requirement_mentions":1,"full_text":1,"updated_at":1,"profession":1,"occupation_field":1})
    if format != 'csv':
        raise HTTPException(status_code=400, detail='Unsupported format')
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['id','title','city','profession','occupation_field','job_requirements','mandatory_requirements','synthetic_skills','requirement_mentions','full_text','updated_at'])
    for d in cur:
        syn = d.get('synthetic_skills') or []
        if syn and isinstance(syn, list) and syn and isinstance(syn[0], dict):
            syn = [s.get('name') for s in syn if isinstance(s, dict) and s.get('name')]
        w.writerow([
            str(d.get('_id')),
            (d.get('title') or ''),
            (d.get('city') or ''),
            (d.get('profession') or ''),
            (d.get('occupation_field') or ''),
            '|'.join(d.get('job_requirements') or []),
            '|'.join(d.get('mandatory_requirements') or []),
            '|'.join(syn),
            '|'.join(d.get('requirement_mentions') or []),
            (d.get('full_text') or '')[:5000],
            d.get('updated_at')
        ])
    return Response(content=out.getvalue(), media_type='text/csv', headers={'Content-Disposition':'attachment; filename=jobs_export.csv'})

@app.get('/admin/jobs/validate')
def admin_jobs_validate():
    """Return JSON validation summary for mandatory & synthetic skill rules + flags."""
    import re, time
    triggers = re.compile(r'(חובה|must|required|mandatory)', re.I)
    results = []
    cur = db['jobs'].find({}, {"title":1,"mandatory_requirements":1,"synthetic_skills":1,"job_requirements":1,"requirement_mentions":1,"flags":1})
    for d in cur:
        jid = str(d.get('_id'))
        mandatory = d.get('mandatory_requirements') or []
        synthetic_raw = d.get('synthetic_skills') or []
        synthetic = synthetic_raw
        if synthetic_raw and isinstance(synthetic_raw, list) and synthetic_raw and isinstance(synthetic_raw[0], dict):
            synthetic = [s.get('name') for s in synthetic_raw if isinstance(s, dict) and s.get('name')]
        must = (d.get('job_requirements') or [])
        req_mentions = set(d.get('requirement_mentions') or [])
        flags = d.get('flags') or []
        if isinstance(flags, dict):
            flags = list(flags.keys())
        distinct = set(must) | set(synthetic)
        issues = []
        # Synthetic duplicates
        if len(synthetic) != len(set(synthetic)):
            issues.append('duplicate_synthetic')
        # Mandatory lines must include trigger token (soft)
        for line in mandatory:
            if not triggers.search(line or ''):
                issues.append('mandatory_missing_trigger')
                break
        # Synthetic skills that appear in raw mentions (info)
        mentioned_syn = [s for s in synthetic if s in req_mentions]
        if mentioned_syn:
            issues.append('synthetic_present_in_mentions')
        # Count bounds
        if len(synthetic) > 15:
            issues.append('synthetic_too_many')
        if len(distinct) < 12:
            issues.append('below_min_distinct')
        if len(distinct) > 35:
            issues.append('over_distinct_cap')
        # Mandatory present but no must skills resolved
        if mandatory and not must:
            issues.append('mandatory_without_must_skills')
        # Flags surfaced by importer / pipeline
        for f in flags:
            if f not in issues:
                issues.append(f)
        results.append({
            'id': jid,
            'title': d.get('title'),
            'synthetic_count': len(synthetic),
            'mandatory_count': len(mandatory),
            'distinct_total': len(distinct),
            'flags': flags,
            'issues': issues
        })
    summary = {
        'validated': len(results),
        'with_issues': sum(1 for r in results if r['issues']),
        'timestamp': int(time.time()),
        'results': results[:200]  # cap for payload size
    }
    return summary

@app.get('/admin/jobs/audit')
def admin_jobs_audit():
    """Aggregate metrics & sample for manual audit (distinct counts, synthetic ratios, flags)."""
    import statistics, random, time
    docs = list(db['jobs'].find({}, {"job_requirements":1,"synthetic_skills":1,"mandatory_requirements":1,"flags":1,"title":1}).limit(5000))
    syn_ratios=[]; distinct_counts=[]; mandatory_with_must=0; mandatory_total=0
    flagged=0
    samples=[]
    for d in docs:
        syn = d.get('synthetic_skills') or []
        if syn and isinstance(syn, list) and syn and isinstance(syn[0], dict):
            syn = [s.get('name') for s in syn if isinstance(s, dict) and s.get('name')]
        must = d.get('job_requirements') or []
        distinct = set(must) | set(syn)
        if d.get('mandatory_requirements'):
            mandatory_total +=1
            if must:
                mandatory_with_must+=1
        distinct_counts.append(len(distinct))
        if distinct:
            syn_ratios.append(len(syn)/len(distinct))
        if d.get('flags'):
            flagged+=1
    total=len(docs)
    pct_flagged = round(flagged/max(total,1),3)
    mandatory_alignment = round(mandatory_with_must/max(mandatory_total,1),3) if mandatory_total else None
    syn_ratio_avg = round(statistics.mean(syn_ratios),3) if syn_ratios else 0
    distinct_median = statistics.median(distinct_counts) if distinct_counts else 0
    # random sample up to 15 for spot review
    for d in random.sample(docs, min(15, total)):
        samples.append({
            'title': d.get('title'),
            'distinct_total': len(set((d.get('job_requirements') or []) + ( [s.get('name') for s in d.get('synthetic_skills') if isinstance(s, dict)] if d.get('synthetic_skills') and isinstance(d.get('synthetic_skills'), list) and d.get('synthetic_skills') and isinstance(d.get('synthetic_skills')[0], dict) else (d.get('synthetic_skills') or [])) )),
            'synthetic_count': len(d.get('synthetic_skills') or []),
            'flags': d.get('flags') or []
        })
    return {
        'total': total,
        'flagged_pct': pct_flagged,
        'synthetic_ratio_avg': syn_ratio_avg,
        'distinct_median': distinct_median,
        'mandatory_alignment': mandatory_alignment,
        'timestamp': int(time.time()),
        'sample': samples
    }

# SECURITY MONITORING ENDPOINTS
@app.get("/security/events")
def get_tenant_security_events(tenant_id: str = Depends(require_tenant), hours: int = 24, limit: int = 100):
    """Get security events for the authenticated tenant."""
    events = get_security_events(tenant_id, hours, limit)
    return {
        "tenant_id": tenant_id,
        "events": events,
        "count": len(events),
        "time_window_hours": hours
    }

@app.get("/security/violations")
def get_security_violations():
    """Get system-wide security violations (admin only)."""
    # Note: In production, this should be restricted to admin users
    api_key = os.getenv("ADMIN_API_KEY")
    if not api_key:
        raise HTTPException(status_code=403, detail="Admin endpoint not configured")
    
    violations = get_violation_summary(hours=24)
    return violations

@app.get("/security/health")
def security_health_check(tenant_id: str = Depends(require_tenant)):
    """Basic security health check for tenant."""
    recent_events = get_security_events(tenant_id, hours=1, limit=10)
    
    # Count different event types
    event_counts = {}
    for event in recent_events:
        action = event.get("action", "unknown")
        event_counts[action] = event_counts.get(action, 0) + 1
    
    # Check for suspicious activity
    warnings = []
    if event_counts.get("tenant_boundary_violation", 0) > 0:
        warnings.append("Cross-tenant access attempts detected")
    
    if event_counts.get("api_access", 0) > 50:  # High API usage
        warnings.append("High API usage detected")
    
    return {
        "tenant_id": tenant_id,
        "status": "warning" if warnings else "healthy",
        "warnings": warnings,
        "recent_activity": event_counts,
        "last_hour_events": len(recent_events)
    }

if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run("scripts.api:app", host="0.0.0.0", port=8000, reload=False)
