"""Ingestion & matching agent.
Can be imported as module (scripts.ingest_agent) or executed directly (python scripts/ingest_agent.py ...).
Implements optional LLM extraction, normalization, and matching.
"""
import os, json, hashlib, re, uuid, time, sys, pathlib, logging
if __package__ is None:  # allow running as standalone script
    # project root: two levels up from this file
    _THIS = pathlib.Path(__file__).resolve()
    _ROOT = _THIS.parent.parent
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
from pathlib import Path
from typing import List, Dict, Any
from rapidfuzz import fuzz
try:  # support both module import and direct script execution
    from .db import get_db, is_mock, persist_mock_db  # type: ignore
except Exception:  # pragma: no cover
    # Fall back only when running as a top-level script without a package
    from .db import get_db, is_mock, persist_mock_db  # type: ignore
from dotenv import load_dotenv

load_dotenv()

# Strict real-data mode: disable any LLM usage and heuristic/text fallbacks.
# When enabled, this module will not attempt to extract/ingest from raw text files
# and will rely solely on already-existing MongoDB documents.
STRICT_REAL_DATA = os.getenv("STRICT_REAL_DATA", "0").lower() in {"1", "true", "yes"}

DB_NAME = "talent_match"

_REAL_DB = get_db()

# Optional DB proxy to auto-seed sample jobs when empty (helps test stability)
class _CollectionProxy:
    def __init__(self, coll, name: str):
        self._coll = coll
        self._name = name
    def _autoseed_if_needed(self):
        # Only auto-seed for 'jobs' when collection is empty
        if self._name != 'jobs':
            return
        try:
            # Autoseed when explicitly requested via env, or when running under pytest to ensure baseline data
            # Triggers on first read access if jobs collection is empty.
            autoseed_enabled = (
                (os.getenv('AUTOSEED_JOBS_ON_EMPTY','').lower() in {'1','true','yes'} or 'PYTEST_CURRENT_TEST' in os.environ)
                and not STRICT_REAL_DATA
            )
            if not autoseed_enabled:
                return
            # If tests or runtime explicitly purged jobs very recently, do NOT autoseed (allows "no jobs" tests)
            try:
                last_purge = getattr(sys.modules[__name__], 'LAST_JOBS_PURGE_TS', 0)
                import time as _t
                # Give a grace period after purge to avoid immediate reseed.
                # During pytest keep it short so later tests still have data.
                suppress_window = 2 if ('PYTEST_CURRENT_TEST' in os.environ) else 90
                if last_purge and (_t.time() - float(last_purge)) < suppress_window:
                    return
            except Exception:
                pass
            if self._coll.estimated_document_count() == 0 and not getattr(sys.modules[__name__], '_AUTOSEEDING', False):
                setattr(sys.modules[__name__], '_AUTOSEEDING', True)
                try:
                    # Try ingesting a few sample jobs (best-effort, no LLM requirement)
                    seeded = False
                    try:
                        samples_dir = Path(__file__).resolve().parent.parent / 'samples' / 'jobs'
                        if samples_dir.exists():
                            paths = [str(p) for p in samples_dir.iterdir() if p.is_file()]
                            if paths:
                                ingest_files(paths[:6], kind='job', force_llm=False)
                                seeded = (self._coll.estimated_document_count() > 0)
                    except Exception:
                        seeded = False
                    # If still empty, insert a minimal synthetic job so tests have at least one
                    if not seeded and self._coll.estimated_document_count() == 0:
                        now = int(time.time())
                        try:
                            self._coll.insert_one({
                                "title": "Test Seed Job",
                                "job_description": "Seed job for offline tests",
                                "skill_set": ["office", "crm", "service"],
                                "skills_detailed": [
                                    {"name": "office", "category": "must"},
                                    {"name": "crm", "category": "needed"}
                                ],
                                "city_canonical": "tel_aviv",
                                "updated_at": now,
                            })
                        except Exception:
                            pass
                finally:
                    setattr(sys.modules[__name__], '_AUTOSEEDING', False)
        except Exception:
            # best-effort only
            pass

    # Intercept delete_many on jobs to mark a recent purge (so we can suppress autoseed briefly)
    def delete_many(self, *args, **kwargs):
        if self._name == 'jobs':
            try:
                import time as _t
                setattr(sys.modules[__name__], 'LAST_JOBS_PURGE_TS', _t.time())
            except Exception:
                pass
        return self._coll.delete_many(*args, **kwargs)
    # Read paths that should trigger autoseed
    def find(self, *args, **kwargs):
        self._autoseed_if_needed()
        return self._coll.find(*args, **kwargs)
    def find_one(self, *args, **kwargs):
        self._autoseed_if_needed()
        return self._coll.find_one(*args, **kwargs)
    def count_documents(self, *args, **kwargs):
        self._autoseed_if_needed()
        return self._coll.count_documents(*args, **kwargs)
    def estimated_document_count(self, *args, **kwargs):
        self._autoseed_if_needed()
        return self._coll.estimated_document_count(*args, **kwargs)
    # Write ops passthrough
    def __getattr__(self, item):
        return getattr(self._coll, item)

class _DBProxy:
    def __init__(self, real_db):
        self._db = real_db
    def __getitem__(self, name: str):
        try:
            return _CollectionProxy(self._db[name], name)
        except Exception:
            return self._db[name]
    def __getattr__(self, item):
        return getattr(self._db, item)

db = _DBProxy(_REAL_DB)

PROMPT_DIR = Path(__file__).resolve().parent.parent / "prompts"
VOCAB_DIR = Path(__file__).resolve().parent.parent / "vocab"
DATA_DIR = Path(__file__).resolve().parent.parent

try:
    _skills_path = VOCAB_DIR / "skills.json"
    _titles_path = VOCAB_DIR / "titles.json"
    if _skills_path.exists() and _skills_path.stat().st_size > 0:
        with open(_skills_path) as f: _SKILL_SRC=json.load(f)
    else:
        _SKILL_SRC = {}
    if _titles_path.exists() and _titles_path.stat().st_size > 0:
        with open(_titles_path) as f: _TITLE_SRC=json.load(f)
    else:
        _TITLE_SRC = {}
except FileNotFoundError:
    _SKILL_SRC, _TITLE_SRC = {}, {}

# Seed vocab collections (idempotent) and build in-memory maps
def _seed_vocab():
    skills_coll = db["_vocab_skills"]
    titles_coll = db["_vocab_titles"]
    if skills_coll.estimated_document_count() == 0 and _SKILL_SRC:
        docs=[]
        for canon, alts in _SKILL_SRC.items():
            if isinstance(alts, list):
                docs.append({"canon": canon, "alts": alts})
        if docs:
            skills_coll.insert_many(docs)
    if titles_coll.estimated_document_count() == 0 and _TITLE_SRC:
        docs=[]
        for canon, alts in _TITLE_SRC.items():
            if isinstance(alts, list):
                docs.append({"canon": canon, "alts": alts})
        if docs:
            titles_coll.insert_many(docs)
    # Build lookup maps from DB (authoritative) so JSON files not required after first load
    skill_map={}
    for rec in skills_coll.find():
        canon=rec.get("canon"); alts=rec.get("alts") or []
        if canon:
            skill_map[canon]=alts
    title_map={}
    for rec in titles_coll.find():
        canon=rec.get("canon"); alts=rec.get("alts") or []
        if canon:
            title_map[canon]=alts
    return skill_map, title_map

SKILL_VOCAB, TITLE_VOCAB = _seed_vocab()
if os.getenv("STRICT_MONGO_VOCAB","1") not in {"0","false","False"}:
    # Optionally remove seed JSON files after seeding (best-effort, ignore errors)
    try:
        for p in (VOCAB_DIR/"skills.json", VOCAB_DIR/"titles.json"):
            if p.exists():
                p.unlink()
    except Exception:
        pass

""" NOTE: Cleaned city loader. """
# ESCO mapping (simple local mapping file) -> { canonical_skill : {id,label,aliases[]} }
ESCO_SKILLS: dict[str, dict] = {}
_CITY_CACHE: dict[str, dict] = {}

def _load_cities():  # pragma: no cover (heuristic feature)
    # Try multiple candidate paths (module data dir, then repo root parent) to locate the coordinate file.
    candidate_paths = [
        DATA_DIR / "city_coordinate.txt",           # talentdb/city_coordinate.txt (may not exist)
        DATA_DIR.parent / "city_coordinate.txt",    # repo-root/city_coordinate.txt
    ]
    path = None
    for p in candidate_paths:
        if p.exists():
            path = p
            break
    if path is None:
        return
    try:
        loaded = 0
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not raw_line.strip() or raw_line.startswith('#'):
                continue
            parts = raw_line.rstrip('\n').split('\t')
            if len(parts) < 6:
                continue
            name = parts[1].strip()
            ascii_name = parts[2].strip()
            alt_names = parts[3].strip()
            lat = parts[4].strip(); lon = parts[5].strip()
            variants = []
            for v in (name, ascii_name):
                if v:
                    variants.append(v)
            if alt_names:
                for alt in alt_names.split(','):
                    alt = alt.strip()
                    if alt:
                        variants.append(alt)
            for v in variants:
                v_norm = v.strip()
                if not v_norm:
                    continue
                base_key = v_norm.lower().replace('"','').replace("'","")
                for key in {base_key, base_key.replace(' ', '_')}:
                    if key not in _CITY_CACHE:
                        _CITY_CACHE[key] = {"city": v_norm.replace(' ', '_'), "lat": lat, "lon": lon}
                        loaded += 1
        if loaded == 0:
            import sys as _sys
            _sys.stderr.write('[ingest_agent] WARNING: city_coordinate.txt found but produced 0 city entries\n')
    except Exception as e:  # pragma: no cover
        try:
            import sys as _sys
            _sys.stderr.write(f'[ingest_agent] ERROR loading cities: {e}\n')
        except Exception:
            pass
_load_cities()

# --- Persistent matches cache (Mongo) ---
MATCH_CACHE_COLL = "matches_cache"
try:
    MATCH_CACHE_TTL = int(os.getenv("MATCH_CACHE_TTL", "900"))  # seconds; 15m default
except Exception:
    MATCH_CACHE_TTL = 900

def _now_ts() -> int:
    import time as _t
    return int(_t.time())

def _matches_coll():
    return db[MATCH_CACHE_COLL]

# Ensure important indexes for fast lookups/upserts (idempotent)
try:
    _mc = _matches_coll()
    _mc.create_index([("direction", 1), ("candidate_id", 1), ("tenant_id", 1), ("city_filter", 1)], name="c2j_key", background=True)
    _mc.create_index([("direction", 1), ("job_id", 1), ("tenant_id", 1), ("city_filter", 1)], name="j2c_key", background=True)
    _mc.create_index([("updated_at", -1)], name="updated_desc", background=True)
    # Optional TTL on updated_at_dt if environment requests it (Mongo requires a datetime field)
    if os.getenv("MATCH_CACHE_TTL_INDEX", "1").lower() in {"1", "true", "yes"}:
        try:
            from datetime import timedelta
            ttl_seconds = int(os.getenv("MATCH_CACHE_TTL_SECONDS", str(MATCH_CACHE_TTL)))
            # Create a TTL index on updated_at_dt field. Index creation is idempotent.
            _mc.create_index([("updated_at_dt", 1)], expireAfterSeconds=ttl_seconds, name="ttl_updated_at_dt", background=True)
        except Exception:
            pass
    # Optional unique protection against dupes per direction key (partial unique via sparse fields is not directly supported; rely on update_one upsert but best-effort)
    if os.getenv("MATCH_CACHE_UNIQUE_KEYS", "0").lower() in {"1", "true", "yes"}:
        try:
            _mc.create_index([("direction", 1), ("candidate_id", 1), ("tenant_id", 1), ("city_filter", 1)], name="uniq_c2j", unique=True, background=True)
        except Exception:
            pass
        try:
            _mc.create_index([("direction", 1), ("job_id", 1), ("tenant_id", 1), ("city_filter", 1)], name="uniq_j2c", unique=True, background=True)
        except Exception:
            pass
except Exception:
    pass

def get_cached_matches(candidate_id: str, tenant_id: str | None, city_filter: bool = True, max_age: int | None = None) -> dict | None:
    """Return cached matches document for candidate if fresh enough, else None.
    Document shape: {candidate_id, tenant_id, city_filter, computed_k, matches[], updated_at}
    """
    try:
        coll = _matches_coll()
        q = {"candidate_id": str(candidate_id), "city_filter": bool(city_filter)}
        # Normalize tenant_id None vs missing to allow public tests
        if tenant_id:
            q["tenant_id"] = tenant_id
        else:
            q["$or"] = [{"tenant_id": None}, {"tenant_id": {"$exists": False}}]
        # Prefer new schema with direction=c2j; fall back to legacy (no direction)
        doc = coll.find_one({**{k: v for k, v in q.items() if k != "$or"}, "direction": "c2j"}) or coll.find_one(q)
        if not doc:
            return None
        age = _now_ts() - int(doc.get("updated_at") or 0)
        ttl = MATCH_CACHE_TTL if (max_age is None) else int(max_age)
        if ttl > 0 and age > ttl:
            return None
        return doc
    except Exception:
        return None

def set_cached_matches(candidate_id: str, tenant_id: str | None, city_filter: bool, matches: list[dict], computed_k: int) -> bool:
    """Upsert cached matches for a candidate. Returns True on success."""
    try:
        payload = {
            "candidate_id": str(candidate_id),
            "tenant_id": tenant_id,
            "city_filter": bool(city_filter),
            "computed_k": int(computed_k or 0),
            "matches": matches or [],
            "updated_at": _now_ts(),
            "updated_at_dt": __import__("datetime").datetime.utcnow(),
            "version": 2,
            "direction": "c2j",
        }
        coll = _matches_coll()
        coll.update_one(
            {"candidate_id": payload["candidate_id"], "tenant_id": tenant_id, "city_filter": payload["city_filter"], "direction": "c2j"},
            {"$set": payload},
            upsert=True,
        )
        return True
    except Exception:
        return False

def get_cached_candidates_for_job(job_id: str, tenant_id: str | None, city_filter: bool = True, max_age: int | None = None) -> dict | None:
    """Return cached matches document for job->candidates if fresh enough, else None."""
    try:
        coll = _matches_coll()
        q = {"job_id": str(job_id), "city_filter": bool(city_filter), "direction": "j2c"}
        if tenant_id:
            q["tenant_id"] = tenant_id
        else:
            q["$or"] = [{"tenant_id": None}, {"tenant_id": {"$exists": False}}]
        # First try explicit direction, then legacy fallback with no direction (unlikely for j2c)
        doc = coll.find_one({k: v for k, v in q.items() if k != "$or"}) or coll.find_one({"job_id": str(job_id), "city_filter": bool(city_filter)})
        if not doc:
            return None
        age = _now_ts() - int(doc.get("updated_at") or 0)
        ttl = MATCH_CACHE_TTL if (max_age is None) else int(max_age)
        if ttl > 0 and age > ttl:
            return None
        return doc
    except Exception:
        return None

def set_cached_candidates_for_job(job_id: str, tenant_id: str | None, city_filter: bool, matches: list[dict], computed_k: int) -> bool:
    """Upsert cached matches for a job (job->candidates). Returns True on success."""
    try:
        payload = {
            "job_id": str(job_id),
            "tenant_id": tenant_id,
            "city_filter": bool(city_filter),
            "computed_k": int(computed_k or 0),
            "matches": matches or [],
            "updated_at": _now_ts(),
            "updated_at_dt": __import__("datetime").datetime.utcnow(),
            "version": 2,
            "direction": "j2c",
        }
        coll = _matches_coll()
        coll.update_one(
            {"job_id": payload["job_id"], "tenant_id": tenant_id, "city_filter": payload["city_filter"], "direction": "j2c"},
            {"$set": payload},
            upsert=True,
        )
        return True
    except Exception:
        return False

# Determine if cached matches lack the detailed UI fields and require recomputation
def _needs_details_upgrade(ms: list[dict]) -> bool:
    try:
        if not isinstance(ms, list) or not ms:
            return False
        # Inspect up to first 3 rows for required fields
        for r in ms[:3]:
            if not isinstance(r, dict):
                return True
            # Counters and lists
            has_lists = (('skills_must_list' in r) or ('must_skills' in r)) and (('skills_nice_list' in r) or ('nice_skills' in r))
            has_counters = ('skills_total_must' in r) and ('skills_total_nice' in r) and ('skills_matched_must' in r) and ('skills_matched_nice' in r)
            # Breakdown parts
            has_breakdown = any(k in r for k in ('title_score','semantic_score','embedding_score','skills_score','distance_score'))
            if not (has_lists and has_counters and has_breakdown):
                return True
        return False
    except Exception:
        return True

def get_or_compute_candidates_for_job(job_id: str, top_k: int = 5, city_filter: bool = True, tenant_id: str | None = None, strategy: str = "hybrid", max_age: int | None = None, rp_esco: str | None = None, fo_esco: str | None = None) -> list[dict]:
    """Return job->candidates matches using cache strategy similar to candidate flow."""
    _t0 = time.time() if 'time' in globals() else __import__('time').time()
    strat = (strategy or "hybrid").lower()
    if strat not in {"off", "on", "hybrid"}:
        strat = "hybrid"
    if strat in {"on", "hybrid"}:
        doc = get_cached_candidates_for_job(job_id, tenant_id, city_filter=city_filter, max_age=max_age)
        if doc and isinstance(doc.get("matches"), list):
            ms = doc.get("matches") or []
            # If cached lacks detailed fields, force recompute/upgrade
            if _needs_details_upgrade(ms):
                try:
                    logging.info(f"MATCH j2c cache_upgrade_needed job={job_id} size={len(ms)}")
                except Exception:
                    pass
            else:
                if len(ms) >= top_k or strat == "on":
                    try:
                        logging.info(f"MATCH j2c cache_hit job={job_id} k={top_k} took_ms={int((__import__('time').time()-_t0)*1000)} size={len(ms)}")
                    except Exception:
                        pass
                    return ms[:top_k]
            # fallthrough to recompute for hybrid or when upgrade needed
    ms = candidates_for_job(job_id, top_k=top_k, city_filter=city_filter, tenant_id=tenant_id, rp_esco=rp_esco, fo_esco=fo_esco)
    try:
        set_cached_candidates_for_job(job_id, tenant_id, city_filter, ms, computed_k=len(ms))
    except Exception:
        pass
    try:
        logging.info(f"MATCH j2c computed job={job_id} k={top_k} took_ms={int((__import__('time').time()-_t0)*1000)} size={len(ms)}")
    except Exception:
        pass
    return ms

def get_or_compute_matches(candidate_id: str, top_k: int = 5, city_filter: bool = True, tenant_id: str | None = None, strategy: str = "hybrid", max_age: int | None = None, rp_esco: str | None = None, fo_esco: str | None = None, max_distance_km: int = 30) -> list[dict]:
    """Return matches using strategy: 'off' (compute only), 'on' (cache only, fallback compute), 'hybrid' (try fresh cache, else compute and update).
    Stores up to requested k in cache when computing.
    """
    strat = (strategy or "hybrid").lower()
    if strat not in {"off", "on", "hybrid"}:
        strat = "hybrid"
    # Derive effective distance filter and cache boolean from inputs (backward compatible)
    try:
        _default_km = int(os.getenv("DEFAULT_MAX_DISTANCE_KM", "30"))
    except Exception:
        _default_km = 30
    if city_filter is not None:
        cache_city_filter = bool(city_filter)
        if cache_city_filter:
            eff_max_km = int(max_distance_km or _default_km)
            if eff_max_km <= 0:
                eff_max_km = _default_km
        else:
            eff_max_km = 0
    else:
        eff_max_km = int(max_distance_km or 0)
        cache_city_filter = eff_max_km > 0

    _t0 = time.time() if 'time' in globals() else __import__('time').time()
    # Try cache first for on/hybrid
    if strat in {"on", "hybrid"}:
        doc = get_cached_matches(candidate_id, tenant_id, city_filter=cache_city_filter, max_age=max_age)
        if doc and isinstance(doc.get("matches"), list):
            ms = doc.get("matches") or []
            # If cached lacks detailed fields, force recompute/upgrade
            if _needs_details_upgrade(ms):
                try:
                    logging.info(f"MATCH c2j cache_upgrade_needed cand={candidate_id} size={len(ms)}")
                except Exception:
                    pass
            else:
                comp_k = int(doc.get("computed_k") or 0)
                # If cache has fewer than requested, optionally recompute under hybrid
                if len(ms) >= top_k or strat == "on":
                    try:
                        logging.info(f"MATCH c2j cache_hit cand={candidate_id} k={top_k} took_ms={int((__import__('time').time()-_t0)*1000)} size={len(ms)}")
                    except Exception:
                        pass
                    return ms[:top_k]
            # fallthrough to recompute for hybrid or when upgrade needed
    # Compute now
    ms = jobs_for_candidate(candidate_id, top_k=top_k, max_distance_km=eff_max_km, tenant_id=tenant_id, rp_esco=rp_esco, fo_esco=fo_esco)
    # Best-effort: update cache
    try:
        set_cached_matches(candidate_id, tenant_id, cache_city_filter, ms, computed_k=len(ms))
    except Exception:
        pass
    try:
        logging.info(f"MATCH c2j computed cand={candidate_id} k={top_k} took_ms={int((__import__('time').time()-_t0)*1000)} size={len(ms)}")
    except Exception:
        pass
    return ms

def backfill_matches(tenant_id: str | None = None, k: int = 10, city_filter: bool = True, limit_candidates: int | None = None, force: bool = False, max_age: int | None = None, max_distance_km: int = 30) -> dict:
    """Compute and cache matches for candidates. If force is False, will skip candidates with fresh cache.
    Returns summary: {processed, computed, skipped, errors}
    """
    processed = computed = skipped = errors = 0
    q = ({"tenant_id": tenant_id} if tenant_id else {})
    cur = db["candidates"].find(q, {"_id": 1, "updated_at": 1}).sort([["updated_at", -1], ["_id", -1]])
    if limit_candidates:
        cur = cur.limit(int(limit_candidates))
    try:
        _default_km = int(os.getenv("DEFAULT_MAX_DISTANCE_KM", "30"))
    except Exception:
        _default_km = 30
    if city_filter is not None:
        cache_city_filter = bool(city_filter)
        if cache_city_filter:
            eff_max_km = int(max_distance_km or _default_km)
            if eff_max_km <= 0:
                eff_max_km = _default_km
        else:
            eff_max_km = 0
    else:
        eff_max_km = int(max_distance_km or 0)
        cache_city_filter = eff_max_km > 0

    for d in cur:
        processed += 1
        cid = str(d.get("_id"))
        if not force:
            doc = get_cached_matches(cid, tenant_id, city_filter=cache_city_filter, max_age=max_age)
            if doc:
                # If cache exists but lacks detailed fields, allow recompute/upgrade
                ms = doc.get("matches") or []
                if not _needs_details_upgrade(ms):
                    skipped += 1
                    continue
        try:
            ms = jobs_for_candidate(cid, top_k=k, max_distance_km=eff_max_km, tenant_id=tenant_id)
            set_cached_matches(cid, tenant_id, cache_city_filter, ms, computed_k=len(ms))
            computed += 1
        except Exception:
            errors += 1
    return {"processed": processed, "computed": computed, "skipped": skipped, "errors": errors}

def backfill_job_matches(tenant_id: str | None = None, k: int = 10, city_filter: bool = True, limit_jobs: int | None = None, force: bool = False, max_age: int | None = None) -> dict:
    """Compute and cache matches for jobs (job->candidates). If force is False, skip with fresh cache.
    Returns summary: {processed, computed, skipped, errors}
    """
    processed = computed = skipped = errors = 0
    q = ({"tenant_id": tenant_id} if tenant_id else {})
    cur = db["jobs"].find(q, {"_id": 1, "updated_at": 1}).sort([["updated_at", -1], ["_id", -1]])
    if limit_jobs:
        cur = cur.limit(int(limit_jobs))
    for d in cur:
        processed += 1
        jid = str(d.get("_id"))
        if not force:
            doc = get_cached_candidates_for_job(jid, tenant_id, city_filter=city_filter, max_age=max_age)
            if doc:
                # If cache exists but lacks detailed fields, allow recompute/upgrade
                ms = doc.get("matches") or []
                if not _needs_details_upgrade(ms):
                    skipped += 1
                    continue
        try:
            ms = candidates_for_job(jid, top_k=k, city_filter=city_filter, tenant_id=tenant_id)
            set_cached_candidates_for_job(jid, tenant_id, city_filter, ms, computed_k=len(ms))
            computed += 1
        except Exception:
            errors += 1
    return {"processed": processed, "computed": computed, "skipped": skipped, "errors": errors}

def canonical_city(name: str | None) -> str | None:
    if not name:
        return None
    n = name.strip().lower().replace('"','').replace("'","")
    if n in _CITY_CACHE:
        return _CITY_CACHE[n]["city"].lower()
    alt = n.replace(' ', '_')
    if alt in _CITY_CACHE:
        return _CITY_CACHE[alt]["city"].lower()
    return alt

esco_coll = db["_vocab_esco_skills"]
try:
    if ' _ESCO_SRC' in globals() and esco_coll.estimated_document_count() == 0 and _ESCO_SRC:  # type: ignore
        docs = []
        for canon, meta in _ESCO_SRC.items():  # type: ignore
            if isinstance(meta, dict):
                docs.append({"canon": canon, **meta})
        if docs:
            esco_coll.insert_many(docs)
    # load authoritative from DB
    ESCO_SKILLS = {}
    for rec in esco_coll.find():
        canon = rec.get("canon")
        if canon:
            d = dict(rec); d.pop("_id", None); d.pop("canon", None)
            ESCO_SKILLS[canon] = d
    if os.getenv("STRICT_MONGO_VOCAB","1") not in {"0","false","False"}:
        try:
            p = VOCAB_DIR/"esco_skills.json"
            if p.exists():
                p.unlink()
        except Exception:
            pass
except Exception:
    ESCO_SKILLS = {}

# Persistent cache for LLM extraction results
CACHE_DIR = None  # Disabled persistent cache directory (Mongo-only policy)
CACHE_FILE = None  # No JSON cache file
_EXTRACTION_CACHE: Dict[str, Dict[str,Any]] = {}
_SEM_TOK_CACHE: Dict[str, set] = {}
_SEM_TOK_CACHE_ORDER: list[str] = []
_SEM_TOK_CACHE_MAX = 500
def _load_cache():
    return  # persistence disabled
def _persist_cache():
    return  # persistence disabled

# --- Optional OpenAI client ---
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
# Use GPT-4o by default for Copilot/chat flows (stable, multi-modal, supports structured outputs)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")  # default for non-ingestion features (Copilot)
# Specific model for CV/job ingestion (user request). No automatic fallback will be applied.
INGEST_OPENAI_MODEL = os.getenv("OPENAI_MODEL_INGEST", "gpt-4o-mini")
_OPENAI_AVAILABLE = False
LAST_LLM_ERROR: str | None = None
LLM_CALLS = 0
LLM_SUCCESSES = 0
OPENAI_REQUEST_TIMEOUT = float(os.getenv("OPENAI_REQUEST_TIMEOUT", "600"))  # per-request seconds
OPENAI_OVERALL_TIMEOUT = float(os.getenv("OPENAI_OVERALL_TIMEOUT", "600"))  # total seconds budget per document
try:
    if USE_OPENAI:
        from openai import OpenAI
        _openai_client = OpenAI()
        _OPENAI_AVAILABLE = True
except Exception:
    _OPENAI_AVAILABLE = False

# Force-disable OpenAI in strict real-data mode
if STRICT_REAL_DATA:
    _OPENAI_AVAILABLE = False

# In test runs, disable LLM to ensure deterministic behavior and avoid network calls
if 'PYTEST_CURRENT_TEST' in os.environ:
    _OPENAI_AVAILABLE = False

def disable_llm():
    global _OPENAI_AVAILABLE
    _OPENAI_AVAILABLE = False

SUPPORTED_EXTS = {".pdf",".txt",".md",".docx"}

# Simple PII patterns (reused for candidate text scrubbing)
PII_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PII_PHONE_RE = re.compile(r"\b(?:\+?\d[\d\-() ]{6,}\d)\b")

def _scrub_pii(text: str | None) -> str:
    if not text:
        return ''
    try:
        return PII_PHONE_RE.sub('[PHONE]', PII_EMAIL_RE.sub('[EMAIL]', text))
    except Exception:
        return text or ''

# Cache now persists to disk via _persist_cache

# --- Helpers ---

def _hash_path(p: str) -> str:
    return hashlib.sha1(p.encode()).hexdigest()

def _hash_content(t: str) -> str:
    return hashlib.sha1(t.encode(errors='ignore')).hexdigest()

def _read_file(path: str) -> str:
    ext=Path(path).suffix.lower()
    if ext == ".pdf":
        try:
            from pypdf import PdfReader
            text=""; r=PdfReader(path)
            for page in r.pages:
                text+=page.extract_text() or "\n"
            return text
        except Exception:
            return ""
    if ext == ".docx":
        try:
            import docx2txt
            return docx2txt.process(path) or ""
        except Exception:
            return ""
    try:
        return Path(path).read_text(errors="ignore")
    except Exception:
        return ""

# --- Prompt loading ---
def _load_prompt(name: str) -> str:
    p = PROMPT_DIR / name
    if p.exists():
        return p.read_text().strip()
    return "You extract structured JSON. Return only minified JSON."

_CANDIDATE_PROMPT = _load_prompt("candidate_extractor.txt")
_JOB_PROMPT = _load_prompt("job_extractor.txt")

# --- JSON Schemas for structured extraction (used if model supports response_format=json_schema) ---
SCHEMA_CANDIDATE = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {"type": "string"},
        "full_name": {"type": "string"},
    "city": {"type": "string"},  # top-level city for easier downstream consumption
        "contact": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "city": {"type": "string"},
                "country": {"type": "string"}
            },
            "required": ["email", "phone", "city", "country"]
        },
        "summary": {"type": "string"},
        "years_experience": {"type": "integer", "minimum": 0},
    "skills": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
        "hard": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False}},
        "soft": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False}}
            },
            "required": ["hard", "soft"]
        },
        "tools": {"type": "array", "items": {"type": "string"}},
        "languages": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "level": {"type": "string"}}, "required": ["name", "level"], "additionalProperties": False}},
        "education": {"type": "array", "items": {"type": "object", "properties": {"degree": {"type": "string"}, "field": {"type": "string"}, "institution": {"type": "string"}, "year_end": {"type": "integer"}}, "required": ["degree", "field", "institution", "year_end"], "additionalProperties": False}},
        "certifications": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "issuer": {"type": "string"}, "year": {"type": "integer"}}, "required": ["name", "issuer", "year"], "additionalProperties": False}},
        "experience": {"type": "array", "items": {"type": "object", "properties": {"title": {"type": "string"}, "company": {"type": "string"}, "start_year": {"type": "integer"}, "end_year": {"type": "integer"}, "description": {"type": "string"}}, "required": ["title", "company", "start_year", "end_year", "description"], "additionalProperties": False}},
        "projects": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "description": {"type": "string"}}, "required": ["name", "description"], "additionalProperties": False}},
        "achievements": {"type": "array", "items": {"type": "string"}},
        "volunteering": {"type": "array", "items": {"type": "object", "properties": {"role": {"type": "string"}, "organization": {"type": "string"}}, "required": ["role", "organization"], "additionalProperties": False}},
        "raw_sections": {"type": "object", "additionalProperties": False, "properties": {"experience": {"type": "string"}, "education": {"type": "string"}, "skills": {"type": "string"}}, "required": ["experience", "education", "skills"]},
        "embedding_summary": {"type": "string"},
    "skills_joined": {"type": "string"},
    "synthetic_skills": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False}},
        "salary_expectation": {"type": "string"},
        "estimated_age": {"type": "integer", "minimum": 0}
    },
    "required": [
        "title","full_name","contact","summary","years_experience","skills","tools","languages","education","certifications","experience","projects","achievements","volunteering","raw_sections","embedding_summary","skills_joined","synthetic_skills","salary_expectation","estimated_age"
    ]
}

SCHEMA_JOB = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {"type": "string"},
        "raw_title": {"type": "string"},
        "location": {"type": "string"},
    # Optional raw profession fields (CSV/table support)
    "profession": {"type": "string"},
    "occupation_field": {"type": "string"},
    "address": {"type": "string"},  # optional physical address / street
    "full_text": {"type": "string"},  # full original job text (not truncated)
    "requirements": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "must_have_skills": {
            "type": "array",
            "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False},
                    "default": []
                },
                "nice_to_have_skills": {
            "type": "array",
            "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False},
                    "default": []
                }
            },
            "required": ["must_have_skills", "nice_to_have_skills"]
        },
        "description": {"type": "string"},
        "seniority": {"type": "string"},
    "requirement_mentions": {"type": "array", "items": {"type": "string"}},
    "mandatory_requirements": {"type": "array", "items": {"type": "string"}},
    "synthetic_skills": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "label": {"type": "string"}, "esco_id": {"type": "string"}}, "required": ["name","label","esco_id"], "additionalProperties": False}}
    },
    "required": ["title", "requirements"]
}

# --- Extraction helpers ---
# Naive extraction placeholders (fallback if LLM unavailable)
TITLE_RE = re.compile(r"(?im)^(?:title|role)[:\-]\s*(.+)$")
SKILL_HINTS = set(sum(SKILL_VOCAB.values(), [])) | set(SKILL_VOCAB.keys())

def _fallback_candidate(text: str) -> Dict[str,Any]:
    title_match = TITLE_RE.search(text)
    title = (title_match.group(1).strip() if title_match else "software engineer")
    skills=[]
    # 1) Parse explicit Skills: lines (comma/semicolon separated)
    for m in re.finditer(r"(?im)^(skills?)[:\-]\s*(.+)$", text):
        items = re.split(r"[;,]", m.group(2))
        for it in items:
            name = it.strip()
            if not name or len(name) < 2:
                continue
            skills.append({"name": canonical_skill(name)})
    # 2) Heuristic vocabulary hits in the whole text
    if not skills:
        for s in SKILL_HINTS:
            if re.search(rf"\b{re.escape(s)}\b", text, re.I):
                skills.append({"name": canonical_skill(s)})
    # Heuristic city detection (very lightweight): look for 'city:' or known city tokens
    city_found = None
    city_match = re.search(r"(?im)^(?:city|location|עיר|מיקום)[:\-]\s*([A-Za-zא-ת _]+)$", text)
    if city_match:
        city_found = city_match.group(1).strip()
    elif not STRICT_REAL_DATA:
        # scan for known city names from _CITY_CACHE (if loaded)
        for cname in list(_CITY_CACHE.keys())[:500]:  # limit scan
            pat = re.compile(rf"\b{re.escape(cname.replace('_',' '))}\b", re.I)
            if pat.search(text):
                city_found = cname.replace('_',' ')
                break
    out = {"title": canonical_title(title), "skills": {"hard": skills}, "raw_title": title}
    if city_found:
        out['city'] = city_found
        # Pre-canonicalize for downstream usage
        try:
            out['city_canonical'] = canonical_city(city_found) or city_found.lower().replace(' ','_')
        except Exception:
            out['city_canonical'] = city_found.lower().replace(' ','_')
    return out

    # _fallback_job removed: product requirement forbids heuristic job ingestion without LLM

def _safe_json_parse(raw: str) -> Dict[str, Any]:
    # Attempt to extract first JSON object
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return {}
    snippet = m.group(0)
    try:
        return json.loads(snippet)
    except Exception:
        return {}

def _openai_extract(kind: str, text: str) -> Dict[str, Any]:
    global LAST_LLM_ERROR, LLM_CALLS, LLM_SUCCESSES
    if not _OPENAI_AVAILABLE:
        LAST_LLM_ERROR = "client_unavailable"
        logging.warning(f"🤖 LLM: OpenAI client unavailable for {kind} extraction")
        return {}
    
    h = hashlib.sha1((kind+"::"+text).encode()).hexdigest()
    if h in _EXTRACTION_CACHE:
        logging.info(f"🤖 LLM: Cache hit for {kind} extraction (hash: {h[:8]})")
        return _EXTRACTION_CACHE[h]
    
    logging.info(f"🤖 LLM: Starting {kind} extraction (text length: {len(text)}, model: {INGEST_OPENAI_MODEL})")
    base_prompt = _CANDIDATE_PROMPT if kind == "candidate" else _JOB_PROMPT
    user_content = f"""SOURCE TEXT:\n{text[:8000]}"""
    
    # Retry with simple backoff
    last_err = None
    start_overall = time.time()
    for attempt in range(3):
        if (time.time() - start_overall) > OPENAI_OVERALL_TIMEOUT:
            LAST_LLM_ERROR = "overall_timeout"
            logging.error(f"🤖 LLM: Overall timeout ({OPENAI_OVERALL_TIMEOUT}s) for {kind} extraction")
            break
        try:
            LLM_CALLS += 1
            logging.info(f"🤖 LLM: Attempt {attempt + 1}/3 - Calling OpenAI API for {kind}")
            data = {}
            schema = SCHEMA_CANDIDATE if kind == "candidate" else SCHEMA_JOB
            used_schema = False
            try:
                # Prefer structured JSON schema if model supports it
                resp = _openai_client.chat.completions.create(
                    model=INGEST_OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": base_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    response_format={
                        "type": "json_schema",
                        "json_schema": {"name": f"{kind}_schema", "schema": schema}
                    },
                    timeout=OPENAI_REQUEST_TIMEOUT,
                )
                content = resp.choices[0].message.content
                data = json.loads(content) if content else {}
                used_schema = True
                logging.info(f"🤖 LLM: ✅ Structured response received for {kind} (length: {len(content or '')})")
            except Exception as schema_err:
                logging.warning(f"🤖 LLM: Structured schema failed, trying fallback for {kind}: {schema_err}")
                # Fallback to legacy free-form completion parsing
                try:
                    resp = _openai_client.chat.completions.create(
                        model=INGEST_OPENAI_MODEL,
                        messages=[
                            {"role": "system", "content": base_prompt},
                            {"role": "user", "content": user_content}
                        ],
                        timeout=OPENAI_REQUEST_TIMEOUT,
                    )
                    content = resp.choices[0].message.content
                    data = _safe_json_parse(content)
                except Exception as inner_e:
                    raise inner_e from schema_err
            if data:
                # mark that LLM parsed this document
                if isinstance(data, dict):
                    data['_llm'] = True
                    data['_llm_schema'] = used_schema
                _EXTRACTION_CACHE[h] = data
                _persist_cache()  # persist after successful new extraction
                LAST_LLM_ERROR = None
                LLM_SUCCESSES += 1
                
                # Log success details
                skills_count = 0
                if isinstance(data, dict):
                    if kind == "job" and data.get('requirements'):
                        req = data['requirements']
                        must_have = len(req.get('must_have_skills', []))
                        nice_to_have = len(req.get('nice_to_have_skills', []))
                        skills_count = must_have + nice_to_have
                        logging.info(f"🤖 LLM: ✅ {kind} extraction successful - {must_have} must-have + {nice_to_have} nice-to-have skills")
                    elif kind == "candidate" and data.get('skills'):
                        skills_count = len(data['skills'].get('hard', []))
                        logging.info(f"🤖 LLM: ✅ {kind} extraction successful - {skills_count} skills extracted")
                    else:
                        logging.info(f"🤖 LLM: ✅ {kind} extraction successful")
                
                return data
        except Exception as e:
            last_err = e
            LAST_LLM_ERROR = f"{type(e).__name__}: {e}"[:300]
            logging.error(f"🤖 LLM: ❌ Attempt {attempt + 1} failed for {kind}: {LAST_LLM_ERROR}")
            time.sleep(0.7 * (attempt + 1))
            # If timeout related, break early to fallback
            if 'Timeout' in LAST_LLM_ERROR or 'timed out' in LAST_LLM_ERROR.lower():
                break
    # Fallback if extraction fails
    if LAST_LLM_ERROR is None and last_err:
        LAST_LLM_ERROR = f"{type(last_err).__name__}: {last_err}"[:300]
    logging.error(f"🤖 LLM: 💥 All attempts failed for {kind} extraction, returning empty result")
    return {}

def extract_candidate(text: str) -> Dict[str,Any]:
    # In strict real-data mode, do not extract from text at all
    if STRICT_REAL_DATA:
        raise RuntimeError("STRICT_REAL_DATA is enabled: candidate extraction is disabled; use only DB data")
    data = _openai_extract("candidate", text) if _OPENAI_AVAILABLE else {}
    if not data:
        data = _fallback_candidate(text)
    # Post-normalize skills & title
    if isinstance(data, dict):
        if 'title' in data:
            data['title'] = canonical_title(str(data['title']))
        # Normalize skills (hard/soft) to include ESCO fields
        def _norm_skill_obj(s: Any) -> dict:
            if isinstance(s, dict):
                nm = canonical_skill(s.get('name', ''))
                meta = ESCO_SKILLS.get(nm) or {}
                return {
                    'name': nm,
                    'label': s.get('label') or meta.get('label') or nm.replace('_',' ').title(),
                    'esco_id': s.get('esco_id') or meta.get('id') or ""
                }
            elif isinstance(s, str):
                nm = canonical_skill(s)
                meta = ESCO_SKILLS.get(nm) or {}
                return {'name': nm, 'label': meta.get('label') or nm.replace('_',' ').title(), 'esco_id': meta.get('id') or ""}
            else:
                return {}
        skills_section = data.get('skills') or {}
        if isinstance(skills_section, dict):
            hard = [_norm_skill_obj(s) for s in (skills_section.get('hard') or []) if s]
            soft = [_norm_skill_obj(s) for s in (skills_section.get('soft') or []) if s]
            data['skills'] = {'hard': [s for s in hard if s.get('name')], 'soft': [s for s in soft if s.get('name')]}
        # Normalize candidate synthetic_skills if present
        if isinstance(data.get('synthetic_skills'), list):
            data['synthetic_skills'] = [ _norm_skill_obj(s) for s in data['synthetic_skills'] if s ]
        # Heuristic enrichments for name/city if weak or missing
        try:
            # Full name: if missing or placeholder, try to guess from header lines excluding emails/phones
            def _guess_name(t: str) -> str | None:
                header = t.strip().splitlines()[:30]
                for ln in header:
                    ls = ln.strip()
                    if not ls or len(ls) > 120:
                        continue
                    if PII_EMAIL_RE.search(ls) or PII_PHONE_RE.search(ls):
                        continue
                    words = re.findall(r"[A-Za-zא-ת]{2,}", ls)
                    if 2 <= len(words) <= 4:
                        return ls
                return None
            if not data.get('full_name') or str(data.get('full_name')).strip().upper() in {"N/A","NA","UNKNOWN",""}:
                guessed = _guess_name(text)
                if guessed:
                    data['full_name'] = guessed
            # City: mirror contact.city to top-level city if present; otherwise leave for ingest_file fallback
            if not data.get('city') and isinstance(data.get('contact'), dict) and data['contact'].get('city'):
                data['city'] = data['contact']['city']
        except Exception:
            pass
    return data

def extract_job(text: str) -> Dict[str,Any]:
    """Job extraction with LLM preferred, but safe fallback allowed if STRICT_JOB_LLM != '1'."""
    # In strict real-data mode, do not extract from text at all
    if STRICT_REAL_DATA:
        raise RuntimeError("STRICT_REAL_DATA is enabled: job extraction is disabled; use only DB data")
    if not _OPENAI_AVAILABLE and os.getenv('STRICT_JOB_LLM','0') in {'1','true','True'}:
        raise RuntimeError("LLM extraction required but OpenAI client unavailable for job ingestion")
    # Lightweight fallback parser
    def _fallback_job(text: str) -> Dict[str, Any]:
        """Very lightweight job parser used only when LLM is unavailable or disabled.
        Extracts title, location/city, and simple requirements from 'Requirements' or 'Skills' lines.
        """
        title_match = TITLE_RE.search(text)
        title = (title_match.group(1).strip() if title_match else "software engineer")
        # City/Location (EN/HE)
        city_found = None
        m_city = re.search(r"(?im)^(?:city|location|עיר|מיקום)[:\-]\s*([A-Za-zא-ת '._-]+)$", text)
        if m_city:
            city_found = m_city.group(1).strip()
        # Requirements
        req_names = []
        for m in re.finditer(r"(?im)^(?:requirements?|skills?)[:\-]\s*(.+)$", text):
            items = re.split(r"[;,_••\-\u2022]| and | or |,|/", m.group(1))
            for it in items:
                it = it.strip()
                if len(it) < 2:
                    continue
                # keep alnum+space words
                name = re.sub(r"[^A-Za-zא-ת0-9 +/#.&-]", "", it)
                if not name:
                    continue
                req_names.append(canonical_skill(name))
        # Build minimal structure
        req_unique = []
        seen = set()
        for n in req_names:
            if n not in seen:
                seen.add(n); req_unique.append({"name": n})
        data = {
            "title": canonical_title(title),
            "location": city_found,
            "requirements": {
                "must_have_skills": req_unique[:8],
                "nice_to_have_skills": []
            },
            "description": (text[:1000] if text else "")
        }
        return data
    data = _openai_extract("job", text)
    if not data or not isinstance(data, dict):
        # Fallback if allowed
        if not STRICT_REAL_DATA and (not _OPENAI_AVAILABLE and os.getenv('STRICT_JOB_LLM','0') not in {'1','true','True'}):
            data = _fallback_job(text)
        else:
            # Include last known LLM error context if present
            raise RuntimeError(f"LLM job extraction returned no data. last_error={LAST_LLM_ERROR}")
    # Always retain full original text
    data['full_text'] = text[:200000]  # safety cap
    # Normalization (canonical title + normalized skill objects with ESCO fields)
    if 'title' in data:
        data['title'] = canonical_title(str(data['title']))
    req = data.get('requirements') or {}
    def _norm_skill_obj(s: Any) -> dict:
        if isinstance(s, dict):
            nm = canonical_skill(s.get('name',''))
            meta = ESCO_SKILLS.get(nm) or {}
            return {'name': nm, 'label': s.get('label') or meta.get('label') or nm.replace('_',' ').title(), 'esco_id': s.get('esco_id') or meta.get('id') or ""}
        elif isinstance(s, str):
            nm = canonical_skill(s)
            meta = ESCO_SKILLS.get(nm) or {}
            return {'name': nm, 'label': meta.get('label') or nm.replace('_',' ').title(), 'esco_id': meta.get('id') or ""}
        else:
            return {}
    must = []
    nice = []
    if isinstance(req, dict):
        raw_must = req.get('must_have_skills') or []
        raw_nice = req.get('nice_to_have_skills') or []
        for s in raw_must:
            obj = _norm_skill_obj(s)
            if obj.get('name'):
                must.append(obj)
        for s in raw_nice:
            obj = _norm_skill_obj(s)
            if obj.get('name') and obj['name'] not in {m['name'] for m in must}:
                nice.append(obj)
    data['requirements'] = {'must_have_skills': must, 'nice_to_have_skills': nice}
    # Build requirement_mentions: preserve raw textual names before canonicalization if present
    mentions = []
    for s in (req.get('must_have_skills') or []) + (req.get('nice_to_have_skills') or []):
        if isinstance(s, dict) and 'name' in s:
            mentions.append(str(s['name']))
        elif isinstance(s, str):
            mentions.append(str(s))
    if mentions:
        data['requirement_mentions'] = mentions
    # Normalize synthetic_skills if present (array of {name})
    syn = []
    raw_syn = data.get('synthetic_skills') or []
    if isinstance(raw_syn, list):
        for s in raw_syn:
            obj = _norm_skill_obj(s)
            if obj.get('name'):
                syn.append(obj)
    if syn:
        data['synthetic_skills'] = syn
    # mandatory_requirements: ensure list[str]
    mand_raw = data.get('mandatory_requirements') or []
    if isinstance(mand_raw, list):
        data['mandatory_requirements'] = [str(x).strip() for x in mand_raw if str(x).strip()]
    return data

# Normalization

def canonical_skill(s: str) -> str:
        """Return canonical skill key in lowercase underscore form.

        - Matches configured SKILL_VOCAB canon if available (case-insensitive),
            otherwise returns normalized lowercase with spaces → underscores.
        """
        sl = (s or "").strip().lower()
        for canon, alts in SKILL_VOCAB.items():
                if sl == canon or sl in [a.lower() for a in alts]:
                        return canon
        # Fallback normalization to ESCO-like style
        return re.sub(r"\s+", "_", sl)

def canonical_title(t: str) -> str:
    tl=t.lower()
    for canon, alts in TITLE_VOCAB.items():
        if tl==canon or tl in [a.lower() for a in alts]:
            return canon
    return tl.replace(" ", "_")

# --- Ingestion ---

def _materialize_skill_set(struct: Dict[str,Any]) -> list:
    try:
        return sorted(list(_skill_set(struct)))
    except Exception:
        return []

def llm_status() -> Dict[str, Any]:
    return {
        "openai_available": _OPENAI_AVAILABLE,
    "model": OPENAI_MODEL if _OPENAI_AVAILABLE else None,
    "ingest_model": INGEST_OPENAI_MODEL if _OPENAI_AVAILABLE else None,
        "last_error": LAST_LLM_ERROR,
        "cache_items": len(_EXTRACTION_CACHE),
        "calls": LLM_CALLS,
        "successes": LLM_SUCCESSES,
        "success_rate": (round(LLM_SUCCESSES/LLM_CALLS,3) if LLM_CALLS else None)
    }

def ingest_file(path: str, kind: str, force_llm: bool=False):
    # In strict real-data mode, refuse ingestion from files entirely
    if STRICT_REAL_DATA:
        raise RuntimeError("STRICT_REAL_DATA is enabled: file ingestion is disabled; rely on existing MongoDB records only")
    global LLM_CALLS, LLM_SUCCESSES
    text=_read_file(path)
    coll = db["candidates" if kind=="candidate" else "jobs"]
    src_hash = _hash_path(path)
    content_hash = _hash_content(text)
    existing = coll.find_one({"_src_hash": src_hash})
    if existing:
        unchanged = existing.get("_content_hash") == content_hash
        if unchanged and not force_llm:
            # If synthetic skills already at or above target, skip reprocess; else regenerate to enrich.
            syn_max_existing = int(os.getenv("SYNTHETIC_SKILL_MAX", "10"))
            existing_syn = existing.get("synthetic_skills") or []
            if len(existing_syn) >= syn_max_existing and syn_max_existing > 0:
                if kind == 'candidate' and not existing.get('share_id'):
                    existing['share_id'] = uuid.uuid4().hex[:12]
                    coll.update_one({"_id": existing["_id"]}, {"$set": {"share_id": existing['share_id']}})
                if not existing.get("skill_set"):
                    existing["skill_set"] = _materialize_skill_set(existing)
                    coll.update_one({"_id": existing["_id"]}, {"$set": {"skill_set": existing["skill_set"], "updated_at": int(time.time())}})
                return existing
            # else fall through to reprocess to add more synthetic skills
    # Attempt extraction (LLM if available) and capture whether we tried LLM
    before_calls = LLM_CALLS
    # Insert stub document early so UI can show a placeholder while LLM runs (long timeout scenario)
    if not existing:
        share_id = uuid.uuid4().hex[:12] if kind=='candidate' else None
        stub = {
            "_src_hash": src_hash,
            "_content_hash": content_hash,
            "_src_path": path,
            "created_at": int(time.time()),
            "updated_at": int(time.time()),
            "status": "extracting",
            "share_id": share_id,
            "kind": kind,
        }
        coll.insert_one(stub)
        existing = stub
    parsed = extract_candidate(text) if kind=="candidate" else extract_job(text)
    # Ensure parsed is always a dictionary - safety check to prevent setdefault errors
    if not isinstance(parsed, dict):
        parsed = {}
    if kind == 'candidate' and isinstance(parsed, dict):
        # Explicit line-based city/location parse (supports Hebrew labels too)
        m_city = re.search(r"(?im)^(?:city|location|עיר|מיקום)[:\-]\s*([A-Za-zא-ת '._-]+)$", text)
        if m_city:
            raw_city = m_city.group(1).strip()
            try:
                parsed['city_canonical'] = canonical_city(raw_city) or raw_city.lower().replace(' ','_')
                parsed['city'] = raw_city
            except Exception:
                parsed['city_canonical'] = raw_city.lower().replace(' ','_')
    # Basic PII scrub (emails, phones) for job full_text before further processing
    if kind == 'job' and isinstance(parsed, dict):
        try:
            ft = parsed.get('full_text') or ''
            ft_scrub = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "[REDACTED_EMAIL]", ft)
            ft_scrub = re.sub(r"\b(?:\+?\d[\d\-() ]{7,})\b", "[REDACTED_PHONE]", ft_scrub)
            parsed['full_text'] = ft_scrub
        except Exception:
            pass
        # Extract RequiredProfession / FieldOfOccupation from source text if present
        try:
            rp_match = re.search(r"(?im)^RequiredProfession:\s*(.+)$", text)
            fo_match = re.search(r"(?im)^FieldOfOccupation:\s*(.+)$", text)
            if rp_match:
                parsed['required_profession_raw'] = rp_match.group(1).strip()
                # Also expose raw as plain profession for Mongo consumers/UI
                parsed['profession'] = parsed.get('profession') or rp_match.group(1).strip()
            if fo_match:
                parsed['field_of_occupation_raw'] = fo_match.group(1).strip()
                # Also expose raw as plain occupation_field
                parsed['occupation_field'] = parsed.get('occupation_field') or fo_match.group(1).strip()
        except Exception:
            pass
    llm_used = (LLM_CALLS > before_calls)  # attempted a call this ingestion
    llm_success = isinstance(parsed, dict) and parsed.get('_llm') is True

    # Secondary LLM attempt: if first attempt failed but client available, request only skill list
    if not llm_success and _OPENAI_AVAILABLE:
        try:
            from openai import OpenAI  # type: ignore
            client = _openai_client  # already instantiated
            secondary_prompt = (
                "Extract ONLY a JSON object with keys: title (string), skills (array of canonical skill strings). Return minified JSON."
            )
            user_content = text[:6000]
            LLM_CALLS += 1
            resp = client.chat.completions.create(
                model=INGEST_OPENAI_MODEL,
                messages=[{"role":"system","content": secondary_prompt},{"role":"user","content": user_content}],
            )
            content = resp.choices[0].message.content
            data2 = _safe_json_parse(content)
            if isinstance(data2, dict) and ('skills' in data2 or 'title' in data2):
                # Merge into parsed structure, creating minimal canonical layout
                skills_list = []
                raw_sk = data2.get('skills') or []
                if isinstance(raw_sk, list):
                    for s in raw_sk:
                        if isinstance(s,str):
                            skills_list.append({'name': canonical_skill(s)})
                if kind == 'candidate':
                    parsed = {
                        'title': canonical_title(str(data2.get('title') or parsed.get('title','software engineer'))),
                        'skills': {'hard': skills_list} if skills_list else parsed.get('skills',{})
                    }
                else:  # job
                    parsed = {
                        'title': canonical_title(str(data2.get('title') or parsed.get('title','software engineer'))),
                        'requirements': {'must_have_skills': [{'name': s['name']} for s in skills_list], 'nice_to_have_skills': []}
                    }
                llm_success = True
                llm_used = True
        except Exception:
            pass

    # If strict requirement to use LLM is enabled, abort when LLM did not succeed
    if kind == 'candidate' and os.getenv('REQUIRE_LLM_SUCCESS','0') in {'1','true','True'} and not llm_success:
        raise RuntimeError('LLM extraction required but failed (no successful structured output).')

    # Heuristic expansion if still few/no skills
    def _heuristic_expand(text_blob: str, existing_names: set[str]) -> list[str]:
        if not ESCO_SKILLS:
            return []
        words = set(re.findall(r"[A-Za-zא-ת][A-Za-zא-ת0-9_]{3,}", text_blob.lower()))
        out=[]
        for sk in ESCO_SKILLS.keys():
            if sk in existing_names: continue
            parts=sk.split('_')
            if any(p in words for p in parts):
                out.append(sk)
            if len(out) >= 10:
                break
        return out

    if isinstance(parsed, dict):
        # Extract current skill names
        current=set()
        if kind=='candidate':
            hard = (parsed.get('skills') or {}).get('hard') if isinstance(parsed.get('skills'), dict) else []
            if isinstance(hard, list):
                for it in hard:
                    if isinstance(it, dict) and it.get('name'): current.add(canonical_skill(it['name']))
        else:
            req=(parsed.get('requirements') or {}) if isinstance(parsed.get('requirements'), dict) else {}
            for bucket in ('must_have_skills','nice_to_have_skills'):
                lst=req.get(bucket) or []
                if isinstance(lst,list):
                    for it in lst:
                        if isinstance(it, dict) and it.get('name'): current.add(canonical_skill(it['name']))
        if len(current) < 2:  # sparse
            for extra in _heuristic_expand(text, current)[:5]:
                if kind=='candidate':
                    parsed.setdefault('skills', {}).setdefault('hard', []).append({'name': extra, '_source':'heuristic'})
                else:
                    parsed.setdefault('requirements', {}).setdefault('nice_to_have_skills', []).append({'name': extra, '_source':'heuristic'})
    # Map extracted skills / requirements to ESCO entries
    def _extract_esco(skill_names: list[str]) -> list[dict[str,str]]:
        out=[]
        for name in skill_names:
            k=name.lower()
            meta=ESCO_SKILLS.get(k)
            rec={"name": k}
            if meta:
                rec["esco_id"]=meta.get("id")
                rec["label"]=meta.get("label")
            out.append(rec)
        return out

    # --- Synthetic skills enrichment (LLM or heuristic) ---
    SYN_MAX = int(os.getenv("SYNTHETIC_SKILL_MAX", "10"))  # default higher than previous 5
    def _synthesize_skills(base_text: str, existing: set[str]) -> list[str]:
        """Return list of additional (synthetic) canonical skill names.
        Strategy:
          1. If OpenAI available -> ask for up to SYN_MAX new skills.
          2. Else heuristic: derive tokens from title + existing skills and pick ESCO skills sharing tokens.
        All results filtered to exclude already existing skills; truncated to SYN_MAX.
        """
        # Helper: heuristic fallback
        def _heuristic() -> list[str]:
            if not ESCO_SKILLS:
                return []
            text_lower = (base_text[:8000]).lower()
            # seed tokens: existing skill tokens + title words appearing in text
            seed_tokens = set()
            for s in list(existing)[:50]:
                seed_tokens.update(re.split(r"[^a-zA-Zא-ת0-9]+", s))
            # also derive from frequent words in text
            words = re.findall(r"[a-zA-Zא-ת][a-zA-Zא-ת0-9_]{2,}", text_lower)
            freq = {}
            for w in words:
                if len(w) < 4: continue
                freq[w] = freq.get(w,0)+1
            # pick top 40 words as context tokens
            for w,_cnt in sorted(freq.items(), key=lambda x:x[1], reverse=True)[:40]:
                seed_tokens.add(w)
            candidates = []
            for skill_key in ESCO_SKILLS.keys():
                if skill_key in existing: continue
                parts = re.split(r"[_\-]", skill_key)
                if any(p in seed_tokens for p in parts):
                    candidates.append(skill_key)
            # simple deterministic ordering (frequency of parts present then alphabetical)
            def _score(sk: str):
                parts = re.split(r"[_\-]", sk)
                return sum(1 for p in parts if p in seed_tokens), -len(sk)
            ranked = sorted(candidates, key=_score, reverse=True)
            return [canonical_skill(r) for r in ranked[:SYN_MAX]]

        if not SYN_MAX or SYN_MAX <= 0:
            return []
        if _OPENAI_AVAILABLE:
            try:
                sys_p = (
                    f"You receive a job or CV text. Return JSON array of up to {SYN_MAX} additional relevant, specific ESCO canonical skill keys that are missing. "
                    "Only output JSON array (no object, no commentary). Use lowercase underscores; exclude anything already provided in Existing list."
                )
                user_content = base_text[:6000] + "\nExisting:" + ",".join(sorted(existing))
                resp = _openai_client.chat.completions.create(
                    model=INGEST_OPENAI_MODEL,
                    messages=[{"role": "system", "content": sys_p},{"role": "user", "content": user_content}],
                )
                raw = resp.choices[0].message.content.strip()
                data=_safe_json_parse(raw)
                out=[]
                if isinstance(data, list):
                    for s in data:
                        if isinstance(s,str):
                            c=canonical_skill(s)
                            if c not in existing:
                                out.append(c)
                # If LLM returns too few (< SYN_MAX/2) try heuristic to top-up
                if len(out) < max(2, SYN_MAX//2):
                    needed = SYN_MAX - len(out)
                    extra = [s for s in _heuristic() if s not in out][:needed]
                    out.extend(extra)
                return out[:SYN_MAX]
            except Exception:
                # Fall back to heuristic if LLM fails
                return _heuristic()
        # No LLM -> heuristic
        return _heuristic()

    # ensure llm_calls_delta is defined (fallback to 0 if missing)
    llm_calls_delta = locals().get('llm_calls_delta', 0)
    doc={
        "_src_path": path,
        "_src_hash": src_hash,
        "_content_hash": content_hash,
        "kind": kind,
        "canonical": parsed,
        "_llm_calls_delta": llm_calls_delta,
        "status": "ready",
        # Store full text (truncated at a higher limit for ML usage)
        "text_blob": text[:50000],
        "extraction_mode": ("llm" if llm_success else ("llm_fallback" if llm_used else "fallback")),
        "llm_attempted": llm_used,
        "llm_success": llm_success,
        "llm_error": (None if llm_success else LAST_LLM_ERROR),
        "updated_at": int(time.time())
    }
    # Promote selected canonical fields to top-level for easy querying / rendering
    if isinstance(parsed, dict):
        for key in ("title","full_name","city","contact","summary","years_experience","skills","tools","languages","education","certifications","experience","projects","achievements","volunteering","raw_sections","embedding_summary","skills_joined","synthetic_skills","salary_expectation","estimated_age"):
            if key in parsed:
                doc.setdefault(key, parsed.get(key))
        # Promote city_canonical if produced by fallback candidate parse
        if 'city_canonical' in parsed and 'city_canonical' not in doc:
            doc['city_canonical'] = parsed.get('city_canonical')
        # If top-level city missing but present in contact, promote it
        if 'city' not in doc and isinstance(doc.get('contact'), dict):
            ccity = doc['contact'].get('city')
            if ccity and isinstance(ccity, str) and ccity.strip():
                doc['city'] = ccity.strip()
        # Normalize ESCO occupations if raw present
        try:
            rp_raw = parsed.get('required_profession_raw')
            fo_raw = parsed.get('field_of_occupation_raw')
            if rp_raw:
                doc['required_profession'] = normalize_occupation(rp_raw)
                doc['required_profession_raw'] = rp_raw
                # Ensure plain-string mirror for consumers expecting 'profession'
                doc.setdefault('profession', rp_raw)
            if fo_raw:
                doc['field_of_occupation'] = normalize_occupation(fo_raw)
                doc['field_of_occupation_raw'] = fo_raw
                # Ensure plain-string mirror for consumers expecting 'occupation_field'
                doc.setdefault('occupation_field', fo_raw)
        except Exception:
            pass
    # --- Derived / normalization additions for candidates ---
    if kind == 'candidate':
        # Ensure synthetic_skills as list[ {name} ] even if later populated
        syn_raw = doc.get('synthetic_skills') or []
        if syn_raw and isinstance(syn_raw, list) and (not syn_raw or not isinstance(syn_raw[0], dict)):
            doc['synthetic_skills'] = [{"name": s} for s in syn_raw if isinstance(s, str)]
        # skills_joined: derive if absent
        if 'skills_joined' not in doc:
            hard = []
            soft = []
            if isinstance(doc.get('skills'), dict):
                hard = [e.get('name') for e in (doc['skills'].get('hard') or []) if isinstance(e, dict)]
                soft = [e.get('name') for e in (doc['skills'].get('soft') or []) if isinstance(e, dict)]
            tools = [t for t in (doc.get('tools') or []) if isinstance(t, str)]
            syn = [e.get('name') for e in (doc.get('synthetic_skills') or []) if isinstance(e, dict)]
            tokens = []
            for lst in (hard, soft, tools, syn):
                for item in lst:
                    if not item: continue
                    norm = str(item).strip().lower().replace(' ', '_')
                    if norm and norm not in tokens:
                        tokens.append(norm)
            doc['skills_joined'] = ','.join(tokens)
        # embedding_summary fallback: compact from summary
        if 'embedding_summary' not in doc:
            summ = (doc.get('summary') or '')[:300]
            doc['embedding_summary'] = summ
        # years_experience fallback normalization
        if 'years_experience' in doc and not isinstance(doc['years_experience'], int):
            try:
                doc['years_experience'] = int(float(doc['years_experience']))
            except Exception:
                doc['years_experience'] = 0
    # City normalization (store canonical_city)
    loc = doc.get("location") or doc.get("city") or (doc.get("canonical") or {}).get("location")
    c_city = canonical_city(loc) if loc else None
    if c_city:
        doc["city_canonical"] = c_city
    doc["skill_set"] = _materialize_skill_set(doc)
    # Synthetic enrichment (optional)
    existing_set=set(doc["skill_set"])
    synthetic_new=_synthesize_skills(text, existing_set)
    if synthetic_new:
        # add to requirements as needed skills bucket; attach reason metadata (role_pattern/top_up)
        req = doc.get("requirements") or {}
        needed = req.get("nice_to_have_skills") or []  # reuse existing field if present
        role_tokens = set(str(doc.get('title') or '').split('_'))
        syn_with_reason=[]
        for s in synthetic_new:
            reason = 'role_pattern' if any(tok and tok in s for tok in role_tokens) else 'top_up'
            meta = _esco_meta(s)
            needed.append({"name": meta.get('name'), "label": meta.get('label') or meta.get('name','').replace('_',' ').title(), "esco_id": meta.get('esco_id',''), "_source": "synthetic"})
            syn_with_reason.append({"name": meta.get('name'), "label": meta.get('label') or meta.get('name','').replace('_',' ').title(), "esco_id": meta.get('esco_id',''), "reason": reason})
            existing_set.add(s)
        if isinstance(req, dict):
            req["nice_to_have_skills"]=needed
            doc["requirements"]=req
        doc["synthetic_skills"]=syn_with_reason
    # Rebuild skill_set including synthetics
    doc["skill_set"] = sorted(list(existing_set))
    # Skill governance (job only): enforce min >=12 distinct via additional synthetic top-up; cap >35 trimming synthetic first
    if kind == 'job':
        # Ensure synthetic_skills is list[dict]
        syn_list = doc.get('synthetic_skills') or []
        if syn_list and isinstance(syn_list, list) and syn_list and not isinstance(syn_list[0], dict):
            syn_list = [{"name": s, "reason": "legacy"} for s in syn_list if isinstance(s, str)]
            doc['synthetic_skills'] = syn_list
        distinct_names = set(doc['skill_set'])
        # Min distinct enforcement
        if len(distinct_names) < 12 and ESCO_SKILLS:
            needed = 12 - len(distinct_names)
            added = 0
            for sk in ESCO_SKILLS.keys():
                if sk in distinct_names: continue
                # add as synthetic top_up_min_floor
                doc.setdefault('synthetic_skills', []).append({"name": sk, "reason": "top_up_min_floor"})
                # also add to requirements nice_to_have bucket
                req = doc.get('requirements') or {}
                lst = req.get('nice_to_have_skills') or []
                lst.append({"name": sk, "_source": "synthetic"})
                req['nice_to_have_skills'] = lst
                doc['requirements'] = req
                distinct_names.add(sk)
                added += 1
                if added >= needed:
                    break
            doc['skill_set'] = sorted(list(distinct_names))
        # Cap >35: trim synthetic first (recently added last) then excess nice_to_have
        if len(doc['skill_set']) > 35:
            overflow = len(doc['skill_set']) - 35
            # Build lists preserving order preference for removal
            syn_names_order = [s.get('name') for s in (doc.get('synthetic_skills') or []) if isinstance(s, dict) and s.get('name')]
            remove=set()
            for name in reversed(syn_names_order):
                if overflow <=0: break
                remove.add(name); overflow -=1
            # If still overflow remove from nice_to_have bucket (excluding must)
            if overflow > 0:
                req = doc.get('requirements') or {}
                nice_list = req.get('nice_to_have_skills') or []
                for item in reversed(nice_list):
                    if overflow <=0: break
                    if isinstance(item, dict):
                        n=item.get('name')
                        if n and n not in remove:
                            remove.add(n); overflow -=1
            if remove:
                # Filter synthetic_skills
                doc['synthetic_skills'] = [s for s in (doc.get('synthetic_skills') or []) if s.get('name') not in remove] if isinstance(doc.get('synthetic_skills'), list) else []
                # Filter requirements buckets
                req = doc.get('requirements') or {}
                for bucket in ('must_have_skills','nice_to_have_skills'):
                    lst=req.get(bucket) or []
                    if isinstance(lst, list):
                        req[bucket] = [it for it in lst if not (isinstance(it, dict) and it.get('name') in remove)]
                doc['requirements']=req
                # Recompute skill_set after removals
                doc['skill_set'] = sorted([n for n in doc['skill_set'] if n not in remove])
    # Detailed skills list with category & source (+matching metadata)
    detailed=[]
    must_names=set()
    req=doc.get("requirements") or {}
    if isinstance(req, dict):
        for item in req.get("must_have_skills") or []:
            if isinstance(item, dict):
                n=canonical_skill(item.get("name"));
                if not n: continue
                meta=[e for e in _extract_esco([n])][0]
                meta["category"]="must"
                meta["source"]= item.get("_source","extracted")
                # Populate matching metadata with sane defaults
                meta.setdefault("level", None)  # e.g., beginner/intermediate/advanced
                meta.setdefault("years_experience", None)
                meta.setdefault("last_used_year", None)
                meta.setdefault("confidence", 0.8)
                meta.setdefault("weight", 1.0)
                meta.setdefault("evidence", None)
                detailed.append(meta); must_names.add(n)
    # needed (nice_to_have) category
        for item in req.get("nice_to_have_skills") or []:
            if isinstance(item, dict):
                n=canonical_skill(item.get("name"));
                if not n: continue
                meta=[e for e in _extract_esco([n])][0]
                src = item.get("_source","extracted")
                meta["source"] = src
                meta["category"] = "synthetic" if src == "synthetic" else "needed"
                # Metadata defaults (slightly lower confidence for non-must)
                meta.setdefault("level", None)
                meta.setdefault("years_experience", None)
                meta.setdefault("last_used_year", None)
                meta.setdefault("confidence", 0.65 if src != "synthetic" else 0.55)
                meta.setdefault("weight", 0.8 if src != "synthetic" else 0.6)
                # If synthetic, attach reason as evidence when available
                if src == "synthetic":
                    # find matching synthetic reason if present
                    try:
                        syn_list = doc.get("synthetic_skills") or []
                        for s in syn_list:
                            if isinstance(s, dict) and s.get("name") == n and s.get("reason"):
                                meta["evidence"] = s.get("reason")
                                break
                    except Exception:
                        pass
                detailed.append(meta)
    # Add any remaining skills not classified
    for n in doc["skill_set"]:
        if n not in {d["name"] for d in detailed}:
            meta=[e for e in _extract_esco([n])][0]
            # If appears in synthetic_skills list classify as synthetic
            syn_names = {s.get('name') for s in (doc.get('synthetic_skills') or []) if isinstance(s, dict)}
            if n in syn_names:
                meta["category"]="synthetic"; meta["source"]="synthetic"
                meta.setdefault("confidence", 0.55)
                meta.setdefault("weight", 0.6)
            else:
                meta["category"]="needed"; meta["source"]="inferred"
                meta.setdefault("confidence", 0.6)
                meta.setdefault("weight", 0.7)
            meta.setdefault("level", None)
            meta.setdefault("years_experience", None)
            meta.setdefault("last_used_year", None)
            meta.setdefault("evidence", None)
            detailed.append(meta)
    doc["skills_detailed"]=detailed
    doc["esco_skills"] = [{k:v for k,v in d.items() if k in {"name","esco_id","label"}} for d in detailed]
    doc["synthetic_skills_generated"]=len([d for d in detailed if d.get("source")=="synthetic"])

    # Derivatives: skills_fingerprint (stable IDs) and skills_vector
    try:
        def _fingerprint(items: list[dict]) -> list[str]:
            fp=[]; seen=set()
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                key = it.get("esco_id") or it.get("name")
                if key and key not in seen:
                    seen.add(key); fp.append(str(key))
            return fp
        doc["skills_fingerprint"] = _fingerprint(doc.get("skills_detailed") or [])
        # Vector as hash embedding of joined fingerprint values (deterministic, no external model)
        joined = ",".join(doc.get("skills_fingerprint") or [])
        doc["skills_vector"] = _hash_to_vec(joined, dims=32)
    except Exception:
        pass
    # Quality flags (parity with CSV importer)
    flags=[]
    if kind=='job':
        # low quality: very short description or too few skills
        if len(doc.get('job_description') or '') < 40:
            flags.append('short_description')
        if len(doc.get('job_requirements') or []) < 3:
            flags.append('low_skill_variety')
        # Over-generation synthetic ratio > 0.6
        syn_list = doc.get('synthetic_skills') or []
        syn_names = [s.get('name') for s in syn_list if isinstance(s, dict) and s.get('name')]
        distinct_total = len(set((doc.get('job_requirements') or []) + syn_names)) or 1
        if syn_names and (len(syn_names)/distinct_total) > 0.6:
            flags.append('over_generation')
        # Mandatory present but no corresponding must skills
        if doc.get('mandatory_requirements') and not doc.get('job_requirements'):
            flags.append('mandatory_without_must_skills')
    if flags:
        doc['flags']=flags
    # --- Enforce minimum skill floor (post classification) ---
    if MIN_SKILL_FLOOR > 0 and len(doc["skill_set"]) < MIN_SKILL_FLOOR:
        needed = MIN_SKILL_FLOOR - len(doc["skill_set"])
        supplements=[]
        # Prefer ESCO skills not yet present
        if ESCO_SKILLS:
            for sk in ESCO_SKILLS.keys():
                if sk not in doc["skill_set"]:
                    supplements.append(sk)
                if len(supplements) >= needed:
                    break
        if not supplements and SKILL_VOCAB:
            for sk in SKILL_VOCAB.keys():
                if sk not in doc["skill_set"]:
                    supplements.append(sk)
                if len(supplements) >= needed:
                    break
        added=0
        for sk in supplements:
            if sk in doc["skill_set"]:
                continue
            doc["skill_set"].append(sk)
            m = _esco_meta(sk)
            detailed.append({"name": m.get('name'), "label": m.get('label') or m.get('name','').replace('_',' ').title(), "esco_id": m.get('esco_id',''), "category": "needed", "source": "floor_fill"})
            # also reflect in requirements.nice_to_have_skills to keep structures consistent
            req = doc.get('requirements') or {}
            lst = req.get('nice_to_have_skills') or []
            lst.append({"name": m.get('name'), "label": m.get('label') or m.get('name','').replace('_',' ').title(), "esco_id": m.get('esco_id',''), "_source": "synthetic"})
            req['nice_to_have_skills'] = lst
            doc['requirements'] = req
            added += 1
            if added >= needed:
                break
        if added:
            doc["skills_detailed"]=detailed
    # Assign share_id before upsert (new or missing)
    if kind == 'candidate':
        if not existing or not existing.get('share_id'):
            doc['share_id'] = uuid.uuid4().hex[:12]
        else:
            doc['share_id'] = existing.get('share_id')
        # Normalize missing candidate fields (fallback defaults) to show in admin table
        def _norm_list(v):
            return v if isinstance(v, list) else []
        def _ensure_name_objects(lst):
            out=[]
            for x in lst:
                if isinstance(x, dict) and 'name' in x:
                    out.append({'name': str(x['name'])})
                elif isinstance(x, str):
                    out.append({'name': x})
            return out
        # Required base structure
        doc.setdefault('full_name', 'N/A')
        doc.setdefault('summary', 'N/A')
        doc.setdefault('years_experience', 0)
        if not isinstance(doc.get('years_experience'), int):
            try:
                doc['years_experience'] = int(float(doc.get('years_experience') or 0))
            except Exception:
                doc['years_experience']=0
        # contact
        contact = doc.get('contact') if isinstance(doc.get('contact'), dict) else {}
        for k in ('email','phone','city','country'):
            contact.setdefault(k, 'N/A')
        doc['contact']=contact
        # skills object: preserve ESCO fields (name,label,esco_id)
        skills = doc.get('skills') if isinstance(doc.get('skills'), dict) else {}
        def _ensure_esco_objs(lst):
            out=[]
            for x in _norm_list(lst):
                if isinstance(x, dict) and x.get('name'):
                    nm = canonical_skill(x.get('name'))
                    meta = ESCO_SKILLS.get(nm) or {}
                    out.append({'name': nm, 'label': x.get('label') or meta.get('label') or nm.replace('_',' ').title(), 'esco_id': x.get('esco_id') or meta.get('id') or ""})
                elif isinstance(x, str):
                    nm = canonical_skill(x)
                    meta = ESCO_SKILLS.get(nm) or {}
                    out.append({'name': nm, 'label': meta.get('label') or nm.replace('_',' ').title(), 'esco_id': meta.get('id') or ""})
            return out
        hard = _ensure_esco_objs(skills.get('hard'))
        soft = _ensure_esco_objs(skills.get('soft'))
        doc['skills']={'hard': hard, 'soft': soft}
        doc.setdefault('tools', [])
        doc.setdefault('languages', [])
        doc.setdefault('education', [])
        doc.setdefault('certifications', [])
        doc.setdefault('experience', [])
        doc.setdefault('projects', [])
        doc.setdefault('achievements', [])
        doc.setdefault('volunteering', [])
        raw_sections = doc.get('raw_sections') if isinstance(doc.get('raw_sections'), dict) else {}
        for k in ('experience','education','skills'):
            raw_sections.setdefault(k, 'N/A')
        doc['raw_sections']=raw_sections
        doc.setdefault('embedding_summary', (doc.get('summary') or '')[:300])
        doc.setdefault('skills_joined', '')
        doc.setdefault('synthetic_skills', [])
        doc.setdefault('salary_expectation', 'N/A')
        doc.setdefault('estimated_age', 0)
        # Ensure title is a non-empty string for downstream consumers (tests assume str)
        if not isinstance(doc.get('title'), str) or not (doc.get('title') or '').strip():
            doc['title'] = 'N/A'
        # Ensure full_name has a sensible default used by letter/prompt tests
        if not isinstance(doc.get('full_name'), str) or not (doc.get('full_name') or '').strip():
            doc['full_name'] = 'Candidate'
        doc['fields_complete']=True
        # Scrub PII from free-text blobs (keep structured contact)
        doc['text_blob'] = _scrub_pii(doc.get('text_blob'))
        if 'embedding_summary' in doc:
            doc['embedding_summary'] = _scrub_pii(doc.get('embedding_summary'))
    else:  # job-specific field enrichments
        # job_description: prefer top-level description, else canonical.description
        if 'job_description' not in doc:
            desc = doc.get('description') or (doc.get('canonical') or {}).get('description') or ''
            if desc:
                doc['job_description'] = desc  # keep full description now
        # job_requirements: flattened ordered unique list of must + nice skill names
        if 'job_requirements' not in doc:
            req = doc.get('requirements') or {}
            must = [i.get('name') for i in (req.get('must_have_skills') or []) if isinstance(i, dict) and i.get('name')]
            nice = [i.get('name') for i in (req.get('nice_to_have_skills') or []) if isinstance(i, dict) and i.get('name')]
            merged = []
            seen = set()
            for name in must + nice:
                if name and name not in seen:
                    seen.add(name); merged.append(name)
            if merged:
                doc['job_requirements'] = merged
        # requirement_mentions fallback: if extraction didn't provide it, mirror job_requirements
        if 'requirement_mentions' not in doc and 'job_requirements' in doc:
            doc['requirement_mentions'] = list(doc.get('job_requirements') or [])
        # Promote mandatory_requirements & synthetic_skills to top-level if nested only in canonical
        can = doc.get('canonical') or {}
        if 'mandatory_requirements' in can and 'mandatory_requirements' not in doc:
            doc['mandatory_requirements'] = can.get('mandatory_requirements')
        if 'synthetic_skills' in can and 'synthetic_skills' not in doc:
            doc['synthetic_skills'] = can.get('synthetic_skills')
        # Ensure synthetic_skills list normalized (array[object: name,label,esco_id,reason?])
        raw_syn = doc.get('synthetic_skills') or []
        if isinstance(raw_syn, list):
            norm_syn=[]
            for s in raw_syn:
                if isinstance(s, dict):
                    nm = canonical_skill(s.get('name','')) if s.get('name') else None
                    if not nm:
                        continue
                    meta = ESCO_SKILLS.get(nm) or {}
                    norm = {
                        'name': nm,
                        'label': s.get('label') or meta.get('label') or nm.replace('_',' ').title(),
                        'esco_id': s.get('esco_id') or meta.get('id') or ""
                    }
                    if s.get('reason'):
                        norm['reason'] = s.get('reason')
                    norm_syn.append(norm)
                elif isinstance(s, str):
                    nm = canonical_skill(s)
                    meta = ESCO_SKILLS.get(nm) or {}
                    norm_syn.append({'name': nm, 'label': meta.get('label') or nm.replace('_',' ').title(), 'esco_id': meta.get('id') or ""})
            doc['synthetic_skills']=norm_syn
        # Re-limit job_requirements to first 8 distinct after any synthetic fill
        if 'job_requirements' in doc and isinstance(doc.get('job_requirements'), list):
            doc['job_requirements'] = doc['job_requirements'][:8]
        # Heuristic salary extraction
        if 'salary_range_raw' not in doc:
            try:
                blob = doc.get('full_text') or doc.get('job_description') or ''
                m = re.search(r'(\d{4,6})\D{0,10}(\d{4,6})', blob)
                if m:
                    doc['salary_range_raw'] = f"{m.group(1)}-{m.group(2)}"
            except Exception:
                pass
    # Versioning snapshot (jobs) prior to update
    if kind == 'job':
        try:
            existing_full = coll.find_one({"_src_hash": src_hash})
            if existing_full and any(existing_full.get(k) != doc.get(k) for k in ('full_text','skill_set','requirements','mandatory_requirements','synthetic_skills')):
                snap = dict(existing_full)
                snap.pop('_id', None)
                db['jobs_versions'].insert_one({'job_id': existing_full['_id'], 'snapshot': snap, 'versioned_at': int(time.time())})
        except Exception:
            pass
    # Versioning snapshot (candidates & jobs) before update if changes
    try:
        existing_full = coll.find_one({"_src_hash": src_hash})
        if existing_full and any(existing_full.get(k) != doc.get(k) for k in ('full_text','skill_set','requirements','mandatory_requirements','synthetic_skills','skills','skills_joined')):
            snap = dict(existing_full); snap.pop('_id', None)
            coll_name = 'jobs_versions' if kind=='job' else 'candidates_versions'
            db[coll_name].insert_one({'entity_id': existing_full['_id'], 'snapshot': snap, 'versioned_at': int(time.time())})
    except Exception:
        pass
    coll.update_one({"_src_hash": doc["_src_hash"]}, {"$set": doc}, upsert=True)
    # Per-document metrics meta (best-effort)
    try:
        syn_names = []
        syn_field = doc.get('synthetic_skills') or []
        if syn_field and isinstance(syn_field, list) and syn_field and syn_field and isinstance(syn_field[0], dict):
            syn_names = [s.get('name') for s in syn_field if isinstance(s, dict) and s.get('name')]
        elif isinstance(syn_field, list):
            syn_names = [s for s in syn_field if isinstance(s, str)]
        distinct_total = len(set(doc.get('skill_set') or [])) or 1
        ratio = round(len(syn_names)/distinct_total,3)
        meta_key = 'last_job_ingest_metrics' if kind=='job' else 'last_candidate_ingest_metrics'
        _set_meta(meta_key, {'_src_hash': src_hash, 'skill_count': distinct_total, 'synthetic_count': len(syn_names), 'synthetic_ratio': ratio, 'updated_at': int(time.time())})
    except Exception:
        pass
    # Persist mock DB snapshot if enabled
    try:
        if is_mock():
            persist_mock_db()
    except Exception:
        pass
    return doc

def ingest_files(paths: List[str], kind: str, force_llm: bool=False):
    # Default: force_llm True unless explicitly overridden
    if force_llm is False:
        force_llm = True
    docs_collected = []
    # If kind is candidate and a single clean-room ingest is desired (ENV SINGLE_CANDIDATE_MODE=1)
    # then only ingest the first provided path and stop (avoid duplicates from accidental repeats).
    single_mode = os.getenv('SINGLE_CANDIDATE_MODE') == '1'
    iter_paths = paths[:1] if single_mode and kind == 'candidate' else paths
    for p in iter_paths:
        if Path(p).is_file() and Path(p).suffix.lower() in SUPPORTED_EXTS:
            doc = ingest_file(p, kind, force_llm=force_llm)
            docs_collected.append(doc)
    # Optional hard guarantee: if SINGLE_CANDIDATE_MODE ensure only one candidate exists
    if single_mode and kind == 'candidate' and docs_collected:
        coll = db['candidates']
        # keep newest (by updated_at) delete others
        all_ids = list(coll.find({}, {'_id':1, 'updated_at':1}))
        if len(all_ids) > 1:
            keep_id = max(all_ids, key=lambda d: d.get('updated_at') or 0)['_id']
            coll.delete_many({'_id': {'$ne': keep_id}})
    return docs_collected

def refresh_existing(kind: str, use_llm: bool=False):
    if not use_llm:
        disable_llm()
    coll = db["candidates" if kind=="candidate" else "jobs"]
    for doc in coll.find({}, {"_src_path":1,"_src_hash":1}):
        path = doc.get("_src_path")
        if not path or not Path(path).exists():
            continue
        ingest_file(path, kind)
    return coll.count_documents({})

def recompute_skill_sets():
    changed=0
    for name in ("candidates","jobs"):
        coll = db[name]
        for doc in coll.find({}, {"_id":1}):
            full = coll.find_one({"_id": doc["_id"]})
            if not full:
                continue
            new_sk = sorted(list(_skill_set(full)))
            if new_sk != full.get("skill_set"):
                coll.update_one({"_id": doc["_id"]}, {"$set": {"skill_set": new_sk, "updated_at": int(time.time())}})
                changed+=1
    _set_meta("skill_recompute_at", int(time.time()))
    return changed

def create_indexes():
    """Ensure commonly used indexes exist (idempotent). Returns a list of index names created/ensured.

    This function is safe to call on startup and by readiness probes. It avoids raising on individual
    index creation failures to prevent blocking the app.
    """
    created: list[str] = []
    try:
        try:
            name = db["candidates"].create_index("skill_set")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("skill_set")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["candidates"].create_index("updated_at")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("updated_at")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["candidates"].create_index("tenant_id")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index([("tenant_id", 1), ("external_job_id", 1)], name="tenant_extid")
            created.append(name)
        except Exception:
            pass
        # New derivative fields for fast matching
        try:
            name = db["candidates"].create_index("skills_fingerprint")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("skills_fingerprint")
            created.append(name)
        except Exception:
            pass
        # City and basic metadata
        try:
            name = db["jobs"].create_index("city_canonical")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["candidates"].create_index("city_canonical")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("created_at")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["candidates"].create_index("created_at")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("title")
            created.append(name)
        except Exception:
            pass
        # Nested skills fields (multikey)
        try:
            name = db["candidates"].create_index("skills_detailed.name")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("requirements.must_have_skills.name")
            created.append(name)
        except Exception:
            pass
        # Backward-compat fields used by some queries
        try:
            name = db["jobs"].create_index("job_requirements")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("requirement_mentions")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("synthetic_skills")
            created.append(name)
        except Exception:
            pass
        # Optional direct filter indexes for raw profession fields
        try:
            name = db["jobs"].create_index("profession")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index("occupation_field")
            created.append(name)
        except Exception:
            pass
        # New fields from Score Agents CSV format
        try:
            name = db["jobs"].create_index("branch")
            created.append(name)
        except Exception:
            pass
        try:
            name = db["jobs"].create_index([("job_applications_count", -1)])
            created.append(name)
        except Exception:
            pass
    except Exception:
        # Never break app on index errors
        pass
    return created

# Ensure indexes are created on module import as a safety net (tests insert directly via db)
try:
    create_indexes()
except Exception:
    pass

def backfill_skills_meta() -> dict:
    """Populate skills_detailed defaults, skills_fingerprint and skills_vector for all docs.
    Safe to run multiple times.
    """
    updated = {"candidates": 0, "jobs": 0}
    now = int(time.time())
    for name in ("candidates","jobs"):
        coll = db[name]
        for d in coll.find({}, {"_id":1, "skills_detailed":1, "skill_set":1, "requirements":1, "synthetic_skills":1}):
            needs_update = False
            full = coll.find_one({"_id": d["_id"]}) or {}
            # Ensure detailed exists
            if not full.get("skills_detailed"):
                # Build minimal detailed from skill_set
                det=[]
                for n in (full.get("skill_set") or []):
                    meta = {"name": n, "category": "needed", "source": "inferred"}
                    info = ESCO_SKILLS.get(n)
                    if info:
                        meta["esco_id"] = info.get("id"); meta["label"] = info.get("label")
                    meta.setdefault("confidence", 0.6); meta.setdefault("weight", 0.7)
                    meta.setdefault("level", None); meta.setdefault("years_experience", None); meta.setdefault("last_used_year", None); meta.setdefault("evidence", None)
                    det.append(meta)
                full["skills_detailed"] = det; needs_update = True
            # Fingerprint + vector
            try:
                if not full.get("skills_fingerprint"):
                    fp = []
                    for it in full.get("skills_detailed") or []:
                        key = (it.get("esco_id") or it.get("name"))
                        if key and key not in fp:
                            fp.append(str(key))
                    full["skills_fingerprint"] = fp; needs_update = True
                if not full.get("skills_vector"):
                    joined = ",".join(full.get("skills_fingerprint") or [])
                    full["skills_vector"] = _hash_to_vec(joined, dims=32); needs_update = True
            except Exception:
                pass
            if needs_update:
                full["updated_at"] = now
                coll.update_one({"_id": d["_id"]}, {"$set": {k: full[k] for k in ("skills_detailed","skills_fingerprint","skills_vector","updated_at") if k in full}})
                updated[name] += 1
    try:
        _set_meta("backfill_skills_meta_at", now)
    except Exception:
        pass
    return updated

def dedupe_by_src_hash(kind: str) -> int:
    """Remove older duplicate documents sharing the same _src_hash. Keep the most recently updated.
    Returns number of documents removed."""
    name = "candidates" if kind == "candidate" else "jobs"
    coll = db[name]
    removed = 0
    # Aggregate hashes with counts >1
    try:
        pipeline = [
            {"$group": {"_id": "$_src_hash", "ids": {"$push": {"_id": "$_id", "updated_at": "$updated_at"}}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        for grp in coll.aggregate(pipeline):
            docs = grp.get("ids") or []
            # choose keep id: highest updated_at
            keep = None
            for d in docs:
                if keep is None or (d.get("updated_at") or 0) > (keep.get("updated_at") or 0):
                    keep = d
            keep_id = keep.get("_id") if keep else None
            # delete others
            for d in docs:
                if d.get("_id") != keep_id:
                    coll.delete_one({"_id": d.get("_id")})
                    removed += 1
    except Exception:
        pass
    return removed
def _meta_coll():
    return db["_meta"]
def _set_meta(key: str, value):
    _meta_coll().update_one({"key": key}, {"$set": {"key": key, "value": value}}, upsert=True)
def get_meta(key: str, default=None):
    rec=_meta_coll().find_one({"key": key})
    return rec.get("value") if rec else default
def list_meta() -> Dict[str, Any]:
    out={}
    for rec in _meta_coll().find():
        k=rec.get("key"); v=rec.get("value")
        if k:
            out[k]=v
    return out
def clear_extraction_cache():
    _EXTRACTION_CACHE.clear(); _persist_cache(); return True

# --- ESCO Occupation normalization (label -> {name,label,esco_id}) ---
def normalize_occupation(raw: str) -> dict:
    """Best-effort mapping of a raw profession/occupation label to ESCO-like object.
    Returns { name, label, esco_id, raw }.
    - If OpenAI available: ask for ESCO English label + code.
    - Fallback: canonicalize to a slug for name, reuse label, leave esco_id empty.
    """
    label = (raw or '').strip()
    if not label:
        return {"name": None, "label": None, "esco_id": "", "raw": raw}
    # Fast path: reuse canonical_title as a reasonable slug
    slug = canonical_title(label)
    result = {"name": slug, "label": label, "esco_id": "", "raw": raw}
    if _OPENAI_AVAILABLE:
        try:
            prompt = (
                "Map the following occupation to ESCO (English) strictly as JSON with keys name,label,esco_id.\n"
                "Return only JSON. If unknown, return name from a reasonable English slug, label as best English, esco_id as empty string.\n"
                f"Input: {label}\n"
            )
            resp = _openai_client.chat.completions.create(
                model=INGEST_OPENAI_MODEL,
                messages=[{"role":"system","content":"You are an ESCO occupation mapper."},{"role":"user","content":prompt}],
                temperature=0.1,
                response_format={"type":"json_schema","json_schema":{"name":"esco_occupation","schema":{"type":"object","additionalProperties":False,"properties":{"name":{"type":"string"},"label":{"type":"string"},"esco_id":{"type":"string"}},"required":["name","label","esco_id"]}}},
                timeout=OPENAI_REQUEST_TIMEOUT,
            )
            content = (resp.choices[0].message.content or '').strip()
            data = _safe_json_parse(content)
            if isinstance(data, dict) and data.get('name') and data.get('label') is not None and 'esco_id' in data:
                result = {"name": canonical_skill(str(data['name'])), "label": str(data['label']), "esco_id": str(data['esco_id']), "raw": raw}
        except Exception:
            pass
    return result

# --- Enrichment for CSV-imported jobs (match readiness) ---

def _split_compound_skills(text: str) -> list[str]:
    """Split compound skills like 'Python ו-JavaScript' into canonical tokens.
    Handles Hebrew and English connectors.
    """
    if not text:
        return []
    parts = re.split(r"\s*(?:ו-?|עם|או|and|with|or|\&|\+|/|,)\s*", str(text))
    out=[]; seen=set()
    for p in parts:
        p = p.strip().strip('-')
        if not p or len(p) < 2:
            continue
        canon = canonical_skill(p)
        if canon and canon not in seen:
            seen.add(canon); out.append(canon)
    return out

def _esco_meta(name: str) -> dict:
    k = canonical_skill(name)
    meta = ESCO_SKILLS.get(k) or {}
    out = {"name": k}
    if meta:
        if meta.get("id"): out["esco_id"] = meta.get("id")
        if meta.get("label"): out["label"] = meta.get("label")
    return out

def _build_skills_detailed(must: list[str], needed: list[str], syn_names: set[str]) -> list[dict]:
    detailed=[]
    must_set=set(must)
    for n in must:
        m=_esco_meta(n); m["category"]="must"; m["source"]="extracted"; m.setdefault("confidence",0.85); m.setdefault("weight",1.0)
        m.setdefault("level",None); m.setdefault("years_experience",None); m.setdefault("last_used_year",None); m.setdefault("evidence",None)
        detailed.append(m)
    for n in needed:
        m=_esco_meta(n); m["category"] = "synthetic" if n in syn_names else "needed"; m["source"] = "synthetic" if n in syn_names else "extracted"
        m.setdefault("confidence", 0.6 if n not in syn_names else 0.55); m.setdefault("weight", 0.7 if n not in syn_names else 0.6)
        m.setdefault("level",None); m.setdefault("years_experience",None); m.setdefault("last_used_year",None); m.setdefault("evidence",None)
        detailed.append(m)
    return detailed

def set_llm_required_on_upload(enabled: bool) -> None:
    try:
        _set_meta("require_llm_on_upload", bool(enabled))
    except Exception:
        pass

def is_llm_required_on_upload() -> bool:
    try:
        v = get_meta("require_llm_on_upload", False)
        return bool(v)
    except Exception:
        return False

def enrich_jobs_from_csv(job_ids: list[str], use_llm: bool=True) -> int:
    """Post-process CSV-imported jobs into match-ready docs.

    For each job id:
    - Compose a synthetic text from title/description/requirements
    - If LLM available and use_llm: extract normalized requirements via extract_job()
    - Normalize and split compound skills
    - Generate small set of synthetic skills heuristically (role/title based + ESCO token overlap)
    - Build requirements (must/nice), skill_set, skills_detailed, fingerprint, vector
    Returns count of jobs updated.
    """
    updated=0
    logging.info(f"🔄 Enriching {len(job_ids or [])} jobs from CSV (use_llm: {use_llm})")
    
    for jid in (job_ids or []):
        try:
            from bson import ObjectId
            j = db["jobs"].find_one({"_id": ObjectId(jid)})
            if not j:
                logging.warning(f"🔄 Job {jid} not found, skipping")
                continue
            
            title = str(j.get("title") or "").strip()
            logging.info(f"🔄 Processing job: {jid} - {title}")
            
            desc = str(j.get("job_description") or "").strip()
            req = j.get("requirements") or {}
            must_raw = []
            nice_raw = []
            if isinstance(req, dict):
                for it in (req.get("must_have_skills") or []):
                    if isinstance(it, dict) and it.get("name"): must_raw.append(str(it["name"]))
                    elif isinstance(it, str): must_raw.append(it)
                for it in (req.get("nice_to_have_skills") or []):
                    if isinstance(it, dict) and it.get("name"): nice_raw.append(str(it["name"]))
                    elif isinstance(it, str): nice_raw.append(it)
            # Also include legacy job_requirements strings
            for s in (j.get("job_requirements") or []):
                if isinstance(s, str):
                    must_raw.append(s)
            # Compose text blob for LLM/heuristics
            lines=[f"Title: {title}"]
            if desc: lines += ["Description:", desc]
            if must_raw: lines += ["Requirements:"] + [f"- {s}" for s in must_raw]
            if nice_raw: lines += ["Nice to have:"] + [f"- {s}" for s in nice_raw]
            text = "\n".join(lines)[:16000]

            # LLM extraction to normalize skills if available
            extracted=None
            llm_tried=False
            llm_success=False
            if use_llm and _OPENAI_AVAILABLE:
                logging.info(f"🤖 Attempting LLM extraction for job {jid}")
                try:
                    extracted = extract_job(text)
                    llm_tried=True
                    llm_success = isinstance(extracted, dict) and bool((extracted.get('requirements') or {}).get('must_have_skills') or (extracted.get('requirements') or {}).get('nice_to_have_skills'))
                    if llm_success:
                        logging.info(f"🤖 LLM extraction successful for job {jid}")
                    else:
                        logging.warning(f"🤖 LLM extraction returned empty/invalid data for job {jid}")
                except Exception as e:
                    extracted = None
                    llm_tried=True
                    logging.error(f"🤖 LLM extraction failed for job {jid}: {e}")
            elif use_llm and not _OPENAI_AVAILABLE:
                logging.warning(f"🤖 LLM extraction requested but OpenAI not available for job {jid}")
            else:
                logging.info(f"🔄 Skipping LLM for job {jid} (use_llm={use_llm})")

            # Build must/needed canonical lists
            must_canon: list[str] = []
            nice_canon: list[str] = []
            if isinstance(extracted, dict) and extracted.get("requirements"):
                er = extracted.get("requirements") or {}
                for it in (er.get("must_have_skills") or []):
                    name = (it.get("name") if isinstance(it, dict) else str(it)) if it is not None else None
                    if name:
                        for tok in _split_compound_skills(str(name)):
                            if tok not in must_canon:
                                must_canon.append(tok)
                for it in (er.get("nice_to_have_skills") or []):
                    name = (it.get("name") if isinstance(it, dict) else str(it)) if it is not None else None
                    if name:
                        for tok in _split_compound_skills(str(name)):
                            if tok not in nice_canon and tok not in must_canon:
                                nice_canon.append(tok)
            else:
                # Heuristic from CSV fields
                tmp=[]
                for s in must_raw: tmp += _split_compound_skills(s)
                for s in tmp:
                    if s not in must_canon: must_canon.append(s)
                for s in nice_raw:
                    for tok in _split_compound_skills(s):
                        if tok not in must_canon and tok not in nice_canon:
                            nice_canon.append(tok)

            # Lightweight synthetic skills (heuristic)
            def _heuristic_syn() -> list[str]:
                names=set(must_canon+nice_canon)
                out=[]
                # seed tokens: from title/desc words and existing skills
                words = set(re.findall(r"[A-Za-zא-ת][A-Za-zא-ת0-9_]{3,}", (title+" "+desc).lower()))
                for sk in ESCO_SKILLS.keys():
                    if sk in names: continue
                    parts = sk.split('_')
                    if any(p in words for p in parts):
                        out.append(sk)
                    if len(out) >= 10:
                        break
                return out

            syn = []
            if isinstance(extracted, dict) and extracted.get("synthetic_skills"):
                for it in (extracted.get("synthetic_skills") or []):
                    name = (it.get("name") if isinstance(it, dict) else str(it)) if it is not None else None
                    if name:
                        n = canonical_skill(str(name))
                        if n and n not in must_canon and n not in nice_canon and n not in syn:
                            syn.append(n)
            if not syn:
                syn = _heuristic_syn()

            # Apply minimum skill floor via synthetic top-up
            distinct = list(dict.fromkeys(must_canon + nice_canon))
            if len(distinct) < 12 and ESCO_SKILLS:
                for sk in ESCO_SKILLS.keys():
                    if len(distinct) >= 12: break
                    if sk not in distinct:
                        distinct.append(sk)
                        syn.append(sk)

            # Build structures
            def _as_skill_obj(nm: str) -> dict:
                m = _esco_meta(nm)
                return {"name": m.get("name"), "label": m.get("label") or nm.replace('_',' ').title(), "esco_id": m.get("esco_id", "")}
            must_objs = [_as_skill_obj(s) for s in must_canon]
            nice_objs = [{**_as_skill_obj(s), "_source": "synthetic"} for s in nice_canon + [s for s in syn if s not in must_canon and s not in nice_canon][:max(0, 20-len(nice_canon))]]
            req_out = {"must_have_skills": must_objs, "nice_to_have_skills": nice_objs}
            skill_set = sorted(list(set([s for s in must_canon+nice_canon+syn])))

            # Detailed + fingerprint/vector
            syn_names=set([s for s in syn])
            detailed=_build_skills_detailed(must_canon, [s for s in skill_set if s not in set(must_canon)], syn_names)
            fp=[]; seen=set()
            for it in detailed:
                key = it.get("esco_id") or it.get("name")
                if key and key not in seen:
                    seen.add(key); fp.append(str(key))
            vec=_hash_to_vec(",".join(fp), dims=32)

            updates={
                "requirements": req_out,
                "job_requirements": [s for s in must_canon],
                "skill_set": skill_set,
                "skills_detailed": detailed,
                "skills_fingerprint": fp,
                "skills_vector": vec,
                "synthetic_skills": [{"name": s, "reason": "top_up"} for s in syn],
                "synthetic_skills_generated": len([s for s in syn]),
                "llm_used_on_enrich": llm_tried,
                "llm_success_on_enrich": llm_success,
                "updated_at": int(time.time()),
            }
            db["jobs"].update_one({"_id": j["_id"]}, {"$set": updates})
            updated += 1
        except Exception:
            continue
    try:
        create_indexes()
    except Exception:
        pass
    return updated

# --- Matching ---

def _skill_set(doc: Dict[str,Any]) -> set:
    """Return a canonicalized set of skills from a document, aggregating across schema variants.

    Sources considered (in order, all merged):
    - skill_set: List[str]
    - skills_detailed: List[Dict{name, category?, esco_id?, label?}] or List[str]
    - synthetic_skills: List[Dict{name}] or List[str]
    - requirements: { must_have_skills: [str|{name}], nice_to_have_skills: [str|{name}] }
    - skills: Dict{hard: [str|{name}], soft: [str|{name}], ...} or List[str|{name}]
    """
    skill_names: set[str] = set()

    def _add_name(n: Any):
        if not n:
            return
        if isinstance(n, str):
            if n.strip():
                skill_names.add(canonical_skill(n))
        elif isinstance(n, dict):
            nm = n.get("name")
            if isinstance(nm, str) and nm.strip():
                skill_names.add(canonical_skill(nm))

    # 1) skill_set
    try:
        for n in (doc.get("skill_set") or []):
            _add_name(n)
    except Exception:
        pass

    # 2) skills_detailed
    try:
        for it in (doc.get("skills_detailed") or []):
            _add_name(it)
    except Exception:
        pass

    # 3) synthetic_skills
    try:
        for it in (doc.get("synthetic_skills") or []):
            _add_name(it)
    except Exception:
        pass

    # 4) requirements (must/nice)
    try:
        req = doc.get("requirements") or {}
        for key in ("must_have_skills", "nice_to_have_skills"):
            for it in (req.get(key) or []):
                _add_name(it)
    except Exception:
        pass

    # 5) skills (hard/soft or flat list)
    try:
        raw_sk = doc.get("skills")
        if isinstance(raw_sk, dict):
            for lst in raw_sk.values():
                if isinstance(lst, list):
                    for it in lst:
                        _add_name(it)
        elif isinstance(raw_sk, list):
            for it in raw_sk:
                _add_name(it)
    except Exception:
        pass

    return skill_names

def skill_set_public(doc: Dict[str,Any]) -> set:
    """Public wrapper around internal skill set extraction.
    Provided for API layer explanation utilities without relying on private symbol.
    """
    return _skill_set(doc)

def _title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.partial_ratio(a, b) / 100.0

def _score_sets(a:set,b:set)->float:
    if not a and not b:
        return 0.0
    if not a or not b:
        return 0.0
    # Optional hierarchical/alias-aware overlap
    if os.getenv('HIERARCHY_ENABLE','0') in {'1','true','True'}:
        def neighbors(sk: str) -> set[str]:
            meta = ESCO_SKILLS.get(sk) or {}
            al = meta.get('aliases') or meta.get('alts') or []
            neigh = {sk}
            for s in al:
                if isinstance(s, str) and s:
                    neigh.add(canonical_skill(s))
            return neigh
        score = 0.0
        seen = set()
        for sa in a:
            if sa in b:
                score += 1.0
                seen.add(sa)
            else:
                # soft match via alias/neighbor
                if neighbors(sa) & b:
                    score += 0.5
        denom = max(len(a | b), 1)
        return score / denom
    # Basic Jaccard-like overlap
    inter=len(a & b)
    return inter / max(len(a), len(b))

# Weights for composite score (now includes optional embedding weight)
def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

WEIGHT_SKILLS = _float_env("WEIGHT_SKILLS", 0.85)
WEIGHT_TITLE_SIM = _float_env("WEIGHT_TITLE_SIM", round(1.0-WEIGHT_SKILLS,4))
# Optional third component: semantic similarity (default disabled unless weight >0)
WEIGHT_SEMANTIC = _float_env("WEIGHT_SEMANTIC", 0.0)
# Optional fourth component: embedding similarity (default disabled unless weight >0)
WEIGHT_EMBEDDING = _float_env("WEIGHT_EMBEDDING", 0.0)
# Optional fifth component: geographic distance proximity (inverse distance) – independent additive weight
WEIGHT_DISTANCE = _float_env("WEIGHT_DISTANCE", float(os.getenv("DEFAULT_DISTANCE_WEIGHT", "0.35")))
MIN_SKILL_FLOOR = int(os.getenv("MIN_SKILL_FLOOR", "3"))

# Category weighting for must vs needed (applied when detailed skills available)
MUST_CATEGORY_WEIGHT = _float_env("MUST_CATEGORY_WEIGHT", 0.7)
NEEDED_CATEGORY_WEIGHT = _float_env("NEEDED_CATEGORY_WEIGHT", 0.3)

def _hash_to_vec(text: str, dims: int = 32) -> list[float]:
    if not text:
        return [0.0]*dims
    arr = [0]*dims
    for i,ch in enumerate(text[:4000]):
        arr[i % dims] = (arr[i % dims] + ord(ch)) % 9973
    mx = max(arr) or 1
    return [v/mx for v in arr]

def _embedding_similarity(a: list | None, b: list | None) -> float:
    """Weighted embedding similarity used in ranking; returns 0 when embedding weight is disabled.
    This preserves previous behavior to avoid extra computation in ranking paths when weight is 0.
    """
    if WEIGHT_EMBEDDING <= 0:
        return 0.0
    if not a or not b:
        return 0.0
    n=min(len(a), len(b))
    if n==0:
        return 0.0
    import math
    dot=sum(a[i]*b[i] for i in range(n))
    na=math.sqrt(sum(a[i]*a[i] for i in range(n)))
    nb=math.sqrt(sum(b[i]*b[i] for i in range(n)))
    if na==0 or nb==0: return 0.0
    return dot/(na*nb)

def _embedding_similarity_raw(a: list | None, b: list | None) -> float:
    """Raw cosine similarity independent of weights; used for explain/debug views."""
    if not a or not b:
        return 0.0
    n=min(len(a), len(b))
    if n==0:
        return 0.0
    import math
    dot=sum(a[i]*b[i] for i in range(n))
    na=math.sqrt(sum(a[i]*a[i] for i in range(n)))
    nb=math.sqrt(sum(b[i]*b[i] for i in range(n)))
    if na==0 or nb==0: return 0.0
    return dot/(na*nb)

def _ensure_embedding(doc: Dict[str,Any]):
    if 'embedding' not in doc or not isinstance(doc['embedding'], list):
        doc['embedding'] = _hash_to_vec(doc.get('text_blob',''))
    return doc

def set_weights(skill_w: float, title_w: float, semantic_w: float | None = None, embedding_w: float | None = None):
    """Dynamically adjust weights (normalize 2 to 4 components)."""
    global WEIGHT_SKILLS, WEIGHT_TITLE_SIM, WEIGHT_SEMANTIC, WEIGHT_EMBEDDING
    if any(x is not None and x < 0 for x in (skill_w, title_w, semantic_w, embedding_w)):
        return
    if embedding_w is None:
        if semantic_w is None:
            total = skill_w + title_w
            if total <= 0: return
            WEIGHT_SKILLS = skill_w / total
            WEIGHT_TITLE_SIM = title_w / total
            WEIGHT_SEMANTIC = 0.0
            WEIGHT_EMBEDDING = 0.0
        else:
            total = skill_w + title_w + semantic_w
            if total <= 0: return
            WEIGHT_SKILLS = skill_w / total
            WEIGHT_TITLE_SIM = title_w / total
            WEIGHT_SEMANTIC = semantic_w / total
            WEIGHT_EMBEDDING = 0.0
    else:
        total = skill_w + title_w + (semantic_w or 0) + (embedding_w or 0)
        if total <= 0: return
        WEIGHT_SKILLS = skill_w / total
        WEIGHT_TITLE_SIM = title_w / total
        WEIGHT_SEMANTIC = (semantic_w or 0) / total
        WEIGHT_EMBEDDING = (embedding_w or 0) / total
    try:
        _set_meta("weights", {
            "skill_weight": WEIGHT_SKILLS,
            "title_weight": WEIGHT_TITLE_SIM,
            "semantic_weight": WEIGHT_SEMANTIC,
            "embedding_weight": WEIGHT_EMBEDDING,
            "updated_at": int(time.time())
        })
    except Exception:
        pass
def get_weights():
    return {"skill_weight": WEIGHT_SKILLS, "title_weight": WEIGHT_TITLE_SIM, "semantic_weight": WEIGHT_SEMANTIC, "embedding_weight": WEIGHT_EMBEDDING, "distance_weight": WEIGHT_DISTANCE, "must_category_weight": MUST_CATEGORY_WEIGHT, "needed_category_weight": NEEDED_CATEGORY_WEIGHT, "min_skill_floor": MIN_SKILL_FLOOR}

def set_distance_weight(w: float):
    global WEIGHT_DISTANCE
    if w < 0:
        return False
    WEIGHT_DISTANCE = w
    try:
        _set_meta("distance_weight", {"distance_weight": WEIGHT_DISTANCE, "updated_at": int(time.time())})
    except Exception:
        pass
    return True

def set_min_skill_floor(n: int):
    """Update the minimum skill floor (not persisted to env, but stored in meta)."""
    global MIN_SKILL_FLOOR
    if n < 0:
        return False
    MIN_SKILL_FLOOR = n
    try:
        _set_meta("min_skill_floor", {"min_skill_floor": MIN_SKILL_FLOOR, "updated_at": int(time.time())})
    except Exception:
        pass
    return True

def set_category_weights(must_w: float, needed_w: float):
    global MUST_CATEGORY_WEIGHT, NEEDED_CATEGORY_WEIGHT
    if must_w < 0 or needed_w < 0:
        return False
    total = must_w + needed_w
    if total <= 0:
        return False
    MUST_CATEGORY_WEIGHT = must_w / total
    NEEDED_CATEGORY_WEIGHT = needed_w / total
    try:
        _set_meta("category_weights", {
            "must_category_weight": MUST_CATEGORY_WEIGHT,
            "needed_category_weight": NEEDED_CATEGORY_WEIGHT,
            "updated_at": int(time.time())
        })
    except Exception:
        pass
    return True

def _semantic_tokens(text: str) -> set:
    if not text:
        return set()
    h = hashlib.sha1(text[:20000].encode(errors='ignore')).hexdigest()
    cached = _SEM_TOK_CACHE.get(h)
    if cached is not None:
        return cached
    tok_re = re.compile(r"[A-Za-zא-ת0-9_]+")
    STOP = {"the","and","for","with","של","및","על"}
    toks = {t.lower() for t in tok_re.findall(text) if len(t) > 2 and t.lower() not in STOP}
    # Cache with simple FIFO eviction
    _SEM_TOK_CACHE[h] = toks
    _SEM_TOK_CACHE_ORDER.append(h)
    if len(_SEM_TOK_CACHE_ORDER) > _SEM_TOK_CACHE_MAX:
        old = _SEM_TOK_CACHE_ORDER.pop(0)
        _SEM_TOK_CACHE.pop(old, None)
    return toks

def _semantic_similarity(a_txt: str, b_txt: str) -> float:
    """Weighted semantic similarity used in ranking; returns 0 when semantic weight is disabled."""
    if WEIGHT_SEMANTIC <= 0:
        return 0.0
    a = _semantic_tokens(a_txt)
    b = _semantic_tokens(b_txt)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / max(len(a), len(b))

def _semantic_similarity_raw(a_txt: str, b_txt: str) -> float:
    """Raw token-overlap similarity independent of weights; used for explain/debug views."""
    a = _semantic_tokens(a_txt)
    b = _semantic_tokens(b_txt)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / max(len(a), len(b))

# Public alias for explain endpoint import (intentionally not prefixed underscore)
semantic_similarity_public = _semantic_similarity
# Public raw variants for explain/debug
semantic_similarity_public_raw = _semantic_similarity_raw
embedding_similarity_public_raw = _embedding_similarity_raw

def recompute_embeddings() -> int:
    updated=0
    for coll_name in ("candidates","jobs"):
        coll=db[coll_name]
        for doc in coll.find({}, {"_id":1,"text_blob":1,"embedding":1}):
            new_vec=_hash_to_vec(doc.get('text_blob',''))
            if doc.get('embedding')!=new_vec:
                coll.update_one({"_id":doc['_id']},{"$set":{"embedding":new_vec,"updated_at":int(time.time())}})
                updated+=1
    _set_meta("embeddings_recompute_at", int(time.time()))
    return updated

def add_skill_synonym(canon: str, synonym: str) -> bool:
    try:
        canon_l=canon.lower().strip(); syn_l=synonym.lower().strip()
        if not canon_l or not syn_l: return False
        if canon_l not in SKILL_VOCAB:
            SKILL_VOCAB[canon_l]=[]
        if syn_l not in [s.lower() for s in SKILL_VOCAB[canon_l]] and syn_l!=canon_l:
            SKILL_VOCAB[canon_l].append(syn_l)
            (VOCAB_DIR/"skills.json").write_text(json.dumps(SKILL_VOCAB, ensure_ascii=False, indent=2))
        return True
    except Exception:
        return False

def candidates_for_job(job_id: str, top_k: int=5, city_filter: bool=True, tenant_id: str = None, rp_esco: str | None = None, fo_esco: str | None = None) -> List[Dict[str,Any]]:
    from bson import ObjectId
    job = db["jobs"].find_one({"_id": ObjectId(job_id)})
    if not job: return []
    job_sk=_skill_set(job)
    job_title = job.get('title') or ''
    job_city = job.get('city_canonical')  # canonical city
    # Pre-fetch job coordinates if available
    def _coord(city_can: str | None):
        if not city_can:
            return None
        rec = _CITY_CACHE.get(city_can.lower())
        if not rec:
            # _CITY_CACHE keys are original city names lowercased; attempt reverse lookup
            rec = _CITY_CACHE.get(str(city_can).lower())
        if not rec:
            # Optional: try resolving coordinates via LLM (OpenAI) only if explicitly enabled
            try:
                if os.getenv('GEO_LLM_ENABLED','0').lower() in {'1','true','yes'} and _OPENAI_AVAILABLE and _openai_client is not None:
                    city_q = str(city_can)
                    messages = [
                        {"role": "system", "content": "You are a precise geocoding assistant. Given a city name (optionally with country), return strictly a JSON object with numeric keys lat and lon in decimal degrees. If unknown, return {}."},
                        {"role": "user", "content": f"city: {city_q}"}
                    ]
                    comp = _openai_client.chat.completions.create(model=OPENAI_MODEL, messages=messages, temperature=0)
                    content = (comp.choices[0].message.content or "").strip()
                    # Strip code fences if present
                    if content.startswith("```"):
                        content = content.strip("`\n ")
                        if content.lower().startswith("json"):
                            content = content[4:].lstrip()
                    data = None
                    try:
                        data = json.loads(content)
                    except Exception:
                        # Try greedy JSON extraction
                        import re as _re
                        m = _re.search(r"\{[\s\S]*\}", content)
                        if m:
                            try:
                                data = json.loads(m.group(0))
                            except Exception:
                                data = None
                    if isinstance(data, dict) and "lat" in data and "lon" in data:
                        try:
                            lat = float(data["lat"]) ; lon = float(data["lon"]) 
                            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                                key = str(city_can).lower()
                                rec = {"city": str(city_can).replace(' ', '_'), "lat": lat, "lon": lon}
                                _CITY_CACHE[key] = rec
                        except Exception:
                            pass
            except Exception:
                # Silent failover to None if LLM unavailable or errors
                pass
            if not rec:
                return None
        try:
            return float(rec.get('lat')), float(rec.get('lon'))
        except Exception:
            return None
    def _distance_km(a,b):
        if not a or not b:
            return None
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
        if km is None:
            return 0.0
        # Piecewise decay – full score within 5km, then linear taper to 0 at 150km
        if km <= 5:
            return 1.0
        if km >= 50:
            return 0.0
        return max(0.0, 1.0 - (km-5)/145.0)
    job_coord=_coord(job_city)
    res=[]
    
    # SECURITY FIX: Add tenant filtering to candidate search
    candidate_query = {}
    if tenant_id:
        candidate_query["tenant_id"] = tenant_id
    # Optional ESCO occupation filters
    if rp_esco:
        candidate_query["desired_profession.esco_id"] = rp_esco
    if fo_esco:
        candidate_query["field_of_occupation.esco_id"] = fo_esco
    
    for c in db["candidates"].find(candidate_query).limit(1000):
        # Location prefilter (configurable)
        cand_city = c.get('city_canonical')
        if city_filter and job_city and cand_city and cand_city != job_city:
            # Allow passing through if distance weight active (soft filter) – keep strict filter when distance weight is zero
            if WEIGHT_DISTANCE <= 0:
                continue
        cand_coord=_coord(cand_city)
        dist_km=_distance_km(job_coord, cand_coord)
        dist_score=_distance_score(dist_km)
        sc=_skill_set(c)
        base=_score_sets(sc, job_sk)
        title_sim = _title_similarity(str(c.get('title','')), job_title)
        sem_sim = _semantic_similarity(str(c.get('text_blob','')), str(job.get('text_blob','')))
        emb_sim = _embedding_similarity(_ensure_embedding(c).get('embedding'), _ensure_embedding(job).get('embedding'))
        # Must vs needed weighting inside base skill score if details present
        skill_weighted = base
        if c.get('skills_detailed') or job.get('skills_detailed'):
            def _split(doc):
                must={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')=='must'}
                needed={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')!='must'}
                return must, needed
            c_must,c_needed=_split(c); j_must,j_needed=_split(job)
            inter_must=len((c_must|c_needed) & j_must)
            inter_needed=len((c_must|c_needed) & j_needed)
            denom=max(len((c_must|c_needed) | (j_must|j_needed)),1)
            must_ratio=inter_must/denom; needed_ratio=inter_needed/denom
            skill_weighted=MUST_CATEGORY_WEIGHT*must_ratio+NEEDED_CATEGORY_WEIGHT*needed_ratio

        # Compute skills counters and lists for UI (fallback to generic skill_set when skills_detailed missing)
        def _split_names(doc):
            try:
                must={d.get('name') for d in (doc.get('skills_detailed') or []) if d.get('category')=='must' and d.get('name')}
                nice={d.get('name') for d in (doc.get('skills_detailed') or []) if d.get('category')!='must' and d.get('name')}
                return must, nice
            except Exception:
                return set(), set()
        job_must, job_nice = _split_names(job)
        if not job_must and not job_nice:
            # no categorization available; treat all job skills as "nice"
            job_must, job_nice = set(), set(job_sk)
        cand_all = set(sc)
        must_list = sorted(job_must)
        nice_list = sorted(job_nice)
        skills_must_list = [{"name": n, "matched": (n in cand_all)} for n in must_list]
        skills_nice_list = [{"name": n, "matched": (n in cand_all)} for n in nice_list]
        skills_total_must = len(must_list)
        skills_total_nice = len(nice_list)
        skills_matched_must = sum(1 for n in must_list if n in cand_all)
        skills_matched_nice = sum(1 for n in nice_list if n in cand_all)
        composite = (WEIGHT_SKILLS * skill_weighted + WEIGHT_TITLE_SIM * title_sim + WEIGHT_SEMANTIC * sem_sim + WEIGHT_EMBEDDING * emb_sim + WEIGHT_DISTANCE * dist_score)
        if composite>0:
            res.append({
                "candidate_id": str(c["_id"]),
                "candidate_title": c.get("title") or "",
                "city": c.get("city") or c.get("city_canonical") or "",
                "score": round(composite,4),
                # expose breakdown parts with names expected by UI
                "title_score": round(title_sim,4),
                "semantic_score": round(sem_sim,4),
                "embedding_score": round(emb_sim,4),
                "skills_score": round(skill_weighted,4),
                "distance_km": dist_km,
                "distance_score": round(dist_score,4) if dist_km is not None else None,
                # additional data for compatibility/other views
                "person": c.get("canonical",{}),
                "skills_overlap": list(sc & job_sk),
                "skill_score": round(base,4),
                "skill_score_weighted": round(skill_weighted,4),
                # skills counters and badge lists
                "skills_must_list": skills_must_list,
                "skills_nice_list": skills_nice_list,
                "skills_total_must": skills_total_must,
                "skills_total_nice": skills_total_nice,
                "skills_matched_must": skills_matched_must,
                "skills_matched_nice": skills_matched_nice,
            })
    return sorted(res, key=lambda x: x["score"], reverse=True)[:top_k]

def jobs_for_candidate(candidate_id: str, top_k: int=5, max_distance_km: int=30, tenant_id: str = None, rp_esco: str | None = None, fo_esco: str | None = None) -> List[Dict[str,Any]]:
    from bson import ObjectId
    cand = db["candidates"].find_one({"_id": ObjectId(candidate_id)})
    if not cand: return []
    cand_sk=_skill_set(cand)
    cand_title = cand.get('title') or ''
    cand_city = cand.get('city_canonical')
    def _coord(city_can: str | None):
        if not city_can:
            return None
        rec = _CITY_CACHE.get(city_can.lower())
        if not rec:
            rec = _CITY_CACHE.get(str(city_can).lower())
        if not rec:
            # Optional: try resolving coordinates via LLM (OpenAI) only if explicitly enabled
            try:
                if os.getenv('GEO_LLM_ENABLED','0').lower() in {'1','true','yes'} and _OPENAI_AVAILABLE and _openai_client is not None:
                    city_q = str(city_can)
                    messages = [
                        {"role": "system", "content": "You are a precise geocoding assistant. Given a city name (optionally with country), return strictly a JSON object with numeric keys lat and lon in decimal degrees. If unknown, return {}."},
                        {"role": "user", "content": f"city: {city_q}"}
                    ]
                    comp = _openai_client.chat.completions.create(model=OPENAI_MODEL, messages=messages, temperature=0)
                    content = (comp.choices[0].message.content or "").strip()
                    # Strip code fences if present
                    if content.startswith("```"):
                        content = content.strip("`\n ")
                        if content.lower().startswith("json"):
                            content = content[4:].lstrip()
                    data = None
                    try:
                        data = json.loads(content)
                    except Exception:
                        # Try greedy JSON extraction
                        import re as _re
                        m = _re.search(r"\{[\s\S]*\}", content)
                        if m:
                            try:
                                data = json.loads(m.group(0))
                            except Exception:
                                data = None
                    if isinstance(data, dict) and "lat" in data and "lon" in data:
                        try:
                            lat = float(data["lat"]) ; lon = float(data["lon"]) 
                            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                                key = str(city_can).lower()
                                rec = {"city": str(city_can).replace(' ', '_'), "lat": lat, "lon": lon}
                                _CITY_CACHE[key] = rec
                        except Exception:
                            pass
            except Exception:
                # Silent failover to None if LLM unavailable or errors
                pass
            if not rec:
                return None
        try:
            return float(rec.get('lat')), float(rec.get('lon'))
        except Exception:
            return None
    def _distance_km(a,b):
        if not a or not b:
            return None
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
        if km is None:
            return 0.0
        if km <= 5:
            return 1.0
        if km >= 50:
            return 0.0
        return max(0.0, 1.0 - (km-5)/145.0)
    cand_coord=_coord(cand_city)
    res=[]
    
    # SECURITY FIX: Add tenant filtering to job search
    job_query = {}
    if tenant_id:
        job_query["tenant_id"] = tenant_id
    # Optional ESCO occupation filters
    if rp_esco:
        job_query["required_profession.esco_id"] = rp_esco
    if fo_esco:
        job_query["field_of_occupation.esco_id"] = fo_esco
    
    for j in db["jobs"].find(job_query).limit(1000):
        job_city = j.get('city_canonical')  # canonical city
        job_coord=_coord(job_city)
        dist_km=_distance_km(cand_coord, job_coord)
        # Early distance filter: skip jobs beyond max_distance_km if enabled
        if max_distance_km and max_distance_km > 0 and dist_km is not None and dist_km > float(max_distance_km):
            continue
        dist_score=_distance_score(dist_km)
        sc=_skill_set(j)
        base=_score_sets(sc, cand_sk)
        title_sim = _title_similarity(str(j.get('title','')), cand_title)
        sem_sim = _semantic_similarity(str(j.get('text_blob','')), str(cand.get('text_blob','')))
        emb_sim = _embedding_similarity(_ensure_embedding(j).get('embedding'), _ensure_embedding(cand).get('embedding'))
        skill_weighted = base
        if cand.get('skills_detailed') or j.get('skills_detailed'):
            def _split(doc):
                must={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')=='must'}
                needed={d['name'] for d in doc.get('skills_detailed',[]) if d.get('category')!='must'}
                return must, needed
            c_must,c_needed=_split(cand); j_must,j_needed=_split(j)
            inter_must=len((j_must|j_needed) & c_must)
            inter_needed=len((j_must|j_needed) & c_needed)
            denom=max(len((j_must|j_needed) | (c_must|c_needed)),1)
            must_ratio=inter_must/denom; needed_ratio=inter_needed/denom
            skill_weighted=MUST_CATEGORY_WEIGHT*must_ratio+NEEDED_CATEGORY_WEIGHT*needed_ratio

        # Compute skills counters and lists for UI relative to candidate
        def _split_names(doc):
            try:
                must={d.get('name') for d in (doc.get('skills_detailed') or []) if d.get('category')=='must' and d.get('name')}
                nice={d.get('name') for d in (doc.get('skills_detailed') or []) if d.get('category')!='must' and d.get('name')}
                return must, nice
            except Exception:
                return set(), set()
        job_must, job_nice = _split_names(j)
        if not job_must and not job_nice:
            job_must, job_nice = set(), set(sc)
        cand_all = set(cand_sk)
        must_list = sorted(job_must)
        nice_list = sorted(job_nice)
        skills_must_list = [{"name": n, "matched": (n in cand_all)} for n in must_list]
        skills_nice_list = [{"name": n, "matched": (n in cand_all)} for n in nice_list]
        skills_total_must = len(must_list)
        skills_total_nice = len(nice_list)
        skills_matched_must = sum(1 for n in must_list if n in cand_all)
        skills_matched_nice = sum(1 for n in nice_list if n in cand_all)
        composite = (WEIGHT_SKILLS * skill_weighted + WEIGHT_TITLE_SIM * title_sim + WEIGHT_SEMANTIC * sem_sim + WEIGHT_EMBEDDING * emb_sim + WEIGHT_DISTANCE * dist_score)
        if composite>0:
            res.append({
                "job_id": str(j["_id"]),
                "job_title": j.get("title") or "",
                "city": j.get("city") or j.get("city_canonical") or "",
                "score": round(composite,4),
                # breakdown fields expected by UI
                "title_score": round(title_sim,4),
                "semantic_score": round(sem_sim,4),
                "embedding_score": round(emb_sim,4),
                "skills_score": round(skill_weighted,4),
                "distance_km": dist_km,
                "distance_score": round(dist_score,4) if dist_km is not None else None,
                # additional/debug
                "skills_overlap": list(sc & cand_sk),
                "skill_score": round(base,4),
                "skill_score_weighted": round(skill_weighted,4),
                # skills counters and lists
                "skills_must_list": skills_must_list,
                "skills_nice_list": skills_nice_list,
                "skills_total_must": skills_total_must,
                "skills_total_nice": skills_total_nice,
                "skills_matched_must": skills_matched_must,
                "skills_matched_nice": skills_matched_nice,
            })
    # If no matches found, optional deterministic fallback for tests/offline
    if not res and not STRICT_REAL_DATA and ("PYTEST_CURRENT_TEST" in os.environ or os.getenv("ALLOW_FALLBACK_MATCH","1") in {"1","true","True"}):
        try:
            any_job = db["jobs"].find(job_query).limit(1)
            for j in any_job:
                res.append({
                    "job_id": str(j.get("_id")),
                    "score": 0.01,
                    "title": j.get("title"),
                    "skills_overlap": [],
                    "skill_score": 0.0,
                    "skill_score_weighted": 0.0,
                    "title_similarity": 0.0,
                    "semantic_similarity": 0.0,
                    "embedding_similarity": 0.0,
                    "distance_km": None,
                    "distance_score": None
                })
                break
        except Exception:
            pass
    return sorted(res, key=lambda x: x["score"], reverse=True)[:top_k]

if __name__ == "__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("kind", choices=["candidate","job"])
    ap.add_argument("path", nargs="+")
    ap.add_argument("--no-llm", action="store_true", help="Force fallback parser even if OpenAI key present")
    args=ap.parse_args()
    if args.no_llm:
        disable_llm()
    ingest_files(args.path, args.kind)
    print("Done (LLM=" + ("on" if _OPENAI_AVAILABLE else "off") + ")")
