from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from bson import ObjectId
import csv
import io
import time
import re

from .ingest_agent import db, canonical_city, enrich_jobs_from_csv
from .auth import get_current_user, require_admin
from .tenants import create_tenant_record, create_user

router = APIRouter(prefix="/tenants", tags=["tenants"])


class TenantCreate(BaseModel):
    name: str
    admin_name: str
    admin_email: EmailStr
    admin_password: str


class TenantOut(BaseModel):
    tenant_id: str
    slug: str
    name: str
    portal_url: str


class UserOut(BaseModel):
    user_id: str
    email: EmailStr
    role: str
    tenant_id: Optional[str] = None
    name: Optional[str] = None
    created_at: Optional[int] = None


class UserCreateAdmin(BaseModel):
    name: str
    email: EmailStr
    password: str
    role: str
    tenant_id: Optional[str] = None


class CSVImportResponse(BaseModel):
    inserted_count: int
    replaced_count: int


class TenantListItem(BaseModel):
    tenant_id: str
    name: str
    slug: str


_slug_re = re.compile(r"[^a-z0-9]+")


def _ensure_tenant(tenant_id: str) -> str:
    try:
        oid = ObjectId(tenant_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_tenant_id")
    doc = db["tenants"].find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="tenant_not_found")
    return str(oid)


@router.post("", response_model=TenantOut, status_code=status.HTTP_201_CREATED)
def create_tenant(payload: TenantCreate):
    email = payload.admin_email.lower().strip()
    if db["users"].find_one({"email": email}):
        raise HTTPException(status_code=409, detail="email_exists")
    tenant_id, slug = create_tenant_record(payload.name.strip())
    user_id = create_user(tenant_id, email, payload.admin_password, payload.admin_name.strip(), role="vc_admin")
    try:
        db["tenants"].update_one({"_id": ObjectId(tenant_id)}, {"$set": {"created_by": user_id}})
    except Exception:
        pass
    portal_url = f"/portal/{slug}"
    return TenantOut(tenant_id=tenant_id, slug=slug, name=payload.name.strip(), portal_url=portal_url)


@router.get("", response_model=List[TenantListItem], dependencies=[Depends(require_admin)])
def list_tenants():
    items: List[TenantListItem] = []
    cur = db["tenants"].find({}, {"name": 1, "slug": 1}).sort("created_at", -1)
    for doc in cur:
        items.append(TenantListItem(tenant_id=str(doc.get("_id")), name=doc.get("name", ""), slug=doc.get("slug", "")))
    return items


@router.get("/users", dependencies=[Depends(require_admin)])
def list_users():
    items: List[UserOut] = []
    cur = db["users"].find({}).sort("created_at", -1)
    for doc in cur:
        tid = doc.get("tenant_id")
        items.append(UserOut(
            user_id=str(doc.get("_id")),
            email=doc.get("email"),
            role=doc.get("role") or "user",
            tenant_id=str(tid) if tid else None,
            name=doc.get("name"),
            created_at=doc.get("created_at")
        ))
    return {"items": items}


@router.post("/users", status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_admin)])
def create_user_admin(payload: UserCreateAdmin):
    email = payload.email.lower().strip()
    if db["users"].find_one({"email": email}):
        raise HTTPException(status_code=409, detail="email_exists")
    allowed_roles = {"admin", "vc_admin", "recruiter", "analyst", "candidate"}
    if payload.role not in allowed_roles:
        raise HTTPException(status_code=400, detail="invalid_role")
    tenant_id: Optional[str] = None
    if payload.tenant_id:
        tenant_id = _ensure_tenant(payload.tenant_id)
    user_id = create_user(tenant_id, email, payload.password, payload.name, role=payload.role)
    return {"user_id": user_id}


@router.delete("/users/{user_id}", dependencies=[Depends(require_admin)])
def delete_user(user_id: str):
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_user_id")
    result = db["users"].delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="user_not_found")
    return {"success": True}


@router.post("/{tenant_id}/jobs/upload", response_model=CSVImportResponse)
async def upload_jobs_csv(tenant_id: str, file: UploadFile = File(...), current_user=Depends(get_current_user)):
    if current_user.get("role") not in {"admin", "vc_admin"}:
        raise HTTPException(status_code=403, detail="forbidden")
    tenant_key = _ensure_tenant(tenant_id)
    if current_user.get("role") == "vc_admin":
        if not current_user.get("tenant_id") or current_user.get("tenant_id") != tenant_key:
            raise HTTPException(status_code=403, detail="wrong_tenant")
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="missing_file")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="empty_file")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    # Support legacy / alternate headers by normalizing keys.
    def _canon(k: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", k.lower()).strip("_")

    header_map = {}
    for h in reader.fieldnames or []:
        hk = _canon(h)
        # Map known variants to internal field names
        if hk in {"title", "job_title"}:
            header_map[h] = "title"
        elif hk in {"description", "job_description"}:
            header_map[h] = "description"
        elif hk in {"portfolio_company", "company", "company_name"}:
            header_map[h] = "company_name"
        elif hk in {"city", "location"}:
            header_map[h] = "city"
        elif hk in {"must_have", "must_haves"}:
            header_map[h] = "must_have"
        elif hk in {"nice_to_have", "nice_haves"}:
            header_map[h] = "nice_to_have"
        elif hk in {"remote"}:
            header_map[h] = "remote"
        elif hk in {"external_job_id", "job_id", "external_id"}:
            header_map[h] = "external_job_id"
        elif hk in {"employment_type"}:
            header_map[h] = "employment_type"
        elif hk in {"salary", "salary_range", "comp_range"}:
            header_map[h] = "salary_range"
        else:
            header_map[h] = hk  # keep a normalized placeholder

    required_core = ["title", "description"]  # external_job_id will be auto-generated if missing
    jobs: List[dict] = []
    now = int(time.time())
    total_rows = 0
    skipped_empty = 0
    skipped_missing_fields = 0

    def _gen_external_id(idx: int, title: str, company: str) -> str:
        base = re.sub(r"[^a-z0-9]+", "-", (company or "")[:12].lower() + "-" + title.lower())
        base = re.sub(r"-+", "-", base).strip("-")[:40]
        return f"{base or 'job'}-{idx+1}"

    for idx, row in enumerate(reader):
        total_rows += 1
        if not row:
            skipped_empty += 1
            continue
        # Build a normalized dict
        norm: dict[str, str] = {}
        for raw_key, val in row.items():
            if raw_key is None:
                continue
            mapped = header_map.get(raw_key, raw_key)
            norm[mapped] = (val or "").strip()

        missing = [f for f in required_core if not norm.get(f)]
        if missing:
            skipped_missing_fields += 1
            continue

        must = [s.strip() for s in (norm.get("must_have") or "").split("|") if s.strip()]
        nice = [s.strip() for s in (norm.get("nice_to_have") or "").split("|") if s.strip()]
        merged: List[str] = []
        seen: set[str] = set()
        for skill in must + nice:
            lk = skill.lower()
            if lk in seen:
                continue
            seen.add(lk)
            merged.append(skill)

        city = norm.get("city") or None
        remote_flag = False
        remote_raw = (norm.get("remote") or "").lower()
        if remote_raw in {"true", "1", "yes"}:
            remote_flag = True
        elif city and city.lower() in {"remote", "anywhere"}:
            remote_flag = True

        company_name = norm.get("company_name") or None
        title_val = norm.get("title") or ""
        external_id = norm.get("external_job_id") or _gen_external_id(idx, title_val, company_name or "")

        job_doc = {
            "tenant_id": tenant_key,
            "external_job_id": external_id,
            "title": title_val,
            "city": city,
            "city_canonical": canonical_city(city) if city else None,
            "job_description": norm.get("description", ""),
            "requirements": {
                "must_have_skills": [{"name": skill} for skill in must],
                "nice_to_have_skills": [{"name": skill} for skill in nice],
            },
            "job_requirements": merged,
            "skill_set": merged,
            "remote": remote_flag,
            "company_name": company_name,
            "application_url": norm.get("application_url") or None,
            "company_website": norm.get("company_website") or None,
            "profession": norm.get("profession") or None,
            "occupation_field": norm.get("occupation_field") or None,
            # Additional optional mapped fields (not yet exposed in portal but stored for future use)
            "employment_type": norm.get("employment_type") or None,
            "salary_range": norm.get("salary_range") or None,
            "created_at": now,
            "updated_at": now,
        }
        jobs.append(job_doc)
    replaced = db["jobs"].delete_many({"tenant_id": tenant_key}).deleted_count
    inserted_count = 0
    inserted_ids: List[str] = []
    if jobs:
        result = db["jobs"].insert_many(jobs)
        inserted_count = len(result.inserted_ids)
        inserted_ids = [str(_id) for _id in result.inserted_ids]
        try:
            if inserted_ids:
                enrich_jobs_from_csv(inserted_ids, use_llm=True)
        except Exception:
            pass
    # Lightweight structured debug summary (stdout). Could be replaced with proper logger.
    try:
        print(
            "CSVUploadSummary | tenant=%s total=%d created=%d skipped_empty=%d skipped_missing_required=%d" % (
                tenant_key, total_rows, len(jobs), skipped_empty, skipped_missing_fields
            )
        )
    except Exception:
        pass

    return CSVImportResponse(inserted_count=inserted_count, replaced_count=replaced)


@router.get("/public/portal/{slug}")
def get_public_portal(slug: str):
    tenant = db["tenants"].find_one({"slug": slug})
    if not tenant and ObjectId.is_valid(slug):
        tenant = db["tenants"].find_one({"_id": ObjectId(slug)})
    if not tenant:
        raise HTTPException(status_code=404, detail="portal_not_found")
    tenant_key = str(tenant.get("_id"))
    jobs_cursor = db["jobs"].find({"tenant_id": tenant_key}).sort([("created_at", -1), ("_id", -1)])
    jobs: List[dict] = []
    companies = set()
    locations = set()
    for job in jobs_cursor:
        company_name = job.get("company_name") or ""
        location = job.get("city") or "Remote"
        if company_name:
            companies.add(company_name)
        if location:
            locations.add(location)
        jobs.append({
            "job_id": str(job.get("_id")),
            "title": job.get("title"),
            "company_name": company_name,
            "description": job.get("job_description", ""),
            "requirements": job.get("skill_set", []) or [],
            "location": location,
            "remote": job.get("remote", False),
            "application_url": job.get("application_url"),
            "company_website": job.get("company_website"),
        })
    return {
        "name": tenant.get("name"),
        "slug": tenant.get("slug"),
        "stats": {
            "job_count": len(jobs),
            "company_count": len(companies),
            "location_count": len(locations),
        },
        "jobs": jobs,
    }
