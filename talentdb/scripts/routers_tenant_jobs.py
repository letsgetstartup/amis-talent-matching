from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from .ingest_agent import db
from .auth import require_tenant
import re


router = APIRouter(prefix="/tenant", tags=["jobs"])


@router.get("/jobs")
def list_tenant_jobs(tenant_id: str = Depends(require_tenant), skip: int = 0, limit: int = 50, q: Optional[str] = None):
    if limit > 200:
        limit = 200
    if skip < 0:
        skip = 0
    query: dict = {"tenant_id": tenant_id}
    if q:
        pattern = f".*{re.escape(q)}.*"
        query["$or"] = [
            {"title": {"$regex": pattern, "$options": "i"}},
            {"city_canonical": {"$regex": pattern, "$options": "i"}},
            {"job_description": {"$regex": pattern, "$options": "i"}},
            {"skill_set": {"$elemMatch": {"$regex": pattern, "$options": "i"}}},
        ]
    total = db["jobs"].count_documents(query)
    cur = (
        db["jobs"]
        .find(query, {"external_job_id": 1, "title": 1, "city_canonical": 1, "updated_at": 1, "llm_used_on_enrich": 1, "llm_success_on_enrich": 1})
        .skip(skip)
        .limit(limit)
        .sort([["updated_at", -1], ["_id", -1]])
    )
    rows = []
    for d in cur:
        rows.append(
            {
                "job_id": str(d.get("_id")),
                "external_job_id": d.get("external_job_id"),
                "title": d.get("title"),
                "city": d.get("city_canonical"),
                "updated_at": d.get("updated_at"),
                "llm_used": d.get("llm_used_on_enrich", False),
                "llm_success": d.get("llm_success_on_enrich", False),
            }
        )
    return {"results": rows, "total": total, "skip": skip, "limit": limit, "q": q}
