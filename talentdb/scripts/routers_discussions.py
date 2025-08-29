from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from bson import ObjectId
import time

from .auth import optional_tenant_id
from .ingest_agent import db

router = APIRouter()

_DISC_COLL = db["discussions"]
try:
    _DISC_COLL.create_index([("tenant_id", 1), ("target_type", 1), ("target_id", 1), ("created_at", -1)], name="by_target", background=True)
except Exception:
    pass


class DiscussionCreate(BaseModel):
    target_type: str  # candidate|job|match
    target_id: str
    text: str
    tags: Optional[List[str]] = None
    actor_name: Optional[str] = None  # optional display name


def _ensure_oid(s: str) -> ObjectId:
    try:
        return ObjectId(str(s))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_object_id")


@router.get("/discussions")
def discussions_get(
    target_type: str = Query(..., regex="^(candidate|job|match)$"),
    target_id: str = Query(...),
    since_days: Optional[int] = Query(None, ge=0, le=3650),
    text_contains: Optional[str] = Query(None, min_length=1, max_length=200),
    limit: int = Query(50, ge=1, le=200),
    tenant_id: str | None = Depends(optional_tenant_id),
):
    oid = _ensure_oid(target_id)
    q = {"target_type": target_type, "target_id": str(oid)}
    if tenant_id:
        q["tenant_id"] = tenant_id
    if since_days is not None and since_days > 0:
        cutoff = time.time() - (since_days * 86400)
        q["created_at"] = {"$gte": cutoff}
    if text_contains:
        # basic case-insensitive substring filter
        q["text"] = {"$regex": text_contains, "$options": "i"}
    items = []
    try:
        cur = _DISC_COLL.find(q).sort("created_at", -1).limit(int(limit))
        for d in cur:
            items.append({
                "id": str(d.get("_id")),
                "actor": d.get("actor_name") or "",
                "text": d.get("text") or "",
                "tags": d.get("tags") or [],
                "created_at": float(d.get("created_at") or 0.0),
            })
    except Exception:
        items = []
    return {"items": items, "count": len(items), "target_type": target_type, "target_id": str(oid)}


@router.post("/discussions")
def discussions_add(body: DiscussionCreate, tenant_id: str | None = Depends(optional_tenant_id)):
    tt = (body.target_type or "").lower()
    if tt not in {"candidate", "job", "match"}:
        raise HTTPException(status_code=400, detail="invalid_target_type")
    oid = _ensure_oid(body.target_id)
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty_text")
    if len(text) > 4000:
        text = text[:4000]
    doc = {
        "tenant_id": tenant_id,
        "target_type": tt,
        "target_id": str(oid),
        "text": text,
        "tags": list(dict.fromkeys([t for t in (body.tags or []) if isinstance(t, str) and t.strip()]))[:10],
        "actor_name": (body.actor_name or ""),
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    try:
        ins = _DISC_COLL.insert_one(doc)
        doc_id = str(ins.inserted_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="insert_failed")
    return {"ok": True, "id": doc_id}
