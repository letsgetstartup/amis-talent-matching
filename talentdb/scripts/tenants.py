"""Tenant utilities: create tenant, create user, create API key.

For MVP, stored in Mongo with minimal fields. All documents include tenant_id.
"""
import time, secrets, re
from typing import Optional
from .ingest_agent import db
from .auth import hash_password

_slug_re = re.compile(r"[^a-z0-9]+")


def generate_slug(name: str) -> str:
    base = _slug_re.sub("-", (name or "").lower()).strip("-")
    if not base:
        base = "tenant"
    slug = base
    counter = 1
    while db["tenants"].find_one({"slug": slug}):
        slug = f"{base}-{counter}"
        counter += 1
    return slug


def _ensure_slug_index() -> None:
    try:
        missing = list(db["tenants"].find(
            {
                "$or": [
                    {"slug": {"$exists": False}},
                    {"slug": None},
                    {"slug": ""},
                ]
            },
            {"_id": 1, "name": 1},
        ))
        for doc in missing:
            slug_candidate = generate_slug(doc.get("name") or f"tenant-{doc.get('_id')}")
            db["tenants"].update_one({"_id": doc["_id"]}, {"$set": {"slug": slug_candidate}})
        db["tenants"].create_index("slug", unique=True)
    except Exception:
        pass


def _ensure_user_index() -> None:
    try:
        coll = db["users"]
        existing = list(coll.list_indexes())
        for idx in existing:
            keys = list(idx.get("key", {}).items())
            if idx.get("name") == "email_1" or keys == [("email", 1)]:
                try:
                    coll.drop_index(idx["name"])
                except Exception:
                    pass
        coll.create_index([("tenant_id", 1), ("email", 1)], unique=True, name="tenant_email_unique")
        coll.create_index([("email", 1)], name="email_lookup")
    except Exception:
        pass


def create_tenant_record(name: str, created_by: Optional[str] = None, *, slug: Optional[str] = None) -> tuple[str, str]:
    now = int(time.time())
    _ensure_slug_index()
    _ensure_user_index()
    slug_value = slug or generate_slug(name)
    rec = {
        "name": name,
        "slug": slug_value,
        "created_at": now,
        "plan": "trial",
        "stats": {"job_count": 0, "company_count": 0, "location_count": 0},
    }
    if created_by:
        rec["created_by"] = created_by
    ins = db["tenants"].insert_one(rec)
    return str(ins.inserted_id), slug_value


def create_tenant(name: str) -> str:
    tenant_id, _ = create_tenant_record(name)
    return tenant_id


def create_user(tenant_id: Optional[str], email: str, password: str, name: Optional[str] = None, role: str = "admin") -> str:
    now = int(time.time())
    tid = None if tenant_id is None else str(tenant_id)
    rec = {
        "email": email.lower().strip(),
        "password_hash": hash_password(password),
        "name": name or email,
        "role": role,
        "created_at": now,
        "active": True,
    }
    if tid is not None:
        rec["tenant_id"] = tid
    _ensure_user_index()
    ins = db["users"].insert_one(rec)
    return str(ins.inserted_id)


def create_api_key(tenant_id: str, name: str = "default") -> dict:
    key = secrets.token_urlsafe(32)
    now = int(time.time())
    rec = {"tenant_id": str(tenant_id), "name": name, "key": key, "active": True, "created_at": now}
    db["api_keys"].insert_one(rec)
    return {"key": key, "created_at": now}
