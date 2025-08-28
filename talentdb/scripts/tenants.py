"""Tenant utilities: create tenant, create user, create API key.

For MVP, stored in Mongo with minimal fields. All documents include tenant_id.
"""
import os, time, secrets
from typing import Optional
from .ingest_agent import db
from .auth import hash_password


def create_tenant(name: str) -> str:
    now = int(time.time())
    rec = {"name": name, "created_at": now, "plan": "trial"}
    ins = db["tenants"].insert_one(rec)
    return str(ins.inserted_id)


def create_user(tenant_id: str, email: str, password: str, name: Optional[str] = None, role: str = "admin") -> str:
    now = int(time.time())
    rec = {
        "tenant_id": tenant_id,
        "email": email.lower().strip(),
        "password_hash": hash_password(password),
        "name": name or email,
        "role": role,
        "created_at": now,
        "active": True,
    }
    db["users"].create_index([("tenant_id", 1), ("email", 1)], unique=True)
    ins = db["users"].insert_one(rec)
    return str(ins.inserted_id)


def create_api_key(tenant_id: str, name: str = "default") -> dict:
    key = secrets.token_urlsafe(32)
    now = int(time.time())
    rec = {"tenant_id": tenant_id, "name": name, "key": key, "active": True, "created_at": now}
    db["api_keys"].insert_one(rec)
    return {"key": key, "created_at": now}
