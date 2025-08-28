"""Minimal auth and tenancy scaffolding.

This module introduces:
- password hashing helpers
- JWT encode/decode
- FastAPI dependencies to extract tenant_id from session or API key

Note: For MVP we keep it simple; later integrate SSO and stronger RBAC.
"""
import os, time, hmac, hashlib, base64, json
from typing import Optional
from fastapi import Depends, HTTPException, Header, Request
from pydantic import BaseModel
from .ingest_agent import db

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_TTL = int(os.getenv("JWT_TTL", "86400"))


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    pad = 4 - (len(s) % 4)
    if pad and pad < 4:
        s = s + ("=" * pad)
    return base64.urlsafe_b64decode(s)


def hash_password(pw: str) -> str:
    salt = os.getenv("PW_SALT", "static-salt")
    return hashlib.sha256((salt + ":" + pw).encode()).hexdigest()


def verify_password(pw: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(pw), hashed)


def jwt_encode(payload: dict, ttl: int = JWT_TTL) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    body = {**payload, "iat": now, "exp": now + ttl}
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    b = _b64url(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode())
    signing = f"{h}.{b}".encode()
    sig = hmac.new(JWT_SECRET.encode(), signing, hashlib.sha256).digest()
    return f"{h}.{b}.{_b64url(sig)}"


def jwt_decode(token: str) -> dict:
    try:
        h, b, s = token.split(".")
        signing = f"{h}.{b}".encode()
        exp_sig = _b64url_decode(s)
        got_sig = hmac.new(JWT_SECRET.encode(), signing, hashlib.sha256).digest()
        if not hmac.compare_digest(exp_sig, got_sig):
            raise HTTPException(status_code=401, detail="invalid_token")
        body = json.loads(_b64url_decode(b))
        if int(body.get("exp", 0)) < int(time.time()):
            raise HTTPException(status_code=401, detail="token_expired")
        return body
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="invalid_token_format")


class UserCreate(BaseModel):
    email: str
    password: str
    name: Optional[str] = None


def get_tenant_from_apikey(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Optional[str]:
    if not x_api_key:
        return None
    rec = db["api_keys"].find_one({"key": x_api_key, "active": True})
    if not rec:
        raise HTTPException(status_code=401, detail="bad_api_key")
    return str(rec.get("tenant_id")) if rec.get("tenant_id") else None


def optional_tenant_id(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Optional[str]:
    """Return tenant id if a valid API key is provided, else None.

    If an invalid key is provided, raise 401. Missing key returns None.
    """
    if not x_api_key:
        return None
    rec = db["api_keys"].find_one({"key": x_api_key, "active": True})
    if not rec:
        raise HTTPException(status_code=401, detail="bad_api_key")
    return str(rec.get("tenant_id")) if rec.get("tenant_id") else None


def require_tenant(tenant_id: Optional[str] = Depends(get_tenant_from_apikey)) -> str:
    if not tenant_id:
        # In the future we will also read from JWT in cookies/Authorization
        raise HTTPException(status_code=401, detail="tenant_required")
    return tenant_id
