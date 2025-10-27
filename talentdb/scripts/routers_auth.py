from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
import time
import uuid

from .tenants import create_tenant, create_user, create_api_key
from .auth import jwt_encode, verify_password, jwt_decode
from .ingest_agent import db
from bson import ObjectId

router = APIRouter(prefix="/auth", tags=["auth"])


class SignupReq(BaseModel):
    company: str
    name: str
    email: str
    password: str


@router.post("/signup")
def signup(req: SignupReq):
    tenant_id = create_tenant(req.company)
    user_id = create_user(tenant_id, req.email, req.password, req.name, role="admin")
    token = jwt_encode({"sub": user_id, "tenant_id": tenant_id, "role": "admin"})
    return {"tenant_id": tenant_id, "user_id": user_id, "token": token}


class LoginReq(BaseModel):
    email: str
    password: str


@router.post("/login")
def login(req: LoginReq):
    user = db["users"].find_one({"email": req.email.lower().strip()})
    if not user:
        raise HTTPException(status_code=401, detail="invalid_credentials")
    if not verify_password(req.password, user.get("password_hash") or ""):
        raise HTTPException(status_code=401, detail="invalid_credentials")
    token = jwt_encode({
        "sub": str(user.get("_id")),
        "tenant_id": str(user.get("tenant_id")),
        "role": user.get("role") or "user"
    })
    return {"token": token, "tenant_id": str(user.get("tenant_id"))}


class ApiKeyReq(BaseModel):
    tenant_id: str
    name: str = "default"


@router.post("/apikey")
def apikey(req: ApiKeyReq):
    key = create_api_key(req.tenant_id, name=req.name)
    return key


class InviteReq(BaseModel):
    tenant_id: str
    emails: list[str]


@router.post("/invite-collaborators")
def invite_collaborators(req: InviteReq, authorization: str | None = Header(default=None, alias="Authorization")):
    tok = None
    if authorization and authorization.lower().startswith("bearer "):
        tok = authorization.split(" ", 1)[1].strip()
    if not tok:
        raise HTTPException(status_code=401, detail="missing_token")
    body = jwt_decode(tok)
    tid = body.get("tenant_id")
    role = body.get("role") or "user"
    if tid != req.tenant_id:
        raise HTTPException(status_code=403, detail="forbidden")
    # Only admin can invite
    if role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")

    inserted = 0
    for em in (req.emails or []):
        email = (em or "").strip().lower()
        if not email or "@" not in email:
            continue
        if db["users"].find_one({"email": email, "tenant_id": req.tenant_id}):
            continue
        # Create a placeholder user with invite status
        doc = {
            "email": email,
            "tenant_id": req.tenant_id,
            "role": "user",
            "status": "invited",
        }
        db["users"].insert_one(doc)
        inserted += 1

    return {"invited": inserted}


@router.get("/me")
def me(authorization: str | None = Header(default=None, alias="Authorization"), token: str | None = None):
    """Return basic profile and tenant info using the provided JWT token.

    Accepts either Authorization: Bearer <token> header or token query param.
    """
    tok = None
    if authorization and authorization.lower().startswith("bearer "):
        tok = authorization.split(" ", 1)[1].strip()
    elif token:
        tok = token
    if not tok:
        raise HTTPException(status_code=401, detail="missing_token")
    body = jwt_decode(tok)
    uid = body.get("sub")
    tid = body.get("tenant_id")
    if not uid or not tid:
        raise HTTPException(status_code=401, detail="invalid_token_payload")
    from bson import ObjectId
    def _as_oid(v):
        try:
            return ObjectId(v)
        except Exception:
            return v
    u = db["users"].find_one({"_id": _as_oid(uid)})
    t = db["tenants"].find_one({"_id": _as_oid(tid)})
    if not u or not t:
        raise HTTPException(status_code=404, detail="not_found")
    return {
        "user": {
            "id": str(u.get("_id")),
            "email": u.get("email"),
            "name": u.get("name"),
            "role": u.get("role") or "user",
        },
        "tenant": {
            "id": str(t.get("_id")),
            "name": t.get("name"),
        },
    }


class CandidateRegisterReq(BaseModel):
    tenant_id: str
    name: str
    email: EmailStr
    password: str
    phone: Optional[str] = None
    temp_candidate_id: Optional[str] = None


def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    import re
    normalized = re.sub(r"[^0-9+]+", "", phone)
    return normalized or None


@router.post("/register-candidate")
def register_candidate(req: CandidateRegisterReq):
    tenant_id = (req.tenant_id or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_required")
    try:
        tenant_oid = ObjectId(tenant_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_tenant_id")
    tenant_doc = db["tenants"].find_one({"_id": tenant_oid})
    if not tenant_doc:
        raise HTTPException(status_code=404, detail="tenant_not_found")

    email = req.email.lower().strip()
    existing_user = db["users"].find_one({"tenant_id": tenant_id, "email": email})
    if existing_user:
        raise HTTPException(status_code=409, detail="email_exists")

    user_id = create_user(tenant_id, email, req.password, req.name, role="candidate")
    now_ts = int(time.time())
    now_dt = datetime.utcnow()
    phone_value = _normalize_phone(req.phone)
    candidate_id: Optional[str] = None
    temp_id = (req.temp_candidate_id or "").strip()

    if temp_id:
        temp_doc = db["candidates"].find_one({
            "tenant_id": tenant_id,
            "temp_candidate_id": temp_id,
        })
        if temp_doc:
            owner = temp_doc.get("user_id")
            if owner and owner != user_id:
                raise HTTPException(status_code=409, detail="temp_candidate_claimed")
            update_doc = {
                "$set": {
                    "user_id": user_id,
                    "email": email,
                    "full_name": req.name.strip() or temp_doc.get("full_name"),
                    "phone": phone_value or temp_doc.get("phone"),
                    "is_claimed": True,
                    "tenant_id": tenant_id,
                    "updated_at": now_ts,
                },
                "$unset": {
                    "temp_candidate_id": "",
                    "expires_at": "",
                },
            }
            db["candidates"].update_one({"_id": temp_doc["_id"]}, update_doc)
            candidate_id = str(temp_doc.get("_id"))

    if candidate_id is None:
        share_id = uuid.uuid4().hex[:10]
        candidate_doc = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "full_name": req.name.strip(),
            "email": email,
            "phone": phone_value,
            "share_id": share_id,
            "origin": "portal_chat_register",
            "is_claimed": True,
            "created_at": now_dt,
            "updated_at": now_ts,
        }
        inserted = db["candidates"].insert_one(candidate_doc)
        candidate_id = str(inserted.inserted_id)

    share_id: Optional[str] = None
    try:
        share_lookup = db["candidates"].find_one({"_id": ObjectId(candidate_id)})
        if share_lookup:
            share_id = share_lookup.get("share_id")
    except Exception:
        share_id = None

    token = jwt_encode({"sub": user_id, "tenant_id": tenant_id, "role": "candidate"})
    return {
        "token": token,
        "user": {
            "id": user_id,
            "email": email,
            "name": req.name,
            "role": "candidate",
            "tenant_id": tenant_id,
            "candidate_id": candidate_id,
        },
        "share_id": share_id,
    }
