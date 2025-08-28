from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from .tenants import create_tenant, create_user, create_api_key
from .auth import jwt_encode, verify_password, jwt_decode
from .ingest_agent import db

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
