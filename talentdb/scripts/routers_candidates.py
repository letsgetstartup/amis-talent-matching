from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Response, status
from fastapi.responses import StreamingResponse
from typing import Any, Dict, List, Optional
import os, tempfile, uuid, time
from datetime import datetime, timedelta
from bson import ObjectId
from pydantic import BaseModel, EmailStr

from .ingest_agent import ingest_files, db, get_or_compute_matches, canonical_city
from .auth import require_tenant, require_role
from .services.portal_chatbot import build_portal_context
from .services.storage import save_resume, delete_resume, open_resume_stream, resume_exists

router = APIRouter(prefix="/tenant", tags=["candidates"])
portal_router = APIRouter(tags=["portal-candidates"])
profile_router = APIRouter(prefix="/candidates", tags=["candidate-profile"])


def _ensure_candidate_indexes() -> None:
    coll = db["candidates"]
    try:
        coll.create_index([("temp_candidate_id", 1)], unique=True, sparse=True, name="temp_candidate_id_unique")
    except Exception:
        pass
    try:
        coll.create_index([("expires_at", 1)], name="expires_at_idx")
    except Exception:
        pass
    try:
        coll.create_index([("user_id", 1)], name="candidate_user_lookup")
    except Exception:
        pass


_ensure_candidate_indexes()


_PORTAL_ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/plain",
}
_PORTAL_ALLOWED_EXTENSIONS = {"pdf", "doc", "docx", "txt"}
try:
    _PORTAL_MAX_MB = int(os.getenv("PORTAL_RESUME_MAX_MB", os.getenv("MAX_UPLOAD_MB", "10")))
except Exception:
    _PORTAL_MAX_MB = 10


def _generate_temp_candidate_id() -> str:
    return f"tmp_{uuid.uuid4().hex[:16]}"


def _safe_filename(name: Optional[str]) -> str:
    if not name:
        return f"resume_{uuid.uuid4().hex[:8]}.pdf"
    base = os.path.basename(name)
    if not base:
        return f"resume_{uuid.uuid4().hex[:8]}.pdf"
    return base[:180]


def _extension_from_filename(filename: str) -> str:
    if "." not in filename:
        return ""
    return filename.rsplit(".", 1)[-1].lower()


def _validate_portal_upload(file: UploadFile, size_bytes: int) -> None:
    if size_bytes <= 0:
        raise HTTPException(status_code=400, detail="empty_file")
    max_bytes = _PORTAL_MAX_MB * 1024 * 1024
    if size_bytes > max_bytes:
        raise HTTPException(status_code=400, detail="file_too_large")
    if file.content_type and file.content_type.lower() in _PORTAL_ALLOWED_CONTENT_TYPES:
        return
    ext = _extension_from_filename(file.filename or "")
    if ext in _PORTAL_ALLOWED_EXTENSIONS:
        return
    raise HTTPException(status_code=400, detail="unsupported_file_type")


def _candidate_profile_snapshot(doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not doc:
        return {}
    out: Dict[str, Any] = {
        "candidate_id": str(doc.get("_id")) if doc.get("_id") is not None else None,
        "share_id": doc.get("share_id"),
        "full_name": doc.get("full_name"),
        "title": doc.get("title"),
        "headline": doc.get("headline"),
        "city": doc.get("city") or doc.get("city_canonical"),
        "original_city": doc.get("city"),
        "experience_years": doc.get("experience_years"),
        "summary": doc.get("summary"),
    }
    skill_set = doc.get("skill_set") or []
    if isinstance(skill_set, list):
        out["top_skills"] = [s for s in skill_set if isinstance(s, str)][:10]
    details = doc.get("skills_detailed") or []
    if isinstance(details, list) and details:
        out["skills_detailed"] = details[:8]
    roles = doc.get("recent_roles") or []
    if isinstance(roles, list) and roles:
        out["recent_roles"] = roles[:3]
    return {k: v for k, v in out.items() if v not in (None, [], {})}


def _serialize_match(match: Dict[str, Any]) -> Dict[str, Any]:
    location = match.get("city_canonical") or match.get("city") or match.get("location")
    overlap = match.get("skill_overlap") or []
    if isinstance(overlap, list):
        overlap = [s for s in overlap if isinstance(s, str)][:8]
    else:
        overlap = []
    return {
        "job_id": match.get("job_id") or match.get("id"),
        "title": match.get("title"),
        "company_name": match.get("company_name") or match.get("company"),
        "score": match.get("score") or match.get("match_score"),
        "match_reason": match.get("reason"),
        "location": location,
        "remote": match.get("remote"),
        "application_url": match.get("application_url"),
        "skill_overlap": overlap,
        "distance_km": match.get("distance_km"),
    }


def _load_candidate_for_user(tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    return db["candidates"].find_one({"tenant_id": tenant_id, "user_id": str(user_id)})


def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    import re
    normalized = re.sub(r"[^0-9+]+", "", phone)
    return normalized or None


class ClaimCandidateRequest(BaseModel):
    temp_candidate_id: str
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None


class CandidateProfileUpdate(BaseModel):
    full_name: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    skill_set: Optional[List[str]] = None


@portal_router.post("/portal/{portal_slug}/candidates/upload")
async def upload_candidate_portal(
    portal_slug: str,
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(default=None),
    conversation_id: Optional[str] = Form(default=None),
):
    context = build_portal_context(portal_slug)
    if not context:
        raise HTTPException(status_code=404, detail="portal_not_found")
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")

    content = await file.read()
    _validate_portal_upload(file, len(content))
    safe_name = _safe_filename(file.filename)
    ext = _extension_from_filename(safe_name)
    suffix = f".{ext}" if ext else ""

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix or ".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        try:
            docs = ingest_files([tmp_path], kind="candidate", force_llm=True) or []
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"ingest_failed:{str(exc)[:140]}")

        candidate_doc = docs[-1] if docs else {}
        candidate_oid = candidate_doc.get("_id")
        share_id = candidate_doc.get("share_id")
        if not candidate_oid and share_id:
            candidate_doc = db["candidates"].find_one({"share_id": share_id}) or {}
            candidate_oid = candidate_doc.get("_id")

        if not candidate_oid:
            raise HTTPException(status_code=400, detail="candidate_creation_failed")

        candidate_id = str(candidate_oid)
        is_claimed = bool(candidate_doc.get("user_id"))
        existing_temp_id = candidate_doc.get("temp_candidate_id") if not is_claimed else None
        temp_candidate_id = existing_temp_id or _generate_temp_candidate_id()
        previous_resume = candidate_doc.get("resume_file_id")
        resume_file_id = save_resume(
            content,
            filename=safe_name,
            content_type=file.content_type or "application/octet-stream",
            tenant_id=tenant_id,
            candidate_id=candidate_id,
            metadata={
                "portal_slug": portal_slug,
                "session_id": session_id,
                "conversation_id": conversation_id,
                "origin": "portal_chat_upload",
            },
        )
        if previous_resume and str(previous_resume) != str(resume_file_id):
            try:
                delete_resume(previous_resume)
            except Exception:
                pass

        now_dt = datetime.utcnow()
        now_ts = int(time.time())
        update_fields = {
            "tenant_id": tenant_id,
            "resume_file_id": resume_file_id,
            "resume_filename": safe_name,
            "resume_content_type": file.content_type or "application/octet-stream",
            "resume_size_bytes": len(content),
            "resume_uploaded_at": now_dt,
            "portal_slug": portal_slug,
            "portal_session_id": session_id,
            "portal_conversation_id": conversation_id,
            "origin": candidate_doc.get("origin") or "portal_chat_upload",
            "is_claimed": is_claimed,
            "expires_at": now_dt + timedelta(days=7),
            "updated_at": now_ts,
        }
        temp_candidate_response = temp_candidate_id
        if is_claimed:
            temp_candidate_response = None
        else:
            update_fields["temp_candidate_id"] = temp_candidate_id
        db["candidates"].update_one(
            {"_id": candidate_oid},
            {
                "$set": update_fields,
                "$setOnInsert": {"created_at": now_dt},
            },
        )

        candidate_doc = db["candidates"].find_one({"_id": candidate_oid}) or {}
        try:
            matches = get_or_compute_matches(candidate_id, top_k=6, tenant_id=tenant_id, strategy="off") or []
        except Exception:
            matches = []

        return {
            "temp_candidate_id": temp_candidate_response,
            "candidate_id": candidate_id,
            "share_id": candidate_doc.get("share_id"),
            "resume_file_id": str(resume_file_id),
            "resume_filename": safe_name,
            "resume_content_type": file.content_type or "application/octet-stream",
            "portal_slug": portal_slug,
            "profile": _candidate_profile_snapshot(candidate_doc),
            "matches": [_serialize_match(m) for m in matches][:5],
            "total_matches": len(matches),
        }
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@router.post("/candidates/upload")
async def upload_candidates(files: List[UploadFile] = File(...), tenant_id: str = Depends(require_tenant)):
    if not files:
        raise HTTPException(status_code=400, detail="no_files")
    results = []
    max_mb = int(os.getenv('MAX_UPLOAD_MB', '12'))
    allowed = {'pdf','txt','docx','csv'}
    tmp_paths = []
    try:
        # Save and validate
        for f in files:
            name = f.filename or 'upload'
            ext = name.lower().rsplit('.',1)[-1] if '.' in name else ''
            if ext not in allowed:
                raise HTTPException(status_code=400, detail=f"unsupported_type:{ext}")
            data = await f.read()
            if not data:
                raise HTTPException(status_code=400, detail="empty_file")
            if len(data) > max_mb * 1024 * 1024:
                raise HTTPException(status_code=400, detail="file_too_large")
            # Directly handle CSV in-memory by expanding into multiple temp .txt files (one per row)
            if ext == 'csv':
                try:
                    # decode with utf-8, fallback to utf-8-sig then cp1255
                    txt: str
                    for enc in ('utf-8', 'utf-8-sig', 'cp1255'):
                        try:
                            txt = data.decode(enc)
                            break
                        except Exception:
                            txt = ''
                            continue
                    if not txt:
                        raise ValueError('decode_failed')
                    import csv, io, re
                    buf = io.StringIO(txt)
                    reader = csv.reader(buf)
                    rows = list(reader)
                    if not rows:
                        continue
                    headers = [h.strip() for h in rows[0]]
                    # Build header map to canonical keys
                    def _canon(h: str) -> str:
                        h2 = h.strip()
                        if re.search(r"^מספר\s*מועמד", h2, re.I):
                            return 'external_candidate_id'
                        if re.search(r"^מועמד$", h2, re.I):
                            return 'full_name'
                        if re.search(r"^מספר\s*הזמנה", h2, re.I):
                            return 'external_order_id'
                        # Applied job enrichment aliases
                        if re.search(r"(^מספר\s*משרה)|(^מזהה\s*משרה)|(^מס'\s*משרה)|(^מס׳\s*משרה)|(^external_job_id$)", h2, re.I):
                            return 'apply_job_number'
                        if re.search(r"notes?_candidate|^notes$|הערות", h2, re.I):
                            return 'notes'
                        if re.search(r"(^apply_job_id$)|(^job_id$)", h2, re.I):
                            return 'apply_job_id'
                        if re.search(r"מקצוע\s*נדרש", h2, re.I):
                            return 'required_profession'
                        if re.search(r"תחום\s*עיסוק", h2, re.I):
                            return 'field_of_occupation'
                        if re.search(r"^השכלה", h2, re.I):
                            return 'education'
                        if re.search(r"^נסיון|ניסיון", h2, re.I):
                            return 'experience'
                        if re.search(r"^טלפון", h2, re.I):
                            return 'phone'
                        if re.search(r"^מייל|אימייל", h2, re.I):
                            return 'email'
                        if re.search(r"^עיר", h2, re.I):
                            return 'city'
                        return h2
                    canon_headers = [_canon(h) for h in headers]
                    idx = {k:i for i,k in enumerate(canon_headers)}
                    max_rows = int(os.getenv('CANDIDATE_CSV_MAX_ROWS', '500'))
                    for ridx, row in enumerate(rows[1:], start=2):
                        if ridx-1 > max_rows:
                            break
                        # Safely pick each field
                        def g(key: str) -> str:
                            i = idx.get(key)
                            if i is None or i >= len(row):
                                return ''
                            return str(row[i] or '').strip()
                        full_name = g('full_name')
                        city = g('city')
                        phone = g('phone')
                        email = g('email')
                        education = g('education')
                        experience = g('experience')
                        external_cand = g('external_candidate_id')
                        external_order = g('external_order_id')
                        rp_raw = g('required_profession')
                        fo_raw = g('field_of_occupation')
                        apply_job_number = g('apply_job_number')
                        apply_job_id = g('apply_job_id')
                        notes = ''
                        # Support multiple aliases for notes
                        for key in ('notes', 'Notes', 'Notes_candidate', 'הערות'):
                            if not notes:
                                notes = g(key)
                        # Compose text blob
                        parts = []
                        if full_name:
                            parts.append(f"שם: {full_name}")
                        if city:
                            parts.append(f"עיר: {city}")
                        if phone:
                            parts.append(f"טלפון: {phone}")
                        if email:
                            parts.append(f"מייל: {email}")
                        if education:
                            parts.append("\nהשכלה:\n" + education)
                        if experience:
                            parts.append("\nניסיון תעסוקתי:\n" + experience)
                        if notes:
                            parts.append("\nהערות:\n" + notes)
                        text_blob = "\n\n".join(parts).strip()
                        if not text_blob:
                            continue
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
                            tmp.write(text_blob.encode('utf-8'))
                            # store CSV-only aux fields at the end for later enrichment
                            tmp_paths.append((f"{name}#row{ridx}", tmp.name, external_cand, external_order, email, phone, rp_raw, fo_raw, apply_job_number, apply_job_id, notes))
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"csv_parse_failed:{str(e)[:120]}")
            else:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.'+ext) as tmp:
                    tmp.write(data)
                    # preserve tuple shape; extra fields None when not CSV
                    tmp_paths.append((name, tmp.name, None, None, None, None, None, None, None, None, None))
        # Ingest per file and tag tenant
        created_count = 0
        updated_count = 0
        duplicate_count = 0
        error_count = 0
        for tup in tmp_paths:
            # Unpack with backward-compatible padding
            padded = (tup + (None,)*11)[0:11]
            name, path, ext_cand_id, ext_order_id, email, phone, rp_raw, fo_raw, apply_job_number, apply_job_oid, notes = padded
            try:
                # If we have external identifiers, try to upsert by existing doc
                existing_id = None
                try:
                    q = {'tenant_id': tenant_id}
                    ors = []
                    if ext_cand_id:
                        ors.append({'external_candidate_id': ext_cand_id})
                    if email:
                        ors.append({'email': email})
                    if phone:
                        # normalize phone: keep digits and leading +
                        import re as _re
                        ph_n = _re.sub(r"[^0-9+]", "", phone)
                        ors.append({'phone': ph_n})
                    if ors:
                        q['$or'] = ors
                        found = db['candidates'].find_one(q)
                        if found:
                            existing_id = found.get('_id')
                except Exception:
                    pass

                docs = ingest_files([path], kind='candidate', force_llm=True) or []
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"ingest_failed:{str(e)[:120]}")
            created = docs[-1] if docs else None
            if created:
                cid = created.get('_id')
                share_id = created.get('share_id')
                # If ingest_files did not return _id, fetch by share_id to get DB doc
                if (cid is None or isinstance(cid, str) and not cid) and share_id:
                    try:
                        dbdoc = db['candidates'].find_one({'share_id': share_id})
                        if dbdoc:
                            cid = dbdoc.get('_id')
                    except Exception:
                        pass
                try:
                    if cid:
                        set_fields = {'tenant_id': tenant_id, '_source': 'csv_upload' if name.endswith('.csv') or '#row' in (name or '') else 'file_upload'}
                        if ext_cand_id:
                            set_fields['external_candidate_id'] = ext_cand_id
                        if ext_order_id:
                            set_fields['external_order_id'] = ext_order_id
                        if email:
                            set_fields['email'] = email
                        if phone:
                            import re as _re
                            set_fields['phone'] = _re.sub(r"[^0-9+]", "", phone)
                        if notes:
                            set_fields['notes'] = notes
                        db['candidates'].update_one({'_id': cid}, {'$set': set_fields})
                        # Persist ESCO-normalized occupation fields
                        try:
                            from .ingest_agent import normalize_occupation as _norm_occ  # type: ignore
                        except Exception:
                            _norm_occ = None  # type: ignore
                        try:
                            occ_set = {}
                            if _norm_occ is not None:
                                if rp_raw:
                                    occ_set['desired_profession'] = _norm_occ(rp_raw)
                                    occ_set['required_profession_raw'] = rp_raw
                                if fo_raw:
                                    occ_set['field_of_occupation'] = _norm_occ(fo_raw)
                                    occ_set['field_of_occupation_raw'] = fo_raw
                            if occ_set:
                                db['candidates'].update_one({'_id': cid}, {'$set': occ_set})
                        except Exception:
                            pass
                        # If applied job info was provided, enrich candidate and record application
                        try:
                            _maybe_enrich_from_applied_job(tenant_id, cid, apply_job_number, apply_job_oid, source='csv')
                        except Exception:
                            # Non-fatal; continue import even if enrichment fails
                            pass
                except Exception:
                    pass
                # stats
                if existing_id:
                    updated_count += 1
                else:
                    created_count += 1
                results.append({
                    'file': name,
                    'candidate_id': str(cid) if cid else None,
                    'share_id': share_id,
                    'external_candidate_id': ext_cand_id,
                    'external_order_id': ext_order_id,
                    'apply_job_number': apply_job_number or None,
                    'apply_job_id': apply_job_oid or None
                })
            else:
                error_count += 1
        return {'uploaded': results, 'count': len(results), 'created': created_count, 'updated': updated_count, 'duplicates': duplicate_count, 'errors': error_count}
    finally:
        for tup in tmp_paths:
            try:
                p = tup[1]
                if p:
                    os.unlink(p)
            except Exception:
                pass


@profile_router.post("/claim")
def claim_temp_candidate(payload: ClaimCandidateRequest, user=Depends(require_role("candidate"))):
    tenant_id = user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")
    temp_id = (payload.temp_candidate_id or "").strip()
    if not temp_id:
        raise HTTPException(status_code=400, detail="missing_temp_candidate_id")
    doc = db["candidates"].find_one({
        "tenant_id": tenant_id,
        "temp_candidate_id": temp_id,
    })
    if not doc:
        raise HTTPException(status_code=404, detail="temp_candidate_not_found")
    current_user_id = str(user.get("id"))
    owner = doc.get("user_id")
    if owner and owner != current_user_id:
        raise HTTPException(status_code=409, detail="already_claimed")
    email = payload.email.lower().strip() if payload.email else (doc.get("email") or user.get("email") or "")
    name_value = (payload.full_name or doc.get("full_name") or user.get("name") or "").strip()
    phone_value = _normalize_phone(payload.phone) if payload.phone else _normalize_phone(doc.get("phone"))
    update_doc = {
        "$set": {
            "user_id": current_user_id,
            "is_claimed": True,
            "tenant_id": tenant_id,
            "updated_at": int(time.time()),
        },
        "$unset": {"temp_candidate_id": "", "expires_at": ""},
    }
    if email:
        update_doc["$set"]["email"] = email
    if name_value:
        update_doc["$set"]["full_name"] = name_value
    if phone_value:
        update_doc["$set"]["phone"] = phone_value
    db["candidates"].update_one({"_id": doc["_id"]}, update_doc)
    candidate_doc = db["candidates"].find_one({"_id": doc["_id"]}) or {}
    return {
        "candidate_id": str(candidate_doc.get("_id")),
        "share_id": candidate_doc.get("share_id"),
        "profile": _candidate_profile_snapshot(candidate_doc),
    }


@profile_router.get("/me")
def get_candidate_self(user=Depends(require_role("candidate"))):
    tenant_id = user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")
    doc = _load_candidate_for_user(tenant_id, user.get("id"))
    if not doc:
        raise HTTPException(status_code=404, detail="candidate_not_found")
    profile = _candidate_profile_snapshot(doc)
    profile["is_claimed"] = bool(doc.get("is_claimed"))
    profile["temp_candidate_id"] = doc.get("temp_candidate_id")
    profile["resume_filename"] = doc.get("resume_filename")
    profile["resume_uploaded_at"] = doc.get("resume_uploaded_at")
    profile["resume_file_id"] = str(doc.get("resume_file_id")) if doc.get("resume_file_id") else None
    return {"candidate": profile}


@profile_router.put("/me")
def update_candidate_self(payload: CandidateProfileUpdate, user=Depends(require_role("candidate"))):
    tenant_id = user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")
    doc = _load_candidate_for_user(tenant_id, user.get("id"))
    if not doc:
        raise HTTPException(status_code=404, detail="candidate_not_found")
    updates: Dict[str, Any] = {}
    if payload.full_name is not None:
        name_value = payload.full_name.strip()
        if name_value:
            updates["full_name"] = name_value
    if payload.headline is not None:
        updates["headline"] = payload.headline.strip() if payload.headline else None
    if payload.summary is not None:
        updates["summary"] = payload.summary.strip() if payload.summary else None
    if payload.phone is not None:
        updates["phone"] = _normalize_phone(payload.phone)
    if payload.city is not None:
        updates["city"] = payload.city.strip() or None
        updates["city_canonical"] = canonical_city(payload.city) if payload.city else None
    if payload.skill_set is not None:
        cleaned = [s.strip() for s in payload.skill_set if isinstance(s, str) and s.strip()]
        updates["skill_set"] = cleaned
    if not updates:
        return {"candidate": _candidate_profile_snapshot(doc)}
    updates["updated_at"] = int(time.time())
    db["candidates"].update_one({"_id": doc["_id"]}, {"$set": updates})
    refreshed = _load_candidate_for_user(tenant_id, user.get("id")) or {}
    profile = _candidate_profile_snapshot(refreshed)
    profile["is_claimed"] = bool(refreshed.get("is_claimed"))
    profile["resume_filename"] = refreshed.get("resume_filename")
    profile["resume_uploaded_at"] = refreshed.get("resume_uploaded_at")
    return {"candidate": profile}


@profile_router.get("/me/cv")
def download_candidate_cv(user=Depends(require_role("candidate"))):
    tenant_id = user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")
    doc = _load_candidate_for_user(tenant_id, user.get("id"))
    if not doc or not doc.get("resume_file_id"):
        raise HTTPException(status_code=404, detail="resume_not_found")
    try:
        stream = open_resume_stream(doc.get("resume_file_id"))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="resume_not_found")
    media_type = doc.get("resume_content_type") or "application/octet-stream"
    filename = doc.get("resume_filename") or "resume.pdf"
    headers = {
        "Content-Disposition": f"attachment; filename=\"{filename}\"",
    }
    return StreamingResponse(stream, media_type=media_type, headers=headers)


@profile_router.delete("/me/cv")
def delete_candidate_cv(user=Depends(require_role("candidate"))):
    tenant_id = user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_missing")
    doc = _load_candidate_for_user(tenant_id, user.get("id"))
    if not doc or not doc.get("resume_file_id"):
        raise HTTPException(status_code=404, detail="resume_not_found")
    resume_id = doc.get("resume_file_id")
    try:
        delete_resume(resume_id)
    except Exception:
        pass
    db["candidates"].update_one(
        {"_id": doc["_id"]},
        {
            "$unset": {
                "resume_file_id": "",
                "resume_filename": "",
                "resume_content_type": "",
                "resume_size_bytes": "",
                "resume_uploaded_at": "",
            },
            "$set": {"updated_at": int(time.time())},
        },
    )
    refreshed = _load_candidate_for_user(tenant_id, user.get("id")) or {}
    profile = _candidate_profile_snapshot(refreshed)
    profile["is_claimed"] = bool(refreshed.get("is_claimed"))
    return {"removed": True, "candidate": profile}


@router.get("/candidates")
def list_tenant_candidates(tenant_id: str = Depends(require_tenant), skip: int = 0, limit: int = 50, q: Optional[str] = None):
    if limit > 200:
        limit = 200
    if skip < 0:
        skip = 0
    query: dict = {"tenant_id": tenant_id}
    if q:
        import re
        pattern = f".*{re.escape(q)}.*"
        query["$or"] = [
            {"title": {"$regex": pattern, "$options": "i"}},
            {"full_name": {"$regex": pattern, "$options": "i"}},
            {"city_canonical": {"$regex": pattern, "$options": "i"}},
            {"skill_set": {"$elemMatch": {"$regex": pattern, "$options": "i"}}},
        ]
    cur = db['candidates'].find(query, {'_id':1, 'share_id':1, 'title':1, 'full_name':1, 'city_canonical':1, 'updated_at':1}).skip(skip).limit(limit).sort([["updated_at", -1],["_id", -1]])
    rows = []
    for d in cur:
        rows.append({'candidate_id': str(d.get('_id')), 'share_id': d.get('share_id'), 'title': d.get('title'), 'full_name': d.get('full_name'), 'city': d.get('city_canonical'), 'updated_at': d.get('updated_at')})
    total = db['candidates'].count_documents(query)
    return {'results': rows, 'total': total, 'skip': skip, 'limit': limit, 'q': q}


def _flatten_doc(doc: dict, max_list_elems: int = 3, prefix: str = "", out: dict | None = None):
    if out is None:
        out = {}
    for k, v in doc.items():
        path = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict):
            _flatten_doc(v, max_list_elems=max_list_elems, prefix=path, out=out)
        elif isinstance(v, list):
            if not v:
                out[path] = []
                continue
            if all(not isinstance(e, (dict, list)) for e in v):
                try:
                    from bson import ObjectId as _OID
                    out[path] = [str(el) if isinstance(el, _OID) else el for el in v[:max_list_elems]]
                except Exception:
                    out[path] = [str(el) for el in v[:max_list_elems]]
            else:
                out[path] = f"list[{len(v)}]"
                for idx, el in enumerate(v[:max_list_elems]):
                    if isinstance(el, dict):
                        _flatten_doc(el, max_list_elems=max_list_elems, prefix=f"{path}[{idx}]", out=out)
                    else:
                        try:
                            from bson import ObjectId as _OID
                            out[f"{path}[{idx}]"] = str(el) if isinstance(el, _OID) else el
                        except Exception:
                            out[f"{path}[{idx}]"] = str(el)
        else:
            try:
                from bson import ObjectId as _OID
                out[path] = str(v) if isinstance(v, _OID) else v
            except Exception:
                out[path] = str(v)
    return out


@router.get("/candidates/all_fields")
def tenant_candidates_all_fields(tenant_id: str = Depends(require_tenant), skip: int = 0, limit: int = 50):
    if limit > 200:
        limit = 200
    if skip < 0:
        skip = 0
    total = db['candidates'].count_documents({'tenant_id': tenant_id})
    cur = db['candidates'].find({'tenant_id': tenant_id}, {}).skip(skip).limit(limit)
    rows = []
    columns: set[str] = set()
    tmp = []
    for d in cur:
        flat = _flatten_doc(d.copy())
        tmp.append(flat)
        columns.update(flat.keys())
    col_list = ["_id", "share_id", "title", "full_name", "city_canonical", "skill_set"]
    for c in sorted(columns):
        if c not in col_list:
            col_list.append(c)
    for flat in tmp:
        rows.append({c: flat.get(c) for c in col_list})
    return {"total": total, "skip": skip, "limit": limit, "columns": col_list, "rows": rows}


@router.post("/candidates/mapping/preview")
async def preview_candidate_csv(file: UploadFile = File(...), tenant_id: str = Depends(require_tenant)):
    """Parse a candidate CSV and preview how headers/rows will be mapped, without writing to DB.
    Returns: {headers_original, headers_canonical, unknown_headers, samples[]}
    """
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="missing_file")
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty_file")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="file_too_large")
    # decode
    txt = ''
    for enc in ('utf-8', 'utf-8-sig', 'cp1255'):
        try:
            txt = raw.decode(enc)
            break
        except Exception:
            txt = ''
            continue
    if not txt:
        raise HTTPException(status_code=400, detail="decode_failed")
    import csv, io, re
    buf = io.StringIO(txt)
    reader = csv.reader(buf)
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="no_headers")
    headers = [h.strip() for h in rows[0]]
    def _canon(h: str) -> str:
        h2 = h.strip()
        if re.search(r"^מספר\s*מועמד", h2, re.I):
            return 'external_candidate_id'
        if re.search(r"^מועמד$", h2, re.I):
            return 'full_name'
        if re.search(r"^מספר\s*הזמנה", h2, re.I):
            return 'external_order_id'
        # Applied job enrichment aliases
        if re.search(r"(^מספר\s*משרה)|(^מזהה\s*משרה)|(^מס'\s*משרה)|(^מס׳\s*משרה)|(^external_job_id$)", h2, re.I):
            return 'apply_job_number'
        if re.search(r"(^apply_job_id$)|(^job_id$)", h2, re.I):
            return 'apply_job_id'
        if re.search(r"מקצוע\s*נדרש|מקצוע\s*מבוקש", h2, re.I):
            return 'desired_profession'
        if re.search(r"תחום\s*עיסוק", h2, re.I):
            return 'field_of_occupation'
        if re.search(r"^השכלה", h2, re.I):
            return 'education'
        if re.search(r"^נסיון|ניסיון", h2, re.I):
            return 'experience'
        if re.search(r"^טלפון|phone", h2, re.I):
            return 'phone'
        if re.search(r"^מייל|אימייל|email", h2, re.I):
            return 'email'
        if re.search(r"^עיר|city", h2, re.I):
            return 'city'
        if re.search(r"^שם\s*מלא|full\s*name|candidate", h2, re.I):
            return 'full_name'
        if re.search(r"notes?_candidate|^notes$|הערות", h2, re.I):
            return 'notes'
        return h2
    canon_headers = [_canon(h) for h in headers]
    idx = {k: i for i, k in enumerate(canon_headers)}
    allowed = {'external_candidate_id','full_name','external_order_id','apply_job_number','apply_job_id','desired_profession','field_of_occupation','education','experience','phone','email','city','notes'}
    unknown = [h for h in canon_headers if h not in allowed]
    out_samples = []
    for ridx, row in enumerate(rows[1:6], start=2):
        def g(key: str) -> str:
            i = idx.get(key)
            if i is None or i >= len(row):
                return ''
            return str(row[i] or '').strip()
        full_name = g('full_name')
        email = g('email')
        phone = g('phone')
        city = g('city')
        city_can = canonical_city(city) if city else None
        notes = g('notes')
        apply_job_number = g('apply_job_number')
        apply_job_id = g('apply_job_id')
        warnings = []
        if not full_name:
            warnings.append('missing_full_name')
        if not (email or phone):
            warnings.append('missing_contact')
        # Dedup preview
        duplicate = False
        try:
            q = {'tenant_id': tenant_id}
            ors = []
            if email: ors.append({'email': email})
            if phone:
                import re as _re
                ph_n = _re.sub(r"[^0-9+]", "", phone)
                ors.append({'phone': ph_n})
            if ors:
                q['$or'] = ors
                if db['candidates'].find_one(q):
                    duplicate = True
        except Exception:
            duplicate = False
        # Apply-job resolvability preview (best-effort)
        resolved = None
        try:
            if apply_job_id:
                resolved = bool(db['jobs'].find_one({'_id': ObjectId(str(apply_job_id)), 'tenant_id': tenant_id}))
            elif apply_job_number:
                resolved = bool(db['jobs'].find_one({'external_job_id': str(apply_job_number).strip(), 'tenant_id': tenant_id}))
        except Exception:
            resolved = False
        out_samples.append({
            'row': ridx,
            'full_name': full_name,
            'email': email,
            'phone': phone,
            'city': city,
            'city_canonical': city_can,
            'notes': notes or None,
            'apply_job_number': apply_job_number or None,
            'apply_job_id': apply_job_id or None,
            'apply_job_resolvable': resolved,
            'duplicate_contact': duplicate,
            'warnings': warnings,
        })
    return {
        'headers_original': headers,
        'headers_canonical': canon_headers,
    'unknown_headers': unknown,
        'samples': out_samples,
        'total_rows': max(0, len(rows)-1)
    }


def _maybe_enrich_from_applied_job(tenant_id: str, candidate_oid, apply_job_number: Optional[str], apply_job_oid: Optional[str], source: str = 'csv') -> None:
    """If applied job info exists, enrich candidate profile from the job and record an application.
    - Resolve job by (tenant_id, external_job_id=apply_job_number) or by _id (apply_job_oid)
    - Copy must/nice skills into candidate.target_requirements with provenance
    - Set location preference from job city/constraints when present
    - Append synthetic skills (no duplicates, capped)
    - Insert applications record (idempotent on candidate_id+job_id+source timestamp window)
    """
    if not (apply_job_number or apply_job_oid):
        return
    job = None
    if apply_job_oid:
        try:
            job = db['jobs'].find_one({'_id': ObjectId(str(apply_job_oid)), 'tenant_id': tenant_id})
        except Exception:
            job = None
    if (job is None) and apply_job_number:
        job = db['jobs'].find_one({'tenant_id': tenant_id, 'external_job_id': str(apply_job_number).strip()})
    if not job:
        return
    # Build enrichment sets
    req = job.get('requirements') or {}
    must = []
    nice = []
    for it in (req.get('must_have_skills') or []):
        if isinstance(it, dict) and it.get('name'):
            must.append({'name': it.get('name'), 'label': it.get('label'), 'esco_id': it.get('esco_id', ''), 'source': f"apply_job:{job.get('_id')}"})
        elif isinstance(it, str):
            must.append({'name': it, 'label': None, 'esco_id': '', 'source': f"apply_job:{job.get('_id')}"})
    for it in (req.get('nice_to_have_skills') or []):
        if isinstance(it, dict) and it.get('name'):
            nice.append({'name': it.get('name'), 'label': it.get('label'), 'esco_id': it.get('esco_id', ''), 'source': f"apply_job:{job.get('_id')}"})
        elif isinstance(it, str):
            nice.append({'name': it, 'label': None, 'esco_id': '', 'source': f"apply_job:{job.get('_id')}"})
    set_fields = {
        'apply_job': {
            'job_id': str(job.get('_id')),
            'job_number': job.get('external_job_id'),
            'applied_at': int(time.time()),
            'source': source,
        }
    }
    # Location preferences
    city_can = job.get('city_canonical') or None
    if city_can:
        prefs = {
            'preferred_city_canonical': city_can,
        }
    else:
        prefs = {}
    # Merge synthetic skills into candidate.synthetic_skills (avoid dup by name)
    cand = db['candidates'].find_one({'_id': candidate_oid}) or {}
    syn = cand.get('synthetic_skills') or []
    existing_names = {s.get('name') for s in syn if isinstance(s, dict)}
    to_add = []
    for it in must + nice:
        nm = it.get('name') if isinstance(it, dict) else None
        if nm and nm not in existing_names:
            to_add.append({'name': nm, 'reason': 'apply_job_top_up', 'source': it.get('source')})
            existing_names.add(nm)
        if len(to_add) >= 15:
            break
    update_doc = {'$set': set_fields}
    if prefs:
        update_doc['$set']['preferences'] = {**(cand.get('preferences') or {}), **prefs}
    if must or nice:
        tr = cand.get('target_requirements') or {}
        tr.setdefault('must_have_skills', [])
        tr.setdefault('nice_to_have_skills', [])
        tr['must_have_skills'] = (tr.get('must_have_skills') or []) + must
        tr['nice_to_have_skills'] = (tr.get('nice_to_have_skills') or []) + nice
        update_doc['$set']['target_requirements'] = tr
    if to_add:
        update_doc.setdefault('$set', {})
        update_doc['$set']['synthetic_skills'] = (syn or []) + to_add
    update_doc.setdefault('$set', {})
    update_doc['$set']['updated_at'] = int(time.time())
    db['candidates'].update_one({'_id': candidate_oid}, update_doc)
    # Record application (idempotent-ish)
    app = {
        'tenant_id': tenant_id,
        'candidate_id': str(candidate_oid),
        'job_id': str(job.get('_id')),
        'external_job_id': job.get('external_job_id'),
        'applied_at': int(time.time()),
        'channel': source,
        'status': 'applied'
    }
    try:
        # Avoid exact duplicates within short window
        exists = db['applications'].find_one({'tenant_id': tenant_id, 'candidate_id': str(candidate_oid), 'job_id': str(job.get('_id'))})
        if not exists:
            db['applications'].insert_one(app)
    except Exception:
        pass
