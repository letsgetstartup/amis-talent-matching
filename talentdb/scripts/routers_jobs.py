from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
from .ingest_agent import db, enrich_jobs_from_csv, is_llm_required_on_upload, canonical_city
from .auth import require_tenant
import time
import csv, io, os

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover
    openpyxl = None

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobCreate(BaseModel):
    external_job_id: str
    title: str
    city: Optional[str] = None
    must_have: Optional[List[str]] = None
    nice_to_have: Optional[List[str]] = None
    description: Optional[str] = None
    agency_email: Optional[str] = None
    remote: Optional[bool] = None
    # New raw fields from CSV/table
    profession: Optional[str] = None
    occupation_field: Optional[str] = None


@router.post("")
def create_job(req: JobCreate, tenant_id: str = Depends(require_tenant)):
    now = int(time.time())
    must = [s.strip() for s in (req.must_have or []) if s and s.strip()]
    nice = [s.strip() for s in (req.nice_to_have or []) if s and s.strip()]
    merged = []
    seen = set()
    for s in must + nice:
        if s not in seen:
            seen.add(s); merged.append(s)
    city_can = canonical_city(req.city) if req.city else None
    rec = {
        "tenant_id": tenant_id,
        "external_job_id": req.external_job_id,
        "title": req.title,
        "city": req.city,
        "city_canonical": city_can,
        "job_description": req.description or "",
        "requirements": {
            "must_have_skills": [{"name": s} for s in must],
            "nice_to_have_skills": [{"name": s} for s in nice],
        },
        "job_requirements": merged,
        "skill_set": merged,
        "agency_email": req.agency_email,
        "remote": bool(req.remote) if req.remote is not None else None,
        "created_at": now,
        "updated_at": now,
    }
    # Optional raw fields
    if req.profession is not None:
        rec["profession"] = req.profession
    if req.occupation_field is not None:
        rec["occupation_field"] = req.occupation_field
    # Uniqueness per tenant on external_job_id
    existing = db["jobs"].find_one({"tenant_id": tenant_id, "external_job_id": req.external_job_id})
    if existing:
        raise HTTPException(status_code=409, detail="external_job_id_exists")
    ins = db["jobs"].insert_one(rec)
    # Post-enrich to be match-ready
    try:
        enrich_jobs_from_csv([str(ins.inserted_id)], use_llm=True)
    except Exception:
        pass
    return {"job_id": str(ins.inserted_id)}


@router.get("/{job_id}")
def get_job(job_id: str, tenant_id: str = Depends(require_tenant)):
    from bson import ObjectId
    try:
        oid = ObjectId(job_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_id")
    # Enforce tenant isolation
    doc = db["jobs"].find_one({"_id": oid, "tenant_id": tenant_id})
    if not doc:
        raise HTTPException(status_code=404, detail="not_found")
    out = {k: doc.get(k) for k in [
        "title","city_canonical","job_requirements","external_job_id","agency_email",
        "job_description","requirements","seniority","llm_used_on_enrich","llm_success_on_enrich",
        "mandatory_requirements","synthetic_skills","profession","occupation_field","required_profession","field_of_occupation"
    ]}
    out["job_id"] = job_id
    return out


@router.get("/external/{ext_id}")
def get_job_by_external(ext_id: str, tenant_id: str = Depends(require_tenant)):
    # Enforce tenant isolation
    q = {"external_job_id": ext_id, "tenant_id": tenant_id}
    doc = db["jobs"].find_one(q)
    if not doc:
        raise HTTPException(status_code=404, detail="not_found")
    return {"job_id": str(doc.get("_id"))}


def _split_skills(val: Optional[str]):
    if not val:
        return []
    if isinstance(val, list):
        return [s.strip() for s in val if s and str(s).strip()]
    return [s.strip() for s in str(val).replace("\r", "\n").replace(";", ",").splitlines() for s in s.split(",") if s and s.strip()]


@router.post("/batch_csv")
async def create_jobs_batch_csv(
    file: UploadFile = File(...),
    tenant_id: str = Depends(require_tenant),
    upsert: bool = False,
    format: Optional[str] = None,
):
    # Basic validations
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="missing_file")
    filename = (file.filename or "").lower()
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty_file")
    max_mb = 8
    if len(raw) > max_mb * 1024 * 1024:
        raise HTTPException(status_code=400, detail="file_too_large")
    # Detect file type (CSV vs. Excel)
    content_type = (file.content_type or "").lower()
    is_excel = filename.endswith(".xlsx") or filename.endswith(".xlsm") or "spreadsheetml" in content_type or content_type.endswith("excel")

    def _normalize_headers(hdrs):
        return [str(h or "").strip().lower() for h in hdrs]

    # Unified alias map (can be extended by 'format')
    def _aliases(fmt: Optional[str]):
            base = {
                "external_job_id": [
                    "external_job_id","external id","externalid","ext_id","extid","jobid",
                    "מזהה","מזהה משרה","מספר משרה","מס' משרה","מס׳ משרה","קוד משרה","קוד","מספר הזמנה"
                ],
                "title": [
                    "title","job_title","כותרת","תפקיד","שם משרה","שם התפקיד","כותרת משרה"
                ],
                "city": [
                    "city","location","עיר","מיקום","מיקום עבודה","מקום עבודה","יישוב","עיר/אזור","אזור"
                ],
                "must_have": [
                    "must_have","must","required","דרישות חובה","דרישות","דרישות התפקיד","כישורים נדרשים","דרישות סף","מיומנויות נדרשות"
                ],
                "nice_to_have": [
                    "nice_to_have","nice","optional","יתרון","יתרונות","כישורים יתרון"
                ],
                "description": [
                    "description","desc","job_description",
                    "תיאור","תיאור משרה","תיאור תפקיד",
                    "תאור","תאור משרה","תאור תפקיד",
                    "פירוט","הערות"
                ],
                "agency_email": [
                    "agency_email","email","contact_email","אימייל","דוא""ל","מייל"
                ],
                "remote": [
                    "remote","is_remote","עבודה מרחוק","היברידי","עבודה היברידית"
                ],
                # Raw fields from table
                "profession": ["profession"],
                "occupation_field": ["occupation_field","occupation field"],
            }
            # Optional format-specific extensions
            if fmt and fmt.lower() == "agency_template":
                pass
            return base

    def _pick(row: dict, names: list[str]):
        for n in names:
            if n in row and row[n] is not None and str(row[n]).strip():
                return str(row[n]).strip()
        return None

    rows: list[dict] = []
    if is_excel:
        if not openpyxl:
            raise HTTPException(status_code=400, detail="excel_not_supported_install_openpyxl")
        try:
            wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=True)
            ws = wb.active
            all_rows = list(ws.iter_rows(values_only=True))
            if not all_rows:
                raise HTTPException(status_code=400, detail="no_headers")
            # Find best header row among first 10 rows by alias matches
            alias_map = _aliases(format)
            alias_set = set([a for arr in alias_map.values() for a in arr])
            best_idx, best_score = 0, -1
            for idx, r in enumerate(all_rows[:10]):
                hdrs = _normalize_headers([h if h is not None else "" for h in r])
                score = sum(1 for h in hdrs if h and (h in alias_set))
                if score > best_score:
                    best_score, best_idx = score, idx
            header = all_rows[best_idx]
            hdrs = _normalize_headers([h if h is not None else "" for h in header])
            for line in all_rows[best_idx+1:]:
                if line is None:
                    continue
                row = {}
                for i, h in enumerate(hdrs):
                    if not h:
                        continue
                    if i < len(line):
                        v = line[i]
                        row[h] = None if v is None else str(v).strip()
                # Skip completely empty rows
                if any(v for v in row.values()):
                    rows.append(row)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"excel_parse_error: {e}")
    else:
        # CSV path with encoding fallbacks (UTF-8, then cp1255 for Hebrew)
        try:
            text = raw.decode("utf-8-sig", errors="ignore")
        except Exception:
            try:
                text = raw.decode("cp1255", errors="ignore")
            except Exception:
                text = raw.decode("utf-8", errors="ignore")
        f = io.StringIO(text)
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="no_headers")
        headers = _normalize_headers(reader.fieldnames)
        reader.fieldnames = headers
        rows = list(reader)

    results = []
    rownum = 1  # including header row for reference
    alias_map = _aliases(format)
    created_job_ids: list[str] = []
    for row in rows:
        rownum += 1
        try:
            # Remap using aliases to canonical keys
            external_id = _pick(row, alias_map["external_job_id"]) if "external_job_id" in alias_map else None
            title = _pick(row, alias_map["title"]) if "title" in alias_map else None
            if not external_id or not title:
                results.append({"row": rownum, "status": "error", "error": "missing_external_id_or_title"})
                continue
            city = _pick(row, alias_map["city"]) if "city" in alias_map else None
            city_can = canonical_city(city) if city else None
            must = _split_skills(_pick(row, alias_map["must_have"]) if "must_have" in alias_map else None)
            nice = _split_skills(_pick(row, alias_map["nice_to_have"]) if "nice_to_have" in alias_map else None)
            desc = _pick(row, alias_map["description"]) if "description" in alias_map else None
            desc = desc or ""
            agency_email = _pick(row, alias_map["agency_email"]) if "agency_email" in alias_map else None
            remote_val = (_pick(row, alias_map["remote"]) if "remote" in alias_map else "" ) or ""
            remote_val = str(remote_val).lower()
            remote = True if remote_val in ("true", "1", "yes", "y", "כן") else False if remote_val in ("false", "0", "no", "n", "לא") else None

            # Optional occupation field (raw string from table)
            occ_raw = _pick(row, alias_map["occupation_field"]) if "occupation_field" in alias_map else None

            merged = []
            seen = set()
            for s in must + nice:
                if s not in seen:
                    seen.add(s); merged.append(s)

            existing = db["jobs"].find_one({"tenant_id": tenant_id, "external_job_id": external_id})
            now = int(time.time())
            if existing:
                if upsert:
                    upd = {
                        "title": title,
                        "city": city,
                        "city_canonical": city_can,
                        "job_description": desc,
                        "requirements": {
                            "must_have_skills": [{"name": s} for s in must],
                            "nice_to_have_skills": [{"name": s} for s in nice],
                        },
                        "job_requirements": merged,
                        "skill_set": merged,
                        "agency_email": agency_email,
                        "remote": bool(remote) if remote is not None else None,
                        "updated_at": now,
                    }
                    # Optional raw fields
                    prof_raw = _pick(row, alias_map["profession"]) if "profession" in alias_map else None
                    if prof_raw is not None:
                        upd["profession"] = prof_raw
                    occ_raw = _pick(row, alias_map["occupation_field"]) if "occupation_field" in alias_map else None
                    if occ_raw is not None:
                        upd["occupation_field"] = occ_raw
                    db["jobs"].update_one({"_id": existing["_id"]}, {"$set": upd})
                    results.append({"row": rownum, "external_job_id": external_id, "status": "updated", "job_id": str(existing["_id"])})
                    created_job_ids.append(str(existing["_id"]))
                else:
                    results.append({"row": rownum, "external_job_id": external_id, "status": "duplicate"})
                continue

            rec = {
                "tenant_id": tenant_id,
                "external_job_id": external_id,
                "title": title,
                "city": city,
                "city_canonical": city_can,
                "job_description": desc,
                "requirements": {
                    "must_have_skills": [{"name": s} for s in must],
                    "nice_to_have_skills": [{"name": s} for s in nice],
                },
                "job_requirements": merged,
                "skill_set": merged,
                "agency_email": agency_email,
                "remote": bool(remote) if remote is not None else None,
                "created_at": now,
                "updated_at": now,
            }
            # Optional raw fields
            prof_raw = _pick(row, alias_map["profession"]) if "profession" in alias_map else None
            if prof_raw is not None:
                rec["profession"] = prof_raw
            occ_raw = _pick(row, alias_map["occupation_field"]) if "occupation_field" in alias_map else None
            if occ_raw is not None:
                rec["occupation_field"] = occ_raw
            ins = db["jobs"].insert_one(rec)
            jid = str(ins.inserted_id)
            results.append({"row": rownum, "external_job_id": external_id, "status": "created", "job_id": jid})
            created_job_ids.append(jid)
        except Exception as e:
            results.append({"row": rownum, "status": "error", "error": str(e)[:200]})

    allowed = {"created","updated","duplicate","error"}
    summary = {
        "created": sum(1 for r in results if r.get("status") == "created"),
        "updated": sum(1 for r in results if r.get("status") == "updated"),
        "duplicates": sum(1 for r in results if r.get("status") == "duplicate"),
        "errors": sum(1 for r in results if r.get("status") == "error" or r.get("status") not in allowed),
        "total": len(results),
    }
    # Post-enrich created/updated jobs to be match-ready
    try:
        enriched = enrich_jobs_from_csv(created_job_ids, use_llm=True)
    except Exception:
        enriched = 0
    # Compute LLM usage/success stats for the uploaded rows (best-effort)
    llm_used_count = 0
    llm_success_count = 0
    if created_job_ids:
        try:
            from bson import ObjectId
            cur = db["jobs"].find({"_id": {"$in": [ObjectId(j) for j in created_job_ids]}}, {"llm_used_on_enrich": 1, "llm_success_on_enrich": 1})
            for d in cur:
                if d.get("llm_used_on_enrich"):
                    llm_used_count += 1
                if d.get("llm_success_on_enrich"):
                    llm_success_count += 1
        except Exception:
            pass
    # If admin requires LLM success, mark rows that didn't achieve it
    require_llm = False
    try:
        require_llm = is_llm_required_on_upload()
    except Exception:
        require_llm = False
    if require_llm and created_job_ids:
        from bson import ObjectId
        for r in results:
            jid = r.get("job_id")
            if not jid:
                continue
            try:
                doc = db["jobs"].find_one({"_id": ObjectId(jid)})
                if doc and not doc.get("llm_success_on_enrich"):
                    r["status"] = "error"
                    r["error"] = "llm_required_failed"
            except Exception:
                pass
        # Recompute summary errors
        summary["errors"] = sum(1 for r in results if r.get("status") == "error")
    summary["enriched"] = enriched
    summary["llm_used"] = llm_used_count
    summary["llm_success"] = llm_success_count
    return {"summary": summary, "results": results}
