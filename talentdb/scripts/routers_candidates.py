from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from typing import List, Optional
import os, tempfile
from .ingest_agent import ingest_files, db
from .auth import require_tenant
import time
from bson import ObjectId
from .ingest_agent import canonical_city

router = APIRouter(prefix="/tenant", tags=["candidates"])


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
