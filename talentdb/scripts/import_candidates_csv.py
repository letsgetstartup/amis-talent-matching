"""Import candidates from a CSV file and ingest via LLM (ESCO-normalized).

Usage:
  python scripts/import_candidates_csv.py <csv_path> [--tenant TENANT_ID] [--max-rows N]

Notes:
- Mirrors the /tenant/candidates/upload CSV logic.
- Tries encodings: utf-8, utf-8-sig, cp1255, latin-1.
- Detects common Hebrew headers and maps to canonical keys.
"""
from __future__ import annotations
import sys, csv, io, re, pathlib, json, os, time, tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ingest_agent import ingest_files, db  # type: ignore
from scripts.header_mapping import CandidateHeaderPolicy, canon_header  # type: ignore
from typing import Dict, Any, List

MAPPING_PATH = (pathlib.Path(__file__).resolve().parent / "mappings" / "candidate_csv_mapping.json")


AUTHORITATIVE_IDENT_KEYS = {"full_name", "city"}


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1255", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    raise UnicodeDecodeError("decode", data, 0, 0, "no suitable encoding")


def compose_text_blob(row: dict[str,str]) -> str:
    parts: list[str] = []
    full_name = row.get("full_name") or ""
    city = row.get("city") or ""
    phone = row.get("phone") or ""
    email = row.get("email") or ""
    education = row.get("education") or ""
    experience = row.get("experience") or ""
    notes = row.get("notes") or ""
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
    return "\n\n".join(parts).strip()


def _load_mapping() -> Dict[str, str]:
    try:
        if MAPPING_PATH.exists():
            obj = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
            mp = obj.get("mapping") or {}
            if isinstance(mp, dict):
                # normalize keys by stripping BOM and whitespace
                return { (k or '').replace('\ufeff','').strip(): v for k, v in mp.items() }
    except Exception:
        pass
    return {}


def _normalize_headers(headers: List[str]) -> Dict[str, int]:
    """Return a map canonical_key -> index using config mapping and heuristics.
    Priority order: explicit config mapping > header_mapping.canon_header > regex heuristics.
    """
    cfg = _load_mapping()
    idx: Dict[str, int] = {}
    for i, raw in enumerate(headers):
        raw_clean = (raw or '').replace('\ufeff','').strip()
        # 1) Config mapping
        canon = cfg.get(raw_clean)
        # 2) Shared mapping
        if not canon:
            canon = canon_header(raw_clean, kind="candidate")
        # 3) Heuristics
        if not canon:
            if re.search(r"email|mail|דוא\"?ל|אימייל|מייל", raw_clean, re.I):
                canon = "email"
            elif re.search(r"phone|טלפון|נייד", raw_clean, re.I):
                canon = "phone"
            elif re.search(r"notes?_candidate|^notes$|הערות", raw_clean, re.I):
                canon = "notes"
        if canon and canon not in idx:
            idx[canon] = i
    return idx


def _mask(v: str) -> str:
    if not v:
        return v
    if '@' in v:
        at = v.find('@')
        return (v[:2] + "***" + v[at-1:]) if at > 2 else "***@" + v.split('@',1)[1]
    # phone: keep last 3
    digits = re.sub(r"[^0-9]", "", v)
    return "***" + digits[-3:] if len(digits) >= 3 else "***"


def upsert_metadata(cid, tenant_id: str | None, ext_cand_id: str | None, ext_order_id: str | None, email: str | None, phone: str | None):
    try:
        if not cid:
            return
        set_fields: dict[str, str] = {"_source": "csv_import"}
        if tenant_id:
            set_fields["tenant_id"] = tenant_id
        if ext_cand_id:
            set_fields["external_candidate_id"] = ext_cand_id
        if ext_order_id:
            set_fields["external_order_id"] = ext_order_id
        if email:
            set_fields["email"] = email
        if phone:
            set_fields["phone"] = re.sub(r"[^0-9+]", "", phone)
        db["candidates"].update_one({"_id": cid}, {"$set": set_fields})
    except Exception:
        pass


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/import_candidates_csv.py <csv_path> [--tenant TENANT_ID] [--max-rows N]", file=sys.stderr)
        return 1
    csv_path = argv[1]
    tenant_id: str | None = None
    max_rows = int(os.getenv("CANDIDATE_CSV_MAX_ROWS", "500"))
    if "--tenant" in argv:
        try:
            tenant_id = argv[argv.index("--tenant") + 1]
        except Exception:
            print("--tenant requires a value", file=sys.stderr)
            return 2
    if "--max-rows" in argv:
        try:
            max_rows = int(argv[argv.index("--max-rows") + 1])
        except Exception:
            print("--max-rows requires an integer", file=sys.stderr)
            return 2

    p = pathlib.Path(csv_path)
    if not p.exists():
        print(f"CSV file not found: {p}", file=sys.stderr)
        return 3

    data = p.read_bytes()
    txt = decode_bytes(data)
    buf = io.StringIO(txt)
    reader = csv.reader(buf)
    rows = list(reader)
    if not rows:
        print("CSV is empty", file=sys.stderr)
        return 4

    headers = [ (h or '').replace('\ufeff','').strip() for h in rows[0] ]
    # Policy for authoritative identity tracking (for metrics), index for extraction
    policy = CandidateHeaderPolicy.from_headers(headers)
    idx = _normalize_headers(headers)
    # Authoritative coverage check
    auth = policy.authoritative_sources()

    created = []
    errors = 0
    processed = 0
    start_ts = time.time()
    run_id = f"cand_csv_{int(start_ts)}"
    coll_metrics = db["import_metrics"]
    coll_failures = db["import_failures"]
    batch: List[Dict[str, Any]] = []
    batch_size = int(os.getenv("CANDIDATE_CSV_BATCH", "10"))

    for ridx, row in enumerate(rows[1:], start=2):
        if processed >= max_rows:
            break
        def g(key: str) -> str:
            i = idx.get(key)
            if i is None or i >= len(row):
                return ""
            return str(row[i] or "").strip()

        # Enforce authoritative sheet-first identity fields
        full_name = g("full_name")
        city = g("city")
        phone = g("phone")
        email = g("email")
        education = g("education")
        experience = g("experience")
        notes = g("notes")
        ext_cand = g("external_candidate_id")
        ext_order = g("external_order_id")

        text_blob = compose_text_blob({
            "full_name": full_name,
            "city": city,
            "phone": phone,
            "email": email,
            "education": education,
            "experience": experience,
            "notes": notes,
        })
        if not text_blob:
            continue
        try:
            # Write to a temp .txt to reuse ingest_files API
            with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as tmp:
                tmp.write(text_blob)
                tmp_path = tmp.name
            docs = ingest_files([tmp_path], kind="candidate", force_llm=True) or []
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            created_doc = docs[-1] if docs else None
            if created_doc:
                cid = created_doc.get("_id")
                share_id = created_doc.get("share_id")
                if share_id and not cid:
                    try:
                        dbdoc = db["candidates"].find_one({"share_id": share_id})
                        if dbdoc:
                            cid = dbdoc.get("_id")
                    except Exception:
                        pass
                # Save ESCO-normalized occupation fields if provided
                try:
                    from scripts.ingest_agent import normalize_occupation  # type: ignore
                except Exception:
                    normalize_occupation = None  # type: ignore
                updates = {}
                if normalize_occupation is not None:
                    rp_raw = g("required_profession")
                    fo_raw = g("field_of_occupation")
                    if rp_raw:
                        updates["desired_profession"] = normalize_occupation(rp_raw)
                        updates["required_profession_raw"] = rp_raw
                    if fo_raw:
                        updates["field_of_occupation"] = normalize_occupation(fo_raw)
                        updates["field_of_occupation_raw"] = fo_raw
                if updates:
                    try:
                        db["candidates"].update_one({"_id": cid}, {"$set": updates})
                    except Exception:
                        pass
                # Persist authoritative identity fields explicitly (do not let LLM override)
                try:
                    updates_identity = {}
                    if full_name:
                        updates_identity["full_name"] = full_name
                    if city:
                        updates_identity["city"] = city
                    if updates_identity:
                        db["candidates"].update_one({"_id": cid}, {"$set": updates_identity})
                except Exception:
                    pass

                # Persist metadata and notes
                upsert_metadata(cid, tenant_id, ext_cand, ext_order, email, phone)
                if notes:
                    try:
                        db["candidates"].update_one({"_id": cid}, {"$set": {"notes": notes}})
                    except Exception:
                        pass
                created.append({
                    "row": ridx,
                    "candidate_id": str(cid) if cid else None,
                    "share_id": share_id,
                    "external_candidate_id": ext_cand,
                    "external_order_id": ext_order,
                    "authoritative": {
                        "full_name": bool(full_name),
                        "city": bool(city),
                    },
                    "masked": {
                        "email": _mask(email),
                        "phone": _mask(phone),
                    }
                })
                processed += 1
        except Exception as e:
            errors += 1
            msg = str(e)
            print(f"Row {ridx} ingest_failed: {msg[:200]}", file=sys.stderr)
            try:
                coll_failures.insert_one({
                    "run_id": run_id,
                    "row_index": ridx,
                    "stage": "candidate_csv",
                    "reason_code": "ingest_failed",
                    "message": msg[:500],
                    "tenant_id": tenant_id,
                    "created_at": int(time.time())
                })
            except Exception:
                pass

    summary = {
        "created": created,
        "count": len(created),
        "processed": processed,
        "errors": errors,
        "tenant_id": tenant_id,
        "csv": str(p),
        "run_id": run_id,
        "duration_sec": round(time.time() - start_ts, 3)
    }
    try:
        coll_metrics.insert_one({
            "run_id": run_id,
            "tenant_id": tenant_id,
            "file_name": str(p.name),
            "total_rows": max(0, len(rows)-1),
            "processed": processed,
            "success_count": len(created),
            "failure_count": errors,
            "header_coverage": list(idx.keys()),
            "authoritative_present": {k: (auth.get(k) is not None) for k in ("full_name","city")},
            "started_at": int(start_ts),
            "finished_at": int(time.time())
        })
    except Exception:
        pass

    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
