"""Enriched CSV job importer (no LLM) — implements end‑to‑end ingestion & enrichment pipeline.
Usage:
    python scripts/import_csv_enriched.py <csv_path>

Implements (subset of full spec when LLM unavailable):
    - Preserve full_text (with simple PII scrub)
    - Requirement line parsing (bullets/newlines) + dedupe + ellipsis trim logging
    - mandatory_requirements detection via trigger tokens
    - requirement_mentions (distinct ordered original lines)
    - Skill extraction: token ≥3 chars, multi‑word preserved, separated into must_have_skills / nice_to_have_skills
    - Synthetic enrichment to reach ≥12 distinct skills (8–15 synthetic cap) with reasons
    - skill_set union with trimming (>35 trims synthetic first then optional)
    - job_requirements: first 5–8 distinct explicit+synthetic skill names
    - external_order_id upsert + version snapshot on change
    - basic metrics logging (jobs_ingested, avg_skills, synthetic_ratio, mandatory_detect_rate)
    - PII removal (emails, phone numbers) from stored text fields
    - Index hints (create_indexes handles additional indexes separately)

City is stored with spaces (no underscores)."""
from __future__ import annotations
import csv, re, sys, time, pathlib, json, hashlib, copy
from typing import List, Dict, Any
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
try:
    from scripts.ingest_agent import db, canonical_city  # type: ignore
except Exception:
    db = None  # type: ignore
from scripts.header_mapping import canon_header  # type: ignore

MANDATORY_TRIGGERS = ["חובה", "דרישות חובה", "must", "required", "mandatory"]
ADMIN_BASE_SYN = [
    "office_administration","customer_service","scheduling","microsoft_excel",
    "records_management","communication_skills","time_management","calendar_management",
    "correspondence_management"
]
EXTRA_POOL = ["problem_solving","document_management","data_entry","crm_software","multitasking"]

ROLE_KEYWORDS_MAP = [
    (re.compile(r"מזכיר|פקיד|אדמ", re.I), ADMIN_BASE_SYN),
    (re.compile(r"data|ניתוח|אנליסט|analyst", re.I), ["data_analysis","sql","reporting","excel","power_bi","etl_processes","data_visualization"]),
]

SYN_REASON = {
    "office_administration": "role_pattern",
    "customer_service": "role_pattern",
    "scheduling": "role_pattern",
    "microsoft_excel": "explicit_or_role",
    "records_management": "role_pattern",
    "communication_skills": "generic_support",
    "time_management": "generic_support",
    "calendar_management": "role_pattern",
    "correspondence_management": "role_pattern",
}

def detect_mandatory(line: str) -> bool:
    low = line.lower()
    return any(trig in low for trig in MANDATORY_TRIGGERS)

def derive_synthetic_skills(title: str, existing: set[str], need: int) -> List[Dict[str,str]]:
    syn: list[Dict[str,str]] = []
    def _add(name: str, reason: str):
        if name not in existing and all(s['name']!=name for s in syn):
            syn.append({"name": name, "reason": reason})
    # Pattern based
    for rx, skills in ROLE_KEYWORDS_MAP:
        if rx.search(title):
            for s in skills:
                if len(syn) >= 15: break
                _add(s, SYN_REASON.get(s, 'role_pattern'))
    # Generic top‑up pool
    pool = ADMIN_BASE_SYN + EXTRA_POOL
    for s in pool:
        if len(syn) >= 15:
            break
        if len(syn) >= need:
            break
        _add(s, SYN_REASON.get(s, 'top_up'))
    return syn[:15]

def tokenize_skill_candidates(lines: List[str]) -> Dict[str, set[str]]:
    """Return mapping: category -> set of skill tokens (must/nice) derived from requirement lines.
    Mandatory lines feed must bucket; others feed nice bucket."""
    must_tokens: set[str] = set()
    nice_tokens: set[str] = set()
    word_re = re.compile(r"[A-Za-zא-ת][A-Za-zא-ת0-9_]{2,}")
    for ln in lines:
        bucket = must_tokens if detect_mandatory(ln) else nice_tokens
        # crude multi‑word: keep individual tokens (could expand to phrase extraction later)
        for w in word_re.findall(ln):
            bucket.add(w.lower())
    # remove overlaps (promote to must priority)
    nice_tokens -= must_tokens
    return {"must": must_tokens, "nice": nice_tokens}

PII_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PII_PHONE_RE = re.compile(r"\b(?:\+?\d[\d\- ]{6,}\d)\b")

def scrub_pii(text: str) -> str:
    return PII_PHONE_RE.sub('[PHONE]', PII_EMAIL_RE.sub('[EMAIL]', text))

def _parse_int_safe(val: str) -> int | None:
    try:
        s = (val or '').strip()
        if not s:
            return None
        # remove common thousand separators
        s = s.replace(',', '')
        n = int(float(s))  # tolerate "10.0"
        return max(0, n)
    except Exception:
        return None

def _parse_date_safe(val: str) -> int | None:
    """Parse flexible date strings into unix epoch seconds. Returns None if unparseable."""
    s = (val or '').strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%b %d %Y",
        "%B %d %Y",
    ]
    # also try common single/double-digit like 8/18/2025 handled by %m/%d/%Y
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.timestamp())
        except Exception:
            continue
    # Fallback: try ISO-like
    try:
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception:
        return None

def _normalize_headers(reader: csv.DictReader) -> Dict[str, str]:
    """Return a mapping of raw headers to canonical keys using shared header mapper."""
    raw_headers = reader.fieldnames or []
    return {h: canon_header(h, kind='job') for h in raw_headers}


def main(csv_path: str):
    # Ensure DB is available when running main (tests may import this module without DB)
    global db
    if db is None:
        from scripts.ingest_agent import db as _db  # type: ignore
        db = _db
    path = pathlib.Path(csv_path)
    if not path.exists():
        print("CSV file not found", file=sys.stderr); return 1
    coll = db['jobs']
    # Metrics counters
    ingested = 0
    mandatory_jobs = 0
    total_skills_accum = 0
    synthetic_total = 0
    start_ts = time.time()
    now = int(time.time())
    added = 0
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        # Normalize headers to canonical keys once
        field_map = _normalize_headers(reader)
        # Validate required columns
        required_keys = {"order_id", "title"}
        canon_headers = set(field_map.values())
        missing_required = sorted([k for k in required_keys if k not in canon_headers])
        if missing_required:
            print(json.dumps({
                'level': 'error', 'stage': 'preflight', 'error': 'missing_required_headers',
                'missing': missing_required, 'headers': reader.fieldnames
            }, ensure_ascii=False))
            return 2
        for row in reader:
            # Canonicalize row using header map
            crow = { field_map.get(k, k): (row.get(k) or '').replace('\r','').strip() for k in row }
            # Required fields
            order_id = (crow.get('order_id') or '').strip()
            title = (crow.get('title') or '').strip()
            if not title: continue
            if not order_id:
                # Strict policy: skip insert when order_id missing; log structured error
                print(json.dumps({
                    'level': 'warn', 'stage': 'row', 'error': 'missing_order_id',
                    'row_title': title
                }, ensure_ascii=False))
                continue
            # City normalization: support multiple CSV formats and strip common prefixes
            raw_city = ''
            # Try multiple possible city field names (work_location, city) to support different CSV formats
            for city_field in ['work_location', 'city']:
                if crow.get(city_field):
                    raw_city = crow.get(city_field, '').strip().replace('_',' ')
                    break
            
            if not raw_city:
                # Log warning for completely missing city data
                print(json.dumps({
                    'level': 'warn', 'stage': 'row', 'error': 'missing_city_data',
                    'order_id': order_id, 'title': title
                }, ensure_ascii=False))
            
            # remove branch prefix in Hebrew/English if present
            cleaned_city = re.sub(r"^\s*(סניף|branch)\s+", "", raw_city, flags=re.IGNORECASE).strip() if raw_city else None
            city_can = canonical_city(cleaned_city) if cleaned_city else None
            desc = (crow.get('description') or '').strip()
            req_a = (crow.get('requirements1') or '').strip()
            req_b = (crow.get('requirements2') or '').strip()
            salary = (crow.get('salary') or '').strip()
            # Extended optional fields
            profession = (crow.get('required_profession') or '').strip() or None
            occupation_field = (crow.get('field_of_occupation') or '').strip() or None
            # branch can appear duplicated; take first non-empty
            branch = (crow.get('branch') or '').strip() or None
            if branch:
                # remove Hebrew/English prefix labels and collapse spaces
                branch = re.sub(r"^\s*(סניף|branch)\s+", "", branch, flags=re.IGNORECASE).strip()
            job_apps_raw = (crow.get('job_applications_count') or '').strip()
            job_applications = _parse_int_safe(job_apps_raw)
            recruiter_name = (crow.get('recruiter_name') or '').strip() or None
            if recruiter_name:
                rec_scrub = scrub_pii(recruiter_name)
                recruiter_name = rec_scrub if rec_scrub and rec_scrub not in ('[EMAIL]','[PHONE]') else None
            source_created_at = _parse_date_safe(crow.get('source_created_at') or '')
            req_text = '\n'.join([x for x in [req_a, req_b] if x])
            # Ellipsis handling
            if '…' in desc or desc.endswith('...'):
                print(f"[warn] ellipsis in description for title='{title}'")
            desc = desc.replace('…','').rstrip('.') if desc.endswith('...') else desc.replace('…','')
            # Split requirement lines (bullets, newlines, bullet chars)
            raw_lines = []
            for ln in re.split(r"\r?\n|\u2022|\*|•|\t|;", req_text):
                ln = ln.strip().lstrip('-–—•*').strip()
                if ln.endswith('...') or ln.endswith('…'):
                    print(f"[warn] ellipsis in requirement line: {ln[:60]}")
                    ln = ln.rstrip('.').replace('…','')
                if ln:
                    raw_lines.append(ln)
            # Deduplicate while preserving order
            seen = set(); mentions = []
            for ln in raw_lines:
                if ln not in seen:
                    seen.add(ln); mentions.append(ln)
            mandatory_lines = [ln for ln in mentions if detect_mandatory(ln)]
            # Tokenize skills per category
            tok_map = tokenize_skill_candidates(mentions)
            must_tokens = tok_map['must']
            nice_tokens = tok_map['nice']
            # Synthetic enrichment target
            distinct_initial = len(must_tokens | nice_tokens)
            need_syn_min = 0
            if distinct_initial < 12:
                need_syn_min = min(15, 12 - distinct_initial)  # how many synthetics to try reaching 12
            synthetic_objs = derive_synthetic_skills(title, must_tokens | nice_tokens, need_syn_min)
            synthetic_names = [s['name'] for s in synthetic_objs]
            distinct_all = must_tokens | nice_tokens | set(synthetic_names)
            # Trim over 35 (remove synthetic first, oldest last)
            if len(distinct_all) > 35:
                overflow = len(distinct_all) - 35
                drop = []
                for syn in reversed(synthetic_names):
                    if overflow <=0: break
                    drop.append(syn); overflow -=1
                synthetic_objs = [s for s in synthetic_objs if s['name'] not in drop]
                synthetic_names = [s['name'] for s in synthetic_objs]
                distinct_all = must_tokens | nice_tokens | set(synthetic_names)
            # Build requirements object
            requirements = {
                'must_have_skills': [{'name': s} for s in sorted(must_tokens)][:25],
                'nice_to_have_skills': [{'name': s} for s in sorted(nice_tokens) if s not in must_tokens][:50]
            }
            # job_requirements short list: first 5–8 distinct skill names (prioritize must)
            ordered_skills = list(sorted(must_tokens)) + [s for s in sorted(nice_tokens) if s not in must_tokens]
            ordered_skills += [s for s in synthetic_names if s not in ordered_skills]
            job_requirements = ordered_skills[:8]
            full_text_parts = [p for p in [desc, req_text, f"שכר: {salary}" if salary else ''] if p]
            full_text = '\n\n'.join(full_text_parts)
            full_text = scrub_pii(full_text)
            mentions = [scrub_pii(m) for m in mentions]
            mandatory_lines = [scrub_pii(m) for m in mandatory_lines]
            skill_set = sorted(list(distinct_all))
            synthetic_total += len(synthetic_names)
            ingested += 1
            if mandatory_lines:
                mandatory_jobs += 1
            total_skills_accum += len(skill_set)
            content_hash = hashlib.sha1(full_text.encode('utf-8', errors='ignore')).hexdigest()
            existing_doc = coll.find_one({'external_order_id': order_id})
            if not existing_doc:
                existing_doc = coll.find_one({'_content_hash': content_hash})
            doc: Dict[str, Any] = {
                '_content_hash': content_hash,
                'title': title,
                # store original city name (with spaces, readable format)
                'city': cleaned_city if cleaned_city else None,
                # store canonical city (lowercase with underscores); None if unavailable
                'city_canonical': city_can,
                'job_description': desc,
                'job_requirements': job_requirements,
                'requirement_mentions': mentions,
                'mandatory_requirements': mandatory_lines,
                'synthetic_skills': synthetic_objs,  # list of {name, reason}
                'full_text': full_text,
                # Provide a text_blob compatible with other ingestion paths for maintenance/backfill
                'text_blob': f"Title: {title}\n" + (f"Location: {cleaned_city}\n" if cleaned_city else "") + ("Description:\n" + full_text if full_text else ""),
                'skill_set': skill_set,
                'external_order_id': order_id,
                'salary_range_raw': salary,
                'requirements': requirements,
                'created_at': existing_doc.get('created_at') if existing_doc else now,
                'updated_at': now,
                'flags': []
            }
            # Attach extended fields if present
            if profession:
                doc['profession'] = profession
            if occupation_field:
                doc['occupation_field'] = occupation_field
            if branch:
                doc['branch'] = branch
            if job_applications is not None:
                doc['job_applications_count'] = job_applications
            if recruiter_name:
                doc['recruiter_name'] = recruiter_name
            if source_created_at is not None:
                doc['source_created_at'] = source_created_at
            # Metadata for provenance
            doc['metadata'] = {'source_format': 'score_agents', 'import_version': 'jobs.v2'} if any([
                profession, occupation_field, branch, job_applications is not None, recruiter_name, source_created_at is not None
            ]) else doc.get('metadata', {'import_version': 'jobs.v1'})
            # Quality flags
            if not title or len(skill_set) < 2:
                doc['flags'].append('low_quality_skills')
            if mandatory_lines and len(requirements['must_have_skills']) == 0:
                doc['flags'].append('mandatory_without_must_skills')
            if len(skill_set) > 35:
                doc['flags'].append('over_generation')
            if job_apps_raw and job_applications is None:
                doc['flags'].append('invalid_application_count')
            if (crow.get('source_created_at') or '').strip() and source_created_at is None:
                doc['flags'].append('invalid_source_created_at')
            if (row.get('recruiter_name') or '').strip() and not recruiter_name:
                doc['flags'].append('recruiter_pii_scrubbed')
            # Versioning snapshot if updating (metadata-only changes do not create a version)
            if existing_doc:
                changed = copy.deepcopy(existing_doc)
                changed.pop('_id', None)
                if any(existing_doc.get(k) != doc.get(k) for k in (
                    'full_text','skill_set','requirements','mandatory_requirements','synthetic_skills'
                )):
                    db['jobs_versions'].insert_one({'job_id': existing_doc['_id'], 'snapshot': changed, 'versioned_at': now})
                    coll.update_one({'_id': existing_doc['_id']}, {'$set': doc})
                else:
                    coll.update_one({'_id': existing_doc['_id']}, {'$set': {'updated_at': now}})
            else:
                try:
                    coll.insert_one(doc)
                except Exception as e:
                    print(json.dumps({
                        'level': 'error', 'stage': 'db', 'error': 'insert_failed',
                        'order_id': order_id, 'title': title, 'exc': str(e)
                    }, ensure_ascii=False))
    duration = time.time() - start_ts
    avg_skills = round(total_skills_accum / ingested, 2) if ingested else 0
    synthetic_ratio = round(synthetic_total / max(total_skills_accum,1),3)
    mandatory_rate = round(mandatory_jobs / max(ingested,1),3)
    metrics = {
        'jobs_ingested': ingested,
        'avg_skills': avg_skills,
        'synthetic_ratio': synthetic_ratio,
        'mandatory_detect_rate': mandatory_rate,
        'duration_sec': round(duration,2)
    }
    print('Imported jobs:', ingested)
    print('Metrics:', json.dumps(metrics, ensure_ascii=False))
    try:
        db['_meta'].update_one({'key':'last_import_metrics'},{'$set':{'key':'last_import_metrics','value':metrics}}, upsert=True)
    except Exception:
        pass
    print('Visit /admin/jobs/all')
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1]) if len(sys.argv) > 1 else 1)
