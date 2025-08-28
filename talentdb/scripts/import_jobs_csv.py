"""Import job rows from a CSV export (Hebrew headers) into samples/jobs as text files then ingest.
Usage:
  python scripts/import_jobs_csv.py <csv_path> [--ingest]

Detects headers:
  מספר הזמנה (order id)
  שם משרה (title)
  תאור תפקיד (description)
  דרישות תפקיד / דרישות התפקיד (requirements)
  טווח שכר מוצע (salary range)
  לקוח (client)
  סוג העסקה (employment type)
  מצב (status)
  תאריך פתיחה (open date)
  סניף (branch)

Creates files: samples/jobs/job_<מספר הזמנה>.txt
"""
from __future__ import annotations
import csv, sys, os, re, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOBS_DIR = ROOT / 'samples' / 'jobs'
JOBS_DIR.mkdir(parents=True, exist_ok=True)

HEADER_MAP = {
    'מספר הזמנה': 'order_id',
    'שם משרה': 'title',
    'תאור תפקיד': 'description',
    'דרישות תפקיד': 'requirements1',
    'דרישות התפקיד': 'requirements2',
    'טווח שכר מוצע': 'salary',
    'לקוח': 'client',
    'סוג העסקה': 'employment_type',
    'מצב': 'status',
    'תאריך פתיחה': 'open_date',
    'סניף': 'branch',
    'מקום עבודה': 'work_location',  # city / workplace field
    'מקצוע נדרש': 'required_profession',
    'תחום עיסוק': 'field_of_occupation',
        # English aliases (direct exact matches) for external CSVs
        'external_job_id': 'order_id',
        'job_id': 'order_id',
        'title': 'title',
        'job_description': 'description',
        'description': 'description',
        'requirements': 'requirements1',
        'profession': 'required_profession',
        'occupation_field': 'field_of_occupation',
        'city': 'work_location',
}

SAFE = re.compile(r'[^A-Za-z0-9_-]+')

def _canon_header(h: str) -> str:
    # Delegate to shared header mapping with job-specific fuzziness, while preserving local map as exact override.
    from scripts.header_mapping import canon_header as _shared_canon  # type: ignore
    hs = (h or '').strip()
    # Prefer local exact map if present
    if hs in HEADER_MAP:
        return HEADER_MAP[hs]
    return _shared_canon(hs, kind='job')

def sanitize_filename(text: str) -> str:
    t = SAFE.sub('_', text.strip())
    return t.strip('_') or 'job'

def row_to_text(row: dict) -> str:
    req = ' '.join(filter(None, [row.get('requirements1',''), row.get('requirements2','')])).strip()
    # Prefer explicit work_location over branch for Location line if present
    location_val = (row.get('work_location') or '').strip() or (row.get('branch') or '').strip()
    req_prof = (row.get('required_profession') or '').strip()
    field_occ = (row.get('field_of_occupation') or '').strip()
    lines = [
        f"Title: {row.get('title','').strip()}",
        f"Company: {row.get('client','').strip()}",
        f"Employment: {row.get('employment_type','').strip()}",
        f"Salary: {row.get('salary','').strip()}",
        f"Location: {location_val}",
        f"RequiredProfession: {req_prof}",
        f"FieldOfOccupation: {field_occ}",
        f"Status: {row.get('status','').strip()}",
        f"OpenDate: {row.get('open_date','').strip()}",
        "Description:",
        (row.get('description','') or '').strip(),
        "Requirements:",
        req,
    ]
    return '\n'.join(lines).strip() + '\n'

def import_csv(path: str, do_ingest: bool=False):
    import sys as _sys, pathlib as _pathlib
    ROOT_PATH = _pathlib.Path(__file__).resolve().parents[1]
    if str(ROOT_PATH) not in _sys.path:
        _sys.path.insert(0, str(ROOT_PATH))
    from scripts.ingest_agent import ingest_files
    created = []
    with open(path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        # Normalize headers
        fieldnames = reader.fieldnames or []
        field_map = {h: _canon_header(h) for h in fieldnames}
        coverage = {field_map[h]: h for h in fieldnames}
        for raw in reader:
            row = { field_map[k]: (raw[k] or '').replace('\r','').strip() for k in raw }
            if not row.get('title'):
                continue
            oid = row.get('order_id') or sanitize_filename(row.get('title',''))
            fname = JOBS_DIR / f"job_{sanitize_filename(oid)}.txt"
            if not fname.exists():
                fname.write_text(row_to_text(row), encoding='utf-8')
            created.append(str(fname))
    if do_ingest and created:
        ingest_files(created, kind='job')
    return { 'created': created, 'header_map': field_map, 'coverage': coverage }

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/import_jobs_csv.py <csv_path> [--ingest]')
        sys.exit(1)
    src = sys.argv[1]
    ingest_flag = '--ingest' in sys.argv[2:]
    out = import_csv(src, do_ingest=ingest_flag)
    # Preserve original output shape while enriching when possible
    if isinstance(out, dict):
        print(json.dumps(out, ensure_ascii=False))
    else:
        print(json.dumps({'created': out}, ensure_ascii=False))
