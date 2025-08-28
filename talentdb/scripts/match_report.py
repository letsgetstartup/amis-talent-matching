"""Generate best job matches for every ingested candidate and store in Mongo.

Previous behavior wrote JSON lines to stdout / files; deprecated due to Mongo-only policy.
Now results are upserted into collection 'reports_match_topk'.
"""
from pathlib import Path
import glob, json, sys, time
import sys as _sys, pathlib as _pathlib
ROOT_PATH = _pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in _sys.path:
    _sys.path.insert(0, str(ROOT_PATH))
from scripts.ingest_agent import ingest_files, jobs_for_candidate, db as active_db, disable_llm, refresh_existing, recompute_skill_sets, create_indexes

SAMPLES = Path(__file__).resolve().parent.parent / 'samples'

def ensure_ingested():
    """Ingest sample files only if collections are empty.
    Disables LLM to keep operation fast and deterministic for reports.
    """
    disable_llm()
    if active_db['candidates'].count_documents({}) == 0:
        cvs = [p for p in glob.glob(str(SAMPLES / 'cvs' / '*')) if Path(p).is_file()]
        if cvs:
            ingest_files(cvs, kind='candidate')
    if active_db['jobs'].count_documents({}) == 0:
        jobs = [p for p in glob.glob(str(SAMPLES / 'jobs' / '*')) if Path(p).is_file()]
        if jobs:
            ingest_files(jobs, kind='job')

def main(top_k: int = 5, fast: bool=False, refresh: bool=False):
    if refresh:
        # Recompute skill sets and ensure indexes
        disable_llm()
        recompute_skill_sets()
        create_indexes()
    if not fast:
        ensure_ingested()
    coll = active_db['reports_match_topk']
    bulk = []
    for cand in active_db['candidates'].find():
        cand_id = str(cand['_id'])
        matches = jobs_for_candidate(cand_id, top_k=top_k)
        doc = {
            '_id': cand_id,
            'candidate_id': cand_id,
            'title': cand.get('title'),
            'top_k': top_k,
            'generated_at': int(time.time()),
            'matches': [
                {
                    'job_id': str(m['job_id']),
                    'title': m.get('title'),
                    'score': round(m.get('score', 0), 4),
                    'skills_overlap': m.get('skills_overlap', []),
                }
                for m in matches
            ],
        }
        bulk.append(doc)
    # Upsert in batches
    from pymongo import UpdateOne
    ops = [UpdateOne({'_id': d['_id']}, {'$set': d}, upsert=True) for d in bulk]
    if ops:
        coll.bulk_write(ops)
    print(f"Stored {len(bulk)} candidate match reports in 'reports_match_topk'")

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('top_k', nargs='?', type=int, default=5)
    ap.add_argument('--fast', action='store_true', help='Skip any ingestion; just read current DB')
    ap.add_argument('--refresh', action='store_true', help='Recompute skill sets and indexes before matching')
    args = ap.parse_args()
    main(args.top_k, fast=args.fast, refresh=args.refresh)
