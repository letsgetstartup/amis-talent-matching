"""Run an end-to-end demo: ingest sample CV & Job, then show top matches.
Uses real Mongo if available else in-memory mongomock. If OPENAI_API_KEY is set,
LLM extraction is attempted; otherwise fallback heuristic parsing is used.
"""
from scripts.db import get_db
from scripts.ingest_agent import ingest_files, candidates_for_job, jobs_for_candidate, db as active_db
import glob, json

def main():
    cvs = glob.glob('samples/cvs/*')
    jobs = glob.glob('samples/jobs/*')
    ingest_files(cvs, kind='candidate')
    ingest_files(jobs, kind='job')
    d = get_db()
    job = d['jobs'].find_one()
    cand = d['candidates'].find_one()
    if not job or not cand:
        print('Nothing ingested; ensure samples exist.')
        return
    job_matches = candidates_for_job(str(job['_id']), top_k=5)
    cand_matches = jobs_for_candidate(str(cand['_id']), top_k=5)
    print('\nJob -> Candidates (top 5)')
    print(json.dumps(job_matches, default=str, indent=2))
    print('\nCandidate -> Jobs (top 5)')
    print(json.dumps(cand_matches, default=str, indent=2))

if __name__ == '__main__':
    main()
