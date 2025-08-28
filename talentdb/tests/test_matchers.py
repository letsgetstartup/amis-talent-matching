import sys, pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.ingest_agent import candidates_for_job, jobs_for_candidate
from scripts.db import get_db
from scripts import ingest_agent

db=get_db()

def _ensure_seed():
    if not db['candidates'].find_one():
        import glob
        ingest_agent.ingest_files(glob.glob(str(ROOT/ 'samples' / 'cvs' / '*')), kind='candidate')
    if not db['jobs'].find_one():
        import glob
        ingest_agent.ingest_files(glob.glob(str(ROOT/ 'samples' / 'jobs' / '*')), kind='job')

_ensure_seed()

def test_smoke_job_to_candidates():
    j=db["jobs"].find_one(); assert j
    res=candidates_for_job(str(j["_id"]), top_k=3)
    assert isinstance(res,list)

def test_smoke_candidate_to_jobs():
    c=db["candidates"].find_one(); assert c
    res=jobs_for_candidate(str(c["_id"]), top_k=3)
    assert isinstance(res,list)
