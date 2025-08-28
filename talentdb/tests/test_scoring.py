import sys, pathlib, json
ROOT=pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import ingest_agent
from scripts.db import get_db

db=get_db()

def test_composite_scoring_fields():
    # ensure at least one candidate and job
    if not db['candidates'].find_one():
        import glob
        ingest_agent.ingest_files(glob.glob(str(ROOT/ 'samples' / 'cvs' / '*')), kind='candidate')
    if not db['jobs'].find_one():
        import glob
        ingest_agent.ingest_files(glob.glob(str(ROOT/ 'samples' / 'jobs' / '*')), kind='job')
    c = db['candidates'].find_one()
    assert c
    res = ingest_agent.jobs_for_candidate(str(c['_id']), top_k=1)
    assert res and 'score' in res[0]
    assert 'skill_score' in res[0] and 'title_similarity' in res[0]
