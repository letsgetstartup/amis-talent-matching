from fastapi.testclient import TestClient
from scripts.api import app
from scripts.ingest_agent import db, jobs_for_candidate

client = TestClient(app)

def test_explain_endpoint_basic():
    cand = db['candidates'].find_one()
    job = db['jobs'].find_one()
    assert cand and job
    r = client.get(f"/match/explain/{cand['_id']}/{job['_id']}")
    assert r.status_code == 200
    data = r.json()
    assert 'score' in data
    assert 'skill_overlap' in data
    assert 'title_similarity' in data
