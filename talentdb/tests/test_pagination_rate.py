from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)

def test_candidates_pagination():
    r1 = client.get('/candidates?skip=0&limit=2')
    assert r1.status_code == 200
    data1 = r1.json()
    assert 'candidates' in data1 and 'total' in data1
    assert data1['limit'] == 2
    if data1['total'] > 2:
        r2 = client.get('/candidates?skip=2&limit=2')
        assert r2.status_code == 200
        data2 = r2.json()
        # Ensure non-overlapping when enough docs
        if len(data1['candidates']) == 2 and len(data2['candidates']) == 2:
            assert set(data1['candidates']).isdisjoint(set(data2['candidates']))


def test_jobs_pagination():
    r = client.get('/jobs?skip=0&limit=3')
    assert r.status_code == 200
    dj = r.json()
    assert 'jobs' in dj and 'total' in dj
    assert dj['limit'] == 3


def test_rate_limit_headers():
    # Make a few rapid calls and check headers presence
    for _ in range(3):
        resp = client.get('/health')
        assert resp.status_code == 200
        assert 'X-RateLimit-Limit' in resp.headers
        assert 'X-RateLimit-Remaining' in resp.headers
        assert 'X-RateLimit-Reset' in resp.headers
