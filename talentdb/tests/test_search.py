from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)

def test_search_jobs_basic():
    r = client.get('/search/jobs?limit=5')
    assert r.status_code == 200
    data = r.json()
    assert 'results' in data

def test_search_candidates_basic():
    r = client.get('/search/candidates?limit=5')
    assert r.status_code == 200
    data = r.json()
    assert 'results' in data

def test_search_jobs_skill_filter():
    r = client.get('/search/jobs?skill=administration&limit=3')
    assert r.status_code == 200
    data = r.json(); assert 'results' in data

def test_search_jobs_multi_skill_all_mode():
    r = client.get('/search/jobs?skills=administration,customer_service&mode=all&limit=5')
    assert r.status_code == 200
    data=r.json(); assert 'results' in data

def test_search_candidates_sorting():
    r_any = client.get('/search/candidates?skills=administration,customer_service&limit=5&sort_by=matched')
    assert r_any.status_code == 200
    r_recent = client.get('/search/candidates?skills=administration,customer_service&limit=5&sort_by=recent')
    assert r_recent.status_code == 200

def test_search_cache_consistency():
    r1 = client.get('/search/jobs?skill=administration&limit=2')
    r2 = client.get('/search/jobs?skill=administration&limit=2')
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json() == r2.json()
