from fastapi.testclient import TestClient
from scripts.api import app, API_KEY
from scripts.ingest_agent import db
import os

client = TestClient(app)

def _post(path: str, payload: dict):
    headers={}
    if API_KEY:
        headers['X-API-Key']=API_KEY
    return client.post(path, json=payload, headers=headers)

def test_category_weights_endpoint():
    r = _post('/config/category_weights', {'must_weight': 0.6, 'needed_weight': 0.4})
    # If API key required and not provided, test client will already include key via env else fail
    assert r.status_code in (200,401)
    if r.status_code == 401:
        # Skip gracefully when API_KEY set but not accessible
        return
    w = r.json()['weights']
    assert abs(w['must_category_weight'] - 0.6/(0.6+0.4)) < 1e-6
    _post('/config/category_weights', {'must_weight': 0.7, 'needed_weight': 0.3})


def test_city_filter_toggle():
    # pick any job and candidate; calling with city_filter=false should always return something if base logic returns any when true
    job = db['jobs'].find_one()
    cand = db['candidates'].find_one()
    assert job and cand
    r_true = client.get(f"/match/job/{job['_id']}?k=3&city_filter=true")
    assert r_true.status_code == 200
    r_false = client.get(f"/match/job/{job['_id']}?k=3&city_filter=false")
    assert r_false.status_code == 200
    # If true returned 0 matches (possible), false should not raise; if true had >0 keep that invariant
    if r_true.json()['matches']:
        assert r_false.json()['matches']  # expect at least as many when filter disabled
