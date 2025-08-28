from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)

def test_weights_update_and_readback():
    # Read current weights
    r = client.get('/config/weights')
    assert r.status_code == 200
    orig = r.json()['weights']
    # Update
    r2 = client.post('/config/weights', json={'skill_weight': 0.2, 'title_weight': 0.8})
    assert r2.status_code == 200
    updated = r2.json()['weights']
    assert abs(updated['skill_weight'] - 0.2) < 1e-6 or abs(updated['skill_weight'] - 0.2/(0.2+0.8)) < 1e-6
    # Restore original weights to avoid side-effects
    client.post('/config/weights', json={'skill_weight': orig['skill_weight'], 'title_weight': orig['title_weight']})


def test_recompute_and_meta():
    r = client.post('/maintenance/recompute')
    assert r.status_code == 200
    assert 'changed' in r.json()
    meta = client.get('/meta')
    assert meta.status_code == 200
    assert 'meta' in meta.json()


def test_clear_cache_endpoint():
    r = client.post('/maintenance/clear_cache')
    assert r.status_code == 200
    assert r.json().get('cleared') is True
