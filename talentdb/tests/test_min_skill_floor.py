import os
from fastapi.testclient import TestClient
from importlib import reload
import scripts.api as api_module

def test_min_skill_floor_update():
    os.environ['API_KEY']='floorkey'
    reload(api_module)
    app=api_module.app
    client=TestClient(app)
    r=client.post('/config/min_skill_floor', json={'min_skill_floor':5}, headers={'X-API-Key':'floorkey'})
    assert r.status_code==200
    weights=r.json()['weights']
    assert weights.get('min_skill_floor')==5
    # reset to 3
    r2=client.post('/config/min_skill_floor', json={'min_skill_floor':3}, headers={'X-API-Key':'floorkey'})
    assert r2.status_code==200
    del os.environ['API_KEY']
