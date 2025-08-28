import os
from fastapi.testclient import TestClient
from importlib import reload
import scripts.api as api_module

def test_combined_config_endpoint():
    os.environ['API_KEY']='combo'
    reload(api_module)
    app=api_module.app
    client=TestClient(app)
    payload={
        'skill_weight':0.4,
        'title_weight':0.6,
        'distance_weight':0.15,
        'must_weight':0.75,
        'needed_weight':0.25,
        'min_skill_floor':4
    }
    r=client.post('/config/all', json=payload, headers={'X-API-Key':'combo'})
    assert r.status_code==200
    w=r.json()['weights']
    assert w.get('distance_weight')==0.15
    assert w.get('min_skill_floor')==4
    # restore basics
    client.post('/config/weights', json={'skill_weight':0.85,'title_weight':0.15}, headers={'X-API-Key':'combo'})
    client.post('/config/min_skill_floor', json={'min_skill_floor':3}, headers={'X-API-Key':'combo'})
    del os.environ['API_KEY']
