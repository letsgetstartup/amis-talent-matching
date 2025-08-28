from fastapi.testclient import TestClient
import os
from importlib import reload
import scripts.api as api_module

def test_distance_weight_update_and_effect():
    os.environ['API_KEY'] = 'secretkey'
    reload(api_module)
    app = api_module.app
    client = TestClient(app)
    # Update distance weight
    r = client.post('/config/distance_weight', json={'distance_weight': 0.25}, headers={'X-API-Key':'secretkey'})
    assert r.status_code == 200
    weights = r.json()['weights']
    assert abs(weights.get('distance_weight',0) - 0.25) < 1e-6
    # Read back weights
    r2 = client.get('/config/weights')
    assert r2.status_code == 200
    w2 = r2.json()['weights']
    assert w2.get('distance_weight') == 0.25
    # Cleanup
    del os.environ['API_KEY']
