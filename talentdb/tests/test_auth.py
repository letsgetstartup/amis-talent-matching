import os
from fastapi.testclient import TestClient
from importlib import reload
import scripts.api as api_module

def test_auth_protection():
    # Set API_KEY before reloading module so dependency picks it up
    os.environ['API_KEY'] = 'testsecret'
    reload(api_module)
    app = api_module.app
    client = TestClient(app)

    # Without key should be unauthorized for protected endpoint
    r = client.post('/maintenance/recompute')
    assert r.status_code == 401

    # With key should succeed
    r2 = client.post('/maintenance/recompute', headers={'X-API-Key':'testsecret'})
    assert r2.status_code == 200
    # Weights endpoint also protected for POST
    r3 = client.post('/config/weights', json={'skill_weight':0.3,'title_weight':0.7}, headers={'X-API-Key':'testsecret'})
    assert r3.status_code == 200

    # GET weights remains public
    r4 = client.get('/config/weights')
    assert r4.status_code == 200

    # Cleanup
    del os.environ['API_KEY']
