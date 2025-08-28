from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)

def test_admin_jobs_validate_endpoint():
    r = client.get('/admin/jobs/validate')
    assert r.status_code == 200
    data = r.json()
    assert 'validated' in data and 'results' in data


def test_admin_jobs_export_csv():
    r = client.get('/admin/jobs/export?format=csv')
    assert r.status_code == 200
    assert r.text.startswith('id,title,city')


def test_admin_jobs_all_filters():
    # basic load
    r = client.get('/admin/jobs/all')
    assert r.status_code == 200
    # apply a dummy q filter (should still 200)
    r2 = client.get('/admin/jobs/all?q=engineer')
    assert r2.status_code == 200
