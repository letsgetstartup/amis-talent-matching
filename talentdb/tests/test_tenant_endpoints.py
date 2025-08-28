import os, json
from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)

def make_key():
    # signup -> apikey
    r = client.post('/auth/signup', json={'company':'TCo','name':'Admin','email':'t@example.com','password':'p4ssw0rd'})
    assert r.status_code == 200, r.text
    tid = r.json()['tenant_id']
    r2 = client.post('/auth/apikey', json={'tenant_id': tid, 'name': 'test'})
    assert r2.status_code == 200, r2.text
    return r2.json()['key']


def test_jobs_list_and_create():
    key = make_key()
    # empty
    r = client.get('/tenant/jobs', headers={'X-API-Key': key})
    assert r.status_code == 200
    assert r.json()['total'] == 0
    # create
    payload = {"external_job_id":"X1","title":"Engineer","city":"Tel Aviv","must_have":["Python"],"description":"Build"}
    r = client.post('/jobs', headers={'X-API-Key': key}, json=payload)
    assert r.status_code == 200, r.text
    r = client.get('/tenant/jobs', headers={'X-API-Key': key})
    assert r.status_code == 200
    data = r.json()
    assert data['total'] == 1
    assert data['results'][0]['title'] == 'Engineer'


def test_candidates_upload_and_list(tmp_path):
    key = make_key()
    p = tmp_path / 'cv.txt'
    p.write_text('John Doe\nSoftware Engineer\nPython, SQL')
    with open(p, 'rb') as f:
        r = client.post('/tenant/candidates/upload', headers={'X-API-Key': key}, files={'files': ('cv.txt', f, 'text/plain')})
    assert r.status_code == 200, r.text
    r = client.get('/tenant/candidates', headers={'X-API-Key': key})
    assert r.status_code == 200
    data = r.json()
    assert data['total'] >= 1
    assert any('candidate_id' in row for row in data['results'])


def test_all_fields_view():
    key = make_key()
    r = client.get('/tenant/candidates/all_fields', headers={'X-API-Key': key})
    assert r.status_code == 200
    data = r.json()
    assert 'columns' in data and 'rows' in data
