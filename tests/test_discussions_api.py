import sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'talentdb'))

from scripts.ingest_agent import db  # type: ignore
from fastapi.testclient import TestClient
from scripts.api import app  # type: ignore


def test_discussions_add_and_get(tmp_path):
    # Ensure at least one candidate exists
    c = db['candidates'].find_one({})
    if not c:
        db['candidates'].insert_one({"full_name":"Test Cand","city_canonical":"tel_aviv","updated_at":int(time.time())})
        c = db['candidates'].find_one({})
    assert c is not None
    cid = str(c['_id'])

    client = TestClient(app)
    # Add a discussion
    r = client.post('/discussions', json={
        "target_type": "candidate",
        "target_id": cid,
        "text": "בדיקת הערה",
        "actor_name": "pytest"
    })
    assert r.status_code == 200, r.text
    j = r.json()
    assert j.get('ok') is True

    # List discussions
    r2 = client.get('/discussions', params={
        'target_type': 'candidate',
        'target_id': cid,
        'limit': 10,
    })
    assert r2.status_code == 200, r2.text
    data = r2.json()
    assert isinstance(data.get('items'), list)
    # Should contain at least one item for the candidate
    assert any(it.get('text') == 'בדיקת הערה' for it in data['items'])
