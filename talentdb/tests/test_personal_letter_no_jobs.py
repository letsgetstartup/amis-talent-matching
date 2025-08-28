from fastapi.testclient import TestClient
from scripts.api import app, db
import tempfile
from scripts.ingest_agent import ingest_file

def test_personal_letter_no_jobs():
    # Ensure jobs collection empty for this test context (non-destructive: operate on temp DB if configured)
    db['jobs'].delete_many({})
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    tmp.write(b'Title: Data Analyst\nCity: Tel Aviv\nSkills: Python, SQL, Excel, Reporting, BI')
    tmp.flush(); tmp.close()
    cand_doc = ingest_file(tmp.name, kind='candidate', force_llm=False)
    client = TestClient(app)
    # Force letter generation
    r = client.post('/personal-letter', json={'share_id': cand_doc['share_id'], 'force': True})
    assert r.status_code == 200, r.text
    js = r.json()
    letter = js.get('letter') or {}
    assert letter.get('letter_content')
    assert js.get('match_count') == 0
    assert 'key_strengths' in letter
