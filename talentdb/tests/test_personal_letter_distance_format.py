from fastapi.testclient import TestClient
from scripts.api import app, db
from scripts.ingest_agent import ingest_file
import tempfile

def test_letter_distance_format_clean():
    # Ensure at least one job and one candidate
    db['jobs'].delete_many({})
    db['candidates'].delete_many({})
    # Insert a simple job with city
    job_txt = b"Title: Branch Manager\nCity: Petah Tikva\nRequirements: Leadership, Sales, Ops"
    jtmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt'); jtmp.write(job_txt); jtmp.flush(); jtmp.close()
    job_doc = ingest_file(jtmp.name, kind='job', force_llm=False)
    # Candidate with city
    cand_txt = b"Title: Sales Lead\nCity: Rishon LeZion\nSkills: Sales, CRM, Ops"
    ctmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt'); ctmp.write(cand_txt); ctmp.flush(); ctmp.close()
    cand_doc = ingest_file(ctmp.name, kind='candidate', force_llm=False)
    client = TestClient(app)
    r = client.post('/personal-letter', json={'share_id': cand_doc['share_id'], 'force': True})
    assert r.status_code == 200, r.text
    letter = r.json().get('letter', {})
    text = letter.get('letter_content','')
    # Should not contain placeholders
    assert '~N/A' not in text
    assert '{minutes}' not in text
    # If a location line exists, it should be either with proper distance or just city
    # We allow both forms; ensure it doesn't show dangling 'מ ' when distance omitted
    assert 'מ ' + ' ' not in text  # crude check for dangling 'מ '
