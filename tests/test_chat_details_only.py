from fastapi.testclient import TestClient
from talentdb.scripts.api import app

client = TestClient(app)


def test_details_only_hides_table_for_job_query_nonstream():
    job_id = "68ae892edc8b36d3dcc08ac3"  # sample; test tolerant to not-found
    q = f"מועמדים למשרה {job_id}"
    r = client.post('/chat/query', json={"question": q, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    # Ensure no Table is present when detailsOnly is true
    assert not any(b.get('kind') == 'Table' for b in ui)


def test_details_only_hides_table_for_early_objectid_stream():
    job_id = "68ae892edc8b36d3dcc08ac3"
    # Use non-stream fallback behavior path by not setting stream param; ensure no table as well
    r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    assert not any(b.get('kind') == 'Table' for b in ui)
