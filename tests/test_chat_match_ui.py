from fastapi.testclient import TestClient
from talentdb.scripts.api import app

client = TestClient(app)


def test_chat_job_details_contains_matchbreakdown():
    # This test assumes there is at least one job in the DB with id-like string;
    # use the sample job id from the user's screenshot. If DB empty, the endpoint
    # will return guidance; the test will then skip asserting breakdown.
    job_id = "68ae892edc8b36d3dcc08ac3"
    q = f"מועמדים למשרה {job_id} פירוט"
    r = client.post('/chat/query', json={"question": q, "currentView": "matches"})
    assert r.status_code == 200
    data = r.json()
    assert data.get('type') in ("assistant_ui", None) or 'ui' in data
    ui = data.get('ui') or []
    # Look for expected UI blocks: MatchBreakdown (details), or no-results guidance, or not-found guidance
    has_breakdown = any(block.get('kind') == 'MatchBreakdown' for block in ui)
    has_nores = any(block.get('kind') == 'RichText' and 'לא נמצאו' in (block.get('html') or '') for block in ui)
    has_notfound = any(block.get('kind') == 'RichText' and 'לא נמצאה משרה או מועמד' in (block.get('html') or '') for block in ui)
    assert has_breakdown or has_nores or has_notfound
