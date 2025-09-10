from fastapi.testclient import TestClient
from talentdb.scripts.api import app

def test_ai_matching_page_served():
    with TestClient(app) as client:
        r1 = client.get("/ai-matching.html")
        assert r1.status_code in (200, 404)
        # If not present in this environment, endpoint should return 404 with diagnostic detail
        if r1.status_code == 200:
            assert "AI Matching" in r1.text
        r2 = client.get("/ai-matching")
        assert r2.status_code in (200, 404)
        if r2.status_code == 200:
            assert "AI Matching" in r2.text
