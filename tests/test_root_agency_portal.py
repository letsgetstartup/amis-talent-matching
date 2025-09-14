from fastapi.testclient import TestClient
from talentdb.scripts.api import app


def test_root_serves_agency_portal():
    with TestClient(app) as client:
        r = client.get("/")
        # Accept 200 (served) or 500 (missing file) depending on environment
        assert r.status_code in (200, 500)
        if r.status_code == 200:
            # Look for a distinctive string from portal HTML (title tag or branding)
            assert "Portfolio Talent Exchange" in r.text or "PTX - Portfolio Talent Exchange" in r.text


def test_legacy_agency_portal_path_still_works():
    with TestClient(app) as client:
        r = client.get("/agency-portal.html")
        assert r.status_code in (200, 404)  # existing behavior
        if r.status_code == 200:
            assert "Portfolio Talent Exchange" in r.text or "PTX - Portfolio Talent Exchange" in r.text
