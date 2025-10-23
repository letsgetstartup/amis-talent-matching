import time
import urllib.parse
import uuid

import pytest
from fastapi.testclient import TestClient

from talentdb.scripts.api import app, clear_skill_frequency_cache, clear_tenant_cache
from talentdb.scripts.ingest_agent import db


@pytest.fixture(autouse=True)
def reset_skill_frequency_cache():
    clear_skill_frequency_cache()
    clear_tenant_cache()
    yield
    clear_skill_frequency_cache()
    clear_tenant_cache()


@pytest.fixture(autouse=True)
def cleanup_chatbot_collections():
    db["portal_chat_seeds"].delete_many({})
    db["portal_conversations"].delete_many({})
    yield
    db["portal_chat_seeds"].delete_many({})
    db["portal_conversations"].delete_many({})


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def tenant_record():
    slug = f"gh-test-{uuid.uuid4().hex[:8]}"
    now = int(time.time())
    tenant_id = db["tenants"].insert_one({
        "name": f"Greenhouse Redirect Test {slug}",
        "slug": slug,
        "created_at": now,
        "stats": {"job_count": 0, "company_count": 0, "location_count": 0},
    }).inserted_id
    try:
        yield {"id": tenant_id, "slug": slug}
    finally:
        db["jobs"].delete_many({"tenant_id": str(tenant_id)})
        db["tenants"].delete_one({"_id": tenant_id})


def _insert_job(tenant_id, *, external_job_id, city=None, skill_set=None, application_url=None):
    now = int(time.time())
    db["jobs"].insert_one({
        "tenant_id": str(tenant_id),
        "external_job_id": external_job_id,
        "title": "Test Redirect Job",
        "city": city,
        "job_description": "",
        "skill_set": skill_set or [],
        "requirements": {
            "must_have_skills": [],
            "nice_to_have_skills": [],
        },
        "created_at": now,
        "updated_at": now,
        "application_url": application_url,
    })


def _encode_path(url: str) -> str:
    return urllib.parse.quote(url, safe="")


def test_redirect_success_with_location_and_skills(client, tenant_record):
    gh_id = "7390395003"
    _insert_job(
        tenant_record["id"],
        external_job_id=gh_id,
        city="Tel Aviv",
        skill_set=["React", "Node.js", "Python"],
    )
    # Additional jobs to ensure frequency ranking selects the most popular skills
    other_skills = ["React", "Node.js", "Python", "GraphQL"]
    for idx, skill in enumerate(other_skills):
        _insert_job(
            tenant_record["id"],
            external_job_id=f"extra-{idx}",
            city="Tel Aviv",
            skill_set=[skill],
        )
    gh_url = _encode_path(f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}")
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}?location=Tel%20Aviv&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["React", "Node.js", "Python"]


def test_redirect_matches_application_url_when_external_id_differs(client, tenant_record):
    gh_id = "4670592004"
    application_url = f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}"
    _insert_job(
        tenant_record["id"],
        external_job_id="senior-backend-engineer",
        application_url=application_url,
        city="Tel Aviv, Israel",
        skill_set=["Python", "FastAPI"],
    )
    gh_url = _encode_path(application_url)
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}?location=Tel%20Aviv,%20Israel&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["Python", "FastAPI"]


def test_redirect_handles_numeric_prefix_with_slug_suffix(client, tenant_record):
    gh_id = "4860157004"
    app_url = f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}-data-scientist"
    _insert_job(
        tenant_record["id"],
        external_job_id="data-scientist",
        application_url=app_url,
        city="Tel Aviv, Israel",
        skill_set=["LLMs", "Data Wrangling", "ML"],
    )
    gh_url = _encode_path(app_url)
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}?location=Tel%20Aviv,%20Israel&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["LLMs", "Data Wrangling", "ML"]


def test_redirect_job_not_found_falls_back(client, tenant_record):
    gh_url = _encode_path("https://job-boards.greenhouse.io/acme/jobs/999999")
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["location"] == f"/portal/{tenant_record['slug']}"


def test_redirect_missing_tenant(client):
    gh_url = _encode_path("https://job-boards.greenhouse.io/acme/jobs/7390395003")
    response = client.get(
        f"/portal/nonexistent-tenant/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/portal/nonexistent-tenant"


def test_redirect_parses_fallback_query_parameter(client, tenant_record):
    gh_id = "1234567890"
    _insert_job(
        tenant_record["id"],
        external_job_id=gh_id,
        city="New York",
        skill_set=["C#", "C++", "C"],
    )
    gh_url = _encode_path(
        f"https://boards.greenhouse.io/embed/job_app?gh_jid={gh_id}&utm_campaign=rejection"
    )
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}?location=New%20York&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["C#", "C++", "C"]


def test_auto_redirect_without_tenant_slug(client, tenant_record):
    gh_id = "900112233"
    _insert_job(
        tenant_record["id"],
        external_job_id=gh_id,
        city="Haifa",
        skill_set=["Go", "Kubernetes", "GCP"],
    )
    gh_url = _encode_path(f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}")
    response = client.get(f"/portal/redirect/{gh_url}", allow_redirects=False)
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}?location=Haifa&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["Go", "Kubernetes", "GCP"]


def test_auto_redirect_falls_back_when_job_missing(client):
    gh_url = _encode_path("https://job-boards.greenhouse.io/acme/jobs/000000")
    response = client.get(f"/portal/redirect/{gh_url}", allow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/portal"


def test_redirect_includes_chat_seed_and_seed_endpoint(client, tenant_record):
    gh_id = "555667788"
    _insert_job(
        tenant_record["id"],
        external_job_id=gh_id,
        city="Herzliya",
        skill_set=["Python", "LLMs", "FastAPI"],
    )
    gh_url = _encode_path(f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}")
    response = client.get(
        f"/portal/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/{tenant_record['slug']}")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    chat_seed = params.get("chat_seed")
    assert chat_seed, "redirect should include chat_seed parameter"

    seed_doc = db["portal_chat_seeds"].find_one({"_id": chat_seed})
    assert seed_doc is not None
    assert seed_doc.get("portal_slug") == tenant_record["slug"]
    assert seed_doc.get("tenant_id") == str(tenant_record["id"])

    seed_response = client.get(f"/portal/chat/seed/{chat_seed}")
    assert seed_response.status_code == 200
    payload = seed_response.json()
    assert payload["portal_slug"] == tenant_record["slug"]
    assert payload["highlighted_job_ids"], "seed should highlight at least one job"

    # Seed consumption should delete the record
    assert db["portal_chat_seeds"].find_one({"_id": chat_seed}) is None

    second_attempt = client.get(f"/portal/chat/seed/{chat_seed}")
    assert second_attempt.status_code == 404


def test_dynamic_portal_redirect_mirrors_skill_selection(client, tenant_record):
    gh_id = "5551234001"
    _insert_job(
        tenant_record["id"],
        external_job_id=gh_id,
        city="Jerusalem",
        skill_set=["SQL", "ETL", "Python", "Data Modeling"],
    )
    # Boost popularity for SQL and ETL so they rank highest, Python third.
    for extra_id, skills in enumerate(("SQL", "ETL", "SQL", "ETL", "Python")):
        _insert_job(
            tenant_record["id"],
            external_job_id=f"pop-{extra_id}",
            city="Jerusalem",
            skill_set=[skills],
        )

    gh_url = _encode_path(f"https://job-boards.greenhouse.io/acme/jobs/{gh_id}")
    response = client.get(
        f"/portal/dynamic/{tenant_record['slug']}/redirect/{gh_url}",
        allow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(f"/portal/dynamic/{tenant_record['slug']}?location=Jerusalem&skills=")
    _, query = location.split("?", 1)
    params = dict(urllib.parse.parse_qsl(query))
    skills = params.get("skills", "").split(",")
    assert skills == ["SQL", "ETL", "Python"]


def test_redirect_gracefully_handles_internal_errors(monkeypatch, client):
    from talentdb.scripts import api as api_module

    class BoomDB:
        def __getitem__(self, name):
            raise RuntimeError("db down")

    monkeypatch.setattr(api_module, "db", BoomDB())
    gh_url = _encode_path("https://job-boards.greenhouse.io/acme/jobs/4670592004")
    response = client.get(
        f"/portal/demo-tenant/redirect/{gh_url}", allow_redirects=False
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/portal/demo-tenant"