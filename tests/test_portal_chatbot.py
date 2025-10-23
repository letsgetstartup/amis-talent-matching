import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator

import pytest
from fastapi.testclient import TestClient

from talentdb.scripts.api import app, clear_skill_frequency_cache, clear_tenant_cache
from talentdb.scripts.ingest_agent import db


@pytest.fixture(autouse=True)
def reset_caches_and_collections():
    clear_skill_frequency_cache()
    clear_tenant_cache()
    db["portal_chat_seeds"].delete_many({})
    db["portal_conversations"].delete_many({})
    yield
    clear_skill_frequency_cache()
    clear_tenant_cache()
    db["portal_chat_seeds"].delete_many({})
    db["portal_conversations"].delete_many({})


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def tenant_record() -> Iterator[Dict[str, Any]]:
    slug = f"chatbot-test-{uuid.uuid4().hex[:8]}"
    now = int(time.time())
    tenant_id = db["tenants"].insert_one({
        "name": f"Portal Chatbot Test {slug}",
        "slug": slug,
        "created_at": now,
        "stats": {"job_count": 0, "company_count": 0, "location_count": 0},
    }).inserted_id
    try:
        yield {"id": tenant_id, "slug": slug}
    finally:
        db["jobs"].delete_many({"tenant_id": str(tenant_id)})
        db["tenants"].delete_one({"_id": tenant_id})


def _insert_job(tenant_id, *, job_id: str | None = None, city: str | None = None, skill_set: list[str] | None = None):
    now = int(time.time())
    return db["jobs"].insert_one({
        "tenant_id": str(tenant_id),
        "external_job_id": job_id or f"job-{uuid.uuid4().hex[:6]}",
        "title": "QA Automation Engineer",
        "city": city or "Tel Aviv",
        "job_description": "Work closely with developers to ensure quality.",
        "skill_set": skill_set or ["Python", "Playwright"],
        "requirements": {
            "must_have_skills": [],
            "nice_to_have_skills": [],
        },
        "created_at": now,
        "updated_at": now,
        "application_url": "https://example.com/apply",
    }).inserted_id


def test_chat_message_flow_creates_conversation(client, tenant_record):
    _insert_job(tenant_record["id"])

    payload = {
        "portal_slug": tenant_record["slug"],
        "message": "Hi, what roles are available?",
        "current_filters": {"skills": ["Python"]},
    }
    response = client.post("/portal/chat/message", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["conversation_id"]
    assert isinstance(body["message"], str)

    conversation_id = body["conversation_id"]
    record = db["portal_conversations"].find_one({"conversation_id": conversation_id})
    assert record is not None
    assert record.get("portal_slug") == tenant_record["slug"]
    assert len(record.get("messages", [])) == 2

    follow_up = client.post(
        "/portal/chat/message",
        json={
            "portal_slug": tenant_record["slug"],
            "message": "Show me remote roles",
            "conversation_id": conversation_id,
            "current_filters": {"type": "remote"},
        },
    )
    assert follow_up.status_code == 200
    record = db["portal_conversations"].find_one({"conversation_id": conversation_id})
    assert len(record.get("messages", [])) == 4
    metadata = record.get("metadata") or {}
    assert metadata.get("message_count") == 4


def test_chat_history_and_delete_endpoints(client, tenant_record):
    _insert_job(tenant_record["id"])
    first = client.post(
        "/portal/chat/message",
        json={"portal_slug": tenant_record["slug"], "message": "Hello"},
    )
    conversation_id = first.json()["conversation_id"]

    history = client.get(f"/portal/chat/conversation/{conversation_id}")
    assert history.status_code == 200
    history_payload = history.json()
    assert history_payload["conversation_id"] == conversation_id
    assert len(history_payload["messages"]) == 2

    delete_response = client.delete(f"/portal/chat/conversation/{conversation_id}")
    assert delete_response.status_code == 204
    assert db["portal_conversations"].find_one({"conversation_id": conversation_id}) is None


def test_chat_suggestions_endpoint(client, tenant_record):
    _insert_job(tenant_record["id"], city="Jerusalem", skill_set=["React", "TypeScript"])
    response = client.post(
        "/portal/chat/suggest",
        json={"portal_slug": tenant_record["slug"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["starters"], "Expected at least one conversation starter"


def test_chat_seed_endpoint_requires_valid_token(client, tenant_record):
    invalid = client.get("/portal/chat/seed/not-a-real-token")
    assert invalid.status_code == 404

    job_oid = _insert_job(tenant_record["id"], job_id="seed-job")
    chat_seed = db["portal_chat_seeds"].insert_one({
        "_id": "manual-token",
        "portal_slug": tenant_record["slug"],
        "tenant_id": str(tenant_record["id"]),
        "external_url": None,
        "inferred_job_ids": [str(job_oid)],
        "suggested_job_ids": [str(job_oid)],
        "created_at": datetime.now(timezone.utc),
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=600),
    }).inserted_id

    seeded = client.get(f"/portal/chat/seed/{chat_seed}")
    assert seeded.status_code == 200
    assert seeded.json()["portal_slug"] == tenant_record["slug"]
