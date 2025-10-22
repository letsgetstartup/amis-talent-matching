import time
import uuid

import pytest

from talentdb.scripts.api import (
    clear_skill_frequency_cache,
    get_tenant_skill_frequencies,
    select_top_skills,
)
from talentdb.scripts.ingest_agent import db


@pytest.fixture(autouse=True)
def reset_skill_cache():
    clear_skill_frequency_cache()
    yield
    clear_skill_frequency_cache()


@pytest.fixture
def tenant():
    slug = f"skill-cache-{uuid.uuid4().hex[:8]}"
    now = int(time.time())
    tenant_id = db["tenants"].insert_one({
        "name": f"Skill Cache Test {slug}",
        "slug": slug,
        "created_at": now,
    }).inserted_id
    try:
        yield tenant_id
    finally:
        db["jobs"].delete_many({"tenant_id": str(tenant_id)})
        db["tenants"].delete_one({"_id": tenant_id})


def test_select_top_skills_orders_by_frequency_and_job_order():
    job_skills = ["React", "Node.js", "Python", "GraphQL"]
    frequency_map = {"React": 5, "Node.js": 9, "Python": 9, "GraphQL": 1}
    result = select_top_skills(job_skills, frequency_map, top_n=3)
    assert result == ["Node.js", "Python", "React"]


def test_select_top_skills_without_frequency_map_returns_first_n():
    job_skills = ["Kubernetes", "Docker", "Terraform", "AWS"]
    result = select_top_skills(job_skills, frequency_map={}, top_n=2)
    assert result == ["Kubernetes", "Docker"]


def test_get_tenant_skill_frequencies_counts_skills(tenant):
    db["jobs"].insert_many([
        {
            "tenant_id": str(tenant),
            "external_job_id": "job-1",
            "skill_set": ["Python", "SQL", "ETL"],
        },
        {
            "tenant_id": str(tenant),
            "external_job_id": "job-2",
            "skill_set": ["Python", "SQL"],
        },
        {
            "tenant_id": str(tenant),
            "external_job_id": "job-3",
            "skill_set": [],
        },
    ])

    freq = get_tenant_skill_frequencies(str(tenant))
    assert freq["Python"] == 2
    assert freq["SQL"] == 2
    assert freq["ETL"] == 1
    assert "" not in freq


def test_get_tenant_skill_frequencies_cache_persists_until_cleared(tenant):
    db["jobs"].insert_one({
        "tenant_id": str(tenant),
        "external_job_id": "job-cache",
        "skill_set": ["FastAPI"],
    })

    first = get_tenant_skill_frequencies(str(tenant), ttl_seconds=3600)
    assert first == {"FastAPI": 1}

    # Remove from DB; cached result should remain the same until invalidated.
    db["jobs"].delete_many({"tenant_id": str(tenant)})
    second = get_tenant_skill_frequencies(str(tenant), ttl_seconds=3600)
    assert second == first

    clear_skill_frequency_cache(str(tenant))
    third = get_tenant_skill_frequencies(str(tenant), ttl_seconds=3600)
    assert third == {}
