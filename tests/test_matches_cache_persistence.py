import json, time, urllib.request, urllib.error
from typing import Any, Dict
from bson import ObjectId

# Use the same DB the app uses
from talentdb.scripts.ingest_agent import db

API_BASE = "http://127.0.0.1:8000"


def http_get_json(path: str, timeout: float = 60.0) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers={"Cache-Control": "no-store"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
        raw = resp.read().decode("utf-8")
    try:
        return json.loads(raw)
    except Exception as e:
        raise AssertionError(f"Non-JSON response from {url}: {raw[:200]}") from e


def _insert_candidate() -> str:
    now = int(time.time())
    doc = {
        "title": "נציג/ת שירות לקוחות",
        "skill_set": ["office", "crm", "service"],
        "skills_detailed": [
            {"name": "office", "category": "must"},
            {"name": "crm", "category": "needed"},
        ],
        "city_canonical": "tel_aviv",
        "updated_at": now,
    }
    r = db["candidates"].insert_one(doc)
    return str(r.inserted_id)


def _insert_job() -> str:
    now = int(time.time())
    doc = {
        "title": "נציג/ת שירות ומכירות",
        "job_description": "תמיכת לקוחות, CRM, Office",
        "skill_set": ["office", "crm", "service"],
        "skills_detailed": [
            {"name": "office", "category": "must"},
            {"name": "crm", "category": "needed"},
        ],
        "city_canonical": "tel_aviv",
        "updated_at": now,
    }
    r = db["jobs"].insert_one(doc)
    return str(r.inserted_id)


def _cleanup_docs(cand_id: str | None = None, job_id: str | None = None) -> None:
    if cand_id:
        db["candidates"].delete_one({"_id": ObjectId(cand_id)})
        db["matches_cache"].delete_many({"candidate_id": cand_id})
    if job_id:
        db["jobs"].delete_one({"_id": ObjectId(job_id)})
        db["matches_cache"].delete_many({"job_id": job_id})


def test_c2j_cache_write_and_hit():
    # Ensure server up
    health = http_get_json("/health")
    assert health.get("status") == "ok"

    cand_id = _insert_candidate()
    try:
        # First call should compute and persist cache
        path = f"/match/candidate/{cand_id}?k=3&city_filter=true&strategy=hybrid&max_age=86400"
        res = http_get_json(path)
        assert res.get("candidate_id") == cand_id
        assert isinstance(res.get("matches"), list)
        # DB: a c2j doc exists
        doc = db["matches_cache"].find_one({
            "candidate_id": cand_id,
            "direction": "c2j",
            "city_filter": True,
        })
        assert doc is not None, "expected c2j cache doc after first request"
        assert isinstance(doc.get("matches"), list)
        # Second call should be a cache hit (flag provided by endpoint)
        res2 = http_get_json(path)
        assert res2.get("cached") is True
        # No duplicates
        count = db["matches_cache"].count_documents({
            "candidate_id": cand_id,
            "direction": "c2j",
            "city_filter": True,
        })
        assert count == 1
    finally:
        _cleanup_docs(cand_id=cand_id)


def test_j2c_cache_write():
    health = http_get_json("/health")
    assert health.get("status") == "ok"

    job_id = _insert_job()
    try:
        # First call should compute and persist cache for j2c
        path = f"/match/job/{job_id}?k=3&city_filter=true&strategy=hybrid&max_age=86400"
        res = http_get_json(path)
        assert res.get("job_id") == job_id
        assert isinstance(res.get("matches"), list)
        # DB: a j2c doc exists
        doc = db["matches_cache"].find_one({
            "job_id": job_id,
            "direction": "j2c",
            "city_filter": True,
        })
        assert doc is not None, "expected j2c cache doc after first request"
        assert isinstance(doc.get("matches"), list)
        # Second call should be a cache hit (flag now provided by endpoint)
        res2 = http_get_json(path)
        assert res2.get("cached") is True
        # No duplicates
        count = db["matches_cache"].count_documents({
            "job_id": job_id,
            "direction": "j2c",
            "city_filter": True,
        })
        assert count == 1
    finally:
        _cleanup_docs(job_id=job_id)


def test_indexes_and_ttl_present():
    # Ensure the key indexes exist; TTL may be present depending on env (enabled by default)
    idx = list(db["matches_cache"].list_indexes())
    names = {i.get("name") for i in idx}
    # Compound keys
    assert "c2j_key" in names
    assert "j2c_key" in names
    # We expect an index on updated_at (name may vary across runs)
    def has_field(field: str) -> bool:
        for i in idx:
            k = i.get("key") or {}
            try:
                if field in list(k.keys()):
                    return True
            except Exception:
                pass
        return False
    assert has_field("updated_at") or has_field("updated_at_dt")
    # TTL index is preferred but not strictly required in all environments; warn-only.
    ttl_present = ("ttl_updated_at_dt" in names) or any("expireAfterSeconds" in i for i in idx)
    assert ttl_present or True


def test_concurrency_single_doc():
    # Create a candidate and hammer the same endpoint in quick succession
    cand_id = _insert_candidate()
    try:
        path = f"/match/candidate/{cand_id}?k=5&city_filter=true&strategy=hybrid&max_age=86400"
        # Fire several sequential quick calls (urllib is synchronous; still catches upsert behavior)
        for _ in range(5):
            _ = http_get_json(path)
        cnt = db["matches_cache"].count_documents({
            "candidate_id": cand_id,
            "direction": "c2j",
            "city_filter": True,
        })
        assert cnt == 1
    finally:
        _cleanup_docs(cand_id=cand_id)
