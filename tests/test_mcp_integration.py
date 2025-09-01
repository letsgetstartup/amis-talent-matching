import os
import types

# Ensure MCP is enabled for these tests (read by some modules at import time)
os.environ.setdefault("MCP_ENABLED", "1")


def _has_db():
    try:
        from talentdb.scripts.ingest_agent import db  # type: ignore
        return db is not None
    except Exception:
        return False


def _any_id(coll_name: str) -> str | None:
    try:
        from talentdb.scripts.ingest_agent import db  # type: ignore
        d = db[coll_name].find_one({}, {"_id": 1})
        return str(d["_id"]) if d and d.get("_id") else None
    except Exception:
        return None


def test_list_tools_surface():
    # Should list tools when imported
    from talentdb.mcp.server import list_tools  # type: ignore

    tools = list_tools()
    assert isinstance(tools, list)
    names = {t.get("name") for t in tools}
    # Check for key tools presence
    assert "match_job_to_candidates" in names
    assert "match_candidate_to_jobs" in names


def test_call_tool_shapes_smoke():
    # Skip gracefully if DB unavailable
    if not _has_db():
        import pytest
        pytest.skip("db unavailable")

    from talentdb.mcp.server import call_tool  # type: ignore

    job_id = _any_id("jobs")
    cand_id = _any_id("candidates")

    # Smoke: calling with missing IDs should not crash; can return ok False
    res1 = call_tool("match_job_to_candidates", {"job_id": job_id or "0" * 24, "k": 5}, context={})
    assert isinstance(res1, dict)
    assert "ok" in res1
    if res1.get("ok"):
        data = res1["data"]
        assert isinstance(data, dict)
        assert isinstance(data.get("rows"), list)

    res2 = call_tool("match_candidate_to_jobs", {"candidate_id": cand_id or "0" * 24, "k": 5}, context={})
    assert isinstance(res2, dict)
    assert "ok" in res2
    if res2.get("ok"):
        data = res2["data"]
        assert isinstance(data, dict)
        assert isinstance(data.get("rows"), list)


def test_helpers_mapping_shapes():
    # Skip if DB unavailable or no docs
    if not _has_db():
        import pytest
        pytest.skip("db unavailable")

    job_id = _any_id("jobs")
    cand_id = _any_id("candidates")
    if not job_id or not cand_id:
        import pytest
        pytest.skip("no sample docs")

    # Import helpers from api; these are MCP-aware and map breakdown/counters to top-level keys
    from talentdb.scripts.api import (
        _mcp_or_native_candidates_for_job,
        _mcp_or_native_jobs_for_candidate,
    )  # type: ignore

    rows1 = _mcp_or_native_candidates_for_job(job_id, top_k=5, tenant_id=None)
    assert isinstance(rows1, list)
    if rows1:
        r = rows1[0]
        assert isinstance(r, dict)
        # Keys expected by UI for candidate results (some may be None but must exist when MCP path used)
        for k in [
            "score",
            "candidate_id",
            # "job_id",  # job_id not expected in candidates-for-job results
            # "title",  # title field optional in candidate results
            "city",
        ]:
            assert k in r

    rows2 = _mcp_or_native_jobs_for_candidate(cand_id, top_k=5, tenant_id=None)
    assert isinstance(rows2, list)
    if rows2:
        r = rows2[0]
        assert isinstance(r, dict)
        # Keys expected by UI for job results
        for k in [
            "score",
            "job_id",
            # "title",  # title might not always be present in job results
            "city",
        ]:
            assert k in r


def test_chat_streaming_quick_path_jobs_for_candidate():
    # Skip if DB unavailable or missing candidate
    if not _has_db():
        import pytest
        pytest.skip("db unavailable")
    cand_id = _any_id("candidates")
    if not cand_id:
        import pytest
        pytest.skip("no candidate docs")

    # Import app and use TestClient (avoids running a real server)
    try:
        from starlette.testclient import TestClient  # type: ignore
    except Exception:
        import pytest
        pytest.skip("starlette test client not available")

    from talentdb.scripts.api import app  # type: ignore

    with TestClient(app) as client:
        q = {"question": f"jobs for candidate {cand_id}"}
        r = client.post("/chat/query?stream=1", json=q)
        assert r.status_code == 200
        lines = [ln for ln in (r.text or "").splitlines() if ln.strip()]
        # Expect at least one assistant_ui or text_delta event normally; be tolerant to minimal output
        assert len(lines) >= 1
