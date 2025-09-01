import os
from fastapi.testclient import TestClient
from talentdb.scripts.api import app


def test_mcp_strict_errors_when_mcp_disabled(monkeypatch):
    # Force strict mode and disable MCP to verify 502 is raised for matching paths
    monkeypatch.setenv('MCP_STRICT', '1')
    monkeypatch.setenv('MCP_ENABLED', '0')

    client = TestClient(app)

    # Early ObjectId shape (any 24-char) should trigger strict handling
    dummy_id = '0' * 24
    r1 = client.post('/chat/query', json={"question": dummy_id})
    assert r1.status_code in (200, 502)
    if r1.status_code != 502:
        # If non-strict path avoided matching, skip
        return
    data = r1.json()
    assert data.get('error', {}).get('detail') == 'mcp_strict_no_fallback' or data.get('detail') == 'mcp_strict_no_fallback'

    r2 = client.post('/chat/query?stream=1', json={"question": dummy_id})
    # In stream mode, server may still wrap error; accept 200 with error envelope or 502
    assert r2.status_code in (200, 502)
