import os
import json
import time
import requests

BASE = os.getenv('API_BASE', 'http://127.0.0.1:8000')


def post_chat(question: str):
    r = requests.post(f"{BASE}/chat/query", json={"question": question, "detailsOnly": False}, timeout=60)
    r.raise_for_status()
    return r.json()


def assert_envelope(d):
    assert isinstance(d, dict)
    assert d.get('type') == 'assistant_ui'
    assert 'ui' in d and isinstance(d['ui'], list)
    assert '```' not in (d.get('narration') or '')
    # All components must have kind and id
    for i, comp in enumerate(d['ui']):
        assert isinstance(comp, dict), f"ui[{i}] not a dict"
        assert 'kind' in comp, f"ui[{i}] missing kind"
        assert comp.get('id'), f"ui[{i}] missing id"
        if 'type' in comp:
            assert comp['type'] == comp['kind'], f"ui[{i}] has conflicting type vs kind"


def test_jobs_query_table():
    d = post_chat('איפה יש משרות?')
    assert_envelope(d)
    kinds = [c.get('kind') for c in d.get('ui', [])]
    assert 'Table' in kinds


def test_job_details_query_no_codeblocks():
    d = post_chat('תציג משרה מפתח תקווה')
    assert_envelope(d)
    # Allow either JobDetails or Table fallback
    kinds = [c.get('kind') for c in d.get('ui', [])]
    assert any(k in ('JobDetails', 'Table') for k in kinds)
