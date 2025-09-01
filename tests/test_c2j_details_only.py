from fastapi.testclient import TestClient
from talentdb.scripts.api import app
import json

client = TestClient(app)


def _find_ui_blocks(ui, kind):
    return [b for b in (ui or []) if isinstance(b, dict) and b.get('kind') == kind]


def _any_candidate_id():
    try:
        from talentdb.scripts.ingest_agent import db  # type: ignore
        d = db['candidates'].find_one({}, {'_id': 1})
        return str(d['_id']) if d and d.get('_id') else None
    except Exception:
        return None


def test_c2j_details_only_nonstream_returns_matchlist_when_results():
    cand_id = _any_candidate_id()
    if not cand_id:
        import pytest
        pytest.skip('no candidate docs')

    r = client.post('/chat/query', json={"question": cand_id, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    # Never show Table in detailsOnly
    assert not _find_ui_blocks(ui, 'Table')
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results'


def test_c2j_details_only_stream_returns_matchlist_when_results():
    cand_id = _any_candidate_id()
    if not cand_id:
        import pytest
        pytest.skip('no candidate docs')

    r = client.post('/chat/query?stream=1', json={"question": cand_id, "detailsOnly": True})
    assert r.status_code == 200
    lines = [ln for ln in (r.text or '').splitlines() if ln.strip()]
    env = None
    for ln in reversed(lines):
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        if isinstance(obj, dict) and (obj.get('type') == 'assistant_ui'):
            env = obj
            break
    assert env is not None, 'assistant_ui envelope not found in stream'
    ui = env.get('ui') or []
    assert not _find_ui_blocks(ui, 'Table')
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (stream)'
