from fastapi.testclient import TestClient
from talentdb.scripts.api import app


client = TestClient(app)


def _any_job_id():
    try:
        from talentdb.scripts.ingest_agent import db  # type: ignore
        d = db['jobs'].find_one({}, {'_id': 1})
        return str(d['_id']) if d and d.get('_id') else None
    except Exception:
        return None


def test_matchlist_item_minimal_shape_when_results():
    jid = _any_job_id()
    if not jid:
        import pytest
        pytest.skip('no job docs')

    r = client.post('/chat/query', json={"question": jid, "detailsOnly": True})
    assert r.status_code == 200
    ui = (r.json() or {}).get('ui') or []
    ml = next((c for c in ui if isinstance(c, dict) and c.get('kind') == 'MatchList'), None)
    if not ml:
        return  # no results; acceptable
    items = ml.get('items') or []
    assert isinstance(items, list) and items, 'expected items when MatchList present'
    it = items[0]
    # Core fields
    for k in ['id', 'title', 'city', 'counters', 'parts', 'candidate_id', 'job_id']:
        assert k in it
    # Counters structure sanity
    counters = it.get('counters') or {}
    for sec in ['must', 'nice']:
        assert sec in counters and isinstance(counters[sec], dict)
        have = counters[sec].get('have')
        total = counters[sec].get('total')
        assert isinstance(have, int) and isinstance(total, int)
        assert 0 <= have <= max(total, 0)
    # Parts are list of label+pct
    for p in it.get('parts') or []:
        assert 'label' in p and 'pct' in p
        if p['pct'] is not None:
            assert 0 <= p['pct'] <= 100
