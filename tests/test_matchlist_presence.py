from fastapi.testclient import TestClient
from talentdb.scripts.api import app
import json

client = TestClient(app)


def _find_ui_blocks(ui, kind):
    return [b for b in (ui or []) if isinstance(b, dict) and b.get('kind') == kind]


def test_details_only_nonstream_returns_matchlist_when_results():
    # Early ObjectId path; tolerant to empty DB. If results>0, expect MatchList; never a Table.
    job_id = "68ae892edc8b36d3dcc08ac3"
    r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results'


def test_details_only_stream_returns_matchlist_when_results():
    # Streamed early ObjectId path with detailsOnly. Parse NDJSON; if results>0 expect MatchList; never a Table.
    job_id = "68ae892edc8b36d3dcc08ac3"
    r = client.post('/chat/query?stream=1', json={"question": job_id, "detailsOnly": True})
    assert r.status_code == 200
    # NDJSON: last non-"done" line should contain the assistant_ui envelope
    lines = [ln for ln in (r.text or '').splitlines() if ln.strip()]
    # Find the last JSON object that has type assistant_ui
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
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (stream)'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (stream)'


def test_details_only_c2j_nonstream_returns_matchlist_when_results():
    # Candidate-to-jobs (c2j) early ObjectId path; tolerant to empty DB. If results>0, expect MatchList; never a Table.
    cand_id = "68ae892edc8b36d3dcc08ac4"  # Sample; adjust if needed
    r = client.post('/chat/query', json={"question": cand_id, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (c2j)'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (c2j)'


def test_details_only_c2j_stream_returns_matchlist_when_results():
    # Streamed c2j early ObjectId path with detailsOnly. Parse NDJSON; if results>0 expect MatchList; never a Table.
    cand_id = "68ae892edc8b36d3dcc08ac4"
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
    assert env is not None, 'assistant_ui envelope not found in stream (c2j)'
    ui = env.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (c2j stream)'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (c2j stream)'


def test_keyword_query_j2c_details_only():
    # Keyword query "מועמדים למשרה <ID>" should also respect detailsOnly mode
    job_id = "68ae892edc8b36d3dcc08ac3"
    question = f"מועמדים למשרה {job_id}"
    r = client.post('/chat/query', json={"question": question, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (keyword j2c)'


def test_keyword_query_c2j_details_only():
    # Keyword query "משרות למועמד <ID>" should also respect detailsOnly mode
    cand_id = "68ae892edc8b36d3dcc08ac4"
    question = f"משרות למועמד {cand_id}"
    r = client.post('/chat/query', json={"question": question, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (keyword c2j)'


def test_details_only_c2j_nonstream_returns_matchlist_when_results():
    # Candidate-to-jobs (c2j) early ObjectId path; tolerant to empty DB. If results>0, expect MatchList; never a Table.
    cand_id = "68ae892edc8b36d3dcc08ac4"  # Sample; adjust if needed
    r = client.post('/chat/query', json={"question": cand_id, "detailsOnly": True})
    assert r.status_code == 200
    data = r.json()
    ui = data.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (c2j)'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (c2j)'


def test_details_only_c2j_stream_returns_matchlist_when_results():
    # Streamed c2j early ObjectId path with detailsOnly. Parse NDJSON; if results>0 expect MatchList; never a Table.
    cand_id = "68ae892edc8b36d3dcc08ac4"
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
    assert env is not None, 'assistant_ui envelope not found in stream (c2j)'
    ui = env.get('ui') or []
    tables = _find_ui_blocks(ui, 'Table')
    assert len(tables) == 0, 'Table should be hidden in detailsOnly mode (c2j stream)'
    metric = next((b for b in ui if b.get('kind') == 'Metric' and b.get('id') == 'matches-kpi'), None)
    if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
        assert _find_ui_blocks(ui, 'MatchList'), 'Expected MatchList when there are results (c2j stream)'
