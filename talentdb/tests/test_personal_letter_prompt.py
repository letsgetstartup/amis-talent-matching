from fastapi.testclient import TestClient
from scripts.api import app, _DEBUG_LAST_LETTER_PROMPT
from scripts.ingest_agent import ingest_file, db
import pathlib, tempfile

client = TestClient(app)

def ensure_candidate():
    cand = db['candidates'].find_one()
    if not cand:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        tmp.write(b'Title: Data Engineer\nCity: Haifa\nSkills: Python, SQL, Airflow, ETL, AWS, Spark')
        tmp.flush(); tmp.close()
        ingest_file(tmp.name, kind='candidate', force_llm=False)
    return db['candidates'].find_one()

def test_personal_letter_prompt_contains_candidate_fields():
    cand = ensure_candidate()
    # Mock letter output to short-circuit LLM
    from tests.test_pitch import _install_mock
    mock = {
        'letter_content': 'שלום מועמד מתאים מאוד',
        'key_strengths': ['A','B','C'],
        'market_positioning': 'value prop',
        'confidence_boost': 'keep going',
        'next_steps': ['step1','step2'],
        'word_count': 6
    }
    _install_mock(mock)
    r = client.post('/personal-letter', json={'share_id': cand['share_id'], 'force': True})
    assert r.status_code == 200, r.text
    # Fetch debug prompt
    dbg = client.get('/debug/last-letter-prompt').json()
    prompt = dbg.get('prompt','')
    assert cand.get('full_name','').split(' ')[0] in prompt or cand.get('title','') in prompt
    # Ensure skills appear (at least one)
    skill_set = cand.get('skill_set') or []
    if skill_set:
        assert any(sk in prompt for sk in skill_set[:5])


def test_personal_letter_missing_city_errors():
    # Create candidate without city to verify strict error (no fallback allowed)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    tmp.write(b'Title: Backend Developer\nSkills: Python, Django, REST, SQL')
    tmp.flush(); tmp.close()
    doc = ingest_file(tmp.name, kind='candidate', force_llm=False)
    # Remove city fields to simulate missing real data
    db['candidates'].update_one({'_src_hash': doc['_src_hash']}, {'$unset': {'city': '', 'city_canonical': ''}})
    from tests.test_pitch import _install_mock
    mock = {
        'letter_content': 'שלום מכתב',
        'key_strengths': ['X','Y','Z'],
        'market_positioning': 'market',
        'confidence_boost': 'boost',
        'next_steps': ['s1','s2'],
        'word_count': 5
    }
    _install_mock(mock)
    r = client.post('/personal-letter', json={'share_id': doc['share_id'], 'force': True})
    assert r.status_code == 400, r.text
    body = r.json()
    assert body.get('detail', {}).get('error') == 'missing_candidate_data'
    assert 'city' in body.get('detail', {}).get('missing', [])
