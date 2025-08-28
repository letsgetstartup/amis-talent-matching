import json
from fastapi.testclient import TestClient
from scripts.api import app, _PITCH_ALLOWED_TONES
from scripts.ingest_agent import db

client = TestClient(app)

class DummyChoice:  # minimal shape for mock
    def __init__(self, content):
        self.message = type('m', (), {'content': content})

class DummyComp:
    def __init__(self, content):
        self.choices = [DummyChoice(content)]

def _install_mock(payload: dict):
    """Monkeypatch the OpenAI chat completions call path used in generate_pitch."""
    import scripts.ingest_agent as ia
    class MockChat:
        class completions:  # match attribute access chain
            @staticmethod
            def create(model, messages, temperature=0.0, max_tokens=0, **kwargs):  # accept extra kwargs (timeout, response_format)
                return DummyComp(json.dumps(payload))
    class MockClient:
        chat = MockChat()
    ia._openai_client = MockClient()
    ia._OPENAI_AVAILABLE = True

def test_pitch_basic_generation():
    cand = db['candidates'].find_one()
    assert cand, 'Need at least one candidate in test DB'
    jobs = list(db['jobs'].find().limit(2))
    assert jobs, 'Need at least one job in test DB'
    job_ids = [str(j['_id']) for j in jobs]
    per_job_points = []
    for j in jobs:
        per_job_points.append({
            'job_id': str(j['_id']),
            'title': j.get('title') or 'Job',
            'fit_points': ['Strong relevant experience', 'Key matching skill demonstrated']
        })
    mock_json = {
        'intro': 'Intro paragraph highlighting candidate strengths.',
        'job_fit_summary': 'Summary referencing combined suitability.',
        'per_job_points': per_job_points,
        'differentiators': ['Unique trait 1','Unique trait 2'],
        'call_to_action': 'Looking forward to discussing next steps.',
        'candidate_message': 'Personal encouragement and confidence boosting note.',
        'improvement_suggestions': [
            {'skill': 'Data Analysis', 'action': 'Complete an advanced Excel or BI mini-project.'}
        ],
    }
    def wc(s: str):
        return len([w for w in (s or '').split() if w])
    total = wc(mock_json['intro']) + wc(mock_json['job_fit_summary']) + wc(mock_json['call_to_action'])
    for it in mock_json['per_job_points']:
        for fp in it['fit_points'][:6]:
            total += wc(fp)
    for d in mock_json['differentiators'][:10]:
        total += wc(d)
    mock_json['word_count'] = total
    _install_mock(mock_json)
    tone = list(_PITCH_ALLOWED_TONES)[0]
    r = client.post('/pitch', json={'share_id': cand['share_id'], 'job_ids': job_ids, 'tone': tone})
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get('pitch', {}).get('word_count') == total
    assert data['pitch']['candidate_message']
    assert data['pitch']['improvement_suggestions']
    r2 = client.post('/pitch', json={'share_id': cand['share_id'], 'job_ids': job_ids, 'tone': tone})
    assert r2.status_code == 200
    assert r2.json().get('cached') is True

def test_pitch_rejects_missing_candidate_message():
    cand = db['candidates'].find_one()
    jobs = list(db['jobs'].find().limit(1))
    assert cand and jobs
    job_ids = [str(j['_id']) for j in jobs]
    per_job_points = [{
        'job_id': job_ids[0],
        'title': jobs[0].get('title') or 'Job',
        'fit_points': ['Strong relevant experience']
    }]
    bad_json = {
        'intro': 'Intro text',
        'job_fit_summary': 'Summary',
        'per_job_points': per_job_points,
        'differentiators': ['Edge'],
        'call_to_action': 'CTA here',
        # candidate_message intentionally omitted
        'improvement_suggestions': [{'skill':'Skill','action':'Do thing'}],
    }
    def wc(s: str): return len([w for w in (s or '').split() if w])
    total = wc(bad_json['intro']) + wc(bad_json['job_fit_summary']) + wc(bad_json['call_to_action'])
    for it in bad_json['per_job_points']:
        for fp in it['fit_points'][:6]: total += wc(fp)
    for d in bad_json['differentiators'][:10]: total += wc(d)
    bad_json['word_count'] = total
    _install_mock(bad_json)
    tone = list(_PITCH_ALLOWED_TONES)[0]
    r = client.post('/pitch', json={'share_id': cand['share_id'], 'job_ids': job_ids, 'tone': tone, 'force': True})
    assert r.status_code == 500, r.text
    assert 'invalid pitch structure' in r.text

def test_personal_letter_generation():
    """Test the new personal letter endpoint."""
    cand = db['candidates'].find_one()
    assert cand, 'Need at least one candidate in test DB'
    
    # Mock letter response matching our schema
    mock_letter = {
        'letter_content': 'שלום הקורות חיים שלך מציגים פרופיל מרשים זיהיתי התאמה מעולה למספר משרות בתחום המזכירות הרפואית',
        'key_strengths': ['ניסיון בתחום הרפואי', 'כישורי מחשב מתקדמים', 'יכולת עבודה בצוות'],
        'market_positioning': 'השוק זקוק לאנשי מקצוע עם הכישורים שלך',
        'confidence_boost': 'המשך בביטחון יש לך הרבה מה להציע',
        'next_steps': ['הגש מועמדות למשרות המתאימות', 'הכן עצמך לראיונות'],
        'word_count': 15
    }
    
    # Install mock for letter generation
    _install_mock(mock_letter)
    
    # Test personal letter generation
    r = client.post('/personal-letter', json={'share_id': cand['share_id'], 'force': True})
    assert r.status_code == 200, r.text
    data = r.json()
    assert 'letter' in data
    assert data['letter']['letter_content']
    assert len(data['letter']['key_strengths']) >= 3
    assert len(data['letter']['next_steps']) >= 2
    
    # Test retrieval endpoint
    r2 = client.get(f"/personal-letter/{cand['share_id']}")
    assert r2.status_code == 200
    letter_data = r2.json()
    assert letter_data['letter']['letter_content'] == mock_letter['letter_content']
