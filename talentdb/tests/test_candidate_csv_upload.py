import io, csv, time
from fastapi.testclient import TestClient
from scripts.api import app
from scripts.tenants import create_tenant, create_api_key
from scripts.ingest_agent import db

client = TestClient(app)

def _make_csv():
    rows = [
        [
            'מספר מועמד ‏(מועמד) ‏(מועמד)', 'מועמד', 'מספר הזמנה ‏(הזמנה) ‏(הזמנת שירות)', 'השכלה ‏(מועמד) ‏(מועמד)',
            'נסיון ‏(מועמד) ‏(מועמד)', 'טלפון', 'מייל', 'עיר'
        ],
        [
            '2731955', 'רות כהן', '408330', 'השכלה\nאוניברסיטה 2018-2021 | BA',
            'ניסיון\nתפקיד א 2021-2024', '0521234567', 'ruth@example.com', 'תל אביב'
        ]
    ]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerows(rows)
    return buf.getvalue().encode('utf-8')

def test_upload_candidate_csv(monkeypatch):
    # bootstrap a tenant and key
    with TestClient(app) as c:
        tenant_id = create_tenant('test_agency')
        key_rec = create_api_key(tenant_id, name='test')
        api_key = key_rec['key']

        # stub ingest to avoid external LLM and to insert a candidate into Mongo
        def fake_ingest_files(paths, kind='candidate', force_llm=True):
            doc = {
                'tenant_id': tenant_id,
                'full_name': 'רות כהן',
                'title': 'Candidate',
                'share_id': f"share_{int(time.time()*1_000_000)}",
                'city_canonical': 'תל אביב',
                'skill_set': [],
                'updated_at': int(time.time()),
                '_src_hash': f"testhash-{int(time.time()*1_000_000)}",
            }
            ins = db['candidates'].insert_one(doc)
            doc['_id'] = ins.inserted_id
            return [doc]

        monkeypatch.setattr('scripts.routers_candidates.ingest_files', fake_ingest_files)

        csv_bytes = _make_csv()
        files = {'files': ('cands.csv', csv_bytes, 'text/csv')}
        r = c.post('/tenant/candidates/upload', headers={'X-API-Key': api_key}, files=files)
        assert r.status_code == 200, r.text
        js = r.json()
        assert js['count'] >= 1
        assert js['created'] >= 1
        # verify candidates listing shows our tenant data
        r2 = c.get('/tenant/candidates', headers={'X-API-Key': api_key})
        assert r2.status_code == 200
        jj = r2.json()
        assert jj['total'] >= 1
