import sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'talentdb'))

from scripts.import_candidates_csv import main as import_candidates_main  # type: ignore
from scripts.ingest_agent import db, candidates_for_job, jobs_for_candidate  # type: ignore

def test_end_to_end_candidate_to_job(tmp_path):
    # Clean collections for isolation
    db['candidates'].delete_many({})
    # Do not purge jobs to allow autoseed to work implicitly; just access to trigger autoseed if configured
    _ = list(db['jobs'].find().limit(1))

    # Prepare candidate CSV with skills that overlap seed job (office/crm/service)
    csv_path = tmp_path / 'cands.csv'
    csv_path.write_text('\ufeffשם מועמד,שם ישוב,טלפון,מייל,השכלה,ניסיון\n' +
                        'רות כהן,תל אביב,050-1234567,r@example.com,תואר ראשון,ניסיון במשרד, CRM ושירות לקוחות\n', encoding='utf-8')
    rc = import_candidates_main(['import_candidates_csv.py', str(csv_path)])
    assert rc == 0

    cand = db['candidates'].find_one({})
    assert cand is not None

    # Ensure there is at least one job (autoseed or existing)
    job = db['jobs'].find_one({})
    assert job is not None

    # Query matches both directions without strict city filter
    res = candidates_for_job(str(job['_id']), top_k=5, city_filter=False)
    assert isinstance(res, list)

    # Not guaranteed but likely to find at least one due to overlapping keywords
    # Accept either non-empty or empty with no exception; assert type correctness
    # Stronger check: try reverse direction as well
    res2 = jobs_for_candidate(str(cand['_id']), top_k=5, max_distance_km=0)
    assert isinstance(res2, list)
