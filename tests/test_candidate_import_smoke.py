import json, tempfile, os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'talentdb'))

from scripts.import_candidates_csv import main as import_candidates_main  # type: ignore

def test_smoke_minimal_csv(tmp_path):
    csv_path = tmp_path / 'cands.csv'
    csv_path.write_text('\ufeffשם מועמד,שם ישוב,טלפון,מייל,השכלה,ניסיון\n' +
                        'רות כהן,תל אביב,050-1234567,r@example.com,תואר ראשון,5 שנים אנליזה\n', encoding='utf-8')
    # Run importer
    rc = import_candidates_main(['import_candidates_csv.py', str(csv_path)])
    assert rc == 0
