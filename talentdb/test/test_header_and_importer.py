import io, csv, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.header_mapping import canon_header


def test_canon_header_maps_order_id_variants():
    assert canon_header('מספר הזמנה', kind='job') == 'order_id'
    assert canon_header('מספר הזמנה (הזמנה)', kind='job') == 'order_id'
    assert canon_header('מספר הזמנה (הזמנת שירות)', kind='job') == 'order_id'
    assert canon_header('מספר משרה', kind='job') == 'order_id'


def test_dictreader_normalization():
    # Simulate small CSV and ensure header mapping picks canonical keys
    from scripts import import_csv_enriched as imp
    data = 'מספר משרה,שם משרה,תאור תפקיד\n123,כותרת,טקסט\n,ריק,ללא מזהה\n'
    f = io.StringIO(data)
    rdr = csv.DictReader(f)
    fmap = imp._normalize_headers(rdr)
    assert fmap.get('מספר משרה') == 'order_id'
