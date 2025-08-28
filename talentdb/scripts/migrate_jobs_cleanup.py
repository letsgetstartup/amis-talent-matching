#!/usr/bin/env python3
"""
Jobs Cleanup & Integrity Migration
---------------------------------
1) Quarantine or delete jobs with missing/empty external_order_id
2) Create a partial unique index on jobs.external_order_id when present and non-empty
"""
from __future__ import annotations
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ingest_agent import db  # type: ignore


def ensure_partial_unique_index():
    coll = db['jobs']
    name = 'uniq_external_order_id_nonempty'
    try:
        coll.create_index(
            [('external_order_id', 1)],
            name=name,
            unique=True,
            partialFilterExpression={
                'external_order_id': { '$type': 'string', '$ne': '' }
            }
        )
        return {'ok': True, 'index': name}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def quarantine_bad_jobs(mode: str = 'quarantine'):
    jobs = db['jobs']
    q = {'$or': [
        {'external_order_id': {'$exists': False}},
        {'external_order_id': None},
        {'external_order_id': ''},
    ]}
    bad = list(jobs.find(q))
    out = {'matched': len(bad), 'mode': mode, 'moved': 0, 'deleted': 0}
    if not bad:
        return out
    if mode == 'delete':
        res = jobs.delete_many({'_id': {'$in': [d['_id'] for d in bad]}})
        out['deleted'] = getattr(res, 'deleted_count', 0)
        return out
    # default: quarantine
    quarantine = db['jobs_quarantine']
    for d in bad:
        d['_quarantined_at'] = int(__import__('time').time())
    if bad:
        quarantine.insert_many(bad)
        jobs.delete_many({'_id': {'$in': [d['_id'] for d in bad]}})
        out['moved'] = len(bad)
    return out


def main():
    mode = 'quarantine'
    if len(sys.argv) > 1 and sys.argv[1] in ('quarantine', 'delete'):
        mode = sys.argv[1]
    res1 = quarantine_bad_jobs(mode=mode)
    res2 = ensure_partial_unique_index()
    print(json.dumps({'cleanup': res1, 'index': res2}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
