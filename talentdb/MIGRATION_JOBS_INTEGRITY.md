# Jobs Import Integrity Migration

## Purpose
Stabilize job import mapping and enforce data integrity for `external_order_id`.

## What this includes
- Unified header mapping (מספר משרה → order_id)
- Strict importer validation (skip rows missing order_id)
- Cleanup + partial unique index migration

## How to run

1) Cleanup corrupted documents and create index (quarantine by default)

```bash
python -m scripts.migrate_jobs_cleanup          # quarantine invalid jobs and create partial unique index
# Or delete directly (irreversible)
python -m scripts.migrate_jobs_cleanup delete
```

2) Re-run imports (safe and idempotent)

```bash
python scripts/import_csv_enriched.py path/to/jobs.csv
```

## Expected outcomes
- No documents with missing/empty `external_order_id`
- Unique constraint enforced when the field is present and non-empty
- Header variants normalized via shared mapping

## Rollback
- Quarantined docs are preserved in `jobs_quarantine`
- To restore: move back with manual verification
