# Candidate CSV Import

This module imports candidates from CSV into Mongo, normalizes headers (Hebrew/English), composes a clean text blob, and ingests via the shared pipeline.

## Usage

```
python -m talentdb.scripts.import_candidates_csv <csv_path> [--tenant TENANT_ID] [--max-rows N]
```

## Mapping

Header mapping is config-driven at `talentdb/scripts/mappings/candidate_csv_mapping.json` and extended by shared heuristics in `scripts/header_mapping.py`.

## Tests

Run the smoke tests:

```
pytest -q tests/test_candidate_*.py
```

Ensure Mongo is reachable via `MONGO_URI`.
