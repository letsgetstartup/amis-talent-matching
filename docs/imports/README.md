# Imports and Matching – Contracts and Templates

- Jobs CSV template: `jobs-csv-template.he.csv`
- Candidates CSV template: `candidates-csv-template.he.csv`
- API usage: `api-upload.md`

Import expectations:
- Jobs: title, description, city are required. ESCO-normalized skills produced during enrichment.
- Candidates: full_name + (email or phone) + city required. Optional: notes (free text). `apply_job_number` or `apply_job_id` may be provided to enrich the profile and record an application.

Matching:
- Composite score of skills/title/semantic/embedding/distance with tenant isolation.
- Application affinity added via `apply_job` enrichment (skills and location preferences).
