# Upload API – Jobs & Candidates

All requests require headers:
- `X-API-Key: <key>` (if enabled)
- `X-Tenant-Id: <tenant-id>`

## Jobs

- POST `/jobs/batch_csv`
  - multipart/form-data: `file` (.csv/.xlsx/.xlsm)
  - Query: `upsert=true|false`
  - Response: `{ summary, results[] }`

- Required CSV headers (Hebrew or English aliases):
  - כותרת משרה|job_title -> title
  - תיאור משרה|job_description -> job_description
  - עיר|city -> city (canonicalization + coords derived)
  - מזהה משרה חיצוני|external_job_id -> external_job_id
  - Optional: מקצוע נדרש, תחום עיסוק, כישורי חובה, כישורי יתרון, סוג העסקה, שכר, אימייל איש קשר, מיקום חובה

## Candidates

- POST `/tenant/candidates/upload`
  - multipart/form-data: `files[]` (.pdf/.docx/.txt/.csv)
  - CSV headers include: שם מלא, אימייל, טלפון, עיר, מקצוע מבוקש, תחום עיסוק, שנות ניסיון, השכלה, כישורים, ניסיון תעסוקתי, הערות (Notes/Notes_candidate), מזהה חיצוני מועמד, מספר משרה שהוגשה (apply_job_number), מזהה משרה שהוגשה (apply_job_id)
  - Response: `{ uploaded[], count, created, updated, duplicates, errors }`
  - Behavior:
    - CSV rows become candidate records via LLM extraction.
    - If `apply_job_number` or `apply_job_id` provided, the system enriches the candidate from the job (must/nice skills, location preference) and records an application in `applications`.

## Apply API (mobile/web)

- POST `/apply`
  - JSON: `{ share_id, job_id }`
  - Enriches candidate similarly and writes to `applications`.

