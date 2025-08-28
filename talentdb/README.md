# Talent DB Ingestion & Matching (Mongo Only)

Ingest candidate CVs & job descriptions, extract & normalize skills (LLM + heuristics), persist exclusively in MongoDB, and serve rich matching/search/explainability APIs. All runtime persistence uses Mongo; no JSON snapshot files or mock fallbacks. Vocab seed JSON can be auto-removed after bootstrap.

## Quick Start
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"
export DB_NAME="talent_match"
# optional
export OPENAI_API_KEY=sk-...   # enable LLM extraction
export API_KEY=changeme        # protect maintenance endpoints
python -m uvicorn scripts.api:app --host 127.0.0.1 --port 8000
```

Launcher with Mongo wait loop:
```bash
./run_api.sh
```

Docker Compose (Mongo + API + healthchecks):
```bash
docker compose up --build
```

## Email (Gmail SMTP) for Application Confirmations
To enable sending confirmation emails (with a generated PDF CV attachment) from the portal flow, set:

```bash
export GMAIL_USER="your.address@gmail.com"
export GMAIL_APP_PASSWORD="xxxx xxxx xxxx xxxx"  # Gmail App Password (not your login password)
export MAIL_FROM="Recruiting <your.address@gmail.com>"  # optional
```

Endpoint: `POST /confirm/apply` (requires `X-API-Key`) with JSON `{share_id, job_id}`.
Recipient priority: tenant admin email, else the job's `agency_email`.
Subject format: `#SCAGENT# <external_job_id or job_id>`.

## Structure
| Path | Purpose |
|------|---------|
| scripts/ | API + ingestion & matching logic |
| prompts/ | LLM extraction prompt templates |
| vocab/ | Seed vocab JSON (removed if STRICT_MONGO_VOCAB=1) |
| samples/ | Example CVs & jobs |
| tests/ | Pytest suite |
| run_api.sh | Launcher ensuring Mongo readiness |

## MongoDB
Mongo is mandatory. Quick start:
```bash
docker run -d --name talent-mongo -p 27017:27017 -v "$(pwd)/mongo:/data/db" mongo:7.0
```

## Matching Usage
python scripts/ingest_agent.py candidate <file(s)>  # uses OpenAI if key present
python scripts/ingest_agent.py job <file(s)>
Add --no-llm to force heuristic parser.

Use functions in `scripts.ingest_agent` for programmatic access:
- candidates_for_job(job_id, top_k)
- jobs_for_candidate(candidate_id, top_k)

### Composite Scoring & Categories
Composite score:
```
score = skill_w * weighted_skill_component + title_w * title_sim [+ semantic_w * semantic_sim + embed_w * embedding_sim + distance_w * distance_score]
```
Default (env overridable) weights: skill_w=0.85, title_w=0.15, semantic_w=0.0, embed_w=0.0, distance_w=0.0.

Weighted skill component supports MUST vs NEEDED categories (if `skills_detailed` present) with separate category weights (default must=0.7 needed=0.3) adjustable via `POST /config/category_weights`.

Distance component (optional): inverse distance weighting if `WEIGHT_DISTANCE` > 0 or set via `POST /config/distance_weight`. Full score (1.0) within 5km, linear decay to 0 at 150km. When distance weight > 0 the strict same-city filter is softened (documents from other cities are scored instead of filtered out). Returned metrics: `distance_km`, `distance_score`.

Base skill overlap = |A ∩ B| / max(|A|,|B|). Title similarity via RapidFuzz partial ratio.

### ESCO & Synthetic Skills
Ingestion maps canonical skills to ESCO subset loaded from Mongo (`_vocab_esco_skills`). With OpenAI enabled it can propose synthetic ESCO-aligned skills (source=synthetic). All skills live in `skills_detailed` (name, esco_id, label, category, source).

### Location Filtering
City names are normalized (`city_canonical`) using `city_coordinate.txt`. Matching endpoints accept `city_filter` (default true) to restrict matches to same canonical city when both sides have a city.

If distance weighting is enabled (distance_weight > 0) and `city_filter=true`, cross-city records are still considered with a distance penalty instead of being outright excluded.

### Explainability
Endpoint: `GET /match/explain/{candidate_id}/{job_id}` returns:
- composite score & component weights
- raw skill_overlap, base_skill_overlap
- must_ratio, needed_ratio, weighted_skill_score (after category weighting)
- candidate_only_skills, job_only_skills
- title_similarity, semantic_similarity, embedding_similarity
- distance_km, distance_score (if coordinates available)
- effective weights including must/needed category weights and distance_weight

### Runtime Weight & Category Adjustment / Maintenance API
Endpoints (POST unless noted):
 - GET  /config/weights                        -> current weights (skills,title,semantic,embedding,category)
 - POST /config/weights {skill_weight,title_weight[,semantic_weight][,embed_weight]}
 - POST /config/category_weights {must_weight, needed_weight}
 - POST /config/distance_weight {distance_weight}
 - POST /config/min_skill_floor {min_skill_floor}
 - POST /maintenance/recompute                -> recompute materialized skill_set
 - POST /maintenance/recompute_embeddings     -> recompute hash embeddings
 - POST /maintenance/backfill_esco            -> rebuild ESCO mapping array
 - POST /maintenance/refresh/{kind}?use_llm=false  (kind=candidate|job) reprocess source files
 - POST /maintenance/clear_cache              -> clear extraction cache
 - POST /maintenance/skill {canon,synonym}    -> add synonym to local vocab
 - GET  /meta                                 -> meta instrumentation timestamps

### Extraction Cache
In-memory only (no disk). Clear via `POST /maintenance/clear_cache`.

## Fallback Behavior
No fallback. If Mongo is unreachable the service is not ready (`/ready` returns failure).

## Secret Handling Best Practices
1. Never commit `.env` (already ignored). Use `.env.example` as the template.
2. Set secrets in your shell or CI environment variables: `export OPENAI_API_KEY=...`.
3. Validate presence without exposing values: run the "Check Env" task or `python scripts/check_env.py`.
4. Rotate keys regularly; treat leaked keys as compromised and revoke.
5. For production, use a secrets manager (e.g. AWS Secrets Manager, GCP Secret Manager, Vault) and inject at runtime.

## API Key Protection (optional)
Set environment variable `API_KEY=yoursharedsecret` before starting the API. Then send header `X-API-Key: yoursharedsecret` for any maintenance/config endpoints:
 - POST /config/weights
 - POST /maintenance/recompute
 - POST /maintenance/refresh/{kind}
 - POST /maintenance/clear_cache
If `API_KEY` is unset, endpoints remain open (development mode).

## Docker
Build multi-stage image (includes healthcheck):
```bash
docker build -t talentdb .
```
Run with host Mongo:
```bash
docker run --rm -e MONGO_URI=mongodb://host.docker.internal:27017 -p 8000:8000 talentdb
```
With API key:
```bash
docker run --rm -e API_KEY=secret -e MONGO_URI=mongodb://host.docker.internal:27017 -p 8000:8000 talentdb
```

### docker-compose
```bash
docker compose up --build
```
Override example:
```bash
API_KEY=mysecret WEIGHT_SEMANTIC=0.1 docker compose up --build
```

## Pagination
List endpoints support pagination:
 GET /candidates?skip=0&limit=50
 GET /jobs?skip=100&limit=50
Responses include: items list plus skip, limit, total.

## Mongo Express (Browser DB View)
Optional lightweight UI to inspect Mongo collections in the browser.

Install (already added as dev dependency):
```bash
npm install
```
Run:
```bash
npm run mongo-express
```
Then open: http://localhost:8081

Default basic auth (override via env): admin / admin

Environment overrides:
```bash
export ME_CONFIG_MONGODB_SERVER=localhost
export ME_CONFIG_MONGODB_PORT=27017
export ME_CONFIG_BASICAUTH_USERNAME=me
export ME_CONFIG_BASICAUTH_PASSWORD=strongpass
npm run mongo-express
```
Config file: `mongo-express.config.js`

## Rate Limiting
In-memory IP-based fixed window. Default window 60s with limit RATE_LIMIT_PER_MIN (env var, default 60). Responses include headers:
 X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset (seconds until window reset). Exceeding returns HTTP 429.

## New Fields Summary
- skill_set: canonical list
- skills_detailed: list with category/source/esco metadata
- esco_skills: simplified ESCO mapping list
- synthetic_skills_generated: count per doc
- city_canonical: normalized city key
- embedding: deterministic hash vector (if enabled)
- distance_km, distance_score (per match result / explain output when distance weighting active)
- min_skill_floor (reported in /config/weights)

## Example Category Weight Update
```

### Distance Weight Update
```
curl -X POST -H "X-API-Key: $API_KEY" \
   -H 'Content-Type: application/json' \
   -d '{"distance_weight":0.2}' \
   http://localhost:8000/config/distance_weight
```

### Minimum Skill Floor Update
Ensures each ingested document has at least N skills (after extraction + synthetic enrichment) by adding inferred placeholders if needed.
```
curl -X POST -H "X-API-Key: $API_KEY" \
   -H 'Content-Type: application/json' \
   -d '{"min_skill_floor":5}' \
   http://localhost:8000/config/min_skill_floor
```
curl -X POST -H "X-API-Key: $API_KEY" \
   -H 'Content-Type: application/json' \
   -d '{"must_weight":0.8,"needed_weight":0.2}' \
   http://localhost:8000/config/category_weights
```

## Disable City Filter
```
GET /match/job/{job_id}?k=5&city_filter=false
```

## Backfill After Updating ESCO Mapping
If seed JSON retained (STRICT_MONGO_VOCAB=0) edit `vocab/esco_skills.json`; otherwise upsert directly into `_vocab_esco_skills`.
1. Update vocab (JSON or Mongo collection)
2. `POST /maintenance/backfill_esco`
3. Optional: `POST /maintenance/refresh/job?use_llm=true`
If seed files removed and you need to re-seed from JSON, restore files and set `STRICT_MONGO_VOCAB=0` temporarily.


## Search Endpoints
Advanced filtering over jobs and candidates.

Jobs: `GET /search/jobs`
Candidates: `GET /search/candidates`

Query Parameters:
- `skill` single skill (canonical) or `esco:ID`
- `skills` comma-separated list (mix plain + esco:)
- `city` raw city (normalized internally)
- `mode` any|all (default any; all requires each listed skill)
- `skip`, `limit` (limit <=100)
- `sort_by` matched|recent (matched sorts by number of matched skills desc)

Response:
```
{
   "results": [
      {"job_id": "...", "title": "...", "city": "tel_aviv", "matched_skills": [...], "matched_esco": [...], "updated_at": 1234567890}
   ],
   "returned": 1,
   "filtered_total": 10,
   "db_total": 62,
   "skip": 0,
   "limit": 5,
   "query": {"city": "tel_aviv", "skills": ["administration"], "mode": "any", "sort_by": "matched"}
}
```

Caching: in-memory LRU (~200 query variants). Repeat identical query returns cached payload.

Examples:
```
GET /search/jobs?skill=administration&limit=5
GET /search/candidates?skills=administration,customer_service&mode=all&sort_by=matched
GET /search/jobs?skills=esco:12345,customer_service&city=Tel%20Aviv&mode=any
```

