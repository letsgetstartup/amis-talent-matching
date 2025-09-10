# AI Matching Page (Standalone)

A lean, no-build static HTML page that lets recruiters:
- Paste a job description and extract indicative skills
- Find top candidates via MCP tools (match_job_to_candidates)
- Ask free‑form, grounded questions about Mongo data using `/chat/query?stream=1`

## URL
- Backend-served static: `http://localhost:8000/ai-matching.html`
- Alias: `http://localhost:8000/ai-matching`

## Requirements
- X-API-Key created via `/auth/apikey` (optional in dev when API_KEY unset)
- MCP enabled: `MCP_ENABLED=1`

## How it works
- Analyze Job: heuristic tokenization (no LLM dependency) to extract candidate skills
- Find Candidates: 
  1) `POST /mcp/call { name: 'search_jobs', { skills, k:1 } }`
  2) If a job found, `POST /mcp/call { name: 'match_job_to_candidates', { job_id, k:10 } }`
  3) Fallback to `search_candidates` when needed
- Ask the Data: `POST /chat/query?stream=1` with NDJSON streaming; UI appends chunks

## Notes
- Page uses only existing backend endpoints; no TS build needed
- Tenant context flows from X-API-Key
- Results capped at sensible limits to protect Mongo

## Troubleshooting
- 404: ensure frontend/public is mounted; server logs show searched_dirs
- 401 bad_api_key: create a key with `/auth/apikey`
- Empty results: verify data in Mongo and indexes
