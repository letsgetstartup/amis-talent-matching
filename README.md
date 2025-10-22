# TalentDB API + MCP Integration

This service exposes a FastAPI server with optional MCP (Model Context Protocol) integration for recruiter copilot features.

## Quick start

1. Create and activate a virtualenv, then install requirements.
2. Copy `.env.example` to `.env` and adjust values.
3. Start the server:

- Normal mode:

```bash
python run_server.py
```

- MCP mode (feature-flag):

```bash
export MCP_ENABLED=1
./run_server_mcp.sh  # or: python run_server.py
```

4. Smoke checks:

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/mcp/health
curl -s http://127.0.0.1:8000/mcp/tools
```

## Notes
- Uvicorn workers are disabled in config (use a process manager like Gunicorn in prod if needed).
- `/health` is a liveness probe; `/ready` should verify DB connectivity.
- MCP is disabled by default; when enabled, API helpers try MCP first and gracefully fallback to native logic.
 - The root path `/` now serves the Agency Portal (`agency-portal.html`). The legacy path `/agency-portal.html` is still available for backward compatibility.

## Greenhouse redirect optimisation

When candidates reject positions via Greenhouse links we redirect them back to the public portal with filters pre-populated. To avoid over-filtering and improve job discovery, the redirect now:

- Computes skill popularity per tenant (cached for 1 hour).
- Selects only the top three most popular skills present on the rejected job.
- Preserves the job city filter when available.
- Supports both legacy `/portal/{slug}` and dynamic `/portal/dynamic/{slug}` portals via dedicated redirect endpoints.

You can force a cache refresh after bulk job imports by calling `talentdb.scripts.api.clear_skill_frequency_cache()` or restarting the service.
