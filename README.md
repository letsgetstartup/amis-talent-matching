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
