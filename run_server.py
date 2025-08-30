#!/usr/bin/env python3
"""
Entrypoint to run the API server with best practices:
 - No auto-reload by default (prevents mid-request interruptions)
 - Optional reload for local/dev via env flags
 - Restricted reload watchers (dirs/excludes) to avoid noisy restarts

Notes:
 - Use the fully-qualified app path "talentdb.scripts.api:app" so we don't
     mutate sys.path.
 - Load .env automatically if python-dotenv is available.
"""
import os

# Load dotenv lazily if installed (optional, non-fatal)
try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
except Exception:
        pass

# Feature flag for MCP integration
os.environ["MCP_ENABLED"] = os.getenv("MCP_ENABLED", "0")

ROOT_DIR = os.path.dirname(__file__)


def main() -> None:
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log_level = os.getenv("LOG_LEVEL", "info")

    # Reload is OFF by default. Enable only if explicitly requested via UVICORN_RELOAD=1
    # to avoid accidental reloads from ambient envs.
    reload_flag = os.getenv("UVICORN_RELOAD") == "1"

    if reload_flag:
        # Restrict reload to the backend package to avoid noisy restarts
        reload_dirs = [os.path.join(ROOT_DIR, "talentdb")]
        # Exclude noisy or large dirs/files from triggering reloads
        reload_excludes = [
            "server.out",
            "*.log",
            "mongo_backups",
            "frontend",
            "docs",
            "backup",
            "*.csv",
            "*.json",
            "talentdb/__pycache__",
        ]
        config = uvicorn.Config(
            "talentdb.scripts.api:app",
            host=host,
            port=port,
            log_level=log_level,
            reload=True,
            reload_dirs=reload_dirs,
            reload_excludes=reload_excludes,
        )
    else:
        # Note: uvicorn.Config no longer accepts 'workers' in >=0.30.
        # We'll run a single process here; use Gunicorn in production for multi-workers.
        config = uvicorn.Config(
            "talentdb.scripts.api:app", host=host, port=port, log_level=log_level, reload=False
        )

    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    main()
