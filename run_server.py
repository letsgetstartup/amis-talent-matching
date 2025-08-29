#!/usr/bin/env python3
"""
Entrypoint to run the API server with best practices:
 - No auto-reload by default (prevents mid-request interruptions)
 - Optional reload for local/dev via env flags
 - Restricted reload watchers (dirs/excludes) to avoid noisy restarts
"""
import os
import sys


ROOT_DIR = os.path.dirname(__file__)

# Ensure `talentdb` is importable (so `scripts.api:app` resolves)
talentdb_dir = os.path.join(ROOT_DIR, "talentdb")
if talentdb_dir not in sys.path:
    sys.path.insert(0, talentdb_dir)


def main() -> None:
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log_level = os.getenv("LOG_LEVEL", "info")

    # Reload is OFF by default. Enable only if explicitly requested via UVICORN_RELOAD=1
    # to avoid accidental reloads from ambient envs.
    reload_flag = os.getenv("UVICORN_RELOAD") == "1"

    if reload_flag:
        reload_dirs = [talentdb_dir]
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
            "scripts.api:app",
            host=host,
            port=port,
            log_level=log_level,
            reload=True,
            reload_dirs=reload_dirs,
            reload_excludes=reload_excludes,
        )
    else:
        workers = int(os.getenv("WORKERS", "1"))
        config = uvicorn.Config(
            "scripts.api:app", host=host, port=port, log_level=log_level, reload=False, workers=workers
        )

    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    main()
