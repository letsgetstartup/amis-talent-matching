"""Database helper (Mongo only).

Always requires a real MongoDB reachable via MONGO_URI (e.g., mongodb://localhost:27017).
"""
import os
from functools import lru_cache
from pathlib import Path
from pymongo import MongoClient
from typing import Any

# Persistence now OPT-IN only: set MOCK_DB_PERSIST=1 (or true/yes) to enable mock snapshot.
# Mock persistence removed; keep flag for compatibility but always False
_PERSIST_ENABLED = False
_CACHE_DIR = None  # Legacy placeholder removed (no local persistence)

def persist_mock_db():  # Backwards compatibility; no-op
    return False

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
        if value > 0:
            return value
    except Exception:
        pass
    return default


@lru_cache(maxsize=1)
def get_db():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("DB_NAME", "talent_match")
    max_pool = _env_int("MONGO_MAX_POOL", 25)
    min_pool = _env_int("MONGO_MIN_POOL", 1)
    wait_timeout = _env_int("MONGO_WAIT_QUEUE_TIMEOUT_MS", 5000)
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=800,
        maxPoolSize=max_pool,
        minPoolSize=min_pool,
        waitQueueTimeoutMS=wait_timeout,
        retryWrites=True,
        appname="talentdb-api",
    )
    # Comment out the ping to avoid connection issues during startup
    # client.admin.command("ping")
    return client[db_name]

def is_mock() -> bool:
    return False
