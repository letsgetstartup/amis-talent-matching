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

@lru_cache(maxsize=1)
def get_db():
    uri=os.getenv("MONGO_URI","mongodb://localhost:27017")
    db_name=os.getenv("DB_NAME","talent_match")
    client=MongoClient(uri, serverSelectionTimeoutMS=800)
    client.admin.command("ping")
    return client[db_name]

def is_mock() -> bool:
    return False
