"""Centralized helpers for storing and retrieving candidate resumes via MongoDB GridFS.

This module wraps GridFS to avoid scattering file IO logic across routers.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from bson import ObjectId
from gridfs import GridFS, GridFSBucket

from ..db import get_db

_fs: Optional[GridFS] = None
_bucket: Optional[GridFSBucket] = None


def _get_gridfs() -> GridFS:
    global _fs
    if _fs is None:
        _fs = GridFS(get_db())
    return _fs


def _get_bucket() -> GridFSBucket:
    global _bucket
    if _bucket is None:
        _bucket = GridFSBucket(get_db())
    return _bucket


def save_resume(
    content: bytes,
    *,
    filename: str,
    content_type: str,
    tenant_id: str,
    candidate_id: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> ObjectId:
    """Persist a resume in GridFS and return the created file id."""
    fs = _get_gridfs()
    safe_metadata = {
        "tenant_id": tenant_id,
        "candidate_id": candidate_id,
        "uploaded_at": datetime.utcnow(),
    }
    if metadata:
        safe_metadata.update(metadata)
    return fs.put(
        content,
        filename=filename,
        content_type=content_type,
        metadata=safe_metadata,
    )


def delete_resume(file_id: ObjectId | str) -> None:
    fs = _get_gridfs()
    try:
        oid = ObjectId(str(file_id))
    except Exception:
        return
    if fs.exists(oid):
        fs.delete(oid)


def open_resume_stream(file_id: ObjectId | str):
    bucket = _get_bucket()
    try:
        oid = ObjectId(str(file_id))
    except Exception as exc:  # pragma: no cover - invalid ids are simply ignored
        raise FileNotFoundError("invalid_file_id") from exc
    return bucket.open_download_stream(oid)


def resume_exists(file_id: ObjectId | str) -> bool:
    fs = _get_gridfs()
    try:
        oid = ObjectId(str(file_id))
    except Exception:
        return False
    return fs.exists(oid)
