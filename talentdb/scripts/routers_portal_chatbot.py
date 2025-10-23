from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, List, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from .ingest_agent import db
from .services.portal_chatbot import (
    ChatbotResult,
    ChatbotService,
    FilterAction,
    PortalChatSeedStore,
    build_portal_context,
)

router = APIRouter(prefix="/portal/chat", tags=["portal-chat"])

_chatbot_service = ChatbotService()
_seed_store = PortalChatSeedStore()

CHATBOT_RATE_LIMIT_ANON = int(os.getenv("CHATBOT_RATE_LIMIT_ANONYMOUS", "20"))
CHATBOT_RATE_LIMIT_AUTH = int(os.getenv("CHATBOT_RATE_LIMIT_AUTHENTICATED", "100"))
CHATBOT_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("CHATBOT_RATE_LIMIT_WINDOW_MINUTES", "5")) * 60
_CHATBOT_COLLECTION = "portal_conversations"


class _RateLimiter:
    def __init__(self, window_seconds: int) -> None:
        self._window = window_seconds
        self._hits: Dict[str, List[float]] = {}
        self._lock = RLock()

    def allow(self, key: str, limit: int) -> bool:
        now = time.time()
        with self._lock:
            bucket = self._hits.setdefault(key, [])
            # Clean expired timestamps
            while bucket and bucket[0] <= now - self._window:
                bucket.pop(0)
            if len(bucket) >= limit:
                return False
            bucket.append(now)
            return True


_rate_limiter = _RateLimiter(CHATBOT_RATE_LIMIT_WINDOW_SECONDS)


class FilterActionModel(BaseModel):
    type: str
    filter_key: str
    value: Any


class ChatMessagePayload(BaseModel):
    message: str = Field(..., min_length=1, max_length=500)
    portal_slug: str = Field(..., min_length=1)
    conversation_id: Optional[str] = None
    current_filters: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None


class ChatResponsePayload(BaseModel):
    message: str
    conversation_id: str
    filters: Optional[List[FilterActionModel]] = None
    job_ids: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class ConversationMessage(BaseModel):
    role: str
    content: str
    timestamp: float
    filters: Optional[List[FilterActionModel]] = None
    job_ids: Optional[List[str]] = None


class ConversationHistoryResponse(BaseModel):
    conversation_id: str
    portal_slug: str
    messages: List[ConversationMessage]
    metadata: Optional[Dict[str, Any]] = None


class ConversationStarterResponse(BaseModel):
    starters: List[str]


class JobSuggestion(BaseModel):
    job_id: str
    title: Optional[str] = None
    company_name: Optional[str] = None
    location: Optional[str] = None


class PortalContextPayload(BaseModel):
    portal_slug: str = Field(..., min_length=1)


class ChatSeedResponse(BaseModel):
    portal_slug: str
    auto_message: str
    highlighted_job_ids: List[str] = []
    injected_user_message: Literal["find similar jobs for me"] = "find similar jobs for me"
    job_suggestions: List[JobSuggestion]
    metadata: Optional[Dict[str, Any]] = None


def _ensure_conversation_indexes() -> None:
    coll = db[_CHATBOT_COLLECTION]
    coll.create_index("portal_slug", name="portal_slug_lookup")
    coll.create_index("updated_at", name="updated_at_idx")
    coll.create_index("conversation_id", unique=True, name="conversation_id_unique")


_ensure_conversation_indexes()


def _make_rate_key(request: Request) -> str:
    client_host = request.client.host if request.client else "unknown"
    session_hint = request.headers.get("X-Session-Id") or ""
    return f"{client_host}:{session_hint}".strip(":")


async def enforce_rate_limit(request: Request) -> None:
    key = _make_rate_key(request)
    if not key:
        key = "anonymous"
    limit = CHATBOT_RATE_LIMIT_ANON
    if request.headers.get("Authorization"):
        limit = CHATBOT_RATE_LIMIT_AUTH
    allowed = _rate_limiter.allow(key, limit)
    if not allowed:
        raise HTTPException(status_code=429, detail="rate_limited")


def _load_conversation(conversation_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not conversation_id:
        return None
    doc = db[_CHATBOT_COLLECTION].find_one({"conversation_id": conversation_id})
    return doc


def _save_conversation(doc: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    doc["updated_at"] = now
    if "created_at" not in doc:
        doc["created_at"] = now
    db[_CHATBOT_COLLECTION].replace_one({"conversation_id": doc["conversation_id"]}, doc, upsert=True)


def _normalize_filter_actions(actions: Optional[List[FilterAction]]) -> Optional[List[FilterAction]]:
    if not actions:
        return None
    normalized: List[FilterAction] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        if not action.get("filter_key"):
            continue
        normalized.append(FilterAction(type=action.get("type", "set"), filter_key=action["filter_key"], value=action.get("value")))
    return normalized or None


@router.post("/message", response_model=ChatResponsePayload, dependencies=[Depends(enforce_rate_limit)])
def send_message(payload: ChatMessagePayload) -> ChatResponsePayload:
    context = build_portal_context(payload.portal_slug)
    if not context:
        raise HTTPException(status_code=404, detail="portal_not_found")

    conversation_id = payload.conversation_id or uuid.uuid4().hex
    conversation_doc = _load_conversation(conversation_id) or {
        "conversation_id": conversation_id,
        "portal_slug": payload.portal_slug,
        "messages": [],
        "metadata": {},
    }

    history = _chatbot_service.conversation_to_history(conversation_doc.get("messages", []))
    history.append({"role": "user", "content": payload.message})

    result: ChatbotResult = _chatbot_service.process_message(
        user_message=payload.message,
        portal_context=context,
        conversation_history=history,
        current_filters=payload.current_filters,
    )

    timestamp = time.time()
    conversation_doc.setdefault("messages", []).append(
        {
            "role": "user",
            "content": payload.message,
            "timestamp": timestamp,
        }
    )
    assistant_entry = {
        "role": "assistant",
        "content": result.get("response_text"),
        "timestamp": timestamp + 0.01,
    }
    if result.get("filter_actions"):
        assistant_entry["filters"] = result["filter_actions"]
    if result.get("highlighted_job_ids"):
        assistant_entry["job_ids"] = result["highlighted_job_ids"]
    conversation_doc["messages"].append(assistant_entry)

    metadata = conversation_doc.get("metadata") or {}
    metadata.setdefault("message_count", 0)
    metadata["message_count"] += 2
    conversation_doc["metadata"] = metadata

    _save_conversation(conversation_doc)

    filters = _normalize_filter_actions(result.get("filter_actions"))
    return ChatResponsePayload(
        message=result.get("response_text", ""),
        conversation_id=conversation_id,
        filters=[FilterActionModel(**item) for item in filters] if filters else None,
        job_ids=result.get("highlighted_job_ids"),
        metadata=result.get("metadata"),
    )


@router.get("/conversation/{conversation_id}", response_model=ConversationHistoryResponse)
def get_conversation(conversation_id: str) -> ConversationHistoryResponse:
    doc = _load_conversation(conversation_id)
    if not doc:
        raise HTTPException(status_code=404, detail="conversation_not_found")
    messages: List[ConversationMessage] = []
    for item in doc.get("messages", []):
        msg = ConversationMessage(
            role=item.get("role", "assistant"),
            content=item.get("content", ""),
            timestamp=float(item.get("timestamp", 0.0)),
            filters=[FilterActionModel(**f) for f in item.get("filters", [])] if item.get("filters") else None,
            job_ids=item.get("job_ids"),
        )
        messages.append(msg)
    return ConversationHistoryResponse(
        conversation_id=conversation_id,
        portal_slug=doc.get("portal_slug"),
        messages=messages,
        metadata=doc.get("metadata"),
    )


@router.delete("/conversation/{conversation_id}", status_code=204, response_model=None)
def delete_conversation(conversation_id: str) -> Response:
    db[_CHATBOT_COLLECTION].delete_one({"conversation_id": conversation_id})
    return Response(status_code=204)


@router.post("/suggest", response_model=ConversationStarterResponse)
def get_suggestions(payload: PortalContextPayload) -> ConversationStarterResponse:
    context = build_portal_context(payload.portal_slug)
    if not context:
        raise HTTPException(status_code=404, detail="portal_not_found")
    starters = _chatbot_service.get_conversation_starters(context)
    return ConversationStarterResponse(starters=starters)


@router.get("/seed/{token}", response_model=ChatSeedResponse)
def consume_seed(token: str) -> ChatSeedResponse:
    doc = _seed_store.consume_seed(token)
    if not doc:
        raise HTTPException(status_code=404, detail="seed_not_found")

    portal_slug = doc.get("portal_slug")
    context = build_portal_context(portal_slug)
    if not context:
        raise HTTPException(status_code=404, detail="portal_not_found")

    job_ids = doc.get("suggested_job_ids") or doc.get("inferred_job_ids") or []
    if not job_ids:
        suggestions = _seed_store.build_suggestions(
            tenant_id=context.get("tenant_id"),
            exclude_job_id=doc.get("inferred_job_ids", [None])[0],
        )
        job_ids = [item.get("job_id") for item in suggestions if item.get("job_id")]
    else:
        suggestions = []

    seed_payload = _chatbot_service.build_seed_response(
        context=context,
        job_ids=[jid for jid in job_ids if isinstance(jid, str)],
        external_url=doc.get("external_url"),
    )

    jobs_by_id = {job.get("job_id"): job for job in context.get("jobs", [])}
    job_suggestions: List[JobSuggestion] = []
    for jid in job_ids:
        job = jobs_by_id.get(jid)
        if job:
            job_suggestions.append(
                JobSuggestion(
                    job_id=jid,
                    title=job.get("title"),
                    company_name=job.get("company_name"),
                    location=job.get("location"),
                )
            )
    if not job_suggestions and suggestions:
        for item in suggestions:
            job_suggestions.append(JobSuggestion(**item))

    return ChatSeedResponse(
        portal_slug=portal_slug,
        auto_message=seed_payload["message"],
        highlighted_job_ids=seed_payload.get("highlighted_job_ids") or [],
        job_suggestions=job_suggestions,
        metadata=seed_payload.get("metadata"),
    )
