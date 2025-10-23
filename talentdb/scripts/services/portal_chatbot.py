from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from bson import ObjectId

try:  # Optional import for type checking without hard dependency in tests
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = Any  # type: ignore

from ..ingest_agent import _OPENAI_AVAILABLE, _openai_client, db

CHATBOT_MODEL = os.getenv("CHATBOT_MODEL", "gpt-4o")
CHATBOT_TEMPERATURE = float(os.getenv("CHATBOT_TEMPERATURE", "0.6"))
CHATBOT_MAX_TOKENS = int(os.getenv("CHATBOT_MAX_TOKENS", "700"))
CHATBOT_SEED_TTL_SECONDS = int(os.getenv("CHATBOT_SEED_TTL_SECONDS", "900"))
CHATBOT_SUGGESTION_LIMIT = int(os.getenv("CHATBOT_SUGGESTION_LIMIT", "4"))
CHATBOT_FUNCTION_RESP_MAX_TOKENS = int(os.getenv("CHATBOT_FUNCTION_RESP_MAX_TOKENS", "600"))


class FilterAction(TypedDict):
    type: str
    filter_key: str
    value: Any


class ChatbotResult(TypedDict, total=False):
    response_text: str
    filter_actions: List[FilterAction]
    highlighted_job_ids: List[str]
    metadata: Dict[str, Any]


class PortalContext(TypedDict, total=False):
    portal_slug: str
    portal_name: str
    tenant_id: str
    job_count: int
    company_count: int
    location_count: int
    jobs: List[Dict[str, Any]]
    companies: List[str]
    locations: List[str]
    top_skills: List[str]


def build_portal_context(portal_slug: str) -> Optional[PortalContext]:
    tenant = db["tenants"].find_one({"slug": portal_slug})
    if not tenant and len(portal_slug) == 24:
        tenant = db["tenants"].find_one({"_id": portal_slug})
    if not tenant:
        return None

    tenant_id = str(tenant.get("_id"))
    jobs_cursor = (
        db["jobs"].find({"tenant_id": tenant_id}).sort([("created_at", -1), ("_id", -1)])
    )

    jobs: List[Dict[str, Any]] = []
    companies: List[str] = []
    locations: List[str] = []
    skill_frequency: Dict[str, int] = {}

    for job in jobs_cursor:
        job_id = str(job.get("_id"))
        requirements = job.get("skill_set", []) or []
        company_name = (job.get("company_name") or "").strip()
        location = (job.get("city") or "").strip() or "Remote"
        remote = bool(job.get("remote", False))
        application_url = job.get("application_url")

        jobs.append(
            {
                "job_id": job_id,
                "title": job.get("title") or "",
                "company_name": company_name,
                "description": job.get("job_description", ""),
                "requirements": requirements,
                "location": location,
                "remote": remote,
                "application_url": application_url,
                "nice_to_have": job.get("nice_to_have") or [],
                "created_at": job.get("created_at"),
            }
        )

        if company_name and company_name not in companies:
            companies.append(company_name)
        if location and location not in locations:
            locations.append(location)
        for skill in requirements:
            if isinstance(skill, str) and skill.strip():
                normalized = skill.strip().lower()
                skill_frequency[normalized] = skill_frequency.get(normalized, 0) + 1

    top_skills = [skill for skill, _ in sorted(skill_frequency.items(), key=lambda item: -item[1])][:20]

    return PortalContext(
        portal_slug=portal_slug,
        portal_name=tenant.get("name") or portal_slug,
        tenant_id=tenant_id,
        job_count=len(jobs),
        company_count=len(companies),
        location_count=len(locations),
        jobs=jobs,
        companies=companies,
        locations=locations,
        top_skills=top_skills,
    )


class ChatbotService:
    """Encapsulates OpenAI interactions and conversation shaping."""

    def __init__(self) -> None:
        self._client = _openai_client if _OPENAI_AVAILABLE else None

    @property
    def available(self) -> bool:
        return bool(self._client and _OPENAI_AVAILABLE)

    def _tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "apply_job_filters",
                    "description": "Apply filters to the job list based on user preferences",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {"type": "string", "description": "City or region"},
                            "skills": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Required skills",
                            },
                            "company": {"type": "string", "description": "Company name"},
                            "remote": {
                                "type": "boolean",
                                "description": "True for remote roles, false for onsite",
                            },
                            "query": {
                                "type": "string",
                                "description": "General keyword or title search",
                            },
                            "action": {
                                "type": "string",
                                "enum": ["set", "add", "remove", "clear"],
                                "description": "How the filter should be applied",
                            },
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "highlight_specific_jobs",
                    "description": "Highlight specific jobs in the portal UI",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "job_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of job IDs to highlight",
                            },
                            "reason": {
                                "type": "string",
                                "description": "Short explanation describing why these jobs were selected",
                            },
                        },
                        "required": ["job_ids"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_jobs",
                    "description": "Search available jobs and return matching roles",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {"type": "string"},
                            "skills": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "company": {"type": "string"},
                            "remote": {"type": "boolean"},
                            "query": {"type": "string"},
                            "limit": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 10,
                                "default": 5,
                            },
                        },
                    },
                },
            },
        ]

    def _system_prompt(self, context: PortalContext) -> str:
        return (
            "You are a helpful recruiting assistant for a job marketplace portal. "
            "Use concrete data from the provided context. "
            f"There are {context.get('job_count', 0)} jobs across {context.get('company_count', 0)} companies. "
            "Answer succinctly, offer follow-up suggestions, and always confirm filter changes."
        )

    def process_message(
        self,
        *,
        user_message: str,
        portal_context: PortalContext,
        conversation_history: List[Dict[str, str]],
        current_filters: Optional[Dict[str, Any]] = None,
    ) -> ChatbotResult:
        if not user_message.strip():
            return ChatbotResult(response_text="Could you share a bit more about what you're looking for?")

        if not portal_context.get("jobs"):
            return ChatbotResult(
                response_text="I don't see any live roles right now. Please check back soon or contact the recruiter directly."
            )

        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": self._system_prompt(portal_context)}
        ]
        messages.extend(conversation_history[-10:])
        if current_filters:
            messages.append(
                {
                    "role": "system",
                    "content": f"Current active filters: {json.dumps(current_filters, ensure_ascii=False)}",
                }
            )
        messages.append({"role": "user", "content": user_message})

        if not self.available:
            fallback = self._fallback_response(user_message, portal_context)
            return ChatbotResult(response_text=fallback)

        try:
            initial = self._client.chat.completions.create(  # type: ignore[arg-type]
                model=CHATBOT_MODEL,
                messages=messages,
                tools=self._tools(),
                tool_choice="auto",
                temperature=CHATBOT_TEMPERATURE,
                max_tokens=CHATBOT_MAX_TOKENS,
            )
        except Exception:
            fallback = self._fallback_response(user_message, portal_context)
            return ChatbotResult(response_text=fallback)

        choice = initial.choices[0]
        tool_calls = getattr(choice.message, "tool_calls", None)

        filter_actions: List[FilterAction] = []
        highlighted: List[str] = []
        narrative = choice.message.content or ""

        if tool_calls:
            for tool_call in tool_calls:
                if getattr(tool_call, "type", "") != "function":
                    continue
                name = tool_call.function.name
                args_raw = tool_call.function.arguments or "{}"
                try:
                    payload = json.loads(args_raw)
                except json.JSONDecodeError:
                    payload = {}

                if name == "apply_job_filters":
                    filter_actions.extend(self._convert_to_filter_actions(payload))
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"status": "ok", "filters_applied": payload}),
                        }
                    )
                elif name == "highlight_specific_jobs":
                    job_ids = payload.get("job_ids") or []
                    if isinstance(job_ids, list):
                        highlighted.extend([jid for jid in job_ids if isinstance(jid, str) and jid])
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"status": "ok"}),
                        }
                    )
                elif name == "search_jobs":
                    matches = self._search_jobs(payload, portal_context)
                    highlighted.extend([item["job_id"] for item in matches])
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"status": "ok", "results": matches}),
                        }
                    )

            try:
                follow_up = self._client.chat.completions.create(  # type: ignore[arg-type]
                    model=CHATBOT_MODEL,
                    messages=messages,
                    temperature=CHATBOT_TEMPERATURE,
                    max_tokens=CHATBOT_FUNCTION_RESP_MAX_TOKENS,
                )
                narrative = follow_up.choices[0].message.content or narrative
            except Exception:
                pass

        metadata: Dict[str, Any] = {}
        if highlighted:
            metadata["highlighted_count"] = len(set(highlighted))

        return ChatbotResult(
            response_text=narrative.strip() or self._fallback_response(user_message, portal_context),
            filter_actions=filter_actions or None,
            highlighted_job_ids=list(dict.fromkeys(highlighted)) or None,
            metadata=metadata or None,
        )

    def _fallback_response(self, user_message: str, context: PortalContext) -> str:
        sample_jobs = context.get("jobs", [])[:3]
        if not sample_jobs:
            return "I'm having trouble connecting right now. Please refresh or try again later."
        lines = ["Here are a few openings that might interest you:"]
        for job in sample_jobs:
            title = job.get("title") or "Role"
            company = job.get("company_name") or ""
            location = job.get("location") or "Remote"
            lines.append(f"• {title} at {company} ({location})")
        lines.append("Let me know if you want to refine the search.")
        return "\n".join(lines)

    def _convert_to_filter_actions(self, payload: Dict[str, Any]) -> List[FilterAction]:
        action = (payload.get("action") or "set").lower()
        if action not in {"set", "add", "remove", "clear"}:
            action = "set"
        actions: List[FilterAction] = []
        for key in ("location", "skills", "company", "remote", "query"):
            if key not in payload:
                continue
            value = payload[key]
            filter_key = "type" if key == "remote" else key
            if key == "remote":
                value = "remote" if value else "onsite"
            actions.append(FilterAction(type=action, filter_key=filter_key, value=value))
        return actions

    def _search_jobs(self, payload: Dict[str, Any], context: PortalContext) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        jobs = context.get("jobs") or []
        limit = payload.get("limit") or 5
        query = (payload.get("query") or "").lower()
        location = (payload.get("location") or "").strip().lower()
        company = (payload.get("company") or "").strip().lower()
        skills = [skill.strip().lower() for skill in payload.get("skills") or [] if isinstance(skill, str)]
        remote_flag = payload.get("remote")

        for job in jobs:
            title = (job.get("title") or "").lower()
            job_company = (job.get("company_name") or "").lower()
            job_location = (job.get("location") or "").lower()
            job_skills = [str(skill).lower() for skill in job.get("requirements") or []]
            job_remote = bool(job.get("remote"))

            if query and query not in title and query not in job_company:
                continue
            if location and location not in job_location:
                continue
            if company and company != job_company:
                continue
            if skills and not all(skill in job_skills for skill in skills):
                continue
            if remote_flag is True and not job_remote:
                continue
            if remote_flag is False and job_remote:
                continue

            matches.append(
                {
                    "job_id": job.get("job_id"),
                    "title": job.get("title"),
                    "company_name": job.get("company_name"),
                    "location": job.get("location"),
                    "remote": job.get("remote"),
                }
            )
            if len(matches) >= limit:
                break
        return matches

    def conversation_to_history(self, messages: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        history: List[Dict[str, str]] = []
        for msg in messages:
            role = msg.get("role")
            content = msg.get("content")
            if role in {"user", "assistant"} and isinstance(content, str):
                history.append({"role": role, "content": content})
        return history

    def get_conversation_starters(self, context: PortalContext) -> List[str]:
        starters = [
            "What roles do you recommend for me?",
            "Show me remote opportunities",
        ]
        for skill in (context.get("top_skills") or [])[:3]:
            starters.append(f"Do you have any {skill.title()} roles?")
        for location in (context.get("locations") or [])[:2]:
            starters.append(f"What's open in {location}?")
        for company in (context.get("companies") or [])[:2]:
            starters.append(f"Tell me about roles at {company}")
        return starters[:6]

    def build_seed_response(
        self,
        *,
        context: PortalContext,
        job_ids: List[str],
        external_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        jobs_by_id = {job.get("job_id"): job for job in context.get("jobs", [])}
        selected = [jobs_by_id[jid] for jid in job_ids if jid in jobs_by_id]
        if not selected:
            selected = context.get("jobs", [])[:3]
        message_lines = ["I found a few jobs for you:"]
        highlighted_ids: List[str] = []
        for job in selected:
            title = job.get("title") or "Role"
            company = job.get("company_name") or ""
            location = job.get("location") or "Remote"
            message_lines.append(f"• {title} at {company} ({location})")
            highlighted_ids.append(job.get("job_id"))
        message_lines.append("Have you found any of them interesting?")
        message = "\n".join(message_lines)
        metadata = {"external_url": external_url} if external_url else {}
        return {
            "message": message,
            "highlighted_job_ids": [jid for jid in highlighted_ids if jid],
            "metadata": metadata or None,
        }


class PortalChatSeedStore:
    """Persists redirect-triggered chat seeds with TTL."""

    _COLLECTION = "portal_chat_seeds"
    _LOCK = RLock()
    _INDEX_INITIALIZED = False

    def __init__(self) -> None:
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        with self._LOCK:
            if self.__class__._INDEX_INITIALIZED:
                return
            coll = db[self._COLLECTION]
            coll.create_index("expires_at", expireAfterSeconds=0, name="expires_at_ttl")
            coll.create_index("portal_slug", name="portal_slug_lookup")
            self.__class__._INDEX_INITIALIZED = True

    def create_seed(
        self,
        *,
        portal_slug: str,
        tenant_id: str,
        external_url: Optional[str],
        inferred_job_ids: Optional[List[str]] = None,
        suggested_job_ids: Optional[List[str]] = None,
    ) -> str:
        token = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        doc = {
            "_id": token,
            "portal_slug": portal_slug,
            "tenant_id": tenant_id,
            "external_url": external_url,
            "inferred_job_ids": inferred_job_ids or [],
            "suggested_job_ids": suggested_job_ids or [],
            "created_at": now,
            "expires_at": now + timedelta(seconds=CHATBOT_SEED_TTL_SECONDS),
        }
        db[self._COLLECTION].insert_one(doc)
        return token

    def consume_seed(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        doc = db[self._COLLECTION].find_one_and_delete({"_id": token})
        return doc

    def peek_seed(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        return db[self._COLLECTION].find_one({"_id": token})

    def build_suggestions(
        self,
        *,
        tenant_id: str,
        exclude_job_id: Optional[Any] = None,
        limit: int = CHATBOT_SUGGESTION_LIMIT,
    ) -> List[Dict[str, Any]]:
        filters: Dict[str, Any] = {"tenant_id": tenant_id}
        if exclude_job_id:
            try:
                if isinstance(exclude_job_id, ObjectId):
                    oid = exclude_job_id
                else:
                    oid = ObjectId(str(exclude_job_id))
                filters["_id"] = {"$ne": oid}
            except Exception:
                pass
        cursor = (
            db["jobs"].find(filters, {"title": 1, "company_name": 1, "city": 1})
            .sort([("created_at", -1), ("_id", -1)])
            .limit(limit)
        )
        suggestions: List[Dict[str, Any]] = []
        for job in cursor:
            suggestions.append(
                {
                    "job_id": str(job.get("_id")),
                    "title": job.get("title"),
                    "company_name": job.get("company_name"),
                    "location": job.get("city") or "Remote",
                }
            )
        return suggestions
