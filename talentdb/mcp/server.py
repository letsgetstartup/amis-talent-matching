from __future__ import annotations

"""MCP server scaffold for Recruiter Copilot.

This module defines the tool registry and dispatch table. In this phase, we
intentionally do not start a real MCP stdio server; instead we expose a simple
call interface usable by the API while preserving the protocol surface for a
future stdio implementation.
"""

from typing import Any, Dict, List
from .schemas import (
    SearchCandidatesInput, SearchCandidatesOutput,
    SearchJobsInput, SearchJobsOutput,
    MatchJobToCandidatesInput, MatchCandidateToJobsInput, MatchListOutput,
    GetMatchAnalysisInput, GetMatchAnalysisOutput,
    GetCandidateProfileInput, GetCandidateProfileOutput,
    GetJobDetailsInput, GetJobDetailsOutput,
    CreateOutreachMessageInput, CreateOutreachMessageOutput,
    AddDiscussionNoteInput, AddDiscussionNoteOutput,
    GetAnalyticsSummaryInput, GetAnalyticsSummaryOutput,
)
from . import adapters


TOOLS = {
    "search_candidates": {
        "input": SearchCandidatesInput,
        "output": SearchCandidatesOutput,
        "handler": lambda ctx, inp: SearchCandidatesOutput(
            candidates=[
                {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "city": r.get("city"),
                    "skills": r.get("skills") or [],
                    "experience_years": r.get("experience_years"),
                }
                for r in adapters.search_candidates_adapter(
                    ctx.get("tenant_id"), inp.skills, inp.city, inp.min_experience, inp.k
                )
            ]
        ),
    },
    "search_jobs": {
        "input": SearchJobsInput,
        "output": SearchJobsOutput,
        "handler": lambda ctx, inp: SearchJobsOutput(
            jobs=[
                {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "city": r.get("city"),
                    "must_have": r.get("must_have") or [],
                    "nice_to_have": r.get("nice_to_have") or [],
                }
                for r in adapters.search_jobs_adapter(
                    ctx.get("tenant_id"), inp.skills, inp.city, inp.seniority, inp.k
                )
            ]
        ),
    },
    "match_job_to_candidates": {
        "input": MatchJobToCandidatesInput,
        "output": MatchListOutput,
        "handler": lambda ctx, inp: MatchListOutput(
            rows=[
                {
                    "score": r.get("score") or 0.0,
                    "candidate_id": r.get("candidate_id"),
                    "job_id": r.get("job_id"),
                    "title": r.get("title"),
                    "city": r.get("city"),
                    "breakdown": r.get("breakdown") or {},
                    "counters": r.get("counters") or {},
                }
                for r in adapters.match_job_to_candidates_adapter(ctx.get("tenant_id"), inp.job_id, inp.k)
            ]
        ),
    },
    "match_candidate_to_jobs": {
        "input": MatchCandidateToJobsInput,
        "output": MatchListOutput,
        "handler": lambda ctx, inp: MatchListOutput(
            rows=[
                {
                    "score": r.get("score") or 0.0,
                    "candidate_id": r.get("candidate_id"),
                    "job_id": r.get("job_id"),
                    "title": r.get("title"),
                    "city": r.get("city"),
                    "breakdown": r.get("breakdown") or {},
                    "counters": r.get("counters") or {},
                }
                for r in adapters.match_candidate_to_jobs_adapter(ctx.get("tenant_id"), inp.candidate_id, inp.k)
            ]
        ),
    },
    "get_match_analysis": {
        "input": GetMatchAnalysisInput,
        "output": GetMatchAnalysisOutput,
        "handler": lambda ctx, inp: GetMatchAnalysisOutput(
            row=adapters.get_match_analysis_adapter(ctx.get("tenant_id"), inp.candidate_id, inp.job_id) or {}
        ),  # type: ignore[arg-type]
    },
    "get_candidate_profile": {
        "input": GetCandidateProfileInput,
        "output": GetCandidateProfileOutput,
        "handler": lambda ctx, inp: GetCandidateProfileOutput(
            candidate=adapters.get_candidate_profile_adapter(ctx.get("tenant_id"), inp.candidate_id) or {}
        ),  # type: ignore[arg-type]
    },
    "get_job_details": {
        "input": GetJobDetailsInput,
        "output": GetJobDetailsOutput,
        "handler": lambda ctx, inp: GetJobDetailsOutput(
            job=adapters.get_job_details_adapter(ctx.get("tenant_id"), inp.job_id) or {}
        ),  # type: ignore[arg-type]
    },
    "create_outreach_message": {
        "input": CreateOutreachMessageInput,
        "output": CreateOutreachMessageOutput,
        "handler": lambda ctx, inp: CreateOutreachMessageOutput(
            messages=[
                {"job_id": m.get("job_id"), "whatsapp": m.get("whatsapp")}
                for m in adapters.create_outreach_message_adapter(ctx.get("tenant_id"), inp.candidate_id, inp.job_ids, inp.tone)
            ]
        ),
    },
    "add_discussion_note": {
        "input": AddDiscussionNoteInput,
        "output": AddDiscussionNoteOutput,
        "handler": lambda ctx, inp: AddDiscussionNoteOutput(
            **adapters.add_discussion_note_adapter(ctx.get("tenant_id"), inp.target_type, inp.target_id, inp.text)
        ),
    },
    "get_analytics_summary": {
        "input": GetAnalyticsSummaryInput,
        "output": GetAnalyticsSummaryOutput,
        "handler": lambda ctx, inp: GetAnalyticsSummaryOutput(
            **adapters.get_analytics_summary_adapter(ctx.get("tenant_id"), inp.window_days)
        ),
    },
}


def list_tools() -> List[Dict[str, Any]]:
    return [
        {"name": name, "description": spec["input"].__name__}
        for name, spec in TOOLS.items()
    ]


def call_tool(name: str, arguments: Dict[str, Any], context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    spec = TOOLS.get(name)
    if not spec:
        return {"ok": False, "error": {"code": "tool_not_found", "message": name}}
    try:
        inp = spec["input"](**(arguments or {}))
        out = spec["handler"](context or {}, inp)
        # Validate output schema
        spec["output"].model_validate(out.model_dump() if hasattr(out, "model_dump") else out)  # type: ignore[arg-type]
        return {"ok": True, "data": out.model_dump() if hasattr(out, "model_dump") else out}
    except Exception as e:
        return {"ok": False, "error": {"code": "tool_error", "message": str(e)}}
