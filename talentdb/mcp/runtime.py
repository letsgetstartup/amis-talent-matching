"""MCP runtime bootstrap (feature-flagged).

This module exposes a minimal interface that the API can use to:
- Check if MCP is enabled.
- Lazily start a local MCP server (stdio) when needed.
- Provide a lightweight client shim for invoking tools (to be implemented incrementally).

No external processes or network listeners are started unless enabled.
"""
from __future__ import annotations

import os
import threading
from typing import Any, Dict, Optional


class _McpRuntime:
    def __init__(self) -> None:
        import os
        print(f"DEBUG: Current dir: {os.getcwd()}")
        print(f"DEBUG: MCP_ENABLED env var before load_dotenv: {os.getenv('MCP_ENABLED', 'NOT_SET')}")
        try:
            from dotenv import load_dotenv
            load_dotenv()
            print("DEBUG: load_dotenv called")
        except Exception as e:
            print(f"DEBUG: load_dotenv failed: {e}")
        self.enabled = os.getenv("MCP_ENABLED", "0").lower() in {"1", "true", "yes"}
        print(f"DEBUG: MCP_ENABLED env var: {os.getenv('MCP_ENABLED', 'NOT_SET')}, enabled: {self.enabled}")
        self._lock = threading.Lock()
        self._started = False
        # Placeholders for server/client references
        self._server = None
        self._client = None

    def is_enabled(self) -> bool:
        return self.enabled

    def start(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._started:
                return
            # Lazy-start placeholder. In the next phase, wire actual stdio MCP server.
            self._server = object()  # sentinel
            self._client = _McpClientShim()
            self._started = True

    def health(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "started": self._started,
            "server": bool(self._server),
        }

    def client(self) -> Optional["_McpClientShim"]:
        if not self.enabled:
            return None
        if not self._started:
            self.start()
        return self._client


class _McpClientShim:
    """Tiny shim that mimics list_tools / call_tool for early integration.

    In phase 2, replace with real MCP client.
    """

    def list_tools(self) -> list[dict]:
        # Advertise initial tool surface; implemented incrementally.
        return [
            {"name": "search_candidates", "description": "Search candidates by skills/city/experience"},
            {"name": "search_jobs", "description": "Search jobs by skills/city/seniority"},
            {"name": "match_job_to_candidates", "description": "Top-K candidates for a job"},
            {"name": "match_candidate_to_jobs", "description": "Top-K jobs for a candidate"},
            {"name": "get_match_analysis", "description": "Detailed match breakdown for candidate-job"},
            {"name": "get_candidate_profile", "description": "Canonical candidate profile"},
            {"name": "get_job_details", "description": "Canonical job details"},
            {"name": "create_outreach_message", "description": "Generate WhatsApp outreach"},
            {"name": "add_discussion_note", "description": "Add discussion note to candidate/job/match"},
            {"name": "get_analytics_summary", "description": "KPIs and insights"},
        ]

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        # Phase 1 returns a structured error so the API can fall back gracefully.
        return {
            "ok": False,
            "error": {
                "code": "mcp_unimplemented",
                "message": f"Tool '{name}' not yet implemented",
                "args": arguments,
            },
        }


_RUNTIME = _McpRuntime()


def get_mcp_runtime() -> _McpRuntime:
    """Return the singleton MCP runtime (feature-flag aware)."""
    return _RUNTIME
