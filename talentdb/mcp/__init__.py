"""MCP server package for Recruiter Copilot.

Feature-flagged via MCP_ENABLED. Provides tools to search candidates/jobs,
compute matches, generate outreach, and fetch analytics. Safe to import when
disabled; no side effects on import.
"""

import os

# Load dotenv if available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

__all__ = [
    "get_mcp_runtime",
]

from .runtime import get_mcp_runtime  # noqa: E402
