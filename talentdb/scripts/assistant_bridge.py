"""Assistant-MCP bridge.

Runs OpenAI Assistants with function tools mapped to our MCP tools. Handles
tool call cycles by invoking MCP server adapters and submitting tool outputs
until the run completes. Prov        # Start a run and handle tool calls
    if hasattr(client, 'beta') and hasattr(client.beta, 'threads') and hasattr(client.beta.threads, 'runs'):
        run = client.beta.threads.runs.create(
            thread_id=openai_thread_id,
            assistant_id=assistant_id,
            instructions=_assistant_instructions(),
        )
    else:
        raise Exception("Runs API not available on this OpenAI client")
    _execute_tool_calls(run, openai_thread_id, tenant_id)mple helpers for one-shot and NDJSON-like
streamed progress suitable for the existing frontend.
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

from ..mcp import server as mcp_server

try:
    # Reuse the shared OpenAI client and availability flag from ingest_agent
    from .ingest_agent import _openai_client, _OPENAI_AVAILABLE
except Exception:  # pragma: no cover
    _openai_client = None
    _OPENAI_AVAILABLE = False


def _assistant_instructions() -> str:
    base = (
        "You are Recruiter Copilot for a talent-matching platform. "
        "Always use the provided tools to fetch real data (do not invent IDs or data). "
        "Keep answers concise. Prefer Hebrew when the user speaks Hebrew. "
        "CRITICAL: When presenting results, ALWAYS return a structured JSON response with this EXACT format: "
        '{ "type": "assistant_ui", "narration": "your text", "actions": [], "ui": [components...] } '
        "where components MUST use 'kind' (not 'type') attribute. Examples: "
        '{"kind": "Table", "id": "results", "columns": [...], "rows": [...]} '
    '{"kind": "Metric", "id": "count", "label": "Results", "value": "10"} '
        '{"kind": "MatchList", "id": "matches", "items": [...]} '
        '{"kind": "JobDetails", "id": "job_x", "details": {"id": "...", "title": "...", "city": "...", "must_have": [], "nice_to_have": []}} '
        "STRICT RULES: (1) Do NOT include JSON or code fences in narration. (2) Do NOT echo the assistant_ui object anywhere except the final JSON. "
        "(3) Use 'ui' array for all structured content. (4) Keep narration to one short sentence. "
        "Call function tools first, then build proper UI components from the results."
    )
    extra = os.getenv("OPENAI_ASSISTANT_INSTRUCTIONS", "").strip()
    return (extra or base)


# --- Normalization & sanitization helpers -----------------------------------

def _strip_code_fences(text: str) -> str:
    """Remove triple backtick fenced blocks (``` or ```json)."""
    if not text:
        return text
    # Remove any ```lang\n...``` blocks non-greedily
    return re.sub(r"```[a-zA-Z]*\n[\s\S]*?```", "", text)


def _strip_embedded_assistant_ui(text: str) -> str:
    """Remove inline assistant_ui JSON if the model echoed it into narration.

    We try to find occurrences of '"type": "assistant_ui"' or "'type': 'assistant_ui'"
    and remove the surrounding JSON object using a simple brace matcher.
    """
    if not text:
        return text
    patterns = ['"type": "assistant_ui"', "'type': 'assistant_ui'"]
    out = text
    for pat in patterns:
        idx = out.find(pat)
        if idx == -1:
            continue
        # Find start of enclosing '{'
        start = out.rfind('{', 0, idx)
        if start == -1:
            continue
        # Scan forward to find matching '}'
        depth = 0
        end = -1
        for i in range(start, len(out)):
            ch = out[i]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end != -1:
            out = out[:start] + out[end + 1:]
    return out


def _sanitize_narration(narration: str, max_len: int = 240) -> str:
    text = narration or ""
    text = _strip_code_fences(text)
    text = _strip_embedded_assistant_ui(text)
    text = text.strip()
    if len(text) > max_len:
        text = text[:max_len].rstrip() + "…"
    return text


def _normalize_component(comp: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Normalize a single UI component in-place and return it."""
    if not isinstance(comp, dict):
        return {"kind": "RichText", "id": f"component_{index}", "html": str(comp)}
    # type -> kind
    if "kind" not in comp and "type" in comp:
        comp["kind"] = comp.pop("type")
    kind = comp.get("kind") or "RichText"
    comp["kind"] = kind
    # id fallback
    if "id" not in comp or not comp.get("id"):
        comp["id"] = f"{str(kind).lower()}_{index}"
    # Per-kind minimal shape fixes
    if kind == "Table":
        comp.setdefault("columns", [])
        comp.setdefault("rows", [])
        if not isinstance(comp["columns"], list):
            comp["columns"] = []
        if not isinstance(comp["rows"], list):
            comp["rows"] = []
    elif kind == "JobDetails":
        details = comp.get("details")
        if not isinstance(details, dict):
            details = {}
        details.setdefault("id", comp.get("id"))
        details.setdefault("title", "")
        details.setdefault("city", "")
        details.setdefault("must_have", [])
        details.setdefault("nice_to_have", [])
        comp["details"] = details
    return comp


def _normalize_and_sanitize_envelope(env: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(env, dict):
        return {
            "type": "assistant_ui",
            "narration": _sanitize_narration(str(env)),
            "actions": [],
            "ui": [],
        }
    out = dict(env)
    out["type"] = "assistant_ui"
    out["narration"] = _sanitize_narration(out.get("narration") or "")
    out["actions"] = out.get("actions") or []
    ui = out.get("ui") or []
    if not isinstance(ui, list):
        ui = []
    normalized_ui: List[Dict[str, Any]] = []
    for i, comp in enumerate(ui):
        try:
            normalized_ui.append(_normalize_component(comp, i))
        except Exception:
            # If any component breaks, replace with a RichText explaining the issue (but keep it quiet)
            normalized_ui.append({"kind": "RichText", "id": f"component_{i}", "html": ""})
    out["ui"] = normalized_ui
    return out


def _pydantic_model_to_json_schema(model_cls) -> Dict[str, Any]:
    try:
        # pydantic v2
        schema = model_cls.model_json_schema()
    except Exception:  # pragma: no cover
        # pydantic v1 fallback
        schema = model_cls.schema()
    props = schema.get("properties", {})
    required = schema.get("required", [])
    # Convert to OpenAI function tool JSON schema
    return {
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": False,
    }


def build_function_tools_from_mcp() -> List[Dict[str, Any]]:
    tools: List[Dict[str, Any]] = []
    for name, spec in mcp_server.TOOLS.items():
        input_cls = spec.get("input")
        if not input_cls:
            continue
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": spec.get("input").__name__,
                    "parameters": _pydantic_model_to_json_schema(input_cls),
                },
            }
        )
    return tools


def _ensure_assistant_and_thread(thread_doc: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Ensure OpenAI Assistant and OpenAI thread exist, creating if needed.

    Returns (assistant_id, openai_thread_id). Updates the provided thread_doc in-place.
    The caller is responsible for persisting changes to Mongo.
    """
    if not _OPENAI_AVAILABLE or _openai_client is None:
        return (None, None)
    assistant_id = thread_doc.get("assistant_id")
    openai_thread_id = thread_doc.get("openai_thread_id")
    # Create assistant if missing
    if not assistant_id:
        try:
            tools = build_function_tools_from_mcp()
            # Try different ways to access assistants
            if hasattr(_openai_client, 'assistants'):
                a = _openai_client.assistants.create(
                    name=os.getenv("OPENAI_ASSISTANT_NAME", "Recruiter Copilot"),
                    model=os.getenv("OPENAI_ASSISTANT_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o")),
                    instructions=_assistant_instructions(),
                    tools=tools,
                )
            elif hasattr(_openai_client, 'beta') and hasattr(_openai_client.beta, 'assistants'):
                a = _openai_client.beta.assistants.create(
                    name=os.getenv("OPENAI_ASSISTANT_NAME", "Recruiter Copilot"),
                    model=os.getenv("OPENAI_ASSISTANT_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o")),
                    instructions=_assistant_instructions(),
                    tools=tools,
                )
            else:
                raise Exception("Assistants API not available on this OpenAI client")
            assistant_id = a.id
            thread_doc["assistant_id"] = assistant_id
        except Exception:
            assistant_id = None
    # Create thread if missing
    if not openai_thread_id:
        try:
            if hasattr(_openai_client, 'beta') and hasattr(_openai_client.beta, 'threads'):
                th = _openai_client.beta.threads.create()
            else:
                raise Exception("Threads API not available on this OpenAI client")
            openai_thread_id = th.id
            thread_doc["openai_thread_id"] = openai_thread_id
        except Exception:
            openai_thread_id = None
    return (assistant_id, openai_thread_id)


# --- Lightweight per-thread cache of last tool results (for fallback UI) ---
_LAST_TOOL_RESULTS: Dict[str, List[Dict[str, Any]]] = {}

def _remember_tool_result(thread_id: str, name: str, result: Dict[str, Any]) -> None:
    try:
        lst = _LAST_TOOL_RESULTS.setdefault(thread_id, [])
        lst.append({"name": name, "result": result})
        # Keep only the last few entries per thread to limit memory
        if len(lst) > 10:
            del lst[:-10]
    except Exception:
        pass

def _build_fallback_envelope_from_last_tools(thread_id: str, narration: str | None = None) -> Optional[Dict[str, Any]]:
    try:
        items = list(_LAST_TOOL_RESULTS.get(thread_id, []))
        if not items:
            return None
        # Search from last to first for most recent results
        for entry in reversed(items):
            name = entry.get("name")
            result = entry.get("result") or {}
            if not isinstance(result, dict) or not result.get("ok"):
                continue
            data = result.get("data") or {}
            # Fallback for search_candidates → Table of candidates
            if name == "search_candidates" and isinstance(data, dict):
                cands = (data.get("candidates") or [])
                if not isinstance(cands, list) or not cands:
                    continue
                rows = []
                for c in cands[:10]:
                    try:
                        rows.append({
                            "candidate_id": str(c.get("id") or ""),
                            "title": c.get("title") or "",
                            "city": c.get("city") or "",
                            "skills": ", ".join([str(s) for s in (c.get("skills") or [])][:6])
                        })
                    except Exception:
                        continue
                if rows:
                    env = {
                        "type": "assistant_ui",
                        "narration": narration or "הנה מועמדים שמצאתי",
                        "actions": [],
                        "ui": [
                            {
                                "kind": "Table",
                                "id": "candidates",
                                "columns": [
                                    {"key": "candidate_id", "title": "מועמד"},
                                    {"key": "title", "title": "תפקיד"},
                                    {"key": "city", "title": "עיר"},
                                    {"key": "skills", "title": "מיומנויות"},
                                ],
                                "rows": rows,
                                "primaryKey": "candidate_id",
                            }
                        ],
                    }
                    return env
            # Fallback for search_jobs → Table of jobs
            if name == "search_jobs" and isinstance(data, dict):
                jobs = (data.get("jobs") or [])
                if not isinstance(jobs, list) or not jobs:
                    continue
                rows = []
                for j in jobs[:10]:
                    try:
                        rows.append({
                            "job_id": str(j.get("id") or ""),
                            "title": j.get("title") or "",
                            "city": j.get("city") or "",
                            "must": ", ".join([str(s) for s in (j.get("must_have") or [])][:6]),
                        })
                    except Exception:
                        continue
                if rows:
                    env = {
                        "type": "assistant_ui",
                        "narration": narration or "הנה משרות שמצאתי",
                        "actions": [],
                        "ui": [
                            {
                                "kind": "Table",
                                "id": "jobs",
                                "columns": [
                                    {"key": "job_id", "title": "משרה"},
                                    {"key": "title", "title": "כותרת"},
                                    {"key": "city", "title": "עיר"},
                                    {"key": "must", "title": "חובה"},
                                ],
                                "rows": rows,
                                "primaryKey": "job_id",
                            }
                        ],
                    }
                    return env
            
            # Fallback for match results → MatchList or Table
            if name in ["match_job_to_candidates", "match_candidate_to_jobs"] and isinstance(data, dict):
                matches = (data.get("rows") or [])
                if not isinstance(matches, list) or not matches:
                    continue
                
                # Try MatchList format first
                items = []
                for m in matches[:10]:
                    try:
                        score = m.get("score") or 0
                        items.append({
                            "id": f"{m.get('candidate_id', '')}@{m.get('job_id', '')}",
                            "title": m.get("title") or "",
                            "city": m.get("city") or "",
                            "scorePct": int(score * 100) if isinstance(score, (int, float)) else 0,
                            "counters": m.get("counters") or {"must": {"have": 0, "total": 0}, "nice": {"have": 0, "total": 0}},
                            "must": [],
                            "nice": [],
                            "parts": [],
                            "distancePct": None,
                            "candidate_id": str(m.get("candidate_id") or ""),
                            "job_id": str(m.get("job_id") or ""),
                            "summary": {"must": "0/0", "nice": "0/0"}
                        })
                    except Exception:
                        continue
                
                if items:
                    env = {
                        "type": "assistant_ui",
                        "narration": narration or "הנה התאמות שמצאתי",
                        "actions": [],
                        "ui": [
                            {
                                "kind": "MatchList",
                                "id": "matches",
                                "items": items
                            }
                        ],
                    }
                    return env
            
            # Fallback for analytics → Metrics
            if name == "get_analytics_summary" and isinstance(data, dict):
                metrics = []
                for key, value in data.items():
                    if isinstance(value, (int, float)) and key not in ["timestamp"]:
                        label = key.replace("_", " ").replace("total", "סה\"כ").replace("count", "מספר")
                        metrics.append({
                            "kind": "Metric",
                            "id": f"metric_{key}",
                            "label": label,
                            "value": str(value)
                        })
                
                if metrics:
                    env = {
                        "type": "assistant_ui",
                        "narration": narration or "הנה סיכום אנליטי",
                        "actions": [],
                        "ui": metrics[:5]  # Limit to 5 metrics
                    }
                    return env
        return None
    except Exception:
        return None


def _execute_tool_calls(run, thread_id: str, tenant_id: Optional[str]) -> None:
    """While the run requires action, execute MCP tools and submit outputs."""
    # Poll for required_action; use create_and_poll or manual polling
    client = _openai_client
    while True:
        if getattr(run, "status", None) == "requires_action":
            # Extract tool calls from SDK objects robustly
            ra = getattr(run, "required_action", None)
            sto = getattr(ra, "submit_tool_outputs", None) if ra else None
            tool_calls = getattr(sto, "tool_calls", []) if sto else []
            outputs = []
            for tc in tool_calls:
                fn = getattr(tc, "function", None) or {}
                name = (getattr(fn, "name", None) if hasattr(fn, "name") else (fn.get("name") if isinstance(fn, dict) else None))
                arg_json = (getattr(fn, "arguments", None) if hasattr(fn, "arguments") else (fn.get("arguments") if isinstance(fn, dict) else None)) or "{}"
                try:
                    args = json.loads(arg_json)
                except Exception:
                    args = {}
                result = mcp_server.call_tool(name or "", args, {"tenant_id": tenant_id})
                try:
                    if name:
                        _remember_tool_result(thread_id, str(name), result)
                except Exception:
                    pass
                call_id = (getattr(tc, "id", None) if hasattr(tc, "id") else (tc.get("id") if isinstance(tc, dict) else None))
                outputs.append({"tool_call_id": call_id, "output": json.dumps(result, ensure_ascii=False)})
            # Submit tool outputs and continue
            if hasattr(client, 'beta') and hasattr(client.beta, 'threads') and hasattr(client.beta.threads, 'runs'):
                run = client.beta.threads.runs.submit_tool_outputs_and_poll(
                    thread_id=thread_id,
                    run_id=run.id,
                    tool_outputs=outputs,
                    timeout=int(os.getenv("OPENAI_REQUEST_TIMEOUT", "600")),
                )
            else:
                raise Exception("Tool outputs API not available on this OpenAI client")
            continue
        if getattr(run, "status", None) in {"queued", "in_progress"}:
            # Poll until done
            time.sleep(0.2)
            if hasattr(client, 'beta') and hasattr(client.beta, 'threads') and hasattr(client.beta.threads, 'runs'):
                run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            else:
                raise Exception("Run retrieval API not available on this OpenAI client")
            continue
        break


def run_assistant_once(question: str, thread_doc: Dict[str, Any], tenant_id: Optional[str]) -> Dict[str, Any]:
    """Run an assistant turn and return the latest assistant message content.

    Returns dict with keys: ok, text, messages (raw), and optionally envelope (parsed assistant_ui).
    """
    if not _OPENAI_AVAILABLE or _openai_client is None:
        return {"ok": False, "error": "openai_unavailable"}

    assistant_id, openai_thread_id = _ensure_assistant_and_thread(thread_doc)
    if not assistant_id or not openai_thread_id:
        return {"ok": False, "error": "assistant_or_thread_missing"}

    client = _openai_client
    # Add user message to the OpenAI thread
    if hasattr(client, 'beta') and hasattr(client.beta, 'threads') and hasattr(client.beta.threads, 'messages'):
        client.beta.threads.messages.create(
            thread_id=openai_thread_id,
            role="user",
            content=question or "",
        )
    else:
        raise Exception("Messages API not available on this OpenAI client")

    # Start a run and handle tool calls
    run = client.beta.threads.runs.create(
        thread_id=openai_thread_id,
        assistant_id=assistant_id,
        instructions=_assistant_instructions(),
    )
    _execute_tool_calls(run, openai_thread_id, tenant_id)

    # Fetch the latest assistant message
    if hasattr(client, 'beta') and hasattr(client.beta, 'threads') and hasattr(client.beta.threads, 'messages'):
        msgs = client.beta.threads.messages.list(thread_id=openai_thread_id, order="desc", limit=5)
    else:
        raise Exception("Messages list API not available on this OpenAI client")
    text_parts: List[str] = []
    envelope: Dict[str, Any] | None = None
    for m in getattr(msgs, "data", []) or []:
        role = getattr(m, "role", None) or (m.get("role") if isinstance(m, dict) else None)
        if role != "assistant":
            continue
        contents = getattr(m, "content", None) or (m.get("content") if isinstance(m, dict) else []) or []
        for c in contents:
            ctype = getattr(c, "type", None) or (c.get("type") if isinstance(c, dict) else None)
            if ctype == "text":
                t = getattr(c, "text", None) or (c.get("text") if isinstance(c, dict) else None)
                val = getattr(t, "value", None) if t is not None and hasattr(t, "value") else (t.get("value") if isinstance(t, dict) else None)
                if val:
                    text_parts.append(val)
            elif ctype == "json":
                j = getattr(c, "json", None) if hasattr(c, "json") else (c.get("json") if isinstance(c, dict) else None)
                if isinstance(j, dict):
                    envelope = _normalize_and_sanitize_envelope(j)
        break  # newest assistant message only
    text = "\n".join([t for t in text_parts if t]).strip()
    result: Dict[str, Any] = {"ok": True, "text": text, "messages": [m.model_dump() if hasattr(m, "model_dump") else {} for m in getattr(msgs, "data", [])]}
    if envelope and isinstance(envelope, dict):
        result["envelope"] = _normalize_and_sanitize_envelope(envelope)
    else:
        # Attempt to parse an envelope embedded in plain text if present
        if text and ("\"type\": \"assistant_ui\"" in text or "'type': 'assistant_ui'" in text):
            try:
                # Try to find JSON object boundaries more robustly
                start = text.find("{")
                if start == -1:
                    start = text.find("```json\n") + 8 if "```json\n" in text else -1
                if start == -1:
                    start = text.find("```\n") + 4 if "```\n" in text else -1
                
                end = text.rfind("}")
                if end == -1 and "```" in text[start:]:
                    end = text.rfind("```")
                
                if start != -1 and end != -1 and end > start:
                    json_str = text[start:end + 1]
                    # Clean up common formatting issues
                    json_str = json_str.replace("```", "").strip()
                    env = json.loads(json_str)
                    if isinstance(env, dict) and env.get("type") == "assistant_ui":
                        result["envelope"] = _normalize_and_sanitize_envelope(env)
            except Exception as e:
                # Log parsing error for debugging
                print(f"JSON parsing error in assistant response: {e}")
                pass
        # If still missing, build a minimal UI from the most recent tool results
        if "envelope" not in result:
            fb = _build_fallback_envelope_from_last_tools(openai_thread_id, narration=_sanitize_narration(text))
            if fb:
                result["envelope"] = _normalize_and_sanitize_envelope(fb)
    return result


def run_assistant_stream(question: str, thread_doc: Dict[str, Any], tenant_id: Optional[str]) -> Generator[str, None, None]:
    """Yield NDJSON-ish progress events followed by assistant_ui or text."""
    yield json.dumps({"type": "text_delta", "text": "מפעיל אסיסטנט..."}, ensure_ascii=False) + "\n"
    r = run_assistant_once(question, thread_doc, tenant_id)
    if not r.get("ok"):
        error_detail = r.get("error", "assistant_failed")
        print(f"Assistant error: {error_detail}")  # Debug logging
        yield json.dumps({"type": "error", "detail": error_detail}, ensure_ascii=False) + "\n"
        yield json.dumps({"type": "done"}, ensure_ascii=False) + "\n"
        return
    
    env = r.get("envelope")
    if isinstance(env, dict) and env.get("type") == "assistant_ui":
        # Normalize & sanitize before sending
        normalized = _normalize_and_sanitize_envelope(env)
        yield json.dumps(normalized, ensure_ascii=False) + "\n"
        yield json.dumps({"type": "done"}, ensure_ascii=False) + "\n"
        return
    
    text = _sanitize_narration((r.get("text") or "").strip() or "בוצע")
    # Create a fallback envelope with basic structure
    fallback_env = {
        "type": "assistant_ui", 
        "narration": text, 
        "actions": [], 
        "ui": [
            {
                "kind": "RichText",
                "id": "response",
                "html": text.replace("\n", "<br>")
            }
        ]
    }
    yield json.dumps(_normalize_and_sanitize_envelope(fallback_env), ensure_ascii=False) + "\n"
    yield json.dumps({"type": "done"}, ensure_ascii=False) + "\n"
