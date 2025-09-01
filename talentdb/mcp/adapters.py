from __future__ import annotations

from typing import Any, Dict, List, Optional
from bson import ObjectId

try:
    # Import lazily to avoid import-time failures in disabled environments
    from ..scripts.ingest_agent import (
        db,
        get_or_compute_candidates_for_job,
        jobs_for_candidate,
        canonical_skill,
    )
except Exception:  # pragma: no cover - disabled or during setup
    db = None
    get_or_compute_candidates_for_job = None
    jobs_for_candidate = None
    canonical_skill = lambda s: (s or "").strip().lower().replace(" ", "_")  # type: ignore


def _ensure_db():
    if db is None:
        raise RuntimeError("db_unavailable")
    return db


def search_candidates_adapter(tenant_id: Optional[str], skills: List[str], city: Optional[str], min_experience: Optional[int], k: int) -> List[Dict[str, Any]]:
    _ensure_db()
    q: Dict[str, Any] = {}
    if tenant_id:
        q["tenant_id"] = tenant_id
    if city:
        q["city_canonical"] = str(city).strip().lower().replace(" ", "_")
    if skills:
        canon = [canonical_skill(s) for s in skills if s]
        # Match if any required skills appear in skills_detailed.name or skill_set
        q["$or"] = [
            {"skills_detailed.name": {"$in": canon}},
            {"skill_set": {"$in": canon}},
        ]
    cur = db["candidates"].find(q).limit(int(max(1, min(k, 50))))
    out: List[Dict[str, Any]] = []
    for d in cur:
        out.append({
            "id": str(d.get("_id")),
            "title": d.get("title") or d.get("full_name"),
            "city": d.get("city_canonical"),
            "skills": [s.get("name") for s in (d.get("skills_detailed") or []) if isinstance(s, dict) and s.get("name")],
            "experience_years": d.get("experience_years"),
        })
    return out


def search_jobs_adapter(tenant_id: Optional[str], skills: List[str], city: Optional[str], seniority: Optional[str], k: int) -> List[Dict[str, Any]]:
    _ensure_db()
    q: Dict[str, Any] = {}
    if tenant_id:
        q["tenant_id"] = tenant_id
    if city:
        q["city_canonical"] = str(city).strip().lower().replace(" ", "_")
    if skills:
        canon = [canonical_skill(s) for s in skills if s]
        q["$or"] = [
            {"skills_detailed.name": {"$in": canon}},
            {"skill_set": {"$in": canon}},
        ]
    cur = db["jobs"].find(q).limit(int(max(1, min(k, 50))))
    out: List[Dict[str, Any]] = []
    for d in cur:
        must = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "must"]
        nice = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "needed"]
        out.append({
            "id": str(d.get("_id")),
            "title": d.get("title"),
            "city": d.get("city_canonical"),
            "must_have": must,
            "nice_to_have": nice,
        })
    return out


def match_job_to_candidates_adapter(tenant_id: Optional[str], job_id: str, k: int) -> List[Dict[str, Any]]:
    _ensure_db()
    if not get_or_compute_candidates_for_job:
        return []
    try:
        rows = get_or_compute_candidates_for_job(ObjectId(job_id), top_k=int(max(1, min(k, 50))), city_filter=True, tenant_id=tenant_id)
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for r in (rows or [])[:k]:
        sc = 0.0
        try:
            sc = float(r.get("score") or r.get("best_score") or 0.0)
        except Exception:
            pass
        breakdown = {}
        for key in ("title_score","semantic_score","embedding_score","skills_score","distance_score"):
            v = r.get(key)
            if isinstance(v, (int,float)):
                breakdown[key] = float(v)
        counters = {
            "must": {"have": int(r.get("skills_matched_must") or 0), "total": int(r.get("skills_total_must") or 0)},
            "nice": {"have": int(r.get("skills_matched_nice") or 0), "total": int(r.get("skills_total_nice") or 0)},
        }
        out.append({
            "score": sc,
            "candidate_id": str(r.get("candidate_id") or r.get("_id") or ""),
            "job_id": str(r.get("job_id") or job_id),
            "title": r.get("title") or r.get("candidate_title"),
            "city": r.get("city") or r.get("city_canonical"),
            "breakdown": breakdown,
            "counters": counters,
            # pass through detailed skills lists for UI chips when available
            "skills_must_list": r.get("skills_must_list") or r.get("must_skills") or [],
            "skills_nice_list": r.get("skills_nice_list") or r.get("nice_skills") or [],
        })
    return out


def match_candidate_to_jobs_adapter(tenant_id: Optional[str], candidate_id: str, k: int) -> List[Dict[str, Any]]:
    _ensure_db()
    if not jobs_for_candidate:
        return []
    try:
        rows = jobs_for_candidate(ObjectId(candidate_id), top_k=int(max(1, min(k, 50))), max_distance_km=30, tenant_id=tenant_id)
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for r in (rows or [])[:k]:
        sc = 0.0
        try:
            sc = float(r.get("score") or r.get("best_score") or 0.0)
        except Exception:
            pass
        breakdown = {}
        for key in ("title_score","semantic_score","embedding_score","skills_score","distance_score"):
            v = r.get(key)
            if isinstance(v, (int,float)):
                breakdown[key] = float(v)
        counters = {
            "must": {"have": int(r.get("skills_matched_must") or 0), "total": int(r.get("skills_total_must") or 0)},
            "nice": {"have": int(r.get("skills_matched_nice") or 0), "total": int(r.get("skills_total_nice") or 0)},
        }
        out.append({
            "score": sc,
            "candidate_id": str(candidate_id),
            "job_id": str(r.get("job_id") or r.get("_id") or ""),
            "title": r.get("title") or r.get("job_title"),
            "city": r.get("city") or r.get("city_canonical"),
            "breakdown": breakdown,
            "counters": counters,
            # pass through detailed skills lists for UI chips when available
            "skills_must_list": r.get("skills_must_list") or r.get("must_skills") or [],
            "skills_nice_list": r.get("skills_nice_list") or r.get("nice_skills") or [],
        })
    return out


def get_match_analysis_adapter(tenant_id: Optional[str], candidate_id: str, job_id: str) -> Dict[str, Any]:
    _ensure_db()
    # Try job->candidates first
    rows = match_job_to_candidates_adapter(tenant_id, job_id, k=50)
    for r in rows:
        if str(r.get("candidate_id")) == str(candidate_id):
            return r
    # Fallback candidate->jobs
    rows = match_candidate_to_jobs_adapter(tenant_id, candidate_id, k=50)
    for r in rows:
        if str(r.get("job_id")) == str(job_id):
            return r
    return {}


def get_candidate_profile_adapter(tenant_id: Optional[str], candidate_id: str) -> Dict[str, Any]:
    _ensure_db()
    try:
        q: Dict[str, Any] = {"_id": ObjectId(candidate_id)}
        if tenant_id:
            q["tenant_id"] = tenant_id
        d = db["candidates"].find_one(q)
    except Exception:
        d = None
    if not d:
        return {}
    must = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "must"]
    nice = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "needed"]
    return {
        "id": str(d.get("_id")),
        "title": d.get("title") or d.get("full_name"),
        "city": d.get("city_canonical"),
        "skills_must": must,
        "skills_nice": nice,
    }


def get_job_details_adapter(tenant_id: Optional[str], job_id: str) -> Dict[str, Any]:
    _ensure_db()
    try:
        q: Dict[str, Any] = {"_id": ObjectId(job_id)}
        if tenant_id:
            q["tenant_id"] = tenant_id
        d = db["jobs"].find_one(q)
    except Exception:
        d = None
    if not d:
        return {}
    must = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "must"]
    nice = [s.get("name") for s in (d.get("skills_detailed") or []) if s.get("category") == "needed"]
    return {
        "id": str(d.get("_id")),
        "title": d.get("title"),
        "city": d.get("city_canonical"),
        "must_have": must,
        "nice_to_have": nice,
    }


def create_outreach_message_adapter(tenant_id: Optional[str], candidate_id: str, job_ids: List[str], tone: Optional[str]) -> List[Dict[str, Any]]:
    _ensure_db()
    # Phase 1: not implemented (wire to existing outreach later)
    return []


def add_discussion_note_adapter(tenant_id: Optional[str], target_type: str, target_id: str, text: str) -> Dict[str, Any]:
    _ensure_db()
    tt = (target_type or "").lower()
    if tt not in {"candidate","job","match"}:
        return {"ok": False}
    try:
        doc = {
            "tenant_id": tenant_id,
            "target_type": tt,
            "target_id": str(target_id),
            "text": str(text or "").strip()[:4000],
            "tags": [],
            "actor_name": "copilot",
            "created_at": __import__("time").time(),
            "updated_at": __import__("time").time(),
        }
        ins = db["discussions"].insert_one(doc)
        return {"ok": True, "id": str(ins.inserted_id)}
    except Exception:
        return {"ok": False}


def get_analytics_summary_adapter(tenant_id: Optional[str], window_days: int) -> Dict[str, Any]:
    _ensure_db()
    try:
        cand = db["candidates"].count_documents({"tenant_id": tenant_id} if tenant_id else {})
        jobs = db["jobs"].count_documents({"tenant_id": tenant_id} if tenant_id else {})
        # Lightweight: estimate matches by documents in cached collection if exists
        matches = 0
        try:
            matches = db["matches_cache"].count_documents({"tenant_id": tenant_id} if tenant_id else {})
        except Exception:
            matches = 0
        top_skills = []
        try:
            pipeline = [
                {"$match": {"tenant_id": tenant_id} if tenant_id else {}},
                {"$unwind": "$skills_detailed"},
                {"$group": {"_id": "$skills_detailed.name", "c": {"$sum": 1}}},
                {"$sort": {"c": -1}},
                {"$limit": 10},
            ]
            cur = db["candidates"].aggregate(pipeline)
            top_skills = [d.get("_id") for d in cur if d.get("_id")]
        except Exception:
            top_skills = []
        return {"candidates": int(cand), "jobs": int(jobs), "matches": int(matches), "top_skills": top_skills}
    except Exception:
        return {"candidates": 0, "jobs": 0, "matches": 0, "top_skills": []}
