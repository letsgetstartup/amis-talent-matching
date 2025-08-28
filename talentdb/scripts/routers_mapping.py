from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from .ingest_agent import db  # for future persistence hooks
from .header_mapping import canon_header, CandidateHeaderPolicy  # type: ignore

router = APIRouter(prefix="/mapping", tags=["mapping"])


class PreviewRequest(BaseModel):
    kind: str  # 'candidate' | 'job'
    headers: List[str]
    sample_rows: Optional[List[List[str]]] = None


@router.post("/preview")
def mapping_preview(body: PreviewRequest):
    kind = body.kind.lower().strip()
    if kind not in ("candidate", "job"):
        raise HTTPException(status_code=400, detail="invalid_kind")
    headers = [h.strip() for h in (body.headers or [])]
    if not headers:
        raise HTTPException(status_code=400, detail="no_headers")

    # Proposed mapping using shared canonicalization
    proposed: Dict[str, str] = {h: canon_header(h, kind=kind) for h in headers}

    authoritative: Dict[str, Optional[str]] = {"full_name": None, "city": None}
    if kind == "candidate":
        policy = CandidateHeaderPolicy.from_headers(headers)
        authoritative = policy.authoritative_sources()

    mapped = sum(1 for h in headers if proposed.get(h) != h)
    total = len(headers)
    coverage = {
        "mapped": mapped,
        "total": total,
        "percent": round((mapped / total) * 100, 1) if total else 0.0,
    }

    sample_preview: List[Dict[str, Any]] = []
    if body.sample_rows:
        # Show at most first 3 rows
        for i, row in enumerate(body.sample_rows[:3]):
            before = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
            after = {proposed[h]: before.get(h, "") for h in headers}
            sample_preview.append({"row_idx": i + 1, "before": before, "after": after})

    warnings: List[str] = []
    if kind == "candidate":
        if authoritative.get("full_name") is None:
            warnings.append("authoritative_full_name_missing: expected 'שם מועמד'")
        if authoritative.get("city") is None:
            warnings.append("authoritative_city_missing: expected 'שם ישוב'/'שם יישוב'")

    return {
        "proposed": proposed,
        "authoritative": authoritative,
        "coverage": coverage,
        "warnings": warnings,
        "sample_preview": sample_preview,
    }
