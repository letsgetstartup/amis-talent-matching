"""PDF helpers for generating simple candidate CV summaries for email attachments."""
from typing import Optional, Dict
from .ingest_agent import db
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
import io, time


def _safe_text(v: Optional[str]) -> str:
    return (v or "").strip()


def generate_candidate_pdf_by_share(share_id: str, tenant_id: Optional[str]) -> Optional[Dict]:
    """Build a minimalist PDF with candidate key details. Returns attachment dict.

    The PDF includes: name, title, city, years of experience, top skills, and a generated timestamp.
    """
    cand = db["candidates"].find_one({"share_id": share_id, **({"tenant_id": tenant_id} if tenant_id else {})})
    if not cand:
        return None
    full_name = _safe_text(cand.get("full_name") or "Candidate")
    title = _safe_text(cand.get("title"))
    city = _safe_text(cand.get("city_canonical") or cand.get("city"))
    years = cand.get("years_experience")
    skills = cand.get("skill_set") or []
    summary = _safe_text(cand.get("summary") or cand.get("embedding_summary"))

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=40, bottomMargin=36)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"קורות חיים - {full_name}", styles["Title"]))
    story.append(Spacer(1, 8))
    meta = [
        ["תפקיד", title or "—"],
        ["עיר", city or "—"],
        ["שנות ניסיון", str(years) if years is not None else "—"],
    ]
    t = Table(meta, colWidths=[120, 360])
    t.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 0.25, colors.grey),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("ALIGN", (0,0), (0,-1), "RIGHT"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(t)
    story.append(Spacer(1, 10))

    if skills:
        top = skills[:20]
        story.append(Paragraph("כישורים עיקריים:", styles["Heading3"]))
        story.append(Paragraph(", ".join(top), styles["Normal"]))
        story.append(Spacer(1, 8))

    if summary:
        story.append(Paragraph("תקציר:", styles["Heading3"]))
        story.append(Paragraph(summary.replace("\n", "<br/>"), styles["BodyText"]))
        story.append(Spacer(1, 8))

    story.append(Spacer(1, 12))
    story.append(Paragraph(f"נוצר בתאריך: {time.strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))

    doc.build(story)
    content = buf.getvalue()
    buf.close()
    filename = f"cv_{full_name or 'candidate'}.pdf".replace(" ", "_")
    return {"filename": filename, "content": content, "content_type": "application/pdf"}
