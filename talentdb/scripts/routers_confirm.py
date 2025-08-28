from fastapi import APIRouter, HTTPException, Depends
from typing import Optional
from .ingest_agent import db
from .mailer import send_email
from .auth import require_tenant
from pydantic import BaseModel
try:
    from .pdf_utils import generate_candidate_pdf_by_share
except Exception:
    generate_candidate_pdf_by_share = None  # type: ignore

router = APIRouter(prefix="/confirm", tags=["confirm"])


@router.get("/apply")
def confirm_apply(token: str):
    """Token format (simple): share_id:job_id
    In production use signed tokens. This is a dev placeholder.
    """
    if ":" not in token:
        raise HTTPException(status_code=400, detail="invalid_token")
    share_id, job_id = token.split(":", 1)
    cand = db["candidates"].find_one({"share_id": share_id})
    job = None
    from bson import ObjectId
    try:
        job = db["jobs"].find_one({"_id": ObjectId(job_id)})
    except Exception:
        job = None
    if not cand or not job:
        raise HTTPException(status_code=404, detail="not_found")
    ext = job.get("external_job_id") or ""
    to = job.get("agency_email") or ""
    if not to or not ext:
        raise HTTPException(status_code=400, detail="job_missing_agency_email_or_external_id")
    subject = f"#scoreagents# {ext}"
    body = f"Candidate {cand.get('full_name') or cand.get('title') or cand.get('_id')} confirms application to job {ext}."
    # Attach CV placeholder: in future, attach S3 file; for now include share link
    body += f"\nShare: /share/candidate/{share_id}"
    mail_id = send_email(to, subject, body)
    # Log event
    db["analytics_events"].insert_one({
        "type": "candidate_confirmed",
        "payload": {"share_id": share_id, "job_id": job_id, "external_job_id": ext},
    })
    return {"status": "ok", "mail_id": mail_id, "subject": subject}


class ConfirmPostReq(BaseModel):
    share_id: str
    job_id: str


@router.post("/apply")
def confirm_apply_post(req: ConfirmPostReq, tenant_id: str = Depends(require_tenant)):
    """Send confirmation email to agency email with attached PDF CV.

    Priority for recipient:
      1) Tenant admin email (first user in tenant)
      2) job.agency_email
    Subject: "#SCAGENT# {external_job_id or job_id}"
    """
    # Resolve candidate and job
    cand = db["candidates"].find_one({"share_id": req.share_id, "tenant_id": tenant_id})
    if not cand:
        raise HTTPException(status_code=404, detail="share_id_not_found")
    from bson import ObjectId
    try:
        oid = ObjectId(req.job_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_job_id")
    job = db["jobs"].find_one({"_id": oid, "tenant_id": tenant_id})
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")

    # Determine recipient email
    tenant = db["tenants"].find_one({"_id": cand.get("tenant_id")}) if cand.get("tenant_id") else None
    admin_user = db["users"].find_one({"tenant_id": tenant_id}, sort=[("created_at", 1)])
    to_email = None
    if admin_user and admin_user.get("email"):
        to_email = admin_user["email"]
    if not to_email:
        to_email = job.get("agency_email")
    if not to_email:
        raise HTTPException(status_code=400, detail="no_recipient_email")

    ext = job.get("external_job_id") or req.job_id
    subject = f"#SCAGENT# {ext}"
    body = (
        f"Candidate {cand.get('full_name') or cand.get('title') or str(cand.get('_id'))} confirms application.\n"
        f"Share link: /share/candidate/{req.share_id}\n"
        f"Job: {job.get('title') or ''} ({ext})\n"
    )

    # Generate PDF attachment (best effort)
    attachments = []
    if generate_candidate_pdf_by_share:
        try:
            att = generate_candidate_pdf_by_share(req.share_id, tenant_id)  # type: ignore
            if att:
                attachments.append(att)
        except Exception as e:
            # continue without attachment
            print("[CONFIRM] PDF generation failed:", e)

    mail_id = send_email(to_email, subject, body, attachments=attachments)
    db["analytics_events"].insert_one({
        "type": "candidate_confirmed",
        "payload": {"share_id": req.share_id, "job_id": req.job_id, "external_job_id": job.get("external_job_id")},
    })
    return {"status": "ok", "mail_id": mail_id, "subject": subject, "to": to_email}
