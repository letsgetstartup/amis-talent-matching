"""Mobile API endpoints for job viewing and application confirmation.

These endpoints are optimized for mobile devices and SMS-driven workflows.
They provide lightweight, mobile-friendly interfaces for:
- Viewing job details with candidate context
- Confirming job applications from mobile devices
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from .ingest_agent import db, jobs_for_candidate
from .mailer import send_email
from .auth import require_tenant
from bson import ObjectId
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mobile", tags=["mobile"])


class MobileConfirmReq(BaseModel):
    share_id: str
    job_id: str


def _safe_objectid(id_str: str) -> Optional[ObjectId]:
    """Safely convert string to ObjectId."""
    try:
        return ObjectId(id_str)
    except Exception:
        return None


def _get_candidate_by_share_id(share_id: str) -> Optional[Dict[str, Any]]:
    """Get candidate by share_id."""
    return db["candidates"].find_one({"share_id": share_id})


def _get_job_by_id(job_id: str) -> Optional[Dict[str, Any]]:
    """Get job by ObjectId string."""
    obj_id = _safe_objectid(job_id)
    if not obj_id:
        return None
    return db["jobs"].find_one({"_id": obj_id})


def _calculate_match_info(candidate: Dict[str, Any], job: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate why this job matches the candidate."""
    cand_skills = set(candidate.get("skills", []))
    job_skills = set(job.get("skills", []))
    
    # Find matching skills
    matching_skills = list(cand_skills.intersection(job_skills))
    
    # Calculate match score (simple version)
    if cand_skills and job_skills:
        match_score = len(matching_skills) / len(job_skills.union(cand_skills))
    else:
        match_score = 0.0
    
    # Generate match reason
    reasons = []
    if matching_skills:
        reasons.append(f"יש לך {len(matching_skills)} כישורים רלוונטיים")
    
    job_city = job.get("city", "").strip()
    cand_city = candidate.get("city", "").strip()
    if job_city and cand_city and job_city.lower() == cand_city.lower():
        reasons.append("המשרה באזור המגורים שלך")
    
    exp_years = candidate.get("experience_years", 0)
    if exp_years and exp_years >= 2:
        reasons.append("יש לך ניסיון רלוונטי")
    
    if not reasons:
        reasons.append("המשרה מתאימה לפרופיל שלך")
    
    return {
        "matching_skills": matching_skills[:5],  # Top 5 skills
        "match_score": round(match_score, 2),
        "reason": " • ".join(reasons)
    }


@router.get("/job/{job_id}")
def get_mobile_job(job_id: str, share_id: Optional[str] = None):
    """
    Get job details optimized for mobile viewing.
    
    Args:
        job_id: Job ObjectId string
        share_id: Optional candidate share_id for personalization
    
    Returns:
        Job details with optional candidate context and match information
    """
    # Get job
    job = _get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    
    # Prepare response
    response = {
        "job": {
            "id": str(job["_id"]),
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "description": job.get("description", ""),
            "requirements": job.get("requirements", ""),
            "salary": job.get("salary", ""),
            "location": job.get("location", "") or job.get("city", ""),
            "city": job.get("city", ""),
            "employment_type": job.get("employment_type", ""),
            "external_job_id": job.get("external_job_id", ""),
            "agency_email": job.get("agency_email", ""),
            "skills": job.get("skills", [])
        }
    }
    
    # Add candidate context if share_id provided
    if share_id:
        candidate = _get_candidate_by_share_id(share_id)
        if candidate:
            response["candidate"] = {
                "id": str(candidate["_id"]),
                "share_id": candidate["share_id"],
                "full_name": candidate.get("full_name", ""),
                "skills": candidate.get("skills", []),
                "city": candidate.get("city", ""),
                "experience_years": candidate.get("experience_years", 0)
            }
            
            # Calculate match information
            response["match_info"] = _calculate_match_info(candidate, job)
        else:
            logger.warning(f"Candidate not found for share_id: {share_id}")
    
    return response


@router.post("/confirm")
def mobile_confirm_application(req: MobileConfirmReq):
    """
    Confirm job application from mobile device.
    
    This endpoint handles the same confirmation logic as /confirm/apply
    but is optimized for mobile responses.
    """
    share_id = req.share_id
    job_id = req.job_id
    
    # Get candidate
    candidate = _get_candidate_by_share_id(share_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="candidate_not_found")
    
    # Get job
    job = _get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    
    # Extract job details
    external_job_id = job.get("external_job_id", "")
    agency_email = job.get("agency_email", "")
    
    if not external_job_id:
        raise HTTPException(status_code=400, detail="job_missing_external_id")
    
    # Determine recipient email
    recipient_email = agency_email
    if not recipient_email:
        # Fallback: try to find tenant admin email
        # This is a simplified version - in production you might want more sophisticated logic
        recipient_email = "admin@example.com"  # Fallback
        logger.warning(f"No agency email for job {job_id}, using fallback recipient")
    
    if not recipient_email:
        raise HTTPException(status_code=400, detail="no_recipient_email")
    
    # Generate email subject and body
    subject = f"#SCAGENT# {external_job_id}"
    candidate_name = candidate.get("full_name", "") or candidate.get("title", "") or str(candidate["_id"])
    job_title = job.get("title", "")
    
    body = f"""מועמד/ת חדש/ה להצעת העבודה!

פרטי המועמד/ת:
שם: {candidate_name}
תפקיד מבוקש: {job_title}
מזהה חיצוני: {external_job_id}

המועמד/ת אישר/ה את המועמדות דרך המערכת המובילה.
קורות החיים מצורפים למייל זה.

תוכל/י לפנות למועמד/ת ישירות לתיאום ריאיון.

בברכה,
מערכת TalentDB
"""
    
    # Generate and attach CV PDF
    try:
        from .pdf_utils import generate_candidate_pdf_by_share
        if generate_candidate_pdf_by_share:
            pdf_content = generate_candidate_pdf_by_share(share_id, None)
            if pdf_content:
                attachments = [{
                    "filename": f"CV_{candidate_name.replace(' ', '_')}.pdf",
                    "content": pdf_content,
                    "content_type": "application/pdf"
                }]
                body += f"\n\nקורות החיים מצורפים כקובץ PDF."
            else:
                attachments = None
                body += f"\n\nקישור לקורות החיים: /share/candidate/{share_id}"
        else:
            attachments = None
            body += f"\n\nקישור לקורות החיים: /share/candidate/{share_id}"
    except Exception as e:
        logger.error(f"Error generating PDF for candidate {share_id}: {e}")
        attachments = None
        body += f"\n\nקישור לקורות החיים: /share/candidate/{share_id}"
    
    # Send email
    try:
        mail_id = send_email(recipient_email, subject, body, attachments)
    except Exception as e:
        logger.error(f"Error sending confirmation email: {e}")
        raise HTTPException(status_code=500, detail="email_send_failed")
    
    # Log analytics event
    try:
        db["analytics_events"].insert_one({
            "type": "mobile_candidate_confirmed",
            "payload": {
                "share_id": share_id,
                "job_id": job_id,
                "external_job_id": external_job_id,
                "recipient_email": recipient_email,
                "user_agent": "mobile"
            },
            "timestamp": int(time.time())
        })
    except Exception as e:
        logger.warning(f"Error logging analytics event: {e}")
    
    return {
        "status": "success",
        "message": "Application confirmed successfully",
        "mail_id": mail_id,
        "job_title": job_title,
        "external_job_id": external_job_id
    }
