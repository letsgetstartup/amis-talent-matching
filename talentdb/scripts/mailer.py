"""Mailer module.

- Always logs messages into Mongo 'outbox' (for audit/testing).
- If Gmail SMTP credentials are configured, also sends the email with optional attachments.

Environment variables:
  GMAIL_USER, GMAIL_APP_PASSWORD  -> when both set, enable SMTP send via smtp.gmail.com:465
  MAIL_FROM                       -> optional display from address (defaults to GMAIL_USER)
"""
from typing import List, Optional
from .ingest_agent import db
import time, os, ssl, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def _safe_attachment_manifest(atts: Optional[List[dict]]) -> List[dict]:
    """Store only lightweight metadata about attachments in outbox (no raw bytes)."""
    out: List[dict] = []
    for a in atts or []:
        if not isinstance(a, dict):
            continue
        out.append({
            "filename": a.get("filename"),
            "content_type": a.get("content_type") or a.get("mime") or "application/octet-stream",
            "size": len(a.get("content") or b"") if isinstance(a.get("content"), (bytes, bytearray)) else None,
        })
    return out


def send_email(to: str, subject: str, body: str, attachments: Optional[List[dict]] = None) -> str:
    # 1) Always enqueue into outbox (audit trail)
    msg_doc = {
        "to": to,
        "subject": subject,
        "body": body,
        "attachments": _safe_attachment_manifest(attachments),
        "created_at": int(time.time()),
        "status": "queued",
    }
    ins = db["outbox"].insert_one(msg_doc)
    outbox_id = str(ins.inserted_id)

    # 2) Attempt to send via Gmail SMTP if configured
    user = os.getenv("GMAIL_USER")
    app_pw = os.getenv("GMAIL_APP_PASSWORD")
    if not user or not app_pw:
        print(f"[OUTBOX-ONLY] to={to} subject={subject} (Gmail creds not set)")
        return outbox_id

    mail_from = os.getenv("MAIL_FROM", user)
    try:
        mime = MIMEMultipart()
        mime["From"] = mail_from
        mime["To"] = to
        mime["Subject"] = subject
        mime.attach(MIMEText(body or "", "plain", _charset="utf-8"))

        for att in attachments or []:
            try:
                filename = att.get("filename") or "attachment.bin"
                content = att.get("content")
                ctype = att.get("content_type") or "application/octet-stream"
                if isinstance(content, (bytes, bytearray)):
                    part = MIMEBase(*ctype.split("/", 1)) if "/" in ctype else MIMEBase("application", "octet-stream")
                    part.set_payload(content)
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename=\"{filename}\"")
                    mime.attach(part)
            except Exception as e:
                print("[MAILER] attachment skip due to error:", e)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(user, app_pw)
            server.sendmail(mail_from, [to], mime.as_string())
        db["outbox"].update_one({"_id": ins.inserted_id}, {"$set": {"status": "sent", "sent_at": int(time.time())}})
        print(f"[MAILER] sent to={to} subject={subject}")
    except Exception as e:
        db["outbox"].update_one({"_id": ins.inserted_id}, {"$set": {"status": "error", "error": str(e)}})
        print(f"[MAILER] send failed: {e}")
    return outbox_id
