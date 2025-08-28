"""Security audit logging module for tenant isolation monitoring."""
import time
from typing import Optional
from fastapi import Request
from .ingest_agent import db


def audit_log(
    tenant_id: str,
    action: str,
    resource: str,
    resource_id: str,
    user_ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    success: bool = True,
    details: Optional[dict] = None
):
    """Log security-relevant actions for audit trail."""
    try:
        audit_record = {
            "tenant_id": tenant_id,
            "action": action,
            "resource": resource,
            "resource_id": resource_id,
            "timestamp": time.time(),
            "iso_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "ip": user_ip,
            "user_agent": user_agent,
            "success": success,
            "details": details or {}
        }
        db["security_audit"].insert_one(audit_record)
    except Exception as e:
        # Never let audit logging break the main flow
        print(f"[AUDIT ERROR] Failed to log action {action}: {e}")


def log_data_access(tenant_id: str, resource_type: str, resource_id: str, request: Request = None):
    """Log data access events."""
    user_ip = request.client.host if request else None
    user_agent = request.headers.get("user-agent") if request else None
    
    audit_log(
        tenant_id=tenant_id,
        action="data_access",
        resource=resource_type,
        resource_id=resource_id,
        user_ip=user_ip,
        user_agent=user_agent,
        success=True
    )


def log_tenant_boundary_violation(
    attempted_tenant: str,
    actual_tenant: str,
    resource_type: str,
    resource_id: str,
    request: Request = None
):
    """Log attempted cross-tenant access violations."""
    user_ip = request.client.host if request else None
    user_agent = request.headers.get("user-agent") if request else None
    
    audit_log(
        tenant_id=attempted_tenant,
        action="tenant_boundary_violation",
        resource=resource_type,
        resource_id=resource_id,
        user_ip=user_ip,
        user_agent=user_agent,
        success=False,
        details={
            "attempted_tenant": attempted_tenant,
            "actual_resource_tenant": actual_tenant,
            "violation_type": "cross_tenant_access"
        }
    )


def log_auth_event(tenant_id: str, action: str, success: bool, request: Request = None, details: dict = None):
    """Log authentication and authorization events."""
    user_ip = request.client.host if request else None
    user_agent = request.headers.get("user-agent") if request else None
    
    audit_log(
        tenant_id=tenant_id,
        action=action,
        resource="auth",
        resource_id=tenant_id,
        user_ip=user_ip,
        user_agent=user_agent,
        success=success,
        details=details or {}
    )


def get_security_events(tenant_id: str, hours: int = 24, limit: int = 100):
    """Retrieve recent security events for a tenant."""
    cutoff_time = time.time() - (hours * 3600)
    
    events = list(
        db["security_audit"]
        .find(
            {
                "tenant_id": tenant_id,
                "timestamp": {"$gte": cutoff_time}
            }
        )
        .sort([("timestamp", -1)])
        .limit(limit)
    )
    
    for event in events:
        event["_id"] = str(event["_id"])
    
    return events


def get_violation_summary(hours: int = 24):
    """Get summary of security violations across all tenants."""
    cutoff_time = time.time() - (hours * 3600)
    
    violations = list(
        db["security_audit"]
        .find(
            {
                "action": "tenant_boundary_violation",
                "timestamp": {"$gte": cutoff_time}
            }
        )
        .sort([("timestamp", -1)])
        .limit(50)
    )
    
    for violation in violations:
        violation["_id"] = str(violation["_id"])
    
    return {
        "total_violations": len(violations),
        "recent_violations": violations,
        "time_window_hours": hours
    }
