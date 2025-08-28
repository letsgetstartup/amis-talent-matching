#!/usr/bin/env python3
"""Manual security testing script."""
import json
import requests
import time
import sys

API_BASE = "http://localhost:8080"


def test_security_fixes():
    """Test the security fixes manually."""
    print("🔒 Starting Security Validation Tests...")
    
    # Skip if external API server not running
    try:
        r = requests.get(f"{API_BASE}/ready", timeout=2)
        if r.status_code != 200:
            import pytest
            pytest.skip("External API server not running on :8080")
            return True
    except Exception:
        import pytest
        pytest.skip("External API server not running on :8080")
        return True
    # Step 1: Create test tenants
    print("\n1️⃣ Creating test tenants...")
    
    # Create tenant A
    tenant_a_data = {
        "company": "SecurityTestA",
        "name": "Admin A",
        "email": "admin-a@test.com",
        "password": "password123"
    }
    
    response = requests.post(f"{API_BASE}/auth/signup", json=tenant_a_data)
    if response.status_code != 200:
        print(f"❌ Failed to create tenant A: {response.text}")
        return False
    
    tenant_a_info = response.json()
    tenant_a_id = tenant_a_info["tenant_id"]
    
    # Create API key for tenant A
    key_response = requests.post(f"{API_BASE}/auth/apikey", json={
        "tenant_id": tenant_a_id,
        "name": "test_key_a"
    })
    
    if key_response.status_code != 200:
        print(f"❌ Failed to create API key for tenant A: {key_response.text}")
        return False
    
    tenant_a_key = key_response.json()["key"]
    
    # Create tenant B
    tenant_b_data = {
        "company": "SecurityTestB",
        "name": "Admin B",
        "email": "admin-b@test.com",
        "password": "password123"
    }
    
    response = requests.post(f"{API_BASE}/auth/signup", json=tenant_b_data)
    if response.status_code != 200:
        print(f"❌ Failed to create tenant B: {response.text}")
        return False
    
    tenant_b_info = response.json()
    tenant_b_id = tenant_b_info["tenant_id"]
    
    # Create API key for tenant B
    key_response = requests.post(f"{API_BASE}/auth/apikey", json={
        "tenant_id": tenant_b_id,
        "name": "test_key_b"
    })
    
    if key_response.status_code != 200:
        print(f"❌ Failed to create API key for tenant B: {key_response.text}")
        return False
    
    tenant_b_key = key_response.json()["key"]
    
    print(f"✅ Created tenant A: {tenant_a_id[:8]}...")
    print(f"✅ Created tenant B: {tenant_b_id[:8]}...")
    
    # Step 2: Test candidate creation and isolation
    print("\n2️⃣ Testing candidate isolation...")
    
    # Create candidate in tenant A
    candidate_response = requests.post(
        f"{API_BASE}/ingest/candidate",
        json={
            "text": "John Doe\nSoftware Engineer\nPython, FastAPI, MongoDB\nTel Aviv"
        },
        headers={"X-API-Key": tenant_a_key}
    )
    
    if candidate_response.status_code != 200:
        print(f"❌ Failed to create candidate: {candidate_response.text}")
        return False
    
    candidate_data = candidate_response.json()
    candidate_id = None
    
    # Try to extract candidate ID from ingestion response
    if "ingested" in candidate_data:
        print(f"✅ Candidate ingested successfully")
        
        # Get candidates list to find the ID
        candidates_response = requests.get(
            f"{API_BASE}/tenant/candidates",
            headers={"X-API-Key": tenant_a_key}
        )
        
        if candidates_response.status_code == 200:
            candidates = candidates_response.json()["results"]
            if candidates:
                candidate_id = candidates[0]["candidate_id"]
                print(f"✅ Found candidate ID: {candidate_id[:8]}...")
    
    if not candidate_id:
        print("❌ Could not get candidate ID")
        return False
    
    # Step 3: Test unauthorized access (should fail)
    print("\n3️⃣ Testing unauthorized access...")
    
    # Try to access candidate from tenant A using tenant B's key (should fail)
    unauthorized_response = requests.get(
        f"{API_BASE}/candidate/{candidate_id}",
        headers={"X-API-Key": tenant_b_key}
    )
    
    if unauthorized_response.status_code == 404:
        print("✅ Cross-tenant access properly blocked (404)")
    elif unauthorized_response.status_code == 401:
        print("✅ Cross-tenant access properly blocked (401)")
    else:
        print(f"❌ Security breach! Tenant B can access tenant A's candidate: {unauthorized_response.status_code}")
        print(f"Response: {unauthorized_response.text}")
        return False
    
    # Try to access without API key (should fail)
    no_auth_response = requests.get(f"{API_BASE}/candidate/{candidate_id}")
    
    if no_auth_response.status_code == 401:
        print("✅ Unauthenticated access properly blocked")
    else:
        print(f"❌ Security breach! Unauthenticated access allowed: {no_auth_response.status_code}")
        return False
    
    # Step 4: Test authorized access (should succeed)
    print("\n4️⃣ Testing authorized access...")
    
    # Access candidate from tenant A using tenant A's key (should succeed)
    authorized_response = requests.get(
        f"{API_BASE}/candidate/{candidate_id}",
        headers={"X-API-Key": tenant_a_key}
    )
    
    if authorized_response.status_code == 200:
        print("✅ Authorized access works correctly")
        candidate_data = authorized_response.json()
        print(f"   Retrieved candidate: {candidate_data['candidate'].get('title', 'N/A')}")
    else:
        print(f"❌ Authorized access failed: {authorized_response.status_code}")
        print(f"Response: {authorized_response.text}")
        return False
    
    # Step 5: Test job isolation
    print("\n5️⃣ Testing job isolation...")
    
    # Create job in tenant A
    job_response = requests.post(
        f"{API_BASE}/jobs",
        json={
            "external_job_id": "test-job-1",
            "title": "Python Developer",
            "city": "Tel Aviv",
            "description": "Looking for a Python developer",
            "must_have": ["Python", "FastAPI"],
            "nice_to_have": ["MongoDB"]
        },
        headers={"X-API-Key": tenant_a_key}
    )
    
    if job_response.status_code != 200:
        print(f"❌ Failed to create job: {job_response.text}")
        return False
    
    job_data = job_response.json()
    job_id = job_data["job_id"]
    print(f"✅ Created job: {job_id[:8]}...")
    
    # Try to access job from tenant B (should fail)
    unauthorized_job_response = requests.get(
        f"{API_BASE}/job/{job_id}",
        headers={"X-API-Key": tenant_b_key}
    )
    
    if unauthorized_job_response.status_code in [404, 401]:
        print("✅ Cross-tenant job access properly blocked")
    else:
        print(f"❌ Security breach! Tenant B can access tenant A's job: {unauthorized_job_response.status_code}")
        return False
    
    # Access job from tenant A (should succeed)
    authorized_job_response = requests.get(
        f"{API_BASE}/job/{job_id}",
        headers={"X-API-Key": tenant_a_key}
    )
    
    if authorized_job_response.status_code == 200:
        print("✅ Authorized job access works correctly")
    else:
        print(f"❌ Authorized job access failed: {authorized_job_response.status_code}")
        return False
    
    # Step 6: Test matching isolation
    print("\n6️⃣ Testing matching isolation...")
    
    # Try to match candidate from tenant B (should fail)
    unauthorized_match_response = requests.get(
        f"{API_BASE}/match/candidate/{candidate_id}",
        headers={"X-API-Key": tenant_b_key}
    )
    
    if unauthorized_match_response.status_code in [404, 401]:
        print("✅ Cross-tenant matching properly blocked")
    else:
        print(f"❌ Security breach! Tenant B can match tenant A's candidate: {unauthorized_match_response.status_code}")
        return False
    
    # Match candidate from tenant A (should succeed)
    authorized_match_response = requests.get(
        f"{API_BASE}/match/candidate/{candidate_id}",
        headers={"X-API-Key": tenant_a_key}
    )
    
    if authorized_match_response.status_code == 200:
        matches = authorized_match_response.json()["matches"]
        print(f"✅ Authorized matching works correctly ({len(matches)} matches)")
        
        # Verify that matches only include jobs from the same tenant
        if matches:
            for match in matches:
                match_job_id = match["job_id"]
                # Verify this job belongs to tenant A
                job_check = requests.get(
                    f"{API_BASE}/job/{match_job_id}",
                    headers={"X-API-Key": tenant_a_key}
                )
                if job_check.status_code != 200:
                    print(f"❌ Match includes job from different tenant: {match_job_id}")
                    return False
            print("✅ All matches are from the same tenant")
    else:
        print(f"❌ Authorized matching failed: {authorized_match_response.status_code}")
        return False
    
    # Step 7: Test security monitoring endpoints
    print("\n7️⃣ Testing security monitoring...")
    
    # Check security events
    security_response = requests.get(
        f"{API_BASE}/security/events",
        headers={"X-API-Key": tenant_a_key}
    )
    
    if security_response.status_code == 200:
        events = security_response.json()["events"]
        print(f"✅ Security monitoring working ({len(events)} events logged)")
    else:
        print(f"⚠️ Security monitoring endpoint failed: {security_response.status_code}")
    
    # Check security health
    health_response = requests.get(
        f"{API_BASE}/security/health",
        headers={"X-API-Key": tenant_a_key}
    )
    
    if health_response.status_code == 200:
        health_data = health_response.json()
        print(f"✅ Security health check: {health_data['status']}")
    else:
        print(f"⚠️ Security health endpoint failed: {health_response.status_code}")
    
    print("\n🎉 All security tests passed!")
    print("🔒 Tenant isolation is working correctly")
    print("🛡️ Security fixes are effective")
    
    return True


if __name__ == "__main__":
    try:
        success = test_security_fixes()
        if success:
            print("\n✅ SECURITY VALIDATION SUCCESSFUL")
            sys.exit(0)
        else:
            print("\n❌ SECURITY VALIDATION FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test execution failed: {e}")
        sys.exit(1)
