#!/usr/bin/env python3
"""
End-to-end test for the email confirmation feature.
Tests the flow: signup -> create job -> ingest candidate -> confirm application
"""

import requests
import json
import time
import os
import sys

API_BASE = "http://localhost:8080"

def test_email_confirmation_flow():
    """Test the complete email confirmation workflow."""
    print("🚀 Testing email confirmation feature...")
    
    # 1. Sign up a new tenant
    print("1️⃣ Creating tenant...")
    signup_data = {
        "company": "TestCorp",
        "name": "Test Admin", 
        "email": f"test_{int(time.time())}@example.com",
        "password": "test1234"
    }
    
    # Skip gracefully if API not running in this environment
    try:
        resp = requests.get(f"{API_BASE}/ready", timeout=2)
    except Exception:
        import pytest
        pytest.skip("External API server not running on :8080")
        return True
    resp = requests.post(f"{API_BASE}/auth/signup", json=signup_data)
    if resp.status_code != 200:
        print(f"❌ Signup failed: {resp.status_code} {resp.text}")
        return False
    
    signup_result = resp.json()
    tenant_id = signup_result["tenant_id"]
    token = signup_result["token"]
    print(f"✅ Tenant created: {tenant_id}")
    
    # 2. Create API key
    print("2️⃣ Creating API key...")
    key_resp = requests.post(f"{API_BASE}/auth/apikey", json={
        "tenant_id": tenant_id,
        "name": "test-key"
    })
    if key_resp.status_code != 200:
        print(f"❌ API key creation failed: {key_resp.status_code}")
        return False
    
    api_key = key_resp.json()["key"]
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    print(f"✅ API key created")
    
    # 3. Create a test job
    print("3️⃣ Creating job...")
    job_data = {
        "external_job_id": "TEST-001",
        "title": "Python Developer",
        "city": "תל אביב",
        "must_have": ["Python", "FastAPI"],
        "nice_to_have": ["React"],
        "description": "Build amazing web applications",
        "agency_email": "agency@testcorp.com"
    }
    
    job_resp = requests.post(f"{API_BASE}/jobs", json=job_data, headers=headers)
    if job_resp.status_code != 200:
        print(f"❌ Job creation failed: {job_resp.status_code} {job_resp.text}")
        return False
    
    job_id = job_resp.json()["job_id"]
    print(f"✅ Job created: {job_id}")
    
    # 4. Ingest a candidate
    print("4️⃣ Ingesting candidate...")
    candidate_data = {
        "text": "שם: איתי כהן\nעיר: תל אביב\nניסיון תעסוקתי: 4 שנים בפיתוח Python ו-FastAPI\nכישורים: Python, FastAPI, React, Docker, PostgreSQL\nתואר: תואר ראשון במדעי המחשב",
        "filename": "test_cv.txt"
    }
    
    cand_resp = requests.post(f"{API_BASE}/ingest/candidate", json=candidate_data, headers=headers)
    if cand_resp.status_code != 200:
        print(f"❌ Candidate ingestion failed: {cand_resp.status_code} {cand_resp.text}")
        return False
    
    share_id = cand_resp.json()["share_id"]
    print(f"✅ Candidate ingested: {share_id}")
    
    # 5. Test confirm application endpoint
    print("5️⃣ Testing confirm application...")
    confirm_data = {
        "share_id": share_id,
        "job_id": job_id
    }
    
    confirm_resp = requests.post(f"{API_BASE}/confirm/apply", json=confirm_data, headers=headers)
    if confirm_resp.status_code != 200:
        print(f"❌ Confirm application failed: {confirm_resp.status_code} {confirm_resp.text}")
        return False
    
    confirm_result = confirm_resp.json()
    print(f"✅ Confirmation sent!")
    print(f"   📧 To: {confirm_result.get('to')}")
    print(f"   📝 Subject: {confirm_result.get('subject')}")
    print(f"   📮 Mail ID: {confirm_result.get('mail_id')}")
    
    # 6. Verify personal letter can be generated
    print("6️⃣ Testing personal letter generation...")
    letter_data = {"share_id": share_id, "force": True}
    letter_resp = requests.post(f"{API_BASE}/personal-letter", json=letter_data, headers=headers)
    
    if letter_resp.status_code == 200:
        letter_result = letter_resp.json()
        print(f"✅ Personal letter generated ({letter_result.get('match_count', 0)} matches)")
    else:
        print(f"⚠️ Personal letter generation failed (this is optional): {letter_resp.status_code}")
    
    print("\n🎉 All tests passed! Email confirmation feature is working.")
    print("\n📋 Summary:")
    print(f"   • Tenant: {tenant_id}")
    print(f"   • Job: {job_id} (TEST-001)")  
    print(f"   • Candidate: {share_id}")
    print(f"   • Email sent to: {confirm_result.get('to')}")
    
    return True

if __name__ == "__main__":
    try:
        # Test if API is reachable
        resp = requests.get(f"{API_BASE}/ready", timeout=5)
        if resp.status_code != 200:
            print(f"❌ API not ready: {resp.status_code}")
            sys.exit(1)
        
        success = test_email_confirmation_flow()
        sys.exit(0 if success else 1)
        
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Make sure the server is running on port 8080.")
        print("   Run: cd talentdb && ./run_api.sh")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        sys.exit(1)
