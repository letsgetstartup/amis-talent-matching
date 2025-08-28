#!/usr/bin/env python3
"""
Deep UI Analysis Script for Agency Portal
This script will test all UI components and identify issues
"""

import requests
import json
import time
from datetime import datetime

API_BASE = "http://localhost:8080"

def log_test(test_name, result, details=""):
    """Log test results"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    status = "✅ PASS" if result else "❌ FAIL"
    print(f"[{timestamp}] {status} {test_name}")
    if details:
        print(f"    {details}")

def test_api_health():
    """Test basic API health"""
    try:
        response = requests.get(f"{API_BASE}/health", timeout=5)
        log_test("API Health Check", response.status_code == 200, f"Status: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        log_test("API Health Check", False, f"Error: {e}")
        return False

def test_authentication():
    """Test authentication flow"""
    try:
        # Test signup/login
        signup_data = {
            "company": "Test Agency Analysis",
            "name": "Test User",
            "email": f"test_analysis_{int(time.time())}@test.com",
            "password": "test123"
        }
        
        response = requests.post(f"{API_BASE}/auth/signup", json=signup_data)
        if response.status_code != 200:
            log_test("Authentication - Signup", False, f"Status: {response.status_code}")
            return None
            
        auth_data = response.json()
        tenant_id = auth_data.get('tenant_id')
        token = auth_data.get('token')
        
        # Generate API key
        api_key_response = requests.post(
            f"{API_BASE}/auth/apikey",
            json={"tenant_id": tenant_id, "name": "analysis"}
        )
        
        if api_key_response.status_code != 200:
            log_test("Authentication - API Key", False, f"Status: {api_key_response.status_code}")
            return None
            
        api_key = api_key_response.json().get('key')
        log_test("Authentication Flow", True, f"Tenant: {tenant_id}, API Key generated")
        
        return {
            'tenant_id': tenant_id,
            'token': token,
            'api_key': api_key,
            'headers': {'X-API-Key': api_key}
        }
        
    except Exception as e:
        log_test("Authentication Flow", False, f"Error: {e}")
        return None

def test_jobs_endpoint(auth):
    """Test jobs endpoint functionality"""
    try:
        response = requests.get(f"{API_BASE}/tenant/jobs", headers=auth['headers'])
        log_test("Jobs Endpoint", response.status_code == 200, f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            job_count = data.get('total', 0)
            log_test("Jobs Data Retrieval", True, f"Found {job_count} jobs")
            return data
        return None
        
    except Exception as e:
        log_test("Jobs Endpoint", False, f"Error: {e}")
        return None

def test_candidates_endpoint(auth):
    """Test candidates endpoint functionality"""
    try:
        response = requests.get(f"{API_BASE}/tenant/candidates", headers=auth['headers'])
        log_test("Candidates Endpoint", response.status_code == 200, f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            candidate_count = data.get('total', 0)
            log_test("Candidates Data Retrieval", True, f"Found {candidate_count} candidates")
            return data
        return None
        
    except Exception as e:
        log_test("Candidates Endpoint", False, f"Error: {e}")
        return None

def test_job_matches(auth, candidate_id):
    """Test job matching functionality"""
    try:
        response = requests.get(f"{API_BASE}/match/candidate/{candidate_id}?k=5", headers=auth['headers'])
        success = response.status_code == 200
        
        if success:
            data = response.json()
            matches = data.get('matches', [])
            log_test("Job Matches", True, f"Found {len(matches)} matches for candidate {candidate_id}")
            return data
        else:
            log_test("Job Matches", False, f"Status: {response.status_code} for candidate {candidate_id}")
            return None
            
    except Exception as e:
        log_test("Job Matches", False, f"Error: {e}")
        return None

def test_personal_letter(auth, share_id):
    """Test personal letter functionality"""
    try:
        # Test letter availability
        avail_response = requests.get(f"{API_BASE}/personal-letter/availability/{share_id}", headers=auth['headers'])
        
        if avail_response.status_code == 200:
            avail_data = avail_response.json()
            log_test("Personal Letter Availability", True, f"Available: {avail_data.get('available', False)}")
            
            # Try to get the letter
            letter_response = requests.get(f"{API_BASE}/personal-letter/{share_id}", headers=auth['headers'])
            
            if letter_response.status_code == 200:
                letter_data = letter_response.json()
                log_test("Personal Letter Retrieval", True, f"Letter found with {letter_data.get('word_count', 0)} words")
                return letter_data
            elif letter_response.status_code == 404:
                log_test("Personal Letter Retrieval", False, "Letter not found (404)")
                return None
            else:
                log_test("Personal Letter Retrieval", False, f"Status: {letter_response.status_code}")
                return None
        else:
            log_test("Personal Letter Availability", False, f"Status: {avail_response.status_code}")
            return None
            
    except Exception as e:
        log_test("Personal Letter", False, f"Error: {e}")
        return None

def test_candidate_details(auth, candidate_id):
    """Test candidate details retrieval"""
    try:
        response = requests.get(f"{API_BASE}/candidate/{candidate_id}", headers=auth['headers'])
        success = response.status_code == 200
        
        if success:
            data = response.json()
            # Handle both direct candidate data and wrapped data
            candidate = data.get('candidate', data) if 'candidate' in data else data
            log_test("Candidate Details", True, f"Retrieved details for {candidate.get('full_name', 'unknown')}")
            return candidate
        else:
            log_test("Candidate Details", False, f"Status: {response.status_code}")
            return None
            
    except Exception as e:
        log_test("Candidate Details", False, f"Error: {e}")
        return None

def test_global_data():
    """Test if there's any existing data in the system"""
    try:
        # Check global jobs
        jobs_response = requests.get(f"{API_BASE}/jobs")
        if jobs_response.status_code == 200:
            jobs_data = jobs_response.json()
            job_count = len(jobs_data.get('results', []))
            log_test("Global Jobs Check", True, f"Found {job_count} jobs globally")
        
        # Check global candidates
        candidates_response = requests.get(f"{API_BASE}/candidates")
        if candidates_response.status_code == 200:
            candidates_data = candidates_response.json()
            candidate_count = len(candidates_data.get('results', []))
            log_test("Global Candidates Check", True, f"Found {candidate_count} candidates globally")
            return candidates_data.get('results', [])
        
        return []
        
    except Exception as e:
        log_test("Global Data Check", False, f"Error: {e}")
        return []

def analyze_ui_components():
    """Main analysis function"""
    print("\n" + "="*80)
    print("🔍 DEEP UI COMPONENT ANALYSIS - AGENCY PORTAL")
    print("="*80)
    
    # Step 1: Test API health
    if not test_api_health():
        print("\n❌ CRITICAL: API is not responding. Cannot continue analysis.")
        return
    
    # Step 2: Test authentication
    auth = test_authentication()
    if not auth:
        print("\n❌ CRITICAL: Authentication failed. Cannot test tenant-specific features.")
        return
    
    # Step 3: Check for existing global data
    print("\n📊 Checking for existing data in system...")
    global_candidates = test_global_data()
    
    # Step 4: Test tenant-specific endpoints
    print("\n🏢 Testing tenant-specific endpoints...")
    jobs_data = test_jobs_endpoint(auth)
    candidates_data = test_candidates_endpoint(auth)
    
    # Step 5: Test UI-specific functionality if we have candidates
    print("\n🧪 Testing UI component functionality...")
    
    if global_candidates:
        print(f"\n📋 Testing functionality with {len(global_candidates)} global candidates...")
        
        for i, candidate in enumerate(global_candidates[:3]):  # Test first 3 candidates
            candidate_id = candidate.get('candidate_id')
            share_id = candidate.get('share_id')
            
            print(f"\n--- Testing Candidate {i+1}: {candidate.get('full_name', 'Unknown')} ---")
            
            # Test candidate details
            candidate_details = test_candidate_details(auth, candidate_id)
            
            # Test job matches
            matches = test_job_matches(auth, candidate_id)
            
            # Test personal letter if share_id exists
            if share_id:
                letter = test_personal_letter(auth, share_id)
            else:
                log_test("Personal Letter", False, "No share_id available")
    
    else:
        print("\n⚠️  No candidate data found for testing UI components")
    
    print("\n" + "="*80)
    print("📋 ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    analyze_ui_components()
