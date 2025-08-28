#!/usr/bin/env python3
"""
Test script to validate mobile job page fixes
"""

import requests
import json
import sys

def test_mobile_api():
    """Test the mobile API endpoint"""
    print("🔍 Testing mobile API endpoint...")
    
    # Test with a valid job ID
    job_id = "68a20af2725068b9910b9fa4"
    url = f"http://localhost:8080/mobile/job/{job_id}"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ API response received")
            
            # Check data structure
            if 'job' in data:
                job = data['job']
                print(f"   Job title: {job.get('title', 'N/A')}")
                print(f"   Company: {job.get('company', 'N/A')}")
                print(f"   Description: {len(str(job.get('description', ''))) if job.get('description') else 0} chars")
                print(f"   Requirements type: {type(job.get('requirements', None))}")
                
                if isinstance(job.get('requirements'), dict):
                    reqs = job['requirements']
                    must_have = len(reqs.get('must_have_skills', []))
                    nice_to_have = len(reqs.get('nice_to_have_skills', []))
                    print(f"   Must have skills: {must_have}")
                    print(f"   Nice to have skills: {nice_to_have}")
                
            return True
        else:
            print(f"   ❌ API returned {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ API test failed: {e}")
        return False

def test_mobile_html():
    """Test the mobile HTML page"""
    print("\n🔍 Testing mobile HTML page...")
    
    url = "http://localhost:8080/mobile-job.html?job_id=68a20af2725068b9910b9fa4&share_id=test123"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            html = response.text
            
            # Check for critical elements
            checks = [
                ("HTML structure", "<html" in html),
                ("Job title element", 'id="jobTitle"' in html),
                ("Job company element", 'id="jobCompany"' in html),
                ("Job description element", 'id="jobDescription"' in html),
                ("Job requirements element", 'id="jobRequirements"' in html),
                ("Confirm button", 'id="confirmBtn"' in html),
                ("formatText function", "function formatText" in html),
                ("formatRequirements function", "function formatRequirements" in html),
                ("Error handling", "typeof text !== 'string'" in html),
            ]
            
            for check_name, passed in checks:
                status = "✅" if passed else "❌"
                print(f"   {status} {check_name}")
            
            return all(passed for _, passed in checks)
        else:
            print(f"   ❌ HTML page returned {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ HTML test failed: {e}")
        return False

def test_health():
    """Test server health"""
    print("🔍 Testing server health...")
    
    try:
        response = requests.get("http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            print("   ✅ Server is healthy")
            return True
        else:
            print(f"   ❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Health check failed: {e}")
        return False

def main():
    print("🚀 Starting mobile page fixes validation...\n")
    
    # Run all tests
    tests = [
        ("Server Health", test_health),
        ("Mobile API", test_mobile_api),
        ("Mobile HTML", test_mobile_html),
    ]
    
    results = []
    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))
    
    # Summary
    print("\n" + "="*50)
    print("📊 TEST SUMMARY:")
    print("="*50)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*50)
    if all_passed:
        print("🎉 ALL TESTS PASSED! Mobile page should work correctly.")
    else:
        print("⚠️  SOME TESTS FAILED. Please check the issues above.")
    print("="*50)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
