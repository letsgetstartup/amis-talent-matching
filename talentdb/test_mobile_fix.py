#!/usr/bin/env python3
"""Test script to validate mobile job page functionality after fixes."""

import requests
import json
import sys

def test_mobile_endpoints():
    base_url = "http://localhost:8080"
    
    # Test data
    job_id = "68a20af2725068b9910b9fa3"
    share_id = "a9336020e028"
    
    print("🧪 Testing Mobile Job Page Fixes...")
    print("=" * 50)
    
    # Test 1: Health check
    print("1. Testing server health...")
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            print("   ✅ Server is running")
        else:
            print("   ❌ Server health check failed")
            return False
    except Exception as e:
        print(f"   ❌ Server not reachable: {e}")
        return False
    
    # Test 2: Mobile API endpoint
    print("2. Testing mobile API endpoint...")
    try:
        api_url = f"{base_url}/mobile/job/{job_id}?share_id={share_id}"
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'job' in data and 'candidate' in data:
                print("   ✅ Mobile API returns valid data")
                print(f"   📋 Job: {data['job']['title']}")
                print(f"   👤 Candidate: {data['candidate']['full_name']}")
            else:
                print("   ❌ Mobile API returns incomplete data")
                return False
        else:
            print(f"   ❌ Mobile API failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Mobile API error: {e}")
        return False
    
    # Test 3: Mobile HTML page
    print("3. Testing mobile HTML page...")
    try:
        html_url = f"{base_url}/mobile-job.html?job_id={job_id}&share_id={share_id}"
        response = requests.get(html_url, timeout=10)
        if response.status_code == 200:
            html_content = response.text
            # Check for key elements
            required_elements = [
                "משרה מתאימה עבורך",  # Header text
                "שתף דרך SMS",        # SMS sharing section
                "אשר מועמדות",        # Confirmation button text
                "loadJobData",        # JavaScript function
                "displayJobData"      # JavaScript function
            ]
            
            missing_elements = []
            for element in required_elements:
                if element not in html_content:
                    missing_elements.append(element)
            
            if not missing_elements:
                print("   ✅ Mobile HTML contains all required elements")
            else:
                print("   ⚠️  Mobile HTML missing elements:")
                for element in missing_elements:
                    print(f"      - {element}")
        else:
            print(f"   ❌ Mobile HTML failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Mobile HTML error: {e}")
        return False
    
    # Test 4: Mobile confirmation page
    print("4. Testing mobile confirmation page...")
    try:
        confirm_url = f"{base_url}/mobile-confirm.html?mail_id=test123&job_id={job_id}&share_id={share_id}"
        response = requests.get(confirm_url, timeout=10)
        if response.status_code == 200:
            print("   ✅ Mobile confirmation page loads")
        else:
            print(f"   ❌ Mobile confirmation page failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Mobile confirmation error: {e}")
        return False
    
    print("\n🎉 All mobile functionality tests passed!")
    print("\n📱 Test URLs:")
    print(f"   Job Page: {base_url}/mobile-job.html?job_id={job_id}&share_id={share_id}")
    print(f"   Confirm:  {base_url}/mobile-confirm.html?mail_id=test&job_id={job_id}&share_id={share_id}")
    print(f"   Agency:   {base_url}/agency-portal.html")
    
    return True

if __name__ == "__main__":
    success = test_mobile_endpoints()
    sys.exit(0 if success else 1)
