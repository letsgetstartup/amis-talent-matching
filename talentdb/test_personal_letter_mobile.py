#!/usr/bin/env python3
"""
Test personal letter with mobile job links using real authentication
"""

import requests
import json
from scripts.ingest_agent import db

def test_personal_letter_with_mobile_links():
    """Test the full personal letter generation with mobile links"""
    print("🔧 Testing personal letter with mobile job links...")
    
    # Use a real API key and tenant
    api_key = "6Szs8Yqseqrkr_gWCsZSIfmDWm9xSCvm0yZhp9uGmeA"  # From the database
    tenant_id = "68a20abe725068b9910b9f9a"
    
    # Get a candidate with the matching tenant
    candidate = db['candidates'].find_one({'tenant_id': tenant_id}, {'share_id': 1, '_id': 1})
    if not candidate:
        print(f"❌ No candidates found for tenant {tenant_id}")
        return False
    
    candidate_id = str(candidate['_id'])
    share_id = candidate['share_id']
    print(f"✅ Using candidate: {candidate_id}, share_id: {share_id}")
    
    # Prepare the request
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json"
    }
    
    data = {
        "candidate_id": candidate_id,
        "share_id": share_id
    }
    
    try:
        print("📝 Calling personal letter API...")
        response = requests.post(
            "http://localhost:8080/personal-letter",
            headers=headers,
            json=data,
            timeout=120  # Increased timeout for LLM processing
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ API Response keys: {list(result.keys())}")
            
            # The letter content might be in different formats
            letter_data = result.get('letter', '')
            if isinstance(letter_data, dict):
                letter_content = letter_data.get('letter_content', '')
            else:
                letter_content = str(letter_data)
            
            print("✅ Personal letter generated successfully!")
            print(f"Letter length: {len(letter_content)} characters")
            
            # Check for mobile links
            if "לצפייה בפרטי המשרה ולאישור המועמדות" in letter_content:
                print("🎉 Mobile job links found in the letter!")
                
                # Extract and display mobile links
                lines = letter_content.split('\n')
                mobile_links = []
                for line in lines:
                    if "לצפייה בפרטי המשרה ולאישור המועמדות" in line:
                        mobile_links.append(line.strip())
                
                print(f"📱 Found {len(mobile_links)} mobile links:")
                for i, link in enumerate(mobile_links, 1):
                    print(f"  {i}. {link}")
                
                # Show a preview of the letter
                print("\n📄 Letter preview (first 500 chars):")
                print("=" * 50)
                print(letter_content[:500] + "..." if len(letter_content) > 500 else letter_content)
                print("=" * 50)
                
                return True
            else:
                print("⚠️  No mobile job links found in the letter")
                print("📄 Letter content preview:")
                print(letter_content[:300] + "..." if len(letter_content) > 300 else letter_content)
                return False
                
        else:
            print(f"❌ API call failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error calling personal letter API: {e}")
        return False

def main():
    print("🚀 Personal Letter with Mobile Links Test")
    print("=" * 45)
    
    success = test_personal_letter_with_mobile_links()
    
    print("\n📊 Test Result:")
    if success:
        print("🎉 SUCCESS: Personal letter with mobile job links is working!")
    else:
        print("❌ FAILED: Personal letter mobile links not working correctly")

if __name__ == "__main__":
    main()
