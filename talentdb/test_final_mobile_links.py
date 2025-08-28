#!/usr/bin/env python3
"""
Final comprehensive test of the mobile job links in personal letters
"""

import requests
import json
from scripts.ingest_agent import db

def test_full_letter_display():
    """Display the full letter with mobile links"""
    print("📋 Final Mobile Job Links Test")
    print("=" * 40)
    
    # Use the same API key and tenant from the successful test
    api_key = "6Szs8Yqseqrkr_gWCsZSIfmDWm9xSCvm0yZhp9uGmeA"
    tenant_id = "68a20abe725068b9910b9f9a"
    
    # Get a candidate with the matching tenant
    candidate = db['candidates'].find_one({'tenant_id': tenant_id}, {'share_id': 1, '_id': 1, 'extracted_data.name': 1})
    if not candidate:
        print(f"❌ No candidates found for tenant {tenant_id}")
        return False
    
    candidate_id = str(candidate['_id'])
    share_id = candidate['share_id']
    candidate_name = candidate.get('extracted_data', {}).get('name', 'Unknown')
    print(f"📝 Testing for candidate: {candidate_name} (ID: {candidate_id})")
    
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
        print("🔄 Generating personal letter with mobile job links...")
        response = requests.post(
            "http://localhost:8080/personal-letter",
            headers=headers,
            json=data,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Parse the letter content
            letter_data = result.get('letter', '')
            if isinstance(letter_data, dict):
                letter_content = letter_data.get('letter_content', '')
            else:
                letter_content = str(letter_data)
            
            print("✅ Personal letter generated successfully!")
            print(f"📊 Statistics:")
            print(f"   - Letter length: {len(letter_content)} characters")
            print(f"   - Match count: {result.get('match_count', 'Unknown')}")
            print(f"   - Cached: {result.get('cached', False)}")
            
            # Count mobile links
            mobile_link_count = letter_content.count("לצפייה בפרטי המשרה ולאישור המועמדות")
            job_count = letter_content.count("→")
            
            print(f"   - Jobs mentioned: {job_count}")
            print(f"   - Mobile links: {mobile_link_count}")
            
            # Display the full letter
            print("\n📄 FULL PERSONAL LETTER WITH MOBILE LINKS:")
            print("=" * 60)
            print(letter_content)
            print("=" * 60)
            
            # Extract and display just the mobile links
            print("\n📱 MOBILE LINKS EXTRACTED:")
            lines = letter_content.split('\n')
            mobile_links = []
            for line in lines:
                if "לצפייה בפרטי המשרה ולאישור המועמדות" in line:
                    mobile_links.append(line.strip())
            
            for i, link in enumerate(mobile_links, 1):
                print(f"   {i}. {link}")
            
            # Success validation
            if mobile_link_count > 0:
                print(f"\n🎉 SUCCESS! Generated {mobile_link_count} mobile job confirmation links!")
                print("✅ SMS-ready personal letters with mobile job links are working correctly!")
                return True
            else:
                print("\n⚠️  No mobile links found in the letter")
                return False
                
        else:
            print(f"❌ API call failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_full_letter_display()
