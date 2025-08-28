#!/usr/bin/env python3
"""
Test to validate mobile job link functionality with real data
"""

import requests
import json
from scripts.ingest_agent import db
from scripts.api import _generate_mobile_job_link, _shorten_url

def test_mobile_link_generation():
    """Test mobile link generation with real job and candidate data"""
    print("🔧 Testing mobile link generation with real data...")
    
    # Get a real candidate
    candidate = db['candidates'].find_one({}, {'share_id': 1, '_id': 1})
    if not candidate:
        print("❌ No candidates found in database")
        return False
    
    candidate_id = str(candidate['_id'])
    share_id = candidate['share_id']
    print(f"✅ Using candidate: {candidate_id}, share_id: {share_id}")
    
    # Get a real job
    job = db['jobs'].find_one({}, {'_id': 1, 'title': 1})
    if not job:
        print("❌ No jobs found in database")
        return False
    
    job_id = str(job['_id'])
    job_title = job.get('title', 'Unknown')
    print(f"✅ Using job: {job_id}, title: {job_title}")
    
    # Test mobile link generation
    try:
        mobile_link = _generate_mobile_job_link(job_id, share_id)
        print(f"✅ Generated mobile link: {mobile_link}")
        
        # Test URL shortening
        short_link = _shorten_url(mobile_link)
        print(f"✅ Shortened link: {short_link}")
        
        # Validate the mobile job page is accessible
        try:
            response = requests.get(mobile_link, timeout=5)
            if response.status_code == 200:
                print("✅ Mobile job page accessible")
                return True
            else:
                print(f"⚠️  Mobile job page returned status: {response.status_code}")
                return False
        except Exception as e:
            print(f"⚠️  Could not access mobile job page: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error generating mobile link: {e}")
        return False

def test_letter_processing_simulation():
    """Simulate letter processing with mobile links"""
    print("\n📝 Testing letter processing simulation...")
    
    # Sample letter content with job sections
    sample_letter = """שלום יקר/ה,

מצאנו עבורך משרות מתאימות:

→ מזכיר/ה רפואי/ת - בית חולים שיבא
דרישות: ניסיון במערכות מידע רפואיות
מיקום: רמת גן

→ מזכיר/ת מחלקת אחזקה - חברת הייטק
דרישות: אקסל ברמה גבוהה
מיקום: תל אביב

בהצלחה!
"""
    
    # Get real job and candidate data
    candidate = db['candidates'].find_one({}, {'share_id': 1})
    jobs = list(db['jobs'].find({}, {'_id': 1}).limit(2))
    
    if not candidate or len(jobs) < 2:
        print("❌ Insufficient data for letter simulation")
        return False
    
    share_id = candidate['share_id']
    
    # Simulate the letter processing logic
    lines = sample_letter.strip().split('\n')
    processed_lines = []
    
    for line in lines:
        processed_lines.append(line)
        
        # Check if this line contains a job (starts with →)
        if line.strip().startswith('→') and jobs:
            job = jobs.pop(0)  # Get next job
            job_id = str(job['_id'])
            
            try:
                mobile_link = _generate_mobile_job_link(job_id, share_id)
                short_link = _shorten_url(mobile_link)
                link_text = f"לצפייה בפרטי המשרה ולאישור המועמדות: {short_link}"
                processed_lines.append(link_text)
                processed_lines.append("")  # Empty line for spacing
                print(f"✅ Added mobile link for job {job_id}")
            except Exception as e:
                print(f"❌ Error processing job {job_id}: {e}")
                return False
    
    processed_letter = '\n'.join(processed_lines)
    print("\n📄 Processed letter preview:")
    print("=" * 50)
    print(processed_letter)
    print("=" * 50)
    
    # Validate that links were added
    if "לצפייה בפרטי המשרה ולאישור המועמדות" in processed_letter:
        print("✅ Mobile links successfully added to letter")
        return True
    else:
        print("❌ Mobile links not found in processed letter")
        return False

def main():
    print("🚀 Mobile Link Validation Test")
    print("=" * 40)
    
    # Test 1: Mobile link generation
    test1_passed = test_mobile_link_generation()
    
    # Test 2: Letter processing simulation
    test2_passed = test_letter_processing_simulation()
    
    print("\n📊 Test Results:")
    print(f"Mobile Link Generation: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Letter Processing: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All tests passed! Mobile link functionality is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Please check the implementation.")

if __name__ == "__main__":
    main()
