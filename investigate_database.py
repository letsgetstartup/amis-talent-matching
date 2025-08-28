#!/usr/bin/env python3
"""
Database Investigation Script
============================
Investigates what's actually in the database and why the jobs aren't being found.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add talentdb to path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "talentdb"))

try:
    from scripts.ingest_agent import db
    print("✅ Successfully connected to database")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def investigate_database_state():
    """Investigate the current state of the database"""
    print("🔍 Investigating database state...")
    
    # Check total job count
    total_jobs = db['jobs'].count_documents({})
    print(f"📊 Total jobs in database: {total_jobs}")
    
    # Check recent jobs (last hour)
    one_hour_ago = datetime.now().timestamp() - 3600
    recent_jobs = list(db['jobs'].find({
        'created_at': {'$gte': one_hour_ago}
    }).sort('created_at', -1).limit(20))
    
    print(f"📅 Recent jobs (last hour): {len(recent_jobs)}")
    
    # Show sample of recent jobs
    if recent_jobs:
        print("\n🔍 Recent Job Samples:")
        for i, job in enumerate(recent_jobs[:5]):
            print(f"  {i+1}. ID: {job.get('_id')}")
            print(f"     External ID: {job.get('external_order_id', 'MISSING')}")
            print(f"     Title: {job.get('title', 'MISSING')[:50]}...")
            print(f"     Created: {datetime.fromtimestamp(job.get('created_at', 0)).isoformat()}")
            print(f"     Skills: {len(job.get('skill_set', []))}")
            print(f"     Flags: {job.get('flags', [])}")
            print()
    
    # Check for jobs with our test order IDs in any field
    test_ids = ['405627', '405620', '405626', '405665', '405690', 
               '405691', '405692', '405731', '405768', '405777']
    
    for test_id in test_ids:
        # Search in title, external_order_id, and full_text
        matches = list(db['jobs'].find({
            '$or': [
                {'external_order_id': test_id},
                {'title': {'$regex': test_id}},
                {'full_text': {'$regex': test_id}}
            ]
        }))
        
        if matches:
            print(f"🎯 Found {len(matches)} matches for {test_id}:")
            for match in matches:
                print(f"    Title: {match.get('title', 'N/A')}")
                print(f"    External ID: {match.get('external_order_id', 'N/A')}")
                print(f"    Content hash: {match.get('_content_hash', 'N/A')[:10]}...")
                print()
    
    # Check for empty external_order_id
    empty_external_id = db['jobs'].count_documents({
        '$or': [
            {'external_order_id': {'$in': [None, '']}},
            {'external_order_id': {'$exists': False}}
        ]
    })
    print(f"⚠️ Jobs with empty external_order_id: {empty_external_id}")
    
    # Check for jobs with content matching our test jobs
    print("\n🔍 Searching for jobs by content...")
    test_titles = [
        'עובד/ת הרכבה',
        'עובד/ת ייצור עבודה מועדפת',
        'צבעי/ת תעשייתי/ת',
        'מנהל/ת אחזקה'
    ]
    
    for title in test_titles:
        matches = list(db['jobs'].find({'title': {'$regex': title}}))
        if matches:
            print(f"📍 Found {len(matches)} jobs matching title '{title}':")
            for match in matches:
                print(f"    External ID: {match.get('external_order_id', 'MISSING')}")
                print(f"    Full title: {match.get('title')}")
                print(f"    Created: {datetime.fromtimestamp(match.get('created_at', 0)).isoformat()}")
                print()
    
    # Get recent import metrics
    print("\n📊 Recent Import Metrics:")
    meta_doc = db['_meta'].find_one({'key': 'last_import_metrics'})
    if meta_doc:
        print(f"    {meta_doc['value']}")
    
    # Check versions collection for recent activity
    recent_versions = db['jobs_versions'].count_documents({
        'versioned_at': {'$gte': one_hour_ago}
    })
    print(f"📝 Recent job versions: {recent_versions}")

def check_csv_import_process():
    """Check if there's an issue with the CSV import process"""
    print("\n🔧 Checking CSV import process...")
    
    # Let's manually try to import one job to see what happens
    import tempfile
    import csv
    import subprocess
    
    # Create a simple test job
    test_job = {
        'מספר משרה': '999999',
        'שם משרה': 'TEST_JOB_DEBUG',
        'מקום עבודה': 'תל אביב',
        'תאור תפקיד': 'זהו תפקיד בדיקה לניפוי שגיאות',
        'דרישות התפקיד': 'דרישות בדיקה - חובה',
        'מקצוע': 'בדיקות'
    }
    
    # Create temporary CSV
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=test_job.keys())
        writer.writeheader()
        writer.writerow(test_job)
        tmp_csv_path = tmp_file.name
    
    print(f"📄 Created test CSV: {tmp_csv_path}")
    
    # Try to import it
    try:
        cmd = [sys.executable, 'talentdb/scripts/import_csv_enriched.py', tmp_csv_path]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=30)
        
        print(f"🔄 Import command: {' '.join(cmd)}")
        print(f"📤 Exit code: {result.returncode}")
        print(f"📝 STDOUT: {result.stdout}")
        if result.stderr:
            print(f"⚠️ STDERR: {result.stderr}")
        
        # Check if the test job was created
        test_job_in_db = db['jobs'].find_one({'external_order_id': '999999'})
        if test_job_in_db:
            print("✅ Test job successfully created in database")
            print(f"    Title: {test_job_in_db.get('title')}")
            print(f"    External ID: {test_job_in_db.get('external_order_id')}")
            print(f"    Skills: {len(test_job_in_db.get('skill_set', []))}")
            
            # Clean up test job
            db['jobs'].delete_one({'_id': test_job_in_db['_id']})
            print("🧹 Cleaned up test job")
        else:
            print("❌ Test job NOT found in database")
            
            # Check if it was created without external_order_id
            title_match = db['jobs'].find_one({'title': 'TEST_JOB_DEBUG'})
            if title_match:
                print("⚠️ Found job by title but without correct external_order_id")
                print(f"    External ID in DB: {title_match.get('external_order_id')}")
                # Clean up
                db['jobs'].delete_one({'_id': title_match['_id']})
                print("🧹 Cleaned up test job")
    
    except Exception as e:
        print(f"💥 Import test failed: {e}")
    
    finally:
        # Clean up temp file
        import os
        try:
            os.unlink(tmp_csv_path)
        except:
            pass

def main():
    """Main investigation"""
    print("🚀 Starting Database Investigation")
    print("=" * 80)
    
    investigate_database_state()
    check_csv_import_process()
    
    print("\n" + "=" * 80)
    print("🎯 INVESTIGATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
