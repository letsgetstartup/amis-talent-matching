#!/usr/bin/env python3
"""
Debug Script - Trace Import Logic
=================================
Debug exactly what happens during the import process.
"""

import sys
import tempfile
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "talentdb"))

from scripts.ingest_agent import db

def debug_single_job_import():
    """Debug a single job import step by step"""
    print("🔍 Debugging single job import process...")
    
    # Take one of the problematic jobs
    test_job = {
        'מספר משרה': '405690',
        'שם משרה': 'לחברת ביטוח מובילה דרוש/ה נציג/ת שירות למוקד הדיגיטל!',
        'מקום עבודה': 'פתח תקווה',
        'תאור תפקיד': 'תפקיד בחברת ביטוח מובילה',
        'דרישות התפקיד': 'ניסיון בשירות לקוחות - חובה',
        'מקצוע': 'נציג/ת שירות'
    }
    
    print(f"🎯 Testing job: {test_job['מספר משרה']} - {test_job['שם משרה'][:50]}...")
    
    # Clean up any existing job
    db['jobs'].delete_many({'external_order_id': '405690'})
    db['jobs'].delete_many({'title': test_job['שם משרה']})
    
    # Create CSV and import
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=test_job.keys())
        writer.writeheader()
        writer.writerow(test_job)
        tmp_csv_path = tmp_file.name
    
    print(f"📄 Created test CSV: {tmp_csv_path}")
    
    # Import with debug output
    import subprocess
    cmd = [sys.executable, 'talentdb/scripts/import_csv_enriched.py', tmp_csv_path]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    
    print(f"📤 Import result: exit_code={result.returncode}")
    print(f"📝 STDOUT: {result.stdout}")
    if result.stderr:
        print(f"⚠️ STDERR: {result.stderr}")
    
    # Check what was created
    by_order_id = db['jobs'].find_one({'external_order_id': '405690'})
    by_title = db['jobs'].find_one({'title': test_job['שם משרה']})
    
    print(f"\n🔍 Database check:")
    print(f"Found by external_order_id '405690': {'✅' if by_order_id else '❌'}")
    print(f"Found by title: {'✅' if by_title else '❌'}")
    
    if by_title:
        print(f"Job found by title:")
        print(f"  external_order_id: '{by_title.get('external_order_id', 'MISSING')}'")
        print(f"  title: {by_title.get('title', 'MISSING')}")
        print(f"  _content_hash: {by_title.get('_content_hash', 'MISSING')[:10]}...")
        print(f"  skills: {len(by_title.get('skill_set', []))}")
    
    # Clean up
    import os
    os.unlink(tmp_csv_path)
    
    return by_title

def debug_import_logic_manually():
    """Manually step through the import logic to see what happens"""
    print("\n🔬 Manually stepping through import logic...")
    
    # Simulate the import logic for job 405690
    import hashlib
    import re
    from scripts.import_csv_enriched import scrub_pii, tokenize_skill_candidates, derive_synthetic_skills, detect_mandatory
    
    # Test data
    row = {
        'מספר משרה': '405690',
        'שם משרה': 'לחברת ביטוח מובילה דרוש/ה נציג/ת שירות למוקד הדיגיטל!',
        'מקום עבודה': 'פתח תקווה',
        'תאור תפקיד': 'תפקיד בחברת ביטוח מובילה',
        'דרישות התפקיד': 'ניסיון בשירות לקוחות - חובה',
        'מקצוע': 'נציג/ת שירות'
    }
    
    # Step 1: Extract fields (simulating the import script logic)
    order_id = (row.get('מספר הזמנה') or row.get('מספר משרה') or '').strip()
    title = (row.get('שם משרה') or '').strip()
    city = (row.get('מקום עבודה') or '').strip().replace('_',' ')
    desc = (row.get('תאור תפקיד') or '').strip()
    req_a = (row.get('דרישות תפקיד') or '').strip()
    req_b = (row.get('דרישות התפקיד') or '').strip()
    
    print(f"Step 1 - Field extraction:")
    print(f"  order_id: '{order_id}' (length: {len(order_id)})")
    print(f"  title: '{title}'")
    print(f"  city: '{city}'")
    print(f"  desc: '{desc}'")
    print(f"  req_a: '{req_a}'")
    print(f"  req_b: '{req_b}'")
    
    # Step 2: Process requirements
    req_text = '\n'.join([x for x in [req_a, req_b] if x])
    print(f"\nStep 2 - Requirements processing:")
    print(f"  req_text: '{req_text}'")
    
    # Step 3: Build full text and hash
    full_text_parts = [p for p in [desc, req_text] if p]
    full_text = '\n\n'.join(full_text_parts)
    full_text = scrub_pii(full_text)
    content_hash = hashlib.sha1(full_text.encode('utf-8', errors='ignore')).hexdigest()
    
    print(f"\nStep 3 - Content hash:")
    print(f"  full_text: '{full_text}'")
    print(f"  content_hash: {content_hash}")
    
    # Step 4: Check existing docs
    print(f"\nStep 4 - Database lookup:")
    print(f"  order_id truthy: {bool(order_id)}")
    
    existing_by_order_id = None
    if order_id:
        existing_by_order_id = db['jobs'].find_one({'external_order_id': order_id})
        print(f"  Found by external_order_id: {'✅' if existing_by_order_id else '❌'}")
    
    existing_by_hash = db['jobs'].find_one({'_content_hash': content_hash})
    print(f"  Found by content_hash: {'✅' if existing_by_hash else '❌'}")
    
    if existing_by_hash:
        print(f"    Existing job external_order_id: '{existing_by_hash.get('external_order_id', 'MISSING')}'")
        print(f"    Existing job title: {existing_by_hash.get('title', 'MISSING')}")
    
    # The logic would choose existing_by_order_id if found, otherwise existing_by_hash
    existing_doc = existing_by_order_id if existing_by_order_id else existing_by_hash
    
    print(f"\nStep 5 - Final decision:")
    if existing_doc:
        print(f"  Will UPDATE existing job: {existing_doc.get('_id')}")
        print(f"  Current external_order_id: '{existing_doc.get('external_order_id', 'MISSING')}'")
        print(f"  Will set external_order_id to: '{order_id}'")
    else:
        print(f"  Will CREATE new job with external_order_id: '{order_id}'")

def main():
    """Main debugging"""
    print("🚀 Starting Import Logic Debug")
    print("=" * 80)
    
    # Step 1: Debug a single job import
    result_job = debug_single_job_import()
    
    # Step 2: Debug the logic manually
    debug_import_logic_manually()
    
    print("\n" + "=" * 80)
    print("🎯 DEBUG COMPLETE")
    
    if result_job and result_job.get('external_order_id') == '405690':
        print("✅ External order ID correctly set!")
        return 0
    else:
        print("❌ External order ID not correctly set!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
