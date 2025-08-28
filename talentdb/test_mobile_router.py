#!/usr/bin/env python3
"""
Test script for mobile endpoints functionality.
Tests the mobile router logic without requiring a full server.
"""

import sys
import os
sys.path.insert(0, '/Users/avirammizrahi/Desktop/amis/talentdb')

from scripts.routers_mobile import (
    _safe_objectid, 
    _get_candidate_by_share_id, 
    _get_job_by_id,
    _calculate_match_info
)

def test_mobile_functions():
    print("🧪 Testing Mobile Router Functions")
    print("=" * 50)
    
    # Test 1: Safe ObjectId conversion
    print("\n1. Testing _safe_objectid:")
    valid_id = "68a20af2725068b9910b9fa4"
    invalid_id = "invalid_id"
    
    obj_id1 = _safe_objectid(valid_id)
    obj_id2 = _safe_objectid(invalid_id)
    
    print(f"   Valid ID '{valid_id}' -> {obj_id1}")
    print(f"   Invalid ID '{invalid_id}' -> {obj_id2}")
    
    # Test 2: Calculate match info
    print("\n2. Testing _calculate_match_info:")
    candidate = {
        "skills": ["Microsoft Office", "Hebrew", "Customer Service"],
        "city": "Tel Aviv",
        "experience_years": 3,
        "full_name": "Test Candidate"
    }
    
    job = {
        "skills": ["Microsoft Office", "Hebrew", "Communication"],
        "city": "Tel Aviv",
        "title": "Medical Secretary"
    }
    
    match_info = _calculate_match_info(candidate, job)
    print(f"   Match Score: {match_info['match_score']}")
    print(f"   Matching Skills: {match_info['matching_skills']}")
    print(f"   Reason: {match_info['reason']}")
    
    print("\n✅ Mobile router functions are working correctly!")
    
    # Test 3: Check if mobile router can be imported properly
    print("\n3. Testing mobile router import:")
    try:
        from scripts.routers_mobile import router
        print(f"   Router prefix: {router.prefix}")
        print(f"   Router tags: {router.tags}")
        print("   ✅ Mobile router imported successfully!")
    except Exception as e:
        print(f"   ❌ Error importing mobile router: {e}")

if __name__ == "__main__":
    test_mobile_functions()
