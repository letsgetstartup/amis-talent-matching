#!/usr/bin/env python3
"""
Test the specific JavaScript fixes for the mobile page
"""

import requests
import re

def test_javascript_fixes():
    """Test that JavaScript fixes are properly implemented"""
    print("🔍 Testing JavaScript fixes in mobile-job.html...")
    
    try:
        # Get the HTML content
        response = requests.get("http://localhost:8080/mobile-job.html")
        html_content = response.text
        
        # Check for the fixed formatText function
        format_text_pattern = r'function formatText\(text\) \{[^}]*if \(!text \|\| typeof text !== \'string\'\) return \'\';[^}]*\}'
        if re.search(format_text_pattern, html_content, re.DOTALL):
            print("   ✅ formatText function has proper type checking")
        else:
            print("   ❌ formatText function missing type checking")
            return False
        
        # Check for the new formatRequirements function
        if 'function formatRequirements(requirements)' in html_content:
            print("   ✅ formatRequirements function found")
        else:
            print("   ❌ formatRequirements function missing")
            return False
        
        # Check for proper requirements handling in displayJobData
        if 'typeof job.requirements === \'object\'' in html_content:
            print("   ✅ Requirements object handling implemented")
        else:
            print("   ❌ Requirements object handling missing")
            return False
        
        # Check for company field fallback
        if 'חברה לא צוינה' in html_content:
            print("   ✅ Company field fallback implemented")
        else:
            print("   ❌ Company field fallback missing")
            return False
        
        print("   ✅ All JavaScript fixes verified")
        return True
        
    except Exception as e:
        print(f"   ❌ Error testing JavaScript fixes: {e}")
        return False

def test_api_data_structure():
    """Test that the API returns the expected data structure"""
    print("\n🔍 Testing API data structure...")
    
    try:
        response = requests.get("http://localhost:8080/mobile/job/68a20af2725068b9910b9fa4")
        data = response.json()
        
        if 'job' not in data:
            print("   ❌ No 'job' field in response")
            return False
        
        job = data['job']
        
        # Check requirements structure
        if 'requirements' in job and isinstance(job['requirements'], dict):
            reqs = job['requirements']
            if 'must_have_skills' in reqs and 'nice_to_have_skills' in reqs:
                print("   ✅ Requirements object structure is correct")
                print(f"   Must have skills: {len(reqs['must_have_skills'])}")
                print(f"   Nice to have skills: {len(reqs['nice_to_have_skills'])}")
            else:
                print("   ❌ Requirements object missing skill arrays")
                return False
        else:
            print("   ❌ Requirements is not an object")
            return False
        
        # Check other fields
        print(f"   Job title: '{job.get('title', 'N/A')}'")
        print(f"   Company: '{job.get('company', 'N/A')}'")
        print(f"   Description: '{job.get('description', 'N/A')}'")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error testing API structure: {e}")
        return False

def main():
    print("🚀 Testing specific mobile page fixes...\n")
    
    js_fixes_ok = test_javascript_fixes()
    api_structure_ok = test_api_data_structure()
    
    print("\n" + "="*50)
    print("📊 FIXES VALIDATION SUMMARY:")
    print("="*50)
    
    if js_fixes_ok:
        print("✅ JavaScript fixes: IMPLEMENTED")
        print("   - formatText now handles non-string values")
        print("   - formatRequirements function added")
        print("   - Requirements object handling added")
        print("   - Company field fallback added")
    else:
        print("❌ JavaScript fixes: FAILED")
    
    if api_structure_ok:
        print("✅ API data structure: COMPATIBLE")
        print("   - Requirements returned as object with skills arrays")
        print("   - Job fields accessible")
    else:
        print("❌ API data structure: INCOMPATIBLE")
    
    print("\n" + "="*50)
    if js_fixes_ok and api_structure_ok:
        print("🎉 ALL FIXES VERIFIED!")
        print("The 'text.split is not a function' error should be resolved.")
        print("The mobile job page should now display correctly.")
    else:
        print("⚠️  Some fixes failed verification.")
    print("="*50)

if __name__ == "__main__":
    main()
