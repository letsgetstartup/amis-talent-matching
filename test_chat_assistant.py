#!/usr/bin/env python3
"""
Test script to validate Chat Assistant UI Response fixes.
This script tests the chat endpoint and validates proper UI component formatting.
"""

import json
import time
import requests
from typing import Dict, Any, List

def test_chat_endpoint(question: str, expected_ui_types: List[str] = None) -> Dict[str, Any]:
    """Test a single chat query and validate the response format."""
    url = "http://127.0.0.1:8000/chat/query"
    payload = {
        "question": question,
        "detailsOnly": False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Validate basic structure
        assert "type" in data, "Response missing 'type' field"
        assert data["type"] == "assistant_ui", f"Expected 'assistant_ui', got '{data['type']}'"
        assert "ui" in data, "Response missing 'ui' field"
        assert isinstance(data["ui"], list), "UI field must be a list"
        
        # Validate UI components
        ui_components = data["ui"]
        for i, component in enumerate(ui_components):
            assert isinstance(component, dict), f"Component {i} must be a dict"
            assert "kind" in component, f"Component {i} missing 'kind' attribute"
            assert "id" in component, f"Component {i} missing 'id' attribute"
            
            # Ensure no 'type' attribute (should be 'kind')
            assert "type" not in component or component.get("type") == component.get("kind"), \
                f"Component {i} has conflicting 'type' and 'kind' attributes"
        
        # Check expected UI component types
        if expected_ui_types:
            found_types = [comp.get("kind") for comp in ui_components]
            for expected_type in expected_ui_types:
                assert expected_type in found_types, \
                    f"Expected UI component '{expected_type}' not found. Got: {found_types}"
        
        return {
            "success": True,
            "response": data,
            "ui_component_types": [comp.get("kind") for comp in ui_components],
            "message": f"✅ Test passed for question: '{question}'"
        }
        
    except requests.RequestException as e:
        return {
            "success": False,
            "error": f"Request failed: {e}",
            "message": f"❌ Request error for question: '{question}'"
        }
    except AssertionError as e:
        return {
            "success": False,
            "error": f"Validation failed: {e}",
            "message": f"❌ Validation error for question: '{question}'"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {e}",
            "message": f"❌ Unexpected error for question: '{question}'"
        }

def main():
    """Run comprehensive tests for chat assistant UI responses."""
    print("🧪 Testing Chat Assistant UI Response Fixes")
    print("=" * 50)
    
    # Test cases with expected UI component types
    test_cases = [
        {
            "question": "איפה יש משרות?",
            "expected_ui": ["Table"],
            "description": "Jobs location query"
        },
        {
            "question": "תראה מועמדים למשרה 68ae892edc8b36d3dcc08aac",
            "expected_ui": ["MatchList"],
            "description": "Candidates for specific job"
        },
        {
            "question": "פירוט התאמה",
            "expected_ui": ["MatchBreakdown"],
            "description": "Match details breakdown"
        },
        {
            "question": "סיכום אנליטי",
            "expected_ui": ["Metric"],
            "description": "Analytics summary"
        },
        {
            "question": "מועמדים Python בתל אביב",
            "expected_ui": ["Table"],
            "description": "Candidate search with skills and location"
        }
    ]
    
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 Test {i}: {test_case['description']}")
        print(f"   Question: {test_case['question']}")
        
        result = test_chat_endpoint(
            test_case["question"], 
            test_case.get("expected_ui")
        )
        
        print(f"   {result['message']}")
        
        if result["success"]:
            passed += 1
            ui_types = result["ui_component_types"]
            print(f"   UI Components: {ui_types}")
            
            # Print sample of response structure
            response = result["response"]
            print(f"   Narration: {response.get('narration', 'N/A')[:100]}...")
            
        else:
            failed += 1
            print(f"   Error: {result['error']}")
        
        # Wait between requests to avoid rate limiting
        time.sleep(2)
    
    print(f"\n📊 Test Results:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Success Rate: {passed/(passed+failed)*100:.1f}%")
    
    if failed == 0:
        print(f"\n🎉 All tests passed! Chat assistant is working correctly.")
    else:
        print(f"\n⚠️  Some tests failed. Check the errors above and server logs.")
        
    print("\n💡 Professional Best Practices Applied:")
    print("   • Standardized UI component schema (kind vs type)")
    print("   • Enhanced fallback UI generation for all tool types")
    print("   • Improved JSON parsing with error handling")
    print("   • Better assistant instructions for consistent responses")
    print("   • Comprehensive validation and debugging")

if __name__ == "__main__":
    main()
