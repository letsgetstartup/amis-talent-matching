#!/usr/bin/env python3
"""
Minimal chat endpoint test to identify hanging issues
"""

import requests
import sys
import time

def test_minimal_endpoints():
    """Test basic endpoints that don't involve complex logic"""
    base_url = "http://127.0.0.1:8000"
    
    # Test 1: Basic health check
    print("Testing basic health endpoints...")
    
    try:
        # Test root endpoint
        response = requests.get(f"{base_url}/", timeout=5)
        print(f"Root endpoint: {response.status_code}")
    except Exception as e:
        print(f"Root endpoint failed: {e}")
    
    try:
        # Test database counts
        response = requests.get(f"{base_url}/match/count/today", timeout=5)
        print(f"Match count endpoint: {response.status_code}")
        if response.status_code == 200:
            print(f"Response: {response.text[:200]}")
    except Exception as e:
        print(f"Match count failed: {e}")
    
    try:
        # Test candidates endpoint
        response = requests.get(f"{base_url}/candidates?skip=0&limit=1", timeout=10)
        print(f"Candidates endpoint: {response.status_code}")
        if response.status_code == 200:
            print(f"Response: {response.text[:200]}")
    except Exception as e:
        print(f"Candidates endpoint failed: {e}")
    
    # Test 2: Simple chat query without complex logic
    print("\nTesting simple chat query...")
    
    chat_data = {"question": "test"}
    
    try:
        start_time = time.time()
        response = requests.post(
            f"{base_url}/chat/query",
            json=chat_data,
            timeout=10
        )
        end_time = time.time()
        
        print(f"Chat endpoint: {response.status_code} (took {end_time - start_time:.2f}s)")
        if response.status_code == 200:
            print(f"Response: {response.text[:500]}")
        else:
            print(f"Error response: {response.text}")
            
    except requests.exceptions.Timeout:
        print("Chat endpoint timed out!")
    except Exception as e:
        print(f"Chat endpoint failed: {e}")

if __name__ == "__main__":
    test_minimal_endpoints()
