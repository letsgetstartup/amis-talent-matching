#!/usr/bin/env python3
"""
Automated Test Suite for New Features
Run with: python3 test_new_features.py
"""

import json
import sys
import urllib.request
import urllib.error
import time

API = 'http://localhost:8000'

class TestSuite:
    def __init__(self):
        self.tests_passed = 0
        self.tests_total = 0
        self.results = []

    def test(self, name, func):
        self.tests_total += 1
        start_time = time.time()
        try:
            result = func()
            duration = time.time() - start_time
            if result:
                status = "PASS"
                self.tests_passed += 1
            else:
                status = "FAIL"
            self.results.append({
                'name': name,
                'status': status,
                'duration': f"{duration:.2f}s"
            })
            print(f"{'✅' if result else '❌'} {name}: {status} ({duration:.2f}s)")
        except Exception as e:
            duration = time.time() - start_time
            self.results.append({
                'name': name,
                'status': 'ERROR',
                'duration': f"{duration:.2f}s",
                'error': str(e)
            })
            print(f"❌ {name}: ERROR - {e} ({duration:.2f}s)")

    def run_all(self):
        print("🚀 Running Automated Test Suite for New Features...\n")

        # API Tests
        self.test("Health Check", self.test_health)
        self.test("Terms Page Serving", self.test_terms)
        self.test("Privacy Page Serving", self.test_privacy)
        self.test("Signup Endpoint", self.test_signup)
        self.test("Invite Collaborators", self.test_invite)
        self.test("Agency Portal Page", self.test_agency_portal)
        self.test("Login Endpoint", self.test_login)
        self.test("API Key Creation", self.test_apikey)

        # Integration Tests
        self.test("Full Registration Flow", self.test_full_flow)

        self.print_summary()

    def test_health(self):
        req = urllib.request.Request(f"{API}/health")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return data.get('status') == 'ok'

    def test_terms(self):
        req = urllib.request.Request(f"{API}/terms.html")
        with urllib.request.urlopen(req) as resp:
            return resp.status == 200 and b'Terms of Service' in resp.read()

    def test_privacy(self):
        req = urllib.request.Request(f"{API}/privacy.html")
        with urllib.request.urlopen(req) as resp:
            return resp.status == 200 and b'Privacy' in resp.read()

    def test_signup(self):
        payload = json.dumps({
            "company": f"TestCo{int(time.time())}",
            "name": "Test Admin",
            "email": f"test{int(time.time())}@example.com",
            "password": "testpass123"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/signup", data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return 'tenant_id' in data and 'token' in data

    def test_invite(self):
        # First signup
        payload = json.dumps({
            "company": f"InviteTestCo{int(time.time())}",
            "name": "Invite Admin",
            "email": f"invite{int(time.time())}@example.com",
            "password": "invitepass123"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/signup", data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as resp:
            signup_data = json.loads(resp.read())
            token = signup_data['token']
            tid = signup_data['tenant_id']

        # Now invite
        invite_payload = json.dumps({
            "tenant_id": tid,
            "emails": ["hr@test.com", "talent@test.com"]
        }).encode()
        req = urllib.request.Request(f"{API}/auth/invite-collaborators", data=invite_payload,
                                    headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return 'invited' in data and data['invited'] == 2

    def test_agency_portal(self):
        req = urllib.request.Request(f"{API}/agency-portal.html")
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode('utf-8')
            return (resp.status == 200 and
                    'Join' in content and
                    'registrationModal' in content and
                    'adminBtn' in content)

    def test_login(self):
        # Create user first
        email = f"login{int(time.time())}@example.com"
        payload = json.dumps({
            "company": "LoginTestCo",
            "name": "Login Admin",
            "email": email,
            "password": "loginpass123"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/signup", data=payload, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req)

        # Now login
        login_payload = json.dumps({
            "email": email,
            "password": "loginpass123"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/login", data=login_payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return 'token' in data and 'tenant_id' in data

    def test_apikey(self):
        # First signup
        payload = json.dumps({
            "company": f"ApiKeyTestCo{int(time.time())}",
            "name": "ApiKey Admin",
            "email": f"apikey{int(time.time())}@example.com",
            "password": "apikeypass123"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/signup", data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as resp:
            signup_data = json.loads(resp.read())
            token = signup_data['token']
            tid = signup_data['tenant_id']

        # Create API key
        apikey_payload = json.dumps({
            "tenant_id": tid,
            "name": "test-key"
        }).encode()
        req = urllib.request.Request(f"{API}/auth/apikey", data=apikey_payload,
                                    headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return 'key' in data and len(data['key']) > 10

    def test_full_flow(self):
        """Test complete registration flow simulation"""
        try:
            # 1. Signup
            email = f"fullflow{int(time.time())}@example.com"
            payload = json.dumps({
                "company": "FullFlowCo",
                "name": "Full Flow User",
                "email": email,
                "password": "fullflow123"
            }).encode()
            req = urllib.request.Request(f"{API}/auth/signup", data=payload, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req) as resp:
                signup_data = json.loads(resp.read())
                token = signup_data['token']
                tid = signup_data['tenant_id']

            # 2. Invite collaborators
            invite_payload = json.dumps({
                "tenant_id": tid,
                "emails": ["hr@fullflow.com", "recruiter@fullflow.com"]
            }).encode()
            req = urllib.request.Request(f"{API}/auth/invite-collaborators", data=invite_payload,
                                        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})
            with urllib.request.urlopen(req) as resp:
                invite_data = json.loads(resp.read())

            # 3. Login
            login_payload = json.dumps({
                "email": email,
                "password": "fullflow123"
            }).encode()
            req = urllib.request.Request(f"{API}/auth/login", data=login_payload, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req) as resp:
                login_data = json.loads(resp.read())

            # 4. Get profile
            req = urllib.request.Request(f"{API}/auth/me", headers={'Authorization': f'Bearer {token}'})
            with urllib.request.urlopen(req) as resp:
                profile_data = json.loads(resp.read())

            return (signup_data and invite_data and login_data and profile_data and
                    'tenant_id' in signup_data and 'invited' in invite_data and
                    'token' in login_data and 'user' in profile_data)

        except Exception as e:
            print(f"Full flow error: {e}")
            return False

    def print_summary(self):
        print(f"\n📊 Test Results: {self.tests_passed}/{self.tests_total} passed")

        if self.tests_passed == self.tests_total:
            print("🎉 All tests passed!")
        else:
            print(f"⚠️  {self.tests_total - self.tests_passed} tests failed")

        # Print detailed results
        print("\n📋 Detailed Results:")
        for result in self.results:
            status_icon = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "🔥"
            print(f"  {status_icon} {result['name']}: {result['status']} ({result['duration']})")
            if 'error' in result:
                print(f"    Error: {result['error']}")

if __name__ == "__main__":
    suite = TestSuite()
    suite.run_all()
