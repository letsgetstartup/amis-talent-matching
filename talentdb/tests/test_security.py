"""Security test suite for tenant isolation verification."""
import pytest
import time
from fastapi.testclient import TestClient
from scripts.api import app
from scripts.ingest_agent import db
from scripts.tenants import create_tenant, create_api_key
from scripts.auth import hash_password


@pytest.fixture
def client():
    """Test client fixture."""
    return TestClient(app)


@pytest.fixture
def test_tenants():
    """Create test tenants with API keys."""
    # Clean up any existing test data
    db["tenants"].delete_many({"name": {"$regex": "^TestTenant"}})
    db["api_keys"].delete_many({"name": {"$regex": "^test_"}})
    db["candidates"].delete_many({"full_name": {"$regex": "^TestCandidate"}})
    db["jobs"].delete_many({"title": {"$regex": "^TestJob"}})
    
    # Create two test tenants
    tenant_a_id = create_tenant("TestTenantA")
    tenant_b_id = create_tenant("TestTenantB")
    
    # Create API keys
    key_a = create_api_key(tenant_a_id, "test_key_a")["key"]
    key_b = create_api_key(tenant_b_id, "test_key_b")["key"]
    
    return {
        "tenant_a": {"id": tenant_a_id, "key": key_a},
        "tenant_b": {"id": tenant_b_id, "key": key_b}
    }


def test_tenant_isolation_candidates(client, test_tenants):
    """Test that candidates are isolated by tenant."""
    tenant_a = test_tenants["tenant_a"]
    tenant_b = test_tenants["tenant_b"]
    
    # Create a candidate in tenant A
    candidate_a = {
        "_id": db["candidates"].insert_one({
            "tenant_id": tenant_a["id"],
            "full_name": "TestCandidate A",
            "title": "Software Engineer",
            "skill_set": ["Python", "FastAPI"],
            "city_canonical": "tel_aviv"
        }).inserted_id
    }
    
    # Try to access candidate A from tenant B (should fail)
    response = client.get(
        f"/candidate/{candidate_a['_id']}",
        headers={"X-API-Key": tenant_b["key"]}
    )
    assert response.status_code == 404, "Tenant B should not access Tenant A's candidate"
    
    # Access candidate A from tenant A (should succeed)
    response = client.get(
        f"/candidate/{candidate_a['_id']}",
        headers={"X-API-Key": tenant_a["key"]}
    )
    assert response.status_code == 200, "Tenant A should access its own candidate"
    assert response.json()["candidate"]["title"] == "Software Engineer"


def test_tenant_isolation_jobs(client, test_tenants):
    """Test that jobs are isolated by tenant."""
    tenant_a = test_tenants["tenant_a"]
    tenant_b = test_tenants["tenant_b"]
    
    # Create a job in tenant A
    job_a = {
        "_id": db["jobs"].insert_one({
            "tenant_id": tenant_a["id"],
            "title": "TestJob A",
            "job_description": "Python Developer",
            "skill_set": ["Python", "Django"],
            "city_canonical": "tel_aviv"
        }).inserted_id
    }
    
    # Try to access job A from tenant B (should fail)
    response = client.get(
        f"/job/{job_a['_id']}",
        headers={"X-API-Key": tenant_b["key"]}
    )
    assert response.status_code == 404, "Tenant B should not access Tenant A's job"
    
    # Access job A from tenant A (should succeed)
    response = client.get(
        f"/job/{job_a['_id']}",
        headers={"X-API-Key": tenant_a["key"]}
    )
    assert response.status_code == 200, "Tenant A should access its own job"
    assert response.json()["job"]["title"] == "TestJob A"


def test_matching_isolation(client, test_tenants):
    """Test that matching is isolated by tenant."""
    tenant_a = test_tenants["tenant_a"]
    tenant_b = test_tenants["tenant_b"]
    
    # Create candidate and job in tenant A
    candidate_a_id = db["candidates"].insert_one({
        "tenant_id": tenant_a["id"],
        "full_name": "TestCandidate A",
        "title": "Software Engineer",
        "skill_set": ["Python", "FastAPI"],
        "city_canonical": "tel_aviv"
    }).inserted_id
    
    job_a_id = db["jobs"].insert_one({
        "tenant_id": tenant_a["id"],
        "title": "TestJob A",
        "job_description": "Python Developer",
        "skill_set": ["Python", "Django"],
        "city_canonical": "tel_aviv"
    }).inserted_id
    
    # Create job in tenant B
    job_b_id = db["jobs"].insert_one({
        "tenant_id": tenant_b["id"],
        "title": "TestJob B",
        "job_description": "Python Developer",
        "skill_set": ["Python", "Django"],
        "city_canonical": "tel_aviv"
    }).inserted_id
    
    # Try to match candidate A from tenant B (should fail)
    response = client.get(
        f"/match/candidate/{candidate_a_id}",
        headers={"X-API-Key": tenant_b["key"]}
    )
    assert response.status_code == 404, "Tenant B should not match Tenant A's candidate"
    
    # Match candidate A from tenant A (should succeed, but only see tenant A jobs)
    response = client.get(
        f"/match/candidate/{candidate_a_id}",
        headers={"X-API-Key": tenant_a["key"]}
    )
    assert response.status_code == 200, "Tenant A should match its own candidate"
    matches = response.json()["matches"]
    
    # Should only see jobs from tenant A, not tenant B
    job_ids_in_matches = [match["job_id"] for match in matches]
    assert str(job_a_id) in job_ids_in_matches, "Should see tenant A's job"
    assert str(job_b_id) not in job_ids_in_matches, "Should not see tenant B's job"


def test_unauthorized_access(client):
    """Test that endpoints require authentication."""
    # Create a test candidate without tenant context
    candidate_id = db["candidates"].insert_one({
        "tenant_id": "test_tenant",
        "full_name": "Test Candidate",
        "title": "Engineer"
    }).inserted_id
    
    # Try to access without API key
    response = client.get(f"/candidate/{candidate_id}")
    assert response.status_code == 401, "Should require authentication"
    
    # Try to access with invalid API key
    response = client.get(
        f"/candidate/{candidate_id}",
        headers={"X-API-Key": "invalid_key"}
    )
    assert response.status_code == 401, "Should reject invalid API key"


def test_security_audit_logging(client, test_tenants):
    """Test that security events are logged."""
    tenant_a = test_tenants["tenant_a"]
    
    # Create a candidate
    candidate_id = db["candidates"].insert_one({
        "tenant_id": tenant_a["id"],
        "full_name": "TestCandidate Audit",
        "title": "Engineer"
    }).inserted_id
    
    # Access the candidate
    response = client.get(
        f"/candidate/{candidate_id}",
        headers={"X-API-Key": tenant_a["key"]}
    )
    assert response.status_code == 200
    
    # Check audit log
    time.sleep(0.1)  # Allow time for async logging
    audit_records = list(db["security_audit"].find({
        "tenant_id": tenant_a["id"],
        "action": "data_access",
        "resource": "candidate"
    }))
    
    assert len(audit_records) > 0, "Should log data access events"
    assert audit_records[0]["resource_id"] == str(candidate_id)


def test_api_key_validation():
    """Test API key validation logic."""
    from scripts.auth import get_tenant_from_apikey
    from fastapi import HTTPException
    
    # Test with None API key
    result = get_tenant_from_apikey(None)
    assert result is None, "Should return None for missing API key"
    
    # Test with invalid API key
    try:
        get_tenant_from_apikey("invalid_key")
        assert False, "Should raise HTTPException for invalid key"
    except HTTPException as e:
        assert e.status_code == 401
        assert "bad_api_key" in str(e.detail)


if __name__ == "__main__":
    # Run basic security tests
    from fastapi.testclient import TestClient
    
    client = TestClient(app)
    
    print("🔒 Running Security Tests...")
    
    # Create test environment
    test_tenants = {}
    try:
        # Clean up
        db["tenants"].delete_many({"name": {"$regex": "^TestTenant"}})
        db["api_keys"].delete_many({"name": {"$regex": "^test_"}})
        
        # Create test tenants
        tenant_a_id = create_tenant("TestTenantA")
        tenant_b_id = create_tenant("TestTenantB")
        
        key_a = create_api_key(tenant_a_id, "test_key_a")["key"]
        key_b = create_api_key(tenant_b_id, "test_key_b")["key"]
        
        test_tenants = {
            "tenant_a": {"id": tenant_a_id, "key": key_a},
            "tenant_b": {"id": tenant_b_id, "key": key_b}
        }
        
        print("✅ Test environment created")
        
        # Run tenant isolation test
        print("🧪 Testing tenant isolation...")
        test_tenant_isolation_candidates(client, test_tenants)
        test_tenant_isolation_jobs(client, test_tenants)
        test_matching_isolation(client, test_tenants)
        print("✅ Tenant isolation tests passed")
        
        # Test unauthorized access
        print("🧪 Testing unauthorized access...")
        test_unauthorized_access(client)
        print("✅ Unauthorized access tests passed")
        
        print("🎉 All security tests passed!")
        
    except Exception as e:
        print(f"❌ Security test failed: {e}")
        raise
    finally:
        # Cleanup
        try:
            db["tenants"].delete_many({"name": {"$regex": "^TestTenant"}})
            db["api_keys"].delete_many({"name": {"$regex": "^test_"}})
            db["candidates"].delete_many({"full_name": {"$regex": "^TestCandidate"}})
            db["jobs"].delete_many({"title": {"$regex": "^TestJob"}})
            print("🧹 Test cleanup completed")
        except Exception:
            pass
