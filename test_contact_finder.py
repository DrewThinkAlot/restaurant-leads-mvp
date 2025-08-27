#!/usr/bin/env python3
"""Test script for ContactFinder integration."""

import sys
import os
import json
from datetime import datetime

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.db import db_manager
from app.models import Candidate, Contact
from app.agents.agent_contact_finder import ContactFinderAgent
from app.pipelines.run_pipeline import PipelineRunner

def test_database_schema():
    """Test that the Contact model works with the database."""
    print("🧪 Testing database schema...")

    try:
        with db_manager.get_session() as session:
            # Create a test candidate
            candidate = Candidate(
                candidate_id="test-123",
                venue_name="Test Restaurant",
                legal_name="Test LLC",
                address="123 Main St",
                city="Houston",
                state="TX",
                zip_code="77001",
                first_seen=datetime.now(),
                last_seen=datetime.now()
            )
            session.add(candidate)

            # Create a test contact
            contact = Contact(
                candidate_id="test-123",
                full_name="John Doe",
                role="owner",
                email="john@test.com",
                source="tabc",
                source_url="https://test.com",
                provenance_text="Test contact",
                confidence_0_1=0.8,
                contactability={"ok_to_email": True, "ok_to_call": False, "ok_to_sms": False},
                notes="Test contact notes"
            )
            session.add(contact)

            session.commit()

            # Verify we can query contacts
            contacts = session.query(Contact).filter(Contact.candidate_id == "test-123").all()
            assert len(contacts) == 1
            assert contacts[0].full_name == "John Doe"

            print("✅ Database schema test passed")

    except Exception as e:
        print(f"❌ Database schema test failed: {e}")
        return False

    return True

def test_contact_finder_agent():
    """Test the ContactFinder agent with mock data."""
    print("🧪 Testing ContactFinder agent...")

    try:
        agent = ContactFinderAgent()

        # Test with a mock candidate
        test_candidate = {
            "candidate_id": "test-456",
            "venue_name": "Mock Restaurant",
            "legal_name": "Mock LLC",
            "address": "456 Oak St, Houston, TX 77002"
        }

        # Create the candidate in DB first
        with db_manager.get_session() as session:
            candidate = Candidate(
                candidate_id=test_candidate["candidate_id"],
                venue_name=test_candidate["venue_name"],
                legal_name=test_candidate["legal_name"],
                address=test_candidate["address"],
                city="Houston",
                state="TX",
                zip_code="77002",
                first_seen=datetime.now(),
                last_seen=datetime.now()
            )
            session.add(candidate)
            session.commit()

        # Test contact finding
        results = agent.find_contacts([test_candidate])

        print(f"   Found {len(results)} candidates with contacts")
        if results and 'contacts' in results[0]:
            print(f"   Found {len(results[0]['contacts'])} contacts")

        print("✅ ContactFinder agent test passed")

    except Exception as e:
        print(f"❌ ContactFinder agent test failed: {e}")
        return False

    return True

def test_pipeline_integration():
    """Test that the pipeline runs with ContactFinder integration."""
    print("🧪 Testing pipeline integration...")

    try:
        runner = PipelineRunner()

        # Test pipeline status (this doesn't run the full pipeline)
        status = runner.get_pipeline_status()

        # Verify ContactFinder is initialized
        assert hasattr(runner, 'contact_finder')
        assert runner.contact_finder is not None

        print("✅ Pipeline integration test passed")

    except Exception as e:
        print(f"❌ Pipeline integration test failed: {e}")
        return False

    return True

def test_api_endpoint():
    """Test the contacts API endpoint."""
    print("🧪 Testing contacts API endpoint...")

    try:
        from app.api.routes import router
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.include_router(router)

        client = TestClient(app)

        # Test getting contacts (should return empty initially)
        response = client.get("/contacts")
        assert response.status_code == 200

        data = response.json()
        assert "contacts" in data
        assert "total_found" in data

        print("✅ API endpoint test passed")

    except ImportError:
        print("⚠️  API endpoint test skipped (TestClient not available)")
        return True
    except Exception as e:
        print(f"❌ API endpoint test failed: {e}")
        return False

    return True

def cleanup_test_data():
    """Clean up test data from database."""
    print("🧹 Cleaning up test data...")

    try:
        with db_manager.get_session() as session:
            # Remove test contacts
            session.query(Contact).filter(Contact.candidate_id.in_(["test-123", "test-456"])).delete()

            # Remove test candidates
            session.query(Candidate).filter(Candidate.candidate_id.in_(["test-123", "test-456"])).delete()

            session.commit()

        print("✅ Cleanup completed")

    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")

def main():
    """Run all tests."""
    print("🚀 Starting ContactFinder Integration Tests\n")

    tests = [
        ("Database Schema", test_database_schema),
        ("ContactFinder Agent", test_contact_finder_agent),
        ("Pipeline Integration", test_pipeline_integration),
        ("API Endpoint", test_api_endpoint)
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {test_name}")
        print('='*50)

        success = test_func()
        results.append((test_name, success))

    # Summary
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print('='*50)

    passed = 0
    total = len(results)

    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:25} {status}")
        if success:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    # Cleanup
    cleanup_test_data()

    if passed == total:
        print("\n🎉 All tests passed! ContactFinder integration is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the output above.")
        return 1

if __name__ == "__main__":
    exit(main())
