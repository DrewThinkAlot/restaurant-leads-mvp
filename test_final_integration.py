#!/usr/bin/env python3
"""Test FirecrawlContactTool with proper error handling and debugging."""

import os
import sys
import json
import requests

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))


def test_firecrawl_direct():
    """Test Firecrawl API directly to understand the response format."""
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("❌ No API key found")
        return
    
    url = "https://api.firecrawl.dev/v2/scrape"
    
    payload = {
        "url": "https://example.com",
        "formats": [{"type": "json", "prompt": "Extract all contact information including email addresses, phone numbers, and names of key personnel like owners, managers, or decision makers. Return as structured data with email, phone, and full_name fields."}],
        "onlyMainContent": True,
        "timeout": 30000
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    print("🔍 Making direct Firecrawl API call...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        return response.json()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def test_contact_tool():
    """Test the actual ContactFinderAgent with Firecrawl."""
    from app.agents.agent_contact_finder import ContactFinderAgent
    
    print("\n🎯 Testing ContactFinderAgent with Firecrawl...")
    
    agent = ContactFinderAgent()
    
    # Simple test candidate
    test_candidate = {
        "candidate_id": "test-001",
        "venue_name": "Test Restaurant",
        "legal_name": "Test Restaurant LLC",
        "address": "123 Test St, Austin, TX 78701",
        "predicted_open_date": "2025-09-01"
    }
    
    try:
        results = agent.find_contacts([test_candidate])
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ ContactFinder error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🚀 Firecrawl Integration Test")
    print("=" * 50)
    
    # Check configuration
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("❌ Firecrawl API key not found")
        sys.exit(1)
    
    print(f"✅ Firecrawl API key found: {api_key[:8]}...")
    
    # Test direct API call
    result = test_firecrawl_direct()
    
    # Test ContactFinderAgent
    test_contact_tool()
    
    print("\n✅ Integration test completed!")
