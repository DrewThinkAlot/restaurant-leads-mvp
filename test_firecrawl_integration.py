#!/usr/bin/env python3
"""Test script for Firecrawl integration with ContactFinder."""

import os
import sys
import json
from typing import Dict, Any

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.agents.agent_contact_finder import ContactFinderAgent
from app.tools.firecrawl_tools import FirecrawlContactTool


def test_firecrawl_tool():
    """Test FirecrawlContactTool with a sample domain."""
    print("🔍 Testing FirecrawlContactTool...")
    
    tool = FirecrawlContactTool()
    
    # Test with a real restaurant domain
    test_domains = [
        "torchystacos.com",
        "chuy.com",
        "trulucks.com"
    ]
    
    for domain in test_domains:
        print(f"\n📱 Testing domain: {domain}")
        try:
            result = tool._run(domain)
            data = json.loads(result)
            
            if data.get("success"):
                contacts = data.get("contacts", [])
                print(f"✅ Found {len(contacts)} contacts")
                for contact in contacts[:2]:  # Show first 2
                    print(f"   📧 {contact.get('email', 'N/A')} - {contact.get('full_name', 'Unknown')}")
            else:
                print(f"❌ Failed: {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error: {e}")


def test_contact_finder_with_firecrawl():
    """Test ContactFinderAgent with Firecrawl integration."""
    print("\n🎯 Testing ContactFinderAgent with Firecrawl...")
    
    agent = ContactFinderAgent()
    
    # Sample restaurant data
    sample_candidates = [
        {
            "candidate_id": "test-001",
            "venue_name": "Torchy's Tacos",
            "legal_name": "Torchy's Tacos LLC",
            "address": "2801 Guadalupe St, Austin, TX 78705",
            "predicted_open_date": "2025-09-01"
        },
        {
            "candidate_id": "test-002", 
            "venue_name": "Chuy's",
            "legal_name": "Chuy's Restaurants LLC",
            "address": "1728 Barton Springs Rd, Austin, TX 78704",
            "predicted_open_date": "2025-09-15"
        }
    ]
    
    print("📊 Processing sample candidates...")
    try:
        results = agent.find_contacts(sample_candidates)
        
        for result in results:
            print(f"\n🏪 {result.get('venue_name')}")
            contacts = result.get('contacts', [])
            print(f"   📞 Found {len(contacts)} contacts")
            
            for contact in contacts:
                print(f"   👤 {contact.get('full_name')} - {contact.get('email', 'N/A')}")
                print(f"      Source: {contact.get('source')} - Confidence: {contact.get('confidence_0_1')}")
                
    except Exception as e:
        print(f"❌ ContactFinder failed: {e}")


def check_firecrawl_config():
    """Check if Firecrawl API key is configured."""
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if api_key:
        print(f"✅ Firecrawl API key found: {api_key[:8]}...")
        return True
    else:
        print("❌ Firecrawl API key not found")
        print("💡 Set FIRECRAWL_API_KEY environment variable")
        return False


if __name__ == "__main__":
    print("🚀 Firecrawl Integration Test")
    print("=" * 50)
    
    # Check configuration
    if not check_firecrawl_config():
        print("\n⚠️  Please configure Firecrawl API key before testing")
        sys.exit(1)
    
    # Test Firecrawl tool directly
    test_firecrawl_tool()
    
    # Test full ContactFinder integration
    test_contact_finder_with_firecrawl()
    
    print("\n✅ Firecrawl integration test completed!")
