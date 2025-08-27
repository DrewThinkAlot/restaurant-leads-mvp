#!/usr/bin/env python3
"""Test script for Firecrawl integration with ContactFinder."""

import os
import sys
import json
from typing import Dict, Any

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.tools.firecrawl_tools import FirecrawlContactTool


def test_firecrawl_tool_simple():
    """Test FirecrawlContactTool with a simple domain."""
    print("🔍 Testing FirecrawlContactTool...")
    
    tool = FirecrawlContactTool()
    
    # Test with a simple website
    test_domain = "https://httpbin.org/html"  # Simple test page
    
    print(f"\n📱 Testing domain: {test_domain}")
    try:
        result = tool._run(test_domain)
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
    print("🚀 Simple Firecrawl Test")
    print("=" * 30)
    
    # Check configuration
    if not check_firecrawl_config():
        print("\n⚠️  Please configure Firecrawl API key before testing")
        sys.exit(1)
    
    # Test Firecrawl tool directly with a simple page
    test_firecrawl_tool_simple()
    
    print("\n✅ Simple test completed!")
