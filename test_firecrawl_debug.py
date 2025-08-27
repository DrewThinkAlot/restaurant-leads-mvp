#!/usr/bin/env python3
"""Test FirecrawlContactTool with detailed debugging."""

import os
import sys
import json

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.tools.firecrawl_tools import FirecrawlContactTool


def test_firecrawl_detailed():
    """Test FirecrawlContactTool with detailed debugging."""
    print("🔍 Testing FirecrawlContactTool with debugging...")
    
    tool = FirecrawlContactTool()
    
    # Test with a simple working website
    test_domain = "example.com"
    
    print(f"\n📱 Testing domain: {test_domain}")
    try:
        result = tool._run(test_domain)
        data = json.loads(result)
        
        print(f"Result: {json.dumps(data, indent=2)}")
        
        if data.get("success"):
            contacts = data.get("contacts", [])
            print(f"✅ Found {len(contacts)} contacts")
            for contact in contacts:
                print(f"   👤 {contact}")
        else:
            print(f"❌ Failed: {data.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🚀 Detailed Firecrawl Test")
    print("=" * 40)
    
    # Check configuration
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("❌ Firecrawl API key not found")
        sys.exit(1)
    
    print(f"✅ Firecrawl API key found: {api_key[:8]}...")
    
    # Test Firecrawl tool
    test_firecrawl_detailed()
    
    print("\n✅ Detailed test completed!")
