#!/usr/bin/env python3
"""Test script for Firecrawl integration with ContactFinder - using working websites."""

import os
import sys
import json
from typing import Dict, Any

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.tools.firecrawl_tools import FirecrawlContactTool


def test_firecrawl_tool_working_sites():
    """Test FirecrawlContactTool with websites that should be working."""
    print("🔍 Testing FirecrawlContactTool with working websites...")
    
    tool = FirecrawlContactTool()
    
    # Test with working restaurant websites
    test_domains = [
        "https://www.chipotle.com",
        "https://www.mcdonalds.com",
        "https://www.starbucks.com"
    ]
    
    for domain in test_domains:
        print(f"\n📱 Testing domain: {domain}")
        try:
            result = tool._run(domain)
            data = json.loads(result)
            
            if data.get("success"):
                contacts = data.get("contacts", [])
                print(f"✅ Found {len(contacts)} contacts")
                for contact in contacts[:3]:  # Show first 3
                    email = contact.get('email', 'N/A')
                    name = contact.get('full_name', 'Unknown')
                    phone = contact.get('phone', 'N/A')
                    print(f"   👤 {name} | 📧 {email} | 📞 {phone}")
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
    print("🚀 Working Websites Firecrawl Test")
    print("=" * 40)
    
    # Check configuration
    if not check_firecrawl_config():
        print("\n⚠️  Please configure Firecrawl API key before testing")
        sys.exit(1)
    
    # Test Firecrawl tool with working websites
    test_firecrawl_tool_working_sites()
    
    print("\n✅ Working websites test completed!")
