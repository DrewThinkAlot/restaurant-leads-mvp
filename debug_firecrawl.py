#!/usr/bin/env python3
"""Debug Firecrawl API calls."""

import os
import requests
import json

def debug_firecrawl_api():
    """Debug Firecrawl API calls to see what's wrong."""
    
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("❌ No API key found")
        return
    
    # Test with the correct v2 API format
    url = "https://api.firecrawl.dev/v2/scrape"
    
    payload = {
        "url": "https://httpbin.org/html",
        "formats": [{"type": "json", "prompt": "Extract all contact information including email addresses, phone numbers, and names from this page."}],
        "onlyMainContent": True,
        "timeout": 30000
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    print("🔍 Making API call to Firecrawl v2...")
    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Success!")
            print(f"Response: {json.dumps(data, indent=2)}")
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    debug_firecrawl_api()
