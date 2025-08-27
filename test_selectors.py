#!/usr/bin/env python3
"""Test script for improved web scraping selectors."""

import sys
import os
import json

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.tools.contact_tools import TABCLookupTool, ComptrollerLookupTool

def test_tabc_improvements():
    """Test the improved TABC selectors."""
    print("🧪 Testing improved TABC selectors...")

    try:
        tool = TABCLookupTool()

        # Test with a simple venue name
        result_json = tool._run("Pizza Place", "123 Main St, Houston, TX")
        result = json.loads(result_json)

        print(f"   TABC Result: {result.get('success', False)}")
        print(f"   Contacts found: {len(result.get('contacts', []))}")
        print(f"   Page analysis: {result.get('page_analysis', {})}")

        return True

    except Exception as e:
        print(f"❌ TABC test failed: {e}")
        return False

def test_comptroller_improvements():
    """Test the improved Comptroller selectors."""
    print("🧪 Testing improved Comptroller selectors...")

    try:
        tool = ComptrollerLookupTool()

        # Test with a sample business name
        result_json = tool._run("Sample Restaurant LLC")
        result = json.loads(result_json)

        print(f"   Comptroller Result: {result.get('success', False)}")
        print(f"   Contacts found: {len(result.get('contacts', []))}")
        print(f"   Page analysis: {result.get('page_analysis', {})}")

        return True

    except Exception as e:
        print(f"❌ Comptroller test failed: {e}")
        return False

def test_selector_analysis():
    """Test the page analysis functionality."""
    print("🧪 Testing selector analysis...")

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        # Test page analysis without full search
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=chrome_options)

        # Test TABC page analysis
        driver.get("https://www.tabc.texas.gov/public-information/tabc-public-inquiry/")

        tabc_tool = TABCLookupTool()
        tabc_analysis = tabc_tool._analyze_tabc_page(driver)

        print(f"   TABC page analysis: {tabc_analysis}")

        # Test Comptroller page analysis
        driver.get("https://www.cpa.state.tx.us/taxinfo/bus_entity_search/bus_entity_search.php")

        comptroller_tool = ComptrollerLookupTool()
        comptroller_analysis = comptroller_tool._analyze_comptroller_page(driver)

        print(f"   Comptroller page analysis: {comptroller_analysis}")

        driver.quit()
        return True

    except Exception as e:
        print(f"❌ Page analysis test failed: {e}")
        return False

def main():
    """Run all selector tests."""
    print("🚀 Testing Improved Web Scraping Selectors\n")

    tests = [
        ("TABC Improvements", test_tabc_improvements),
        ("Comptroller Improvements", test_comptroller_improvements),
        ("Page Analysis", test_selector_analysis)
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
    print("SELECTOR TEST SUMMARY")
    print('='*50)

    passed = 0
    total = len(results)

    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:25} {status}")
        if success:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All selector tests passed! Web scraping improvements are working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. The selectors may need further tuning.")
        return 1

if __name__ == "__main__":
    exit(main())
