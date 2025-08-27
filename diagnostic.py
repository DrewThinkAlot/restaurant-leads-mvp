#!/usr/bin/env python3
"""
Simple diagnostic script to test basic setup.
"""

import sys
import os
from pathlib import Path

print("🔍 Restaurant Leads MVP - Basic Diagnostic")
print("=" * 50)

# Check Python version
print(f"Python version: {sys.version}")

# Check current directory
cwd = os.getcwd()
print(f"Current directory: {cwd}")

# Check if .env exists
env_path = Path('.env')
if env_path.exists():
    print("✅ .env file found")
else:
    print("❌ .env file missing")

# Check required environment variables
required_vars = ['OPENAI_API_KEY']
optional_vars = ['TABC_APP_TOKEN', 'TX_COMPTROLLER_API_KEY']

print("\n🔧 Environment Variables:")
for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {'*' * len(value)}")
    else:
        print(f"❌ {var}: Not set")

for var in optional_vars:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {'*' * len(value)}")
    else:
        print(f"⚠️  {var}: Not set")

# Try basic imports
print("\n📦 Testing imports:")
try:
    import requests
    print("✅ requests available")
except ImportError:
    print("❌ requests missing")

try:
    from app.data_sources.base_client import BaseAPIClient
    print("✅ BaseAPIClient import successful")
except ImportError as e:
    print(f"❌ BaseAPIClient import failed: {e}")

try:
    from app.data_sources.manager import DataSourceManager
    print("✅ DataSourceManager import successful")
except ImportError as e:
    print(f"❌ DataSourceManager import failed: {e}")

try:
    from app.pipelines.run_pipeline import PipelineRunner
    print("✅ PipelineRunner import successful")
except ImportError as e:
    print(f"❌ PipelineRunner import failed: {e}")

# Check app structure
print("\n📁 App structure check:")
app_path = Path('app')
if app_path.exists():
    print("✅ app/ directory exists")

    data_sources_path = app_path / 'data_sources'
    if data_sources_path.exists():
        print("✅ app/data_sources/ exists")

        required_files = ['__init__.py', 'base_client.py', 'manager.py', 'tabc_client.py']
        for file in required_files:
            if (data_sources_path / file).exists():
                print(f"✅ {file} exists")
            else:
                print(f"❌ {file} missing")
    else:
        print("❌ app/data_sources/ missing")
else:
    print("❌ app/ directory missing")

print("\n🎯 Next steps:")
if not os.getenv('OPENAI_API_KEY'):
    print("- Set OPENAI_API_KEY in .env")
if not os.getenv('TABC_APP_TOKEN'):
    print("- Get TABC app token from https://data.texas.gov")
if not os.getenv('TX_COMPTROLLER_API_KEY'):
    print("- Get TX Comptroller API key if needed")

print("- Run: python test_api_integration.py")
