#!/usr/bin/env python3
"""
Demo script showing how the stable API integration works.
This demonstrates the pipeline without requiring real API keys.
"""

import sys
from pathlib import Path
import json
from datetime import datetime, timedelta

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent / 'app'))

from data_sources.manager import DataSourceManager
from pipelines.run_pipeline import PipelineRunner

def demo_without_api_keys():
    """Demonstrate how the system works without real API calls."""
    print("🎭 DEMO MODE: Simulating API responses")
    print("=" * 50)

    # Create sample data that would come from APIs
    sample_records = [
        {
            "source": "tabc_pending",
            "source_id": "demo_001",
            "venue_name": "Bella Vista Bistro",
            "address": "123 Main St",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77002",
            "application_date": "2024-01-15",
            "license_type": "Mixed Beverage",
            "owner_name": "Sarah Johnson",
            "signal_strength": 0.8,
            "estimated_open_window": {
                "min_days": 60,
                "max_days": 120,
                "confidence": 0.7
            }
        },
        {
            "source": "houston_health",
            "source_id": "demo_002",
            "venue_name": "Downtown Grill House",
            "address": "456 Commerce St",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77002",
            "inspection_date": "2024-02-10",
            "inspection_result": "Satisfactory",
            "signal_strength": 0.6,
            "estimated_open_window": {
                "min_days": 30,
                "max_days": 60,
                "confidence": 0.8
            }
        },
        {
            "source": "harris_permits",
            "source_id": "demo_003",
            "venue_name": "Urban Kitchen & Bar",
            "address": "789 Market Ave",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77002",
            "permit_number": "PERM2024001",
            "permit_status": "Approved",
            "issued_date": "2024-02-05",
            "signal_strength": 0.7,
            "estimated_open_window": {
                "min_days": 45,
                "max_days": 90,
                "confidence": 0.6
            }
        }
    ]

    print(f"📊 Sample data: {len(sample_records)} records from 3 sources")

    # Demonstrate DataSourceManager processing
    print("\n🏗️  Simulating DataSourceManager processing...")

    # Simulate normalization and scoring
    normalized_records = []
    for record in sample_records:
        normalized = record.copy()
        normalized["normalized_at"] = datetime.utcnow().isoformat()
        normalized["composite_lead_score"] = record["signal_strength"] * 0.9  # Slight boost
        normalized_records.append(normalized)

    print(f"✅ Normalized: {len(normalized_records)} records")

    # Simulate deduplication (no duplicates in this sample)
    deduplicated_records = normalized_records.copy()
    print(f"✅ Deduplicated: {len(deduplicated_records)} unique records")

    # Show sample processed record
    print("\n📋 Sample processed record:")
    sample = deduplicated_records[0]
    print(f"   Venue: {sample['venue_name']}")
    print(".2f")
    print(f"   Address: {sample['address']}, {sample['city']}, {sample['state']} {sample['zip_code']}")
    print(f"   Source: {sample['source']}")
    print(f"   License: {sample.get('license_type', 'N/A')}")
    print(f"   Open Window: {sample['estimated_open_window']['min_days']} - {sample['estimated_open_window']['max_days']} days")

    # Simulate CSV export
    print("\n📄 Simulating CSV export...")
    csv_content = "venue_name,address,city,state,zip_code,composite_lead_score,source,estimated_min_days,estimated_max_days\n"

    for record in deduplicated_records[:3]:  # Show first 3
        open_window = record['estimated_open_window']
        csv_content += f"{record['venue_name']},{record['address']},{record['city']},{record['state']},{record['zip_code']},{record['composite_lead_score']:.2f},{record['source']},{open_window['min_days']},{open_window['max_days']}\n"

    # Write demo CSV
    demo_csv_path = "demo_leads.csv"
    with open(demo_csv_path, 'w') as f:
        f.write(csv_content)

    print(f"✅ Demo CSV exported to: {demo_csv_path}")
    print("\n📊 CSV Preview:")
    print(csv_content.strip())

    return True

def show_api_setup_guide():
    """Show guide for setting up API keys."""
    print("\n🔑 API SETUP GUIDE")
    print("=" * 50)

    print("""
1. 🔐 OpenAI API Key (Required for AI agents)
   - Go to: https://platform.openai.com/api-keys
   - Create a new API key
   - Add to .env: OPENAI_API_KEY=your_key_here

2. 🏛️ TABC (Texas Alcoholic Beverage Commission) API
   - Go to: https://data.texas.gov/login
   - Create account and get App Token
   - Add to .env: TABC_APP_TOKEN=your_token_here

3. 🏢 Texas Comptroller API (Optional - for entity enrichment)
   - Request API key from: https://comptroller.texas.gov/api/
   - Add to .env: TX_COMPTROLLER_API_KEY=your_key_here

4. 📝 Update your .env file:
   OPENAI_API_KEY=sk-your-openai-key-here
   TABC_APP_TOKEN=your-socrata-app-token-here
   TX_COMPTROLLER_API_KEY=your-comptroller-key-here

5. 🚀 Test the integration:
   python test_api_integration.py
   """)

def demo_pipeline_runner():
    """Demonstrate how to use PipelineRunner with stable APIs."""
    print("\n🔄 PIPELINE RUNNER DEMO")
    print("=" * 50)

    print("""
# Once you have API keys set up, you can run:

from app.pipelines.run_pipeline import PipelineRunner

runner = PipelineRunner()

# Use stable APIs (recommended)
result = runner.run_complete_pipeline(
    max_candidates=100,
    use_stable_apis=True  # This uses the new API integration!
)

print(f"Generated {result['qualified_leads']} leads")
print(f"CSV export: {result['csv_export_path']}")

# Legacy scraping (fallback)
result_legacy = runner.run_complete_pipeline(
    max_candidates=100,
    use_stable_apis=False  # Uses original scraping approach
)

# Get data source status
status = runner.get_data_source_status()
print("API health:", status)
""")

def main():
    """Main demo function."""
    print("🚀 Restaurant Leads MVP - Stable API Integration Demo")
    print("=" * 60)

    # Show setup guide first
    show_api_setup_guide()

    # Run demo without API keys
    demo_without_api_keys()

    # Show pipeline usage
    demo_pipeline_runner()

    print("\n" + "=" * 60)
    print("🎯 SUMMARY")
    print("=" * 60)
    print("✅ Stable API integration is implemented and ready!")
    print("✅ Data flows: APIs → DataSourceManager → AI Agents → CSV Export")
    print("✅ Benefits: Reliability, compliance, better data quality")
    print("✅ Next step: Set up API keys and test with real data")

    print("\n🔗 Useful Links:")
    print("- TABC Data: https://data.texas.gov/resource/mxm5-tdpj")
    print("- Houston Health: https://data.houstontx.gov/dataset/food-service-facility-inspections")
    print("- Harris Permits: https://www.gis.hctx.net/arcgishcpid/rest/services/Permits/IssuedPermits")
    print("- TX Comptroller: https://api.cpa.texas.gov/public-data/franchise/accounts")

if __name__ == "__main__":
    main()
