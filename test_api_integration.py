#!/usr/bin/env python3
"""
Test script for stable API integrations.

This script validates the new data source integrations and helps ensure
everything is working correctly before running the full pipeline.
"""

import sys
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is on sys.path and import from app package
sys.path.insert(0, str(Path(__file__).parent))

from app.data_sources.manager import DataSourceManager
from app.data_sources.tabc_client import TABCClient
from app.data_sources.houston_health_client import HoustonHealthClient
from app.data_sources.harris_permits_client import HarrisPermitsClient
from app.data_sources.comptroller_client import ComptrollerClient
from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner

def test_env_setup():
    """Test environment variables and API keys."""
    print("🔧 Testing environment setup...")

    # If using local VLLM or OSS model, do not require OpenAI key
    use_local_model = bool(os.getenv('VLLM_BASE_URL')) or os.getenv('MODEL_ID', '').lower().startswith('openai/')
    required_vars = [] if use_local_model else ['OPENAI_API_KEY']
    optional_vars = ['TABC_APP_TOKEN', 'TX_COMPTROLLER_API_KEY']

    missing_required = []
    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)

    if missing_required:
        print(f"❌ Missing required environment variables: {missing_required}")
        print("Please set these in your .env file")
        return False

    if required_vars:
        print("✅ Required environment variables found")
    else:
        print("ℹ️  Running in local model mode (no OpenAI key required)")

    # Check optional API keys
    has_tabc = bool(os.getenv('TABC_APP_TOKEN'))
    has_comptroller = bool(os.getenv('TX_COMPTROLLER_API_KEY'))

    if has_tabc:
        print("✅ TABC API token configured")
    else:
        print("⚠️  TABC API token not configured (some features will be limited)")

    if has_comptroller:
        print("✅ TX Comptroller API key configured")
    else:
        print("⚠️  TX Comptroller API key not configured (enrichment disabled)")

    return True

def test_individual_clients():
    """Test each API client individually."""
    print("\n🔌 Testing individual API clients...")

    results = {}

    # Test TABC Client
    print("Testing TABC Client...")
    try:
        tabc = TABCClient(os.getenv('TABC_APP_TOKEN'))
        records = list(tabc.fetch_records(limit=5))
        results['tabc'] = len(records)
        print(f"✅ TABC: {len(records)} records fetched")
        if records:
            print(f"   Sample: {records[0].get('venue_name', 'Unknown')}")
    except Exception as e:
        results['tabc'] = 0
        print(f"❌ TABC failed: {e}")

    # Test Houston Health Client
    print("Testing Houston Health Client...")
    try:
        health = HoustonHealthClient()
        records = list(health.fetch_records(limit=5))
        results['houston_health'] = len(records)
        print(f"✅ Houston Health: {len(records)} records fetched")
        if records:
            print(f"   Sample: {records[0].get('venue_name', 'Unknown')}")
    except Exception as e:
        results['houston_health'] = 0
        print(f"❌ Houston Health failed: {e}")

    # Test Harris Permits Client
    print("Testing Harris County Permits Client...")
    try:
        permits = HarrisPermitsClient()
        records = list(permits.fetch_records(limit=5))
        results['harris_permits'] = len(records)
        print(f"✅ Harris Permits: {len(records)} records fetched")
        if records:
            print(f"   Sample: {records[0].get('venue_name', 'Unknown')}")
    except Exception as e:
        results['harris_permits'] = 0
        print(f"❌ Harris Permits failed: {e}")

    # Test Comptroller Client (if configured)
    if os.getenv('TX_COMPTROLLER_API_KEY'):
        print("Testing TX Comptroller Client...")
        try:
            comptroller = ComptrollerClient(os.getenv('TX_COMPTROLLER_API_KEY'))
            # Test search
            result = comptroller.search_by_name("ACME RESTAURANT", "HOUSTON")
            results['comptroller'] = len(result.get('matches', []))
            print(f"✅ Comptroller: {len(result.get('matches', []))} matches found")
        except Exception as e:
            results['comptroller'] = 0
            print(f"❌ Comptroller failed: {e}")
    else:
        results['comptroller'] = 0
        print("⚠️  Comptroller client skipped (no API key)")

    return results

def test_data_source_manager():
    """Test the DataSourceManager integration."""
    print("\n🏗️  Testing DataSourceManager...")

    try:
        manager = DataSourceManager(
            tabc_app_token=os.getenv('TABC_APP_TOKEN'),
            comptroller_api_key=os.getenv('TX_COMPTROLLER_API_KEY')
        )

        # Fetch small batch from all sources
        print("Fetching data from all sources...")
        raw_results = manager.fetch_all_sources(limit_per_source=10, parallel=False)

        total_records = sum(len(records) for records in raw_results.values())
        print(f"✅ Fetched {total_records} total records from {len(raw_results)} sources")

        if total_records > 0:
            # Test normalization and scoring
            print("Testing normalization and scoring...")
            normalized = manager.normalize_and_score_records(raw_results)
            print(f"✅ Normalized to {len(normalized)} records")

            # Test deduplication
            print("Testing deduplication...")
            deduplicated = manager.deduplicate_records(normalized)
            print(f"✅ Deduplicated to {len(deduplicated)} unique records")

            # Show sample record
            if deduplicated:
                sample = deduplicated[0]
                print("\n📋 Sample processed record:")
                print(f"   Venue: {sample.get('venue_name', 'Unknown')}")
                if 'composite_lead_score' in sample:
                    try:
                        print(f"   Score: {float(sample.get('composite_lead_score', 0)):.2f}")
                    except Exception:
                        pass
                print(f"   Source: {sample.get('source', 'Unknown')}")
                ew = sample.get('estimated_open_window', {})
                print(f"   Open Window: {ew.get('min_days', '?')} - {ew.get('max_days', '?')} days")

            # Test CSV export
            print("Testing CSV export...")
            csv_path = manager.export_to_csv(deduplicated[:5], "test_export.csv")
            print(f"✅ CSV exported to: {csv_path}")

            return True
        else:
            print("⚠️  No records fetched from any source")
            return False

    except Exception as e:
        print(f"❌ DataSourceManager failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_pipeline_integration():
    """Test the pipeline integration."""
    print("\n🔄 Testing pipeline integration...")

    try:
        runner = EnhancedPipelineRunner()

        # Test with small batch
        print("Running enhanced hybrid pipeline (small test batch)...")
        result = runner.run_hybrid_pipeline(
            max_candidates=20,
            use_stable_apis=True,
            use_ai_enhancement=True
        )

        print("✅ Pipeline completed successfully!")
        print(f"   Total candidates: {result.get('total_candidates')}")
        print(f"   Qualified leads: {result.get('qualified_leads')}")

        if result.get('csv_export_path'):
            print(f"   CSV export: {result['csv_export_path']}")

        return True

    except Exception as e:
        print(f"❌ Pipeline integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_quick_dry_test():
    """Run a quick test without external API calls."""
    print("\n🧪 Running quick dry test (no external calls)...")

    try:
        # Test that all imports work
        from app.data_sources.base_client import BaseAPIClient
        from app.data_sources.watermark_manager import WatermarkManager
        print("✅ All imports successful")

        # Test DataSourceManager initialization
        manager = DataSourceManager()
        print("✅ DataSourceManager initialized")

        # Test pipeline runner
        runner = PipelineRunner()
        print("✅ PipelineRunner initialized")

        return True

    except Exception as e:
        print(f"❌ Quick test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    print("🚀 Restaurant Leads MVP - Stable API Integration Test")
    print("=" * 60)

    # Test 1: Environment setup
    if not test_env_setup():
        print("\n❌ Environment setup failed. Please fix before continuing.")
        return False

    # Test 2: Quick dry test
    if not run_quick_dry_test():
        print("\n❌ Basic setup test failed.")
        return False

    # Test 3: Individual API clients
    client_results = test_individual_clients()

    # Test 4: DataSourceManager
    manager_success = test_data_source_manager()

    # Test 5: Pipeline integration (only if we have some data)
    total_records = sum(client_results.values())
    if total_records > 0:
        pipeline_success = test_pipeline_integration()
    else:
        print("\n⚠️  Skipping pipeline test (no data from APIs)")
        pipeline_success = False

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    print("\n🔌 API Client Results:")
    for client, count in client_results.items():
        status = "✅" if count > 0 else "❌" if count == 0 else "⚠️"
        print(f"   {status} {client}: {count} records")

    print("\n🏗️  DataSourceManager:")
    print(f"   {'✅' if manager_success else '❌'} {'PASS' if manager_success else 'FAIL'}")

    print("\n🔄 Pipeline Integration:")
    if total_records > 0:
        print(f"   {'✅' if pipeline_success else '❌'} {'PASS' if pipeline_success else 'FAIL'}")
    else:
        print("   ⚠️  SKIPPED (no API data)")

    # Recommendations
    print("\n💡 RECOMMENDATIONS:")
    if total_records == 0:
        print("   - Check your API keys in .env file (if using hosted models)")
        print("   - Verify network connectivity to government APIs")
        print("   - Check API rate limits or service status")

    if manager_success and total_records > 0:
        print("   - Ready to run full pipeline!")
        print("   - Consider running incremental updates for daily processing")

    print("\n🎯 Next Steps:")
    print("   1. Fix any failing components")
    print("   2. Run: python test_api_integration.py")
    print("   3. Then try: runner.run_hybrid_pipeline(use_stable_apis=True, use_ai_enhancement=True)")

    return all([manager_success, total_records > 0])

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
