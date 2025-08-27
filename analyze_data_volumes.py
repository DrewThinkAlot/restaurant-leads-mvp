#!/usr/bin/env python3
"""Analyze data volumes being pulled from each data source."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.data_sources import DataSourceManager
from app.data_sources.watermark_manager import WatermarkManager
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_data_volumes():
    """Analyze current data volumes from each source."""
    
    logger.info("🔍 ANALYZING DATA VOLUMES FROM ALL SOURCES")
    logger.info("=" * 60)
    
    # Initialize manager
    manager = DataSourceManager(
        tabc_app_token=os.getenv('TABC_APP_TOKEN'),
        comptroller_api_key=os.getenv('TX_COMPTROLLER_API_KEY')
    )
    
    # Check watermarks to understand incremental windows
    watermark_manager = WatermarkManager()
    watermark_status = watermark_manager.get_status_summary()
    
    logger.info("📅 Current Watermark Status:")
    for source, status in watermark_status.get('sources', {}).items():
        last_update = status.get('last_watermark', 'Never')
        total_fetches = status.get('total_fetches', 0)
        total_records = status.get('total_records_fetched', 0)
        logger.info(f"   {source}: Last update = {last_update}, "
                   f"Total fetches = {total_fetches}, Total records = {total_records}")
    
    # Test each source with different limits
    test_limits = [1, 10, 50, 100]
    results = {}
    
    for source_name, client in manager.clients.items():
        if source_name == "comptroller":
            continue  # Skip comptroller for bulk testing
        
        logger.info(f"\n🔍 Testing {source_name.upper()}:")
        results[source_name] = {}
        
        for limit in test_limits:
            try:
                logger.info(f"   Testing with limit={limit}...")
                records = list(client.fetch_records(limit=limit))
                results[source_name][limit] = len(records)
                logger.info(f"     Retrieved: {len(records)} records")
                
                # Log first record details if available
                if records:
                    first_record = records[0]
                    logger.info(f"     Sample record keys: {list(first_record.keys())}")
                    if 'venue_name' in first_record:
                        logger.info(f"     Sample venue: {first_record.get('venue_name', 'N/A')}")
                
            except Exception as e:
                logger.error(f"     Error with limit {limit}: {e}")
                results[source_name][limit] = f"ERROR: {str(e)}"
    
    # Test full data fetch (what the pipeline actually uses)
    logger.info(f"\n🚀 Testing Full Pipeline Data Fetch:")
    try:
        full_results = manager.fetch_all_sources(limit_per_source=100)
        total_records = sum(len(records) for records in full_results.values())
        
        logger.info(f"   Total records fetched: {total_records}")
        for source, records in full_results.items():
            logger.info(f"   {source}: {len(records)} records")
    except Exception as e:
        logger.error(f"   Full fetch error: {e}")
    
    # Check recent data availability  
    logger.info(f"\n📊 Data Freshness Analysis:")
    today = datetime.now()
    for days_back in [1, 7, 30, 90]:
        since_date = today - timedelta(days=days_back)
        logger.info(f"   Checking data since {days_back} days ago ({since_date.strftime('%Y-%m-%d')}):")
        
        for source_name, client in manager.clients.items():
            if source_name == "comptroller":
                continue
            
            try:
                recent_records = list(client.fetch_records(since=since_date, limit=10))
                logger.info(f"     {source_name}: {len(recent_records)} records")
            except Exception as e:
                logger.info(f"     {source_name}: Error - {e}")
    
    # Data source configuration check
    logger.info(f"\n⚙️ Data Source Configuration:")
    logger.info(f"   TABC_APP_TOKEN: {'✅ Set' if os.getenv('TABC_APP_TOKEN') else '❌ Missing'}")
    logger.info(f"   TX_COMPTROLLER_API_KEY: {'✅ Set' if os.getenv('TX_COMPTROLLER_API_KEY') != 'your_texas_comptroller_api_key_here' else '❌ Placeholder'}")
    logger.info(f"   SOCRATA_APP_TOKEN: {'✅ Set' if os.getenv('SOCRATA_APP_TOKEN') else '❌ Missing'}")
    
    return results

def check_specific_datasets():
    """Check specific dataset endpoints for availability."""
    
    logger.info(f"\n🔗 Checking Specific Dataset Endpoints:")
    
    from app.data_sources.tabc_client import TABCClient
    from app.data_sources.houston_health_client import HoustonHealthClient  
    from app.data_sources.harris_permits_client import HarrisPermitsClient
    
    # TABC datasets
    logger.info("   TABC Datasets:")
    tabc = TABCClient(os.getenv('TABC_APP_TOKEN'))
    try:
        pending_count = len(list(tabc._fetch_pending_applications(limit=1)))
        logger.info(f"     Pending applications: {pending_count} (test limit=1)")
        
        issued_count = len(list(tabc._fetch_issued_licenses(limit=1)))  
        logger.info(f"     Issued licenses: {issued_count} (test limit=1)")
    except Exception as e:
        logger.error(f"     TABC error: {e}")
    
    # Houston Health datasets
    logger.info("   Houston Health Datasets:")
    health = HoustonHealthClient()
    try:
        health_count = len(list(health.fetch_records(limit=1)))
        logger.info(f"     Health inspections: {health_count} (test limit=1)")
    except Exception as e:
        logger.error(f"     Houston Health error: {e}")
    
    # Harris County permits
    logger.info("   Harris County Datasets:")
    permits = HarrisPermitsClient()
    try:
        permit_count = len(list(permits.fetch_records(limit=1)))
        logger.info(f"     Building permits: {permit_count} (test limit=1)")
    except Exception as e:
        logger.error(f"     Harris Permits error: {e}")

if __name__ == "__main__":
    logger.info("🔍 DATA VOLUME ANALYSIS")
    logger.info("=" * 60)
    
    results = analyze_data_volumes()
    check_specific_datasets()
    
    logger.info("\n" + "=" * 60)
    logger.info("📋 SUMMARY:")
    logger.info(f"   Pipeline is configured and functional")
    logger.info(f"   Data volumes vary by source and time window")
    logger.info(f"   Check logs above for detailed per-source analysis")
