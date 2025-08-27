#!/usr/bin/env python3
"""Test data source connections individually."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.data_sources import DataSourceManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_sources():
    """Test each data source connection individually."""
    
    try:
        logger.info("Initializing DataSourceManager...")
        manager = DataSourceManager(
            tabc_app_token=os.getenv('TABC_APP_TOKEN'),
            comptroller_api_key=os.getenv('TX_COMPTROLLER_API_KEY')
        )
        
        results = {}
        
        # Test TABC client
        logger.info("Testing TABC client...")
        try:
            tabc_records = list(manager.tabc_client.fetch_records(limit=1))
            results['tabc'] = {'status': 'success', 'records': len(tabc_records)}
            logger.info(f"✅ TABC: fetched {len(tabc_records)} records")
        except Exception as e:
            results['tabc'] = {'status': 'error', 'error': str(e)}
            logger.error(f"❌ TABC failed: {e}")
        
        # Test Houston Health client
        logger.info("Testing Houston Health client...")
        try:
            health_records = list(manager.houston_health_client.fetch_records(limit=1))
            results['houston_health'] = {'status': 'success', 'records': len(health_records)}
            logger.info(f"✅ Houston Health: fetched {len(health_records)} records")
        except Exception as e:
            results['houston_health'] = {'status': 'error', 'error': str(e)}
            logger.error(f"❌ Houston Health failed: {e}")
        
        # Test Harris Permits client
        logger.info("Testing Harris Permits client...")
        try:
            permits_records = list(manager.harris_permits_client.fetch_records(limit=1))
            results['harris_permits'] = {'status': 'success', 'records': len(permits_records)}
            logger.info(f"✅ Harris Permits: fetched {len(permits_records)} records")
        except Exception as e:
            results['harris_permits'] = {'status': 'error', 'error': str(e)}
            logger.error(f"❌ Harris Permits failed: {e}")
        
        # Test Comptroller client (if configured)
        if manager.comptroller_client:
            logger.info("Testing Comptroller client...")
            try:
                # Test with a simple query
                test_data = {"venue_name": "Test Restaurant", "address": "123 Main St"}
                enrichment = manager.comptroller_client.enrich_candidate(test_data)
                results['comptroller'] = {'status': 'success', 'enrichment_available': bool(enrichment)}
                logger.info("✅ Comptroller: connection successful")
            except Exception as e:
                results['comptroller'] = {'status': 'error', 'error': str(e)}
                logger.error(f"❌ Comptroller failed: {e}")
        else:
            results['comptroller'] = {'status': 'not_configured'}
            logger.warning("⚠️ Comptroller client not configured (missing API key)")
        
        # Summary
        successful = sum(1 for r in results.values() if r['status'] == 'success')
        total_configured = len([r for r in results.values() if r['status'] != 'not_configured'])
        
        logger.info(f"\n📊 Data Source Test Summary:")
        logger.info(f"   Successful: {successful}/{total_configured}")
        logger.info(f"   Results: {results}")
        
        return successful > 0  # Return True if at least one source works
        
    except Exception as e:
        logger.error(f"Data source testing failed: {e}")
        return False

if __name__ == "__main__":
    success = test_data_sources()
    if success:
        logger.info("✅ At least one data source is working!")
        sys.exit(0)
    else:
        logger.error("❌ No data sources are working!")
        sys.exit(1)
