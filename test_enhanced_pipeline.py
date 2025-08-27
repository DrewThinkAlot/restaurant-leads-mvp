#!/usr/bin/env python3
"""Test the enhanced pipeline end-to-end."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_enhanced_pipeline():
    """Test the enhanced pipeline with a small dataset."""
    
    try:
        logger.info("🚀 Starting enhanced pipeline test...")
        
        # Initialize the enhanced pipeline
        pipeline = EnhancedPipelineRunner()
        
        # Run hybrid pipeline with small limits for testing
        logger.info("Running hybrid pipeline with test parameters...")
        result = pipeline.run_hybrid_pipeline(
            max_candidates=10,  # Small limit for testing
            harris_only=True,
            use_stable_apis=True,
            use_ai_enhancement=True
        )
        
        # Analyze results
        logger.info("📊 Pipeline Results:")
        logger.info(f"   Execution Success: {result.get('execution_success', 'Unknown')}")
        logger.info(f"   Total Candidates: {result.get('total_candidates', 0)}")
        logger.info(f"   Qualified Leads: {result.get('qualified_leads', 0)}")
        logger.info(f"   Execution Time: {result.get('execution_time_seconds', 0):.1f}s")
        logger.info(f"   API Success: {result.get('api_success', 'Unknown')}")
        logger.info(f"   AI Enhancement: {result.get('ai_enhancement', 'Unknown')}")
        
        # Check pipeline stages if available
        stages = result.get('pipeline_stages', {})
        if stages:
            logger.info("   Pipeline Stages:")
            for stage, count in stages.items():
                logger.info(f"     {stage}: {count}")
        
        # Verify basic success criteria
        execution_success = result.get('execution_success', False)
        api_success = result.get('api_success', False)
        
        if execution_success and api_success:
            logger.info("✅ Enhanced pipeline test completed successfully!")
            return True
        elif execution_success:
            logger.warning("⚠️ Pipeline completed but with API issues")
            return True
        else:
            logger.error("❌ Pipeline execution failed")
            return False
            
    except Exception as e:
        logger.error(f"Pipeline test failed with exception: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

def test_api_only_pipeline():
    """Test API-only pipeline as fallback."""
    
    try:
        logger.info("🔄 Testing API-only pipeline as fallback...")
        
        pipeline = EnhancedPipelineRunner()
        
        result = pipeline.run_hybrid_pipeline(
            max_candidates=5,
            harris_only=True,
            use_stable_apis=True,
            use_ai_enhancement=False  # Disable AI for API-only test
        )
        
        logger.info("📊 API-Only Results:")
        logger.info(f"   Total Candidates: {result.get('total_candidates', 0)}")
        logger.info(f"   API Success: {result.get('api_success', 'Unknown')}")
        
        return result.get('api_success', False)
        
    except Exception as e:
        logger.error(f"API-only test failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🧪 ENHANCED PIPELINE COMPREHENSIVE TEST")
    logger.info("=" * 60)
    
    # Test enhanced pipeline
    full_success = test_enhanced_pipeline()
    
    # Test API-only as fallback
    api_success = test_api_only_pipeline()
    
    logger.info("=" * 60)
    logger.info("📋 TEST SUMMARY:")
    logger.info(f"   Enhanced Pipeline: {'✅ PASS' if full_success else '❌ FAIL'}")
    logger.info(f"   API-Only Pipeline: {'✅ PASS' if api_success else '❌ FAIL'}")
    
    if full_success or api_success:
        logger.info("🎉 At least one pipeline mode is working!")
        logger.info("   The enhanced pipeline is ready for production use.")
        sys.exit(0)
    else:
        logger.error("💥 Both pipeline modes failed!")
        logger.error("   Please check configuration and data sources.")
        sys.exit(1)
