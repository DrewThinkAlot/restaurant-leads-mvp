#!/usr/bin/env python3
"""Test pipeline with realistic opening signals to ensure proper qualification."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from app.rules import ETARulesEngine
from app.agents.agent_eta import ETAAgent
from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_candidate_with_signals(scenario: str):
    """Create test candidates with different opening signal scenarios."""
    
    base_candidate = {
        "candidate_id": f"test_{scenario}",
        "venue_name": f"New Restaurant {scenario}",
        "legal_name": f"Restaurant Corp {scenario}",
        "address": "123 Main Street",
        "city": "Houston",
        "state": "TX",
        "zip": "77002",
        "source_flags": {"api_sourced": True}
    }
    
    today = datetime.now()
    
    if scenario == "high_probability":
        # Recent TABC pending + approved plan review (should qualify 30-60 days)
        base_candidate["signals"] = {
            "tabc_status": "Original Pending",
            "tabc_dates": {
                "application_date": (today - timedelta(days=20)).isoformat()
            },
            "health_status": "plan_review_approved",
            "permit_types": ["Building Permit Issued", "Plan Review Approved"],
            "milestone_dates": {
                "plan_approved_date": (today - timedelta(days=10)).isoformat(),
                "building_permit_date": (today - timedelta(days=25)).isoformat()
            }
        }
    
    elif scenario == "final_inspection":
        # Final inspection scheduled (should qualify 7-30 days)
        base_candidate["signals"] = {
            "tabc_status": "Original Issued",
            "health_status": "final_inspection_scheduled",
            "permit_types": ["Final Inspection Scheduled", "CO Pending"],
            "milestone_dates": {
                "final_inspection_date": (today + timedelta(days=5)).isoformat(),
                "co_scheduled_date": (today + timedelta(days=10)).isoformat()
            }
        }
    
    elif scenario == "medium_tabc_aged":
        # Aged TABC application (should qualify 30-60 days)
        base_candidate["signals"] = {
            "tabc_status": "Original Pending", 
            "tabc_dates": {
                "application_date": (today - timedelta(days=45)).isoformat()
            },
            "health_status": "application_received",
            "permit_types": ["TABC Application Pending"],
            "milestone_dates": {}
        }
    
    elif scenario == "plan_review_building":
        # Plan review + recent building permit (should qualify 45-90 days)
        base_candidate["signals"] = {
            "health_status": "plan_review_received",
            "permit_types": ["Building Permit", "Tenant Build Out"],
            "milestone_dates": {
                "plan_review_date": (today - timedelta(days=15)).isoformat(),
                "building_permit_date": (today - timedelta(days=30)).isoformat()
            }
        }
    
    elif scenario == "no_signals":
        # Historical record with no opening signals (should NOT qualify)
        base_candidate["signals"] = {
            "tabc_status": "Original Issued",
            "health_status": "inspection_complete",
            "permit_types": ["Historical Inspection"],
            "milestone_dates": {
                "inspection_date": (today - timedelta(days=365)).isoformat()
            }
        }
    
    return base_candidate

def test_eta_rules_engine():
    """Test ETA rules engine with different signal combinations."""
    
    logger.info("🔍 Testing ETA Rules Engine...")
    rules_engine = ETARulesEngine()
    
    test_scenarios = [
        "high_probability",
        "final_inspection", 
        "medium_tabc_aged",
        "plan_review_building",
        "no_signals"
    ]
    
    results = {}
    
    for scenario in test_scenarios:
        candidate = create_test_candidate_with_signals(scenario)
        signals = candidate["signals"]
        
        logger.info(f"Testing scenario: {scenario}")
        result = rules_engine.evaluate_candidate(candidate, signals)
        
        if result:
            should_create_lead = rules_engine.should_create_lead(result)
            results[scenario] = {
                "eta_days": result.eta_days,
                "confidence": result.confidence_0_1,
                "rule_name": result.rule_name,
                "qualifies": should_create_lead
            }
            logger.info(f"  ✅ ETA: {result.eta_days} days, Confidence: {result.confidence_0_1:.2f}, Qualifies: {should_create_lead}")
        else:
            results[scenario] = {"qualifies": False}
            logger.info(f"  ❌ No ETA prediction generated")
    
    return results

def test_eta_agent():
    """Test ETA agent with realistic candidates."""
    
    logger.info("🤖 Testing ETA Agent...")
    eta_agent = ETAAgent()
    
    # Create test candidates
    candidates = [
        create_test_candidate_with_signals("high_probability"),
        create_test_candidate_with_signals("final_inspection"),
        create_test_candidate_with_signals("medium_tabc_aged"),
        create_test_candidate_with_signals("no_signals")
    ]
    
    # Process through ETA agent
    qualified_candidates = eta_agent.estimate_opening_dates(candidates)
    
    logger.info(f"📊 ETA Agent Results:")
    logger.info(f"   Input candidates: {len(candidates)}")
    logger.info(f"   Qualified candidates: {len(qualified_candidates)}")
    
    for candidate in qualified_candidates:
        eta_result = candidate["eta_result"]
        logger.info(f"   {candidate['venue_name']}: {eta_result['eta_days']} days, confidence {eta_result['confidence_0_1']:.2f}")
    
    return len(qualified_candidates) > 0

def test_full_pipeline_with_signals():
    """Test full pipeline with injected opening signals."""
    
    logger.info("🚀 Testing Full Pipeline with Opening Signals...")
    
    # Create a mock data source manager that returns our test candidates
    class MockDataSourceManager:
        def fetch_all_sources(self, limit_per_source=100):
            return {
                "tabc": [],  # Empty to avoid real API calls
                "houston_health": [],
                "harris_permits": []
            }
        
        def normalize_and_score_records(self, raw_results):
            # Return our test candidates as normalized records
            candidates = [
                create_test_candidate_with_signals("high_probability"),
                create_test_candidate_with_signals("final_inspection"),
                create_test_candidate_with_signals("medium_tabc_aged")
            ]
            
            # Add required fields for pipeline processing
            for candidate in candidates:
                candidate.update({
                    "source": "test_mock",
                    "composite_lead_score": 0.8,
                    "signal_strength": 0.9
                })
            
            return candidates
        
        def deduplicate_records(self, records):
            return records
        
        def enrich_with_comptroller(self, records):
            return records
        
        def export_to_csv(self, records, filename):
            return f"./exports/{filename}"
        
        def get_pipeline_summary(self):
            return {"mock": True}
        
        def close(self):
            pass
    
    # Monkey patch the pipeline to use our mock
    pipeline = EnhancedPipelineRunner()
    pipeline.data_source_manager = MockDataSourceManager()
    
    # Run API pipeline (which will use our mock data)
    result = pipeline._run_api_pipeline(max_candidates=10)
    
    logger.info("📊 Full Pipeline Results with Opening Signals:")
    logger.info(f"   Total candidates processed: {result.get('total_candidates', 0)}")
    logger.info(f"   Qualified leads generated: {result.get('qualified_leads', 0)}")
    logger.info(f"   Pipeline stages: {result.get('pipeline_stages', {})}")
    
    # Check if we got qualified leads
    qualified_leads = result.get('qualified_leads', 0)
    return qualified_leads > 0

def validate_30_60_day_predictions():
    """Validate that predictions fall within the 30-60 day target window."""
    
    logger.info("📅 Validating 30-60 Day Prediction Window...")
    
    rules_engine = ETARulesEngine()
    target_scenarios = ["high_probability", "medium_tabc_aged"]
    
    predictions_in_window = 0
    total_predictions = 0
    
    for scenario in target_scenarios:
        candidate = create_test_candidate_with_signals(scenario)
        signals = candidate["signals"]
        result = rules_engine.evaluate_candidate(candidate, signals)
        
        if result and rules_engine.should_create_lead(result):
            total_predictions += 1
            if 30 <= result.eta_days <= 60:
                predictions_in_window += 1
                logger.info(f"   ✅ {scenario}: {result.eta_days} days (in window)")
            else:
                logger.info(f"   ⚠️ {scenario}: {result.eta_days} days (outside window)")
    
    success_rate = predictions_in_window / total_predictions if total_predictions > 0 else 0
    logger.info(f"   30-60 day window success rate: {success_rate:.1%}")
    
    return success_rate >= 0.5  # At least 50% should be in target window

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("🧪 OPENING SIGNALS QUALIFICATION TEST")
    logger.info("=" * 70)
    
    # Test 1: ETA Rules Engine
    rules_results = test_eta_rules_engine()
    rules_success = any(result.get("qualifies", False) for result in rules_results.values())
    
    # Test 2: ETA Agent
    agent_success = test_eta_agent()
    
    # Test 3: Full Pipeline
    pipeline_success = test_full_pipeline_with_signals()
    
    # Test 4: 30-60 Day Window Validation
    window_success = validate_30_60_day_predictions()
    
    logger.info("=" * 70)
    logger.info("📋 OPENING SIGNALS TEST SUMMARY:")
    logger.info(f"   ETA Rules Engine: {'✅ PASS' if rules_success else '❌ FAIL'}")
    logger.info(f"   ETA Agent Processing: {'✅ PASS' if agent_success else '❌ FAIL'}")
    logger.info(f"   Full Pipeline Integration: {'✅ PASS' if pipeline_success else '❌ FAIL'}")
    logger.info(f"   30-60 Day Window: {'✅ PASS' if window_success else '❌ FAIL'}")
    
    if all([rules_success, agent_success, pipeline_success, window_success]):
        logger.info("🎉 ALL TESTS PASSED! Pipeline correctly identifies and qualifies opening signals.")
        sys.exit(0)
    else:
        logger.error("💥 Some tests failed. Pipeline may not properly handle opening signals.")
        sys.exit(1)
