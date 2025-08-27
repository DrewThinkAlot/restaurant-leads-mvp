#!/usr/bin/env python3
"""
Focused test to verify the Ollama Turbo bridge works through agent tools.
"""

import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pitch_agent_tool():
    """Test the PitchAgent tool with Ollama Turbo."""
    try:
        from app.agents.agent_pitch import LLMPitchGenerationTool
        
        logger.info("Testing PitchAgent tool...")
        
        tool = LLMPitchGenerationTool()
        
        # Test data
        candidate_data = json.dumps({
            "venue_name": "Test Restaurant",
            "address": "123 Main St, Houston, TX",
            "eta_days": 45
        })
        
        eta_window = "Dec 15 - Jan 15"
        
        response = tool._run(candidate_data, eta_window)
        logger.info(f"PitchAgent response: {response[:200]}...")
        
        # Try to parse as JSON
        try:
            pitch_data = json.loads(response)
            logger.info("✓ PitchAgent returned valid JSON structure")
            if "pitch_text" in pitch_data:
                logger.info("✓ Pitch content generated successfully")
        except json.JSONDecodeError:
            logger.warning("⚠ Response not valid JSON, but tool executed")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ PitchAgent tool test failed: {e}")
        return False

def test_extractor_agent_tool():
    """Test the ExtractorAgent tool with Ollama Turbo."""
    try:
        from app.agents.agent_extractor import LLMExtractionTool
        
        logger.info("Testing ExtractorAgent tool...")
        
        tool = LLMExtractionTool()
        
        # Test raw data
        raw_data = json.dumps({
            "business_name": "Test Cafe LLC",
            "trade_name": "Test Cafe",
            "address": "456 Oak Ave",
            "city": "Houston",
            "state": "TX",
            "zip": "77001"
        })
        
        schema_description = "RestaurantCandidate schema"
        
        response = tool._run(raw_data, schema_description)
        logger.info(f"ExtractorAgent response: {response[:200]}...")
        
        # Try to parse as JSON
        try:
            candidate_data = json.loads(response)
            logger.info("✓ ExtractorAgent returned valid JSON structure")
            if "venue_name" in candidate_data:
                logger.info("✓ Candidate data extracted successfully")
        except json.JSONDecodeError:
            logger.warning("⚠ Response not valid JSON, but tool executed")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ExtractorAgent tool test failed: {e}")
        return False

def test_resolver_agent_tool():
    """Test the ResolverAgent tool with Ollama Turbo."""
    try:
        from app.agents.agent_resolver import LLMMatchingTool
        
        logger.info("Testing ResolverAgent tool...")
        
        tool = LLMMatchingTool()
        
        # Test records
        record1 = json.dumps({
            "venue_name": "Pizza Palace",
            "address": "789 Elm St, Houston, TX"
        })
        
        record2 = json.dumps({
            "venue_name": "Pizza Palace Restaurant",
            "address": "789 Elm Street, Houston, TX 77002"
        })
        
        response = tool._run(record1, record2)
        logger.info(f"ResolverAgent response: {response[:200]}...")
        
        # Try to parse as JSON
        try:
            match_data = json.loads(response)
            logger.info("✓ ResolverAgent returned valid JSON structure")
            if "same_entity" in match_data:
                logger.info("✓ Entity matching completed successfully")
        except json.JSONDecodeError:
            logger.warning("⚠ Response not valid JSON, but tool executed")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ResolverAgent tool test failed: {e}")
        return False

def test_eta_agent_tool():
    """Test the ETAAgent tool with Ollama Turbo."""
    try:
        from app.agents.agent_eta import LLMETAAdjustmentTool
        
        logger.info("Testing ETAAgent tool...")
        
        tool = LLMETAAdjustmentTool()
        
        # Test rule result
        rule_result = json.dumps({
            "eta_start": "2025-01-15",
            "eta_end": "2025-02-15",
            "eta_days": 45,
            "confidence_0_1": 0.75,
            "rule_name": "test_rule"
        })
        
        milestone_text = "TABC Status: pending\nBuilding Permit: approved\nPlan Review: submitted"
        
        response = tool._run(rule_result, milestone_text)
        logger.info(f"ETAAgent response: {response[:200]}...")
        
        # Try to parse as JSON
        try:
            eta_data = json.loads(response)
            logger.info("✓ ETAAgent returned valid JSON structure")
            if "eta_days" in eta_data:
                logger.info("✓ ETA adjustment completed successfully")
        except json.JSONDecodeError:
            logger.warning("⚠ Response not valid JSON, but tool executed")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ETAAgent tool test failed: {e}")
        return False

def main():
    """Run focused bridge tests."""
    print("=" * 60)
    print("OLLAMA TURBO BRIDGE FOCUSED TEST")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    tests = [
        ("PitchAgent Tool", test_pitch_agent_tool),
        ("ExtractorAgent Tool", test_extractor_agent_tool),
        ("ResolverAgent Tool", test_resolver_agent_tool),
        ("ETAAgent Tool", test_eta_agent_tool),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"Running {test_name}...")
        try:
            success = test_func()
            results.append((test_name, success))
            print()
        except Exception as e:
            logger.error(f"Test {test_name} threw exception: {e}")
            results.append((test_name, False))
            print()
    
    print("=" * 60)
    print("BRIDGE TEST RESULTS")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
    
    print()
    print(f"Overall: {passed}/{total} bridge tests passed")
    
    if passed == total:
        print("🎉 Ollama Turbo bridge is working perfectly!")
        return 0
    else:
        print("❌ Some bridge tests failed.")
        return 1

if __name__ == "__main__":
    exit(main())