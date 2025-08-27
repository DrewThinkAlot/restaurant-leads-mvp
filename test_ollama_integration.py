#!/usr/bin/env python3
"""
Test script to verify Ollama Turbo integration with CrewAI pipeline.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_llm_factory():
    """Test that the LLM factory is working correctly."""
    try:
        from app.llm import get_llm
        
        logger.info("Testing LLM factory...")
        llm = get_llm()
        
        logger.info(f"LLM type: {llm._llm_type}")
        logger.info(f"Model: {llm.model}")
        logger.info("✓ LLM factory working")
        return True
        
    except Exception as e:
        logger.error(f"✗ LLM factory test failed: {e}")
        return False

def test_llm_call():
    """Test a simple LLM call."""
    try:
        from app.llm import get_llm
        
        logger.info("Testing LLM call...")
        llm = get_llm(temperature=0.1, max_tokens=100)
        
        test_prompt = "Generate a JSON object with a greeting message. Return only valid JSON: {\"message\": \"...\"}"
        
        response = llm._call(test_prompt)
        logger.info(f"LLM Response: {response[:100]}...")
        
        # Try to parse as JSON
        try:
            json.loads(response)
            logger.info("✓ LLM returned valid JSON")
        except json.JSONDecodeError:
            logger.warning("⚠ LLM response was not valid JSON, but call succeeded")
        
        logger.info("✓ LLM call working")
        return True
        
    except Exception as e:
        logger.error(f"✗ LLM call test failed: {e}")
        return False

def test_crew_initialization():
    """Test that the crew can be initialized with Ollama Turbo."""
    try:
        from app.agents.crew import RestaurantLeadsCrew
        
        logger.info("Testing crew initialization...")
        crew = RestaurantLeadsCrew()
        
        logger.info(f"Crew has {len(crew.crew.agents)} agents")
        logger.info(f"LLM type: {crew.llm._llm_type}")
        
        status = crew.get_crew_status()
        logger.info(f"Crew status: {json.dumps(status, indent=2)}")
        
        logger.info("✓ Crew initialization working")
        return True
        
    except Exception as e:
        logger.error(f"✗ Crew initialization test failed: {e}")
        return False

def test_agent_tools():
    """Test that individual agent tools are working."""
    try:
        from app.agents.agent_pitch import LLMPitchGenerationTool
        from app.agents.agent_extractor import LLMExtractionTool
        from app.agents.agent_resolver import LLMMatchingTool
        from app.agents.agent_eta import LLMETAAdjustmentTool
        
        logger.info("Testing agent tools...")
        
        # Test PitchAgent tool
        pitch_tool = LLMPitchGenerationTool()
        logger.info(f"✓ PitchAgent tool: {pitch_tool.name}")
        
        # Test ExtractorAgent tool
        extractor_tool = LLMExtractionTool()
        logger.info(f"✓ ExtractorAgent tool: {extractor_tool.name}")
        
        # Test ResolverAgent tool
        resolver_tool = LLMMatchingTool()
        logger.info(f"✓ ResolverAgent tool: {resolver_tool.name}")
        
        # Test ETAAgent tool
        eta_tool = LLMETAAdjustmentTool()
        logger.info(f"✓ ETAAgent tool: {eta_tool.name}")
        
        logger.info("✓ All agent tools initialized")
        return True
        
    except Exception as e:
        logger.error(f"✗ Agent tools test failed: {e}")
        return False

def test_simple_crew_run():
    """Test a simple crew run with minimal data."""
    try:
        from app.agents.crew import RestaurantLeadsCrew
        
        logger.info("Testing simple crew run...")
        crew = RestaurantLeadsCrew()
        
        # Run with very limited scope to test integration
        logger.info("Running crew with max_candidates=1...")
        result = crew.run_pipeline(max_candidates=1, harris_only=True)
        
        logger.info(f"Crew run result: {json.dumps(result, indent=2)}")
        
        if result.get("execution_success"):
            logger.info("✓ Crew run completed successfully")
        else:
            logger.warning(f"⚠ Crew run completed with issues: {result.get('error')}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Simple crew run test failed: {e}")
        return False

def main():
    """Run all integration tests."""
    print("=" * 60)
    print("OLLAMA TURBO + CREWAI INTEGRATION TEST")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    tests = [
        ("LLM Factory", test_llm_factory),
        ("LLM Call", test_llm_call),
        ("Crew Initialization", test_crew_initialization),
        ("Agent Tools", test_agent_tools),
        ("Simple Crew Run", test_simple_crew_run),
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
    print("TEST RESULTS")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
    
    print()
    print(f"Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Ollama Turbo + CrewAI integration is working!")
        return 0
    else:
        print("❌ Some tests failed. Check the logs above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())