#!/usr/bin/env python3
"""Test LLM setup and configuration."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.llm import get_llm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_llm_connection():
    """Test basic LLM connection and functionality."""
    try:
        logger.info("Testing LLM connection...")
        llm = get_llm()
        logger.info(f"LLM created successfully: {type(llm)}")
        
        # Test a simple call
        logger.info("Testing LLM call...")
        test_prompt = "Hello! Please respond with 'LLM connection successful' if you can read this."
        
        # Use the proper method for CrewAI LLM
        response = llm.call([{"role": "user", "content": test_prompt}])
        logger.info(f"LLM response: {response}")
        
        return True
        
    except Exception as e:
        logger.error(f"LLM test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_llm_connection()
    if success:
        logger.info("✅ LLM setup verification completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ LLM setup verification failed!")
        sys.exit(1)
