#!/usr/bin/env python3
"""
Test script to verify CrewAI + LiteLLM + Ollama Turbo cloud integration.
"""

import logging
import sys
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent / "app"))

from app.llm import get_llm
from app.settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_crewai_llm_integration():
    """Test CrewAI LLM integration with Ollama Turbo cloud."""
    print("🧪 Testing CrewAI + LiteLLM + Ollama Turbo Cloud Integration...")
    print(f"📍 Cloud Endpoint: {settings.ollama_base_url}")
    print(f"🤖 Model: {settings.model_id}")
    print(f"🔑 API Key: {settings.openai_api_key[:20]}...")
    print()
    
    try:
        # Reset factory to ensure fresh instance
        from app.llm.factory import LLMFactory
        LLMFactory.reset()
        
        # Create LLM instance
        llm = get_llm(temperature=0.1, max_tokens=100)
        print(f"✅ Created CrewAI LLM instance: {type(llm)}")
        
        # Test simple generation
        test_prompt = "Say 'Hello from Ollama Turbo cloud!' and nothing else."
        print(f"🔍 Testing with prompt: {test_prompt}")
        
        # For CrewAI LLM, use the call method
        messages = [{"role": "user", "content": test_prompt}]
        response = llm.call(messages)
        
        print(f"✅ LLM Response: {response}")
        
        return True
        
    except Exception as e:
        print(f"❌ CrewAI LLM integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_crewai_agent():
    """Test a simple CrewAI agent with Ollama Turbo cloud."""
    print("\n🤖 Testing CrewAI Agent with Ollama Turbo Cloud...")
    
    try:
        from crewai import Agent, Task, Crew
        
        # Create a simple agent
        test_agent = Agent(
            role="Test Assistant",
            goal="Answer questions briefly and accurately",
            backstory="You are a helpful AI assistant for testing Ollama Turbo cloud integration.",
            llm=get_llm(temperature=0.1, max_tokens=100),
            verbose=True
        )
        
        # Create a simple task
        test_task = Task(
            description="Say 'CrewAI integration with Ollama Turbo cloud is working perfectly!' and nothing else.",
            expected_output="Simple confirmation message",
            agent=test_agent
        )
        
        # Create and run crew
        crew = Crew(
            agents=[test_agent],
            tasks=[test_task],
            verbose=True
        )
        
        print("🚀 Starting crew execution...")
        result = crew.kickoff()
        print(f"✅ Crew execution completed!")
        print(f"📋 Result: {result}")
        
        return True
        
    except Exception as e:
        print(f"❌ CrewAI Agent test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("🚀 Testing Ollama Turbo Cloud + CrewAI Integration")
    print("=" * 60)
    
    # Test 1: LLM integration
    if not test_crewai_llm_integration():
        print("\n❌ LLM integration test failed.")
        return 1
    
    # Test 2: Agent integration
    if not test_crewai_agent():
        print("\n❌ Agent integration test failed.")
        return 1
    
    print("\n" + "=" * 60)
    print("🎉 All tests passed! CrewAI + Ollama Turbo Cloud integration is working!")
    print("🌤️  Your agents are now running on cloud infrastructure!")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())