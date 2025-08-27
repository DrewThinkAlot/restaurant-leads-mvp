#!/usr/bin/env python3
"""
Test script to verify CrewAI + LiteLLM + Ollama Turbo integration.
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


def test_ollama_turbo_connection():
    """Test basic connection to Ollama Turbo."""
    print("🔍 Testing Ollama Turbo Connection...")
    print(f"📍 Base URL: {settings.ollama_base_url}")
    print(f"🤖 Model: {settings.model_id}")
    print(f"🔑 API Key: {settings.openai_api_key}")
    print()
    
    try:
        # Test if Ollama server is running
        import requests
        health_url = f"{settings.ollama_base_url}/api/tags"
        response = requests.get(health_url, timeout=10)
        
        if response.status_code == 200:
            print("✅ Ollama server is running")
            models = response.json().get('models', [])
            model_names = [m.get('name', '') for m in models]
            
            if settings.model_id in model_names:
                print(f"✅ Model {settings.model_id} is available")
            else:
                print(f"⚠️  Model {settings.model_id} not found in available models:")
                for model in model_names:
                    print(f"   - {model}")
        else:
            print(f"❌ Ollama server responded with status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Cannot connect to Ollama server: {e}")
        print(f"Make sure Ollama is running on {settings.ollama_base_url}")
        return False
    
    return True


def test_crewai_llm_integration():
    """Test CrewAI LLM integration with Ollama Turbo."""
    print("\n🧪 Testing CrewAI LLM Integration...")
    
    try:
        # Create LLM instance
        llm = get_llm(temperature=0.1, max_tokens=100)
        print(f"✅ Created LLM instance: {type(llm)}")
        
        # Test simple generation
        test_prompt = "Say 'Hello from Ollama Turbo!' and nothing else."
        print(f"🔍 Testing with prompt: {test_prompt}")
        
        response = llm.call([{"role": "user", "content": test_prompt}])
        print(f"✅ LLM Response: {response}")
        
        return True
        
    except Exception as e:
        print(f"❌ CrewAI LLM integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_crewai_agent():
    """Test a simple CrewAI agent with Ollama Turbo."""
    print("\n🤖 Testing CrewAI Agent...")
    
    try:
        from crewai import Agent, Task, Crew
        
        # Create a simple agent
        test_agent = Agent(
            role="Test Assistant",
            goal="Answer questions briefly and accurately",
            backstory="You are a helpful AI assistant for testing purposes.",
            llm=get_llm(temperature=0.1, max_tokens=100),
            verbose=True
        )
        
        # Create a simple task
        test_task = Task(
            description="Say 'CrewAI integration with Ollama Turbo is working!' and nothing else.",
            expected_output="Simple confirmation message",
            agent=test_agent
        )
        
        # Create and run crew
        crew = Crew(
            agents=[test_agent],
            tasks=[test_task],
            verbose=True
        )
        
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
    print("🚀 Starting Ollama Turbo + CrewAI Integration Tests")
    print("=" * 60)
    
    # Test 1: Basic connection
    if not test_ollama_turbo_connection():
        print("\n❌ Basic connection test failed. Exiting.")
        return 1
    
    # Test 2: LLM integration
    if not test_crewai_llm_integration():
        print("\n❌ LLM integration test failed. Exiting.")
        return 1
    
    # Test 3: Agent integration
    if not test_crewai_agent():
        print("\n❌ Agent integration test failed. Exiting.")
        return 1
    
    print("\n" + "=" * 60)
    print("🎉 All tests passed! CrewAI + Ollama Turbo integration is working!")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
