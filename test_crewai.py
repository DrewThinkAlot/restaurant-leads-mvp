#!/usr/bin/env python3
"""Test CrewAI integration with Ollama Turbo."""

from app.agents.crew import RestaurantLeadsCrew
from app.settings import settings

print('=== TESTING CREWAI WITH OLLAMA TURBO ===')
print(f'Model: {settings.model_id}')
print(f'API Base: {settings.vllm_base_url}')
print(f'API Key: {settings.openai_api_key[:20]}...')

try:
    crew = RestaurantLeadsCrew()
    print('✅ CrewAI initialized successfully')
    
    # Test the LLM directly
    print('🔄 Testing LLM directly...')
    response = crew.llm.invoke('Say "Hello from CrewAI!" and nothing else.')
    print(f'✅ LLM Response: "{response.content}"')
    
except Exception as e:
    print(f'❌ CrewAI/LLM error: {e}')
    import traceback
    traceback.print_exc()