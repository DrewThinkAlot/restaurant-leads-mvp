#!/usr/bin/env python3
"""Test script to verify Ollama Turbo hosted service is working."""

import requests
import json

print('=== TESTING OLLAMA TURBO HOSTED SERVICE ===')

# Configuration
API_BASE = 'https://api.ollama.com/v1'
API_KEY = '7a1e9d1162af47b1ada0c61a4a4ae92c.-AZWkLpujOcCcQ61rmDRZKGq'
MODEL = 'llama3.1:8b'

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Test the models endpoint
try:
    response = requests.get(f'{API_BASE}/models', headers=headers, timeout=10)
    print(f'✅ Models API responding: {response.status_code}')
    if response.status_code == 200:
        data = response.json()
        models = [m['id'] for m in data.get('data', [])]
        print(f'   Available models: {models[:5]}...')  # Show first 5
    else:
        print(f'   Error: {response.text}')
except Exception as e:
    print(f'❌ Models API error: {e}')
    exit(1)

# Test a simple completion
try:
    completion_data = {
        'model': MODEL,
        'messages': [
            {'role': 'user', 'content': 'Say "Hello from Ollama Turbo!" and nothing else.'}
        ],
        'max_tokens': 10,
        'temperature': 0.1
    }
    
    print('🔄 Testing chat completion...')
    response = requests.post(
        f'{API_BASE}/chat/completions',
        headers=headers,
        json=completion_data,
        timeout=30
    )
    
    print(f'✅ Chat completion test: {response.status_code}')
    if response.status_code == 200:
        result = response.json()
        message = result['choices'][0]['message']['content']
        print(f'   Response: "{message}"')
    else:
        print(f'   Error: {response.text}')
        
except Exception as e:
    print(f'❌ Chat completion error: {e}')

print('🎉 Ollama Turbo service testing complete!')