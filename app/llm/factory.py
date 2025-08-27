"""
LLM factory for creating consistent CrewAI LLM instances with Ollama Turbo integration.
"""

from typing import Optional
from crewai import LLM
from ..settings import settings
import logging

logger = logging.getLogger(__name__)


class LLMFactory:
    """Factory for creating LLM instances."""
    
    _instance: Optional[LLM] = None
    
    @classmethod
    def get_llm(cls, **kwargs) -> LLM:
        """Get or create CrewAI LLM instance for Ollama Turbo."""
        if cls._instance is None:
            cls._instance = cls.create_llm(**kwargs)
        return cls._instance
    
    @classmethod
    def create_llm(cls, **kwargs) -> LLM:
        """Create a new CrewAI LLM instance configured for Ollama Turbo."""
        
        # For Ollama Turbo cloud, we need to configure it properly
        import os
        if settings.ollama_api_key:
            os.environ['OLLAMA_API_KEY'] = settings.ollama_api_key
        
        # Use custom provider for cloud Ollama with proper authentication
        default_config = {
            'model': f"ollama_chat/{settings.model_id}",  # Use ollama_chat provider for cloud
            'api_base': settings.ollama_base_url,
            'api_key': settings.ollama_api_key or settings.openai_api_key,
            'temperature': 0.1,
            'max_tokens': 2000,
            'custom_llm_provider': 'ollama_chat',
            'headers': {
                'Authorization': f'Bearer {settings.ollama_api_key or settings.openai_api_key}'
            }
        }
        
        # Override defaults with kwargs
        config = {**default_config, **kwargs}
        
        logger.info(f"Creating CrewAI LLM with Ollama Turbo model: {config['model']}")
        logger.info(f"Using base URL: {config['api_base']}")
        return LLM(**config)
    
    @classmethod
    def reset(cls):
        """Reset the singleton instance."""
        cls._instance = None


def get_llm(**kwargs) -> LLM:
    """Convenience function to get LLM instance."""
    return LLMFactory.get_llm(**kwargs)


def create_chat_completion(prompt: str, **kwargs) -> str:
    """Convenience function for direct chat completion."""
    llm = get_llm(**kwargs)
    try:
        # Use CrewAI LLM's call method
        response = llm.call([{"role": "user", "content": prompt}])
        return response.content if hasattr(response, 'content') else str(response)
    except Exception as e:
        logger.error(f"Chat completion failed: {e}")
        return ""