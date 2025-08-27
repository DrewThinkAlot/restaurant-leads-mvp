"""
Custom LangChain LLM wrapper for Ollama Turbo integration.
This bridges the gap between CrewAI/LangChain and Ollama Turbo's native Python API.
"""

from typing import Any, List, Optional, Dict
from langchain.llms.base import LLM
from langchain.schema import Generation, LLMResult
from langchain.callbacks.manager import CallbackManagerForLLMRun, AsyncCallbackManagerForLLMRun
from pydantic import Field
import logging
from ollama import Client

logger = logging.getLogger(__name__)


class OllamaTurboLLM(LLM):
    """Custom LangChain LLM wrapper for Ollama Turbo."""
    
    client: Optional[Client] = None
    model: str = Field(default="gpt-oss:20b")
    api_key: str = Field(default="")
    host: str = Field(default="https://ollama.com")
    temperature: float = Field(default=0.1)
    max_tokens: Optional[int] = Field(default=2000)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.client:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the Ollama client."""
        try:
            self.client = Client(
                host='http://localhost:11434'  # Use local Ollama server
            )
            logger.info(f"Initialized Ollama Turbo client for model: {self.model}")
        except Exception as e:
            logger.error(f"Failed to initialize Ollama client: {e}")
            raise
    
    @property
    def _llm_type(self) -> str:
        """Return identifier of llm type."""
        return "ollama_turbo"
    
    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Call the Ollama Turbo API."""
        if not self.client:
            self._initialize_client()
        
        try:
            # Ensure client is initialized
            if not self.client:
                self._initialize_client()
            
            if not self.client:
                logger.error("Failed to initialize client")
                return ""
                
            # Prepare messages for chat completion
            messages = [
                {
                    'role': 'user',
                    'content': prompt,
                }
            ]
            
            # Make the request with turbo optimization
            response = self.client.chat(
                model=self.model,
                messages=messages,
                stream=False,
                options={
                    'temperature': self.temperature,
                    'num_predict': self.max_tokens,
                    'turbo': True  # Enable turbo mode for faster inference
                }
            )
            
            if response and 'message' in response:
                content = response['message']['content']
                
                # Handle stop sequences
                if stop:
                    for stop_seq in stop:
                        if stop_seq in content:
                            content = content.split(stop_seq)[0]
                            break
                
                return content
            else:
                logger.error(f"Unexpected response format: {response}")
                return ""
                
        except Exception as e:
            logger.error(f"Ollama Turbo API call failed: {e}")
            # Return empty string rather than raising to prevent pipeline failure
            return ""
    
    def _generate(
        self,
        prompts: List[str],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> LLMResult:
        """Generate responses for multiple prompts."""
        generations = []
        
        for prompt in prompts:
            try:
                response_text = self._call(prompt, stop=stop, run_manager=run_manager, **kwargs)
                generations.append([Generation(text=response_text)])
            except Exception as e:
                logger.error(f"Failed to generate for prompt: {e}")
                generations.append([Generation(text="")])
        
        return LLMResult(generations=generations)
    
    async def _acall(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Async call - delegate to sync for now."""
        # Convert async callback manager to sync for delegation
        sync_run_manager = None  # We'll use None for simplicity
        return self._call(prompt, stop=stop, run_manager=sync_run_manager, **kwargs)
    
    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "host": self.host,
        }