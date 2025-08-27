from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    env: str = Field(default="dev", description="Environment: dev, prod")
    db_url: str = Field(default="sqlite:///./leads.db", description="Database URL")
    
    # Socrata settings
    socrata_app_token: Optional[str] = Field(default=None, description="Socrata app token")
    socrata_base: str = Field(default="https://data.texas.gov", description="Socrata base URL")
    enable_socrata_mcp: bool = Field(default=False, description="Enable Socrata MCP")
    socrata_domain: str = Field(default="https://data.texas.gov", description="Socrata domain")
    socrata_dataset_tabc_pending: Optional[str] = Field(default=None, description="TABC pending dataset ID")
    
    # Model serving - Ollama Turbo Configuration
    ollama_base_url: str = Field(default="http://localhost:11434", description="Ollama API base URL")
    ollama_api_key: Optional[str] = Field(default=None, description="Ollama Turbo API key")
    model_id: str = Field(default="gpt-oss:20b", description="Model ID")
    openai_api_key: Optional[str] = Field(default="ollama", description="Dummy API key for Ollama")
    
    # Legacy vLLM support (deprecated)
    vllm_base_url: str = Field(default="http://localhost:8000/v1", description="vLLM API base URL (deprecated)")
    
    # Data source API keys
    tabc_app_token: Optional[str] = Field(default=None, description="TABC app token")
    tx_comptroller_api_key: Optional[str] = Field(default=None, description="TX Comptroller API key")
    
    # Firecrawl configuration
    firecrawl_api_key: Optional[str] = Field(default=None, description="Firecrawl API key for website scraping")
    
    # Additional settings
    crewai_telemetry_opt_out: bool = Field(default=True, description="Disable CrewAI telemetry")
    api_rate_limit_per_second: float = Field(default=2.0, description="API rate limit")
    max_parallel_sources: int = Field(default=4, description="Max parallel data sources")
    watermark_storage_path: str = Field(default="./data/watermarks.json", description="Watermark storage path")
    csv_export_path: str = Field(default="./exports/", description="CSV export path")
    
    # Crawl hygiene
    requests_timeout: int = Field(default=30, description="HTTP request timeout")
    crawl_delay_seconds: int = Field(default=1, description="Delay between requests")
    user_agent: str = Field(default="RestaurantLeadsMVP/1.0 (+https://example.local)", description="User agent")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
