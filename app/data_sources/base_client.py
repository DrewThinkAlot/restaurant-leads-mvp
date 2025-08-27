"""Base API client with common functionality for all data sources."""

import time
import logging
import requests
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Generator
from datetime import datetime, timedelta
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class BaseAPIClient(ABC):
    """Base class for all API clients with common functionality."""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None, 
                 rate_limit_per_second: float = 2.0, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.rate_limit_per_second = rate_limit_per_second
        self.timeout = timeout
        self.last_request_time = 0
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RestaurantLeadsPipeline/1.0'
        })
        
        if api_key:
            self._set_auth_headers()
    
    @abstractmethod
    def _set_auth_headers(self):
        """Set authentication headers specific to each API."""
        pass
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        if self.rate_limit_per_second <= 0:
            return
        
        min_interval = 1.0 / self.rate_limit_per_second
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, method: str, url: str, params: Dict = None, 
                     data: Dict = None, retries: int = 3) -> requests.Response:
        """Make HTTP request with retries and exponential backoff."""
        
        self._rate_limit()
        
        for attempt in range(retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    timeout=self.timeout
                )
                
                if response.status_code == 429:  # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                if response.status_code >= 500:  # Server error
                    if attempt < retries:
                        sleep_time = (2 ** attempt) + (time.time() % 1)  # Jitter
                        logger.warning(f"Server error {response.status_code}, retrying in {sleep_time:.1f}s")
                        time.sleep(sleep_time)
                        continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    sleep_time = (2 ** attempt) + (time.time() % 1)
                    logger.warning(f"Request failed: {e}, retrying in {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"Request failed after {retries} retries: {e}")
                    raise
        
        raise Exception(f"Max retries exceeded for {method} {url}")
    
    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make GET request and return JSON response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self._make_request('GET', url, params=params)
        return response.json()
    
    def post(self, endpoint: str, data: Dict = None, params: Dict = None) -> Dict[str, Any]:
        """Make POST request and return JSON response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self._make_request('POST', url, params=params, data=data)
        return response.json()
    
    @abstractmethod
    def fetch_records(self, since: Optional[datetime] = None, 
                     limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Fetch records with pagination support."""
        pass
    
    @abstractmethod
    def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw API response to standard format."""
        pass
    
    def get_watermark_key(self) -> str:
        """Get unique key for watermark storage."""
        return f"{self.__class__.__name__.lower()}_watermark"
    
    def close(self):
        """Close session and cleanup resources."""
        self.session.close()
