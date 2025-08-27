import requests
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode
import logging
from datetime import datetime, timedelta
import asyncio
import httpx

from ..settings import settings

logger = logging.getLogger(__name__)


class TABCOpenDataClient:
    """Client for TABC open data via Socrata SODA REST API."""
    
    def __init__(self):
        self.base_url = settings.socrata_base
        self.app_token = settings.socrata_app_token
        self.timeout = settings.requests_timeout
        self.delay = settings.crawl_delay_seconds
        
        # Dataset IDs - these would be discovered from actual Socrata site
        # Placeholders for now, to be replaced with actual dataset IDs
        self.licenses_dataset = "kguh-7q9z"  # TABCLicenses - working dataset
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': settings.user_agent,
            'Accept': 'application/json'
        })
        
        if self.app_token:
            self.session.headers['X-App-Token'] = self.app_token
    
    async def query_pending_licenses_async(self, county: str = "Harris", days_back: int = 90) -> List[Dict[str, Any]]:
        """Asynchronously query for pending license applications."""
        
        since_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Use simple fetch without parameters since dataset may not support filtering
        all_records = []
        offset = 0
        limit = 1000
        
        async with httpx.AsyncClient(headers=self._get_headers(), timeout=self.timeout) as client:
            try:
                while len(all_records) < limit:
                    params = {
                        "$limit": min(1000, limit - len(all_records)),
                        "$offset": offset
                    }
                    
                    records = await self._make_request_async(client, f"/resource/{self.licenses_dataset}.json", params)
                    
                    if not records:
                        break
                    
                    all_records.extend(records)
                    offset += len(records)
                    
                    if len(records) < params["$limit"]:
                        break
                    
                    await asyncio.sleep(self.delay)
                    
            except Exception as e:
                logger.error(f"Error during async TABC query: {e}")
                return []
        
        return self._normalize_tabc_records(all_records)

    def query_pending_licenses(self, county: str = "Harris", days_back: int = 90) -> List[Dict[str, Any]]:
        """Query for pending license applications."""
        
        since_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Use simple fetch without parameters since dataset may not support filtering
        all_records = []
        offset = 0
        limit = 1000
        
        try:
            while len(all_records) < limit:
                params = {
                    "$limit": min(1000, limit - len(all_records)),
                    "$offset": offset
                }
                
                records = self._make_request(f"/resource/{self.licenses_dataset}.json", params)
                
                if not records:
                    break
                
                all_records.extend(records)
                offset += len(records)
                
                if len(records) < params["$limit"]:
                    break
                
                time.sleep(self.delay)
                
        except Exception as e:
            logger.error(f"Error querying TABC data: {e}")
            return []
        
        return self._normalize_tabc_records(all_records)
    
    def search_by_business_name(self, business_name: str) -> List[Dict[str, Any]]:
        """Search for licenses by business name."""
        
        params = {
            '$where': f"business_name LIKE '%{business_name}%' OR trade_name LIKE '%{business_name}%'",
            '$order': 'status_date DESC',
            '$limit': 50
        }
        
        try:
            records = self._make_request(f"/resource/{self.licenses_dataset}.json", params)
            return self._normalize_tabc_records(records or [])
        except Exception as e:
            logger.error(f"Error searching TABC by name: {e}")
            return []
    
    def search_by_address(self, address: str, city: str = None) -> List[Dict[str, Any]]:
        """Search for licenses by address."""
        
        where_parts = [f"address LIKE '%{address}%'"]
        if city:
            where_parts.append(f"city = '{city}'")
        
        params = {
            '$where': ' AND '.join(where_parts),
            '$order': 'status_date DESC',
            '$limit': 20
        }
        
        try:
            records = self._make_request(f"/resource/{self.licenses_dataset}.json", params)
            return self._normalize_tabc_records(records or [])
        except Exception as e:
            logger.error(f"Error searching TABC by address: {e}")
            return []
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Make HTTP request with retries and error handling."""
        
        url = f"{self.base_url}{endpoint}"
        query_string = urlencode(params, safe='$:')
        full_url = f"{url}?{query_string}"
        
        for attempt in range(3):
            try:
                response = self.session.get(full_url, timeout=self.timeout)
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        
        return None
    
    async def _fetch_all_pages_async(self, client: httpx.AsyncClient, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch all pages of results for a given query asynchronously."""
        all_records = []
        offset = 0
        limit = params.get('$limit', 1000)

        while True:
            params['$offset'] = offset
            try:
                records = await self._make_request_async(client, endpoint, params)
                if not records:
                    break
                
                all_records.extend(records)
                
                if len(records) < limit:
                    break
                
                offset += limit
                await asyncio.sleep(self.delay)

            except Exception as e:
                logger.error(f"Error during async pagination: {e}")
                break
        
        return all_records

    async def _make_request_async(self, client: httpx.AsyncClient, endpoint: str, params: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Make an asynchronous HTTP request."""
        url = f"{self.base_url}{endpoint}"
        query_string = urlencode(params, safe='$:')
        full_url = f"{url}?{query_string}"

        for attempt in range(3):
            try:
                response = await client.get(full_url)
                response.raise_for_status()
                return response.json()
            except httpx.RequestError as e:
                logger.warning(f"Async request attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise
        return None

    def _normalize_tabc_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize TABC records to common format."""
        
        normalized = []
        
        for record in records:
            try:
                normalized_record = {
                    'source': 'tabc',
                    'source_id': record.get('license_number') or record.get('application_number'),
                    'business_name': record.get('business_name') or record.get('legal_name'),
                    'trade_name': record.get('trade_name') or record.get('dba_name'),
                    'address': record.get('address') or record.get('business_address'),
                    'city': record.get('city'),
                    'state': record.get('state', 'TX'),
                    'zip_code': record.get('zip_code') or record.get('zip'),
                    'county': record.get('county'),
                    'status': record.get('status'),
                    'status_date': record.get('status_date'),
                    'application_date': record.get('application_date'),
                    'license_type': record.get('license_type'),
                    'phone': record.get('phone'),
                    'raw_data': record
                }
                
                normalized.append(normalized_record)
                
            except Exception as e:
                logger.warning(f"Error normalizing TABC record: {e}")
                continue
        
        return normalized
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for HTTP requests."""
        headers = {
            'User-Agent': settings.user_agent,
            'Accept': 'application/json'
        }
        if self.app_token:
            headers['X-App-Token'] = self.app_token
        return headers

    def discover_datasets(self) -> List[Dict[str, Any]]:
        """Discover available TABC-related datasets."""
        
        try:
            # Search for TABC datasets
            search_url = f"{self.base_url}/api/catalog/v1"
            params = {
                'q': 'TABC license',
                'only': 'datasets'
            }
            
            response = self.session.get(search_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            results = response.json()
            datasets = []
            
            for result in results.get('results', []):
                resource = result.get('resource', {})
                datasets.append({
                    'id': resource.get('id'),
                    'name': resource.get('name'),
                    'description': resource.get('description'),
                    'updated_at': resource.get('updatedAt'),
                    'columns': len(resource.get('columns_field_name', []))
                })
            
            return datasets
            
        except Exception as e:
            logger.error(f"Error discovering TABC datasets: {e}")
            return []


# Global client instance
tabc_client = TABCOpenDataClient()


def get_pending_restaurant_licenses(county: str = "Harris", days_back: int = 90) -> List[Dict[str, Any]]:
    """Get pending restaurant/food service licenses."""
    
    # Run the async function in sync context
    try:
        import asyncio
        records = asyncio.run(get_pending_restaurant_licenses_async(county, days_back))
    except RuntimeError:
        # If event loop is already running (e.g., in Jupyter), use nest_asyncio or fallback
        import nest_asyncio
        nest_asyncio.apply()
        records = asyncio.run(get_pending_restaurant_licenses_async(county, days_back))
    
    return records


async def get_pending_restaurant_licenses_async(county: str = "Harris", days_back: int = 90) -> List[Dict[str, Any]]:
    """Asynchronously get pending restaurant/food service licenses."""
    records = await tabc_client.query_pending_licenses_async(county, days_back)
    
    restaurant_keywords = [
        'restaurant', 'food', 'mixed beverage', 'wine', 'beer', 
        'retail', 'on premise', 'caterer'
    ]
    
    filtered_records = []
    for record in records:
        license_type = (record.get('license_type') or '').lower()
        business_name = (record.get('business_name') or '').lower()
        
        if any(keyword in license_type or keyword in business_name for keyword in restaurant_keywords):
            filtered_records.append(record)
            
    return filtered_records
