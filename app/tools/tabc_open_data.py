import requests
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode
import logging
from datetime import datetime, timedelta

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
        self.licenses_dataset = "tabc-licenses-applications"  # Replace with actual ID
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': settings.user_agent,
            'Accept': 'application/json'
        })
        
        if self.app_token:
            self.session.headers['X-App-Token'] = self.app_token
    
    def query_pending_licenses(self, county: str = "Harris", days_back: int = 90) -> List[Dict[str, Any]]:
        """Query for pending license applications in specified county."""
        
        since_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # SoQL query for pending applications
        where_clause = f"county = '{county}' AND status LIKE '%Pending%' AND application_date >= '{since_date}'"
        
        params = {
            '$where': where_clause,
            '$order': 'application_date DESC',
            '$limit': 1000,
            '$offset': 0
        }
        
        all_records = []
        offset = 0
        
        while True:
            params['$offset'] = offset
            
            try:
                records = self._make_request(f"/resource/{self.licenses_dataset}.json", params)
                
                if not records:
                    break
                
                all_records.extend(records)
                
                # Check if we got a full page
                if len(records) < params['$limit']:
                    break
                
                offset += params['$limit']
                time.sleep(self.delay)
                
            except Exception as e:
                logger.error(f"Error querying TABC data: {e}")
                break
        
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
    
    records = tabc_client.query_pending_licenses(county, days_back)
    
    # Filter for restaurant-related licenses
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
