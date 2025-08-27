import requests
from playwright.sync_api import sync_playwright, Page
import time
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from urllib.parse import urljoin, urlencode

from ..settings import settings

logger = logging.getLogger(__name__)


class HCPHFoodPermitsClient:
    """Client for Harris County Public Health food permits and plan reviews."""
    
    def __init__(self):
        # Try ArcGIS REST endpoint first, fallback to web scraping
        self.arcgis_base = "https://services.arcgis.com/..."  # Replace with actual endpoint
        self.web_base = "https://publichealth.harriscountytx.gov"  # Replace with actual URL
        self.delay = settings.crawl_delay_seconds
        self.timeout = settings.requests_timeout
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': settings.user_agent,
            'Accept': 'application/json'
        })
    
    def search_by_address(self, address: str, city: str = None) -> List[Dict[str, Any]]:
        """Search for food permits by address."""
        
        # Try ArcGIS REST endpoint first
        arcgis_results = self._search_arcgis_address(address, city)
        if arcgis_results:
            return arcgis_results
        
        # Fallback to web scraping
        return self._search_web_address(address, city)
    
    def search_by_business_name(self, business_name: str) -> List[Dict[str, Any]]:
        """Search for food permits by business name."""
        
        # Try ArcGIS REST endpoint first
        arcgis_results = self._search_arcgis_name(business_name)
        if arcgis_results:
            return arcgis_results
        
        # Fallback to web scraping
        return self._search_web_name(business_name)
    
    def get_plan_review_status(self, address: str, business_name: str = None) -> Optional[Dict[str, Any]]:
        """Get plan review status for a specific location."""
        
        # Combined search approach
        results = []
        
        if business_name:
            results.extend(self.search_by_business_name(business_name))
        
        results.extend(self.search_by_address(address))
        
        # Find most recent plan review record
        plan_reviews = [r for r in results if 'plan' in r.get('permit_type', '').lower()]
        
        if plan_reviews:
            # Sort by most recent
            plan_reviews.sort(key=lambda x: x.get('application_date', ''), reverse=True)
            return plan_reviews[0]
        
        return None
    
    def _search_arcgis_address(self, address: str, city: str = None) -> List[Dict[str, Any]]:
        """Search using ArcGIS REST endpoint."""
        
        try:
            # ArcGIS FeatureServer query
            query_url = f"{self.arcgis_base}/query"
            
            where_clause = f"ADDRESS LIKE '%{address}%'"
            if city:
                where_clause += f" AND CITY = '{city}'"
            
            params = {
                'where': where_clause,
                'outFields': '*',
                'f': 'json',
                'returnGeometry': 'false',
                'orderByFields': 'APPLICATION_DATE DESC'
            }
            
            response = self.session.get(query_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            features = data.get('features', [])
            
            return [self._normalize_arcgis_record(feature['attributes']) for feature in features]
            
        except Exception as e:
            logger.warning(f"ArcGIS search failed: {e}")
            return []
    
    def _search_arcgis_name(self, business_name: str) -> List[Dict[str, Any]]:
        """Search ArcGIS by business name."""
        
        try:
            query_url = f"{self.arcgis_base}/query"
            
            params = {
                'where': f"BUSINESS_NAME LIKE '%{business_name}%' OR DBA_NAME LIKE '%{business_name}%'",
                'outFields': '*',
                'f': 'json',
                'returnGeometry': 'false',
                'orderByFields': 'APPLICATION_DATE DESC'
            }
            
            response = self.session.get(query_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            features = data.get('features', [])
            
            return [self._normalize_arcgis_record(feature['attributes']) for feature in features]
            
        except Exception as e:
            logger.warning(f"ArcGIS name search failed: {e}")
            return []
    
    def _search_web_address(self, address: str, city: str = None) -> List[Dict[str, Any]]:
        """Fallback web scraping search by address."""
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_default_timeout(self.timeout * 1000)
                
                return self._perform_web_search(page, 'address', address)
                
            except Exception as e:
                logger.error(f"Web address search failed: {e}")
                return []
            finally:
                browser.close()
    
    def _search_web_name(self, business_name: str) -> List[Dict[str, Any]]:
        """Fallback web scraping search by name."""
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_default_timeout(self.timeout * 1000)
                
                return self._perform_web_search(page, 'business_name', business_name)
                
            except Exception as e:
                logger.error(f"Web name search failed: {e}")
                return []
            finally:
                browser.close()
    
    def _perform_web_search(self, page: Page, search_type: str, search_value: str) -> List[Dict[str, Any]]:
        """Perform web search using Playwright."""
        
        try:
            # Navigate to HCPH food permits search
            search_url = f"{self.web_base}/food-permits/search"
            page.goto(search_url)
            
            # Fill search form based on type
            if search_type == 'address':
                page.fill('input[name="address"]', search_value)
            elif search_type == 'business_name':
                page.fill('input[name="business_name"]', search_value)
            
            # Submit search
            page.click('button[type="submit"]')
            page.wait_for_load_state('networkidle')
            
            # Extract results
            results = []
            result_rows = page.query_selector_all('table.search-results tbody tr')
            
            for row in result_rows:
                permit_data = self._extract_web_permit_data(row)
                if permit_data:
                    results.append(permit_data)
            
            return results
            
        except Exception as e:
            logger.error(f"Web search failed: {e}")
            return []
    
    def _extract_web_permit_data(self, row) -> Optional[Dict[str, Any]]:
        """Extract permit data from web table row."""
        
        try:
            cells = row.query_selector_all('td')
            if len(cells) < 4:
                return None
            
            return {
                'source': 'hcph_food_web',
                'permit_id': cells[0].inner_text().strip(),
                'business_name': cells[1].inner_text().strip(),
                'address': cells[2].inner_text().strip(),
                'permit_type': cells[3].inner_text().strip(),
                'status': cells[4].inner_text().strip() if len(cells) > 4 else 'unknown',
                'application_date': cells[5].inner_text().strip() if len(cells) > 5 else None,
                'approved_date': cells[6].inner_text().strip() if len(cells) > 6 else None,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.warning(f"Error extracting web permit data: {e}")
            return None
    
    def _normalize_arcgis_record(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize ArcGIS record to standard format."""
        
        return {
            'source': 'hcph_food_arcgis',
            'permit_id': attributes.get('PERMIT_ID') or attributes.get('APPLICATION_ID'),
            'business_name': attributes.get('BUSINESS_NAME') or attributes.get('DBA_NAME'),
            'address': attributes.get('ADDRESS'),
            'city': attributes.get('CITY'),
            'state': attributes.get('STATE', 'TX'),
            'zip_code': attributes.get('ZIP_CODE'),
            'permit_type': attributes.get('PERMIT_TYPE'),
            'status': attributes.get('STATUS'),
            'application_date': self._convert_arcgis_date(attributes.get('APPLICATION_DATE')),
            'approved_date': self._convert_arcgis_date(attributes.get('APPROVED_DATE')),
            'inspection_date': self._convert_arcgis_date(attributes.get('INSPECTION_DATE')),
            'plan_review_status': attributes.get('PLAN_REVIEW_STATUS'),
            'raw_data': attributes
        }
    
    def _convert_arcgis_date(self, date_value) -> Optional[str]:
        """Convert ArcGIS date to ISO format."""
        
        if not date_value:
            return None
        
        try:
            # ArcGIS often returns dates as milliseconds since epoch
            if isinstance(date_value, (int, float)):
                dt = datetime.fromtimestamp(date_value / 1000)
                return dt.isoformat()
            
            # If it's already a string, try to parse it
            if isinstance(date_value, str):
                return date_value
            
        except Exception as e:
            logger.warning(f"Error converting ArcGIS date: {e}")
        
        return str(date_value) if date_value else None
    
    def get_inspection_history(self, permit_id: str) -> List[Dict[str, Any]]:
        """Get inspection history for a permit."""
        
        try:
            # Try ArcGIS inspections endpoint
            query_url = f"{self.arcgis_base}/inspections/query"
            
            params = {
                'where': f"PERMIT_ID = '{permit_id}'",
                'outFields': '*',
                'f': 'json',
                'orderByFields': 'INSPECTION_DATE DESC'
            }
            
            response = self.session.get(query_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            features = data.get('features', [])
            
            inspections = []
            for feature in features:
                attrs = feature['attributes']
                inspections.append({
                    'inspection_id': attrs.get('INSPECTION_ID'),
                    'permit_id': permit_id,
                    'inspection_date': self._convert_arcgis_date(attrs.get('INSPECTION_DATE')),
                    'inspection_type': attrs.get('INSPECTION_TYPE'),
                    'result': attrs.get('RESULT'),
                    'violations': attrs.get('VIOLATIONS'),
                    'notes': attrs.get('NOTES')
                })
            
            return inspections
            
        except Exception as e:
            logger.warning(f"Failed to get inspection history for {permit_id}: {e}")
            return []


def search_food_permits_by_candidate(candidate_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Search for food permits matching a candidate."""
    
    client = HCPHFoodPermitsClient()
    results = []
    
    # Search by business name if available
    venue_name = candidate_data.get('venue_name')
    legal_name = candidate_data.get('legal_name')
    
    if venue_name:
        results.extend(client.search_by_business_name(venue_name))
    
    if legal_name and legal_name != venue_name:
        results.extend(client.search_by_business_name(legal_name))
    
    # Search by address
    address = candidate_data.get('address')
    city = candidate_data.get('city')
    
    if address:
        results.extend(client.search_by_address(address, city))
    
    # Deduplicate results
    seen_ids = set()
    unique_results = []
    
    for result in results:
        result_id = result.get('permit_id')
        if result_id and result_id not in seen_ids:
            seen_ids.add(result_id)
            unique_results.append(result)
    
    return unique_results
