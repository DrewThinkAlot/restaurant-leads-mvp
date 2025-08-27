from playwright.sync_api import sync_playwright, Page, Browser
import time
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
from urllib.parse import urljoin
import re

from ..settings import settings

logger = logging.getLogger(__name__)


class HarrisCountyPermitsClient:
    """Client for Harris County ePermits data via web scraping."""
    
    def __init__(self):
        self.base_url = "https://edocs.harriscountytx.gov"  # Replace with actual URL
        self.delay = settings.crawl_delay_seconds
        self.timeout = settings.requests_timeout * 1000  # Convert to milliseconds
        
    def search_permits(self, 
                      search_terms: List[str] = None, 
                      date_from: datetime = None,
                      date_to: datetime = None,
                      permit_types: List[str] = None) -> List[Dict[str, Any]]:
        """Search for permits using web interface."""
        
        if not search_terms:
            search_terms = ["restaurant", "food service", "tenant build-out"]
        
        if not date_from:
            date_from = datetime.now() - timedelta(days=90)
        
        if not date_to:
            date_to = datetime.now()
        
        results = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_default_timeout(self.timeout)
                
                for search_term in search_terms:
                    permits = self._search_single_term(page, search_term, date_from, date_to)
                    results.extend(permits)
                    time.sleep(self.delay)
                
            except Exception as e:
                logger.error(f"Error during Harris County permit search: {e}")
            finally:
                browser.close()
        
        return self._deduplicate_permits(results)
    
    def get_permit_details(self, permit_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific permit."""
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_default_timeout(self.timeout)
                
                return self._fetch_permit_details(page, permit_id)
                
            except Exception as e:
                logger.error(f"Error fetching permit details for {permit_id}: {e}")
                return None
            finally:
                browser.close()
    
    def _search_single_term(self, page: Page, search_term: str, 
                           date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Perform search for a single term."""
        
        try:
            # Navigate to permits search page
            page.goto(f"{self.base_url}/permits/search")  # Placeholder URL
            
            # Fill search form
            page.fill('input[name="search"]', search_term)
            page.fill('input[name="date_from"]', date_from.strftime('%m/%d/%Y'))
            page.fill('input[name="date_to"]', date_to.strftime('%m/%d/%Y'))
            
            # Submit search
            page.click('button[type="submit"]')
            page.wait_for_load_state('networkidle')
            
            # Extract results
            results = []
            result_rows = page.query_selector_all('table.permits-table tbody tr')
            
            for row in result_rows:
                permit_data = self._extract_permit_from_row(row)
                if permit_data and self._is_restaurant_related(permit_data):
                    results.append(permit_data)
            
            # Handle pagination if present
            next_button = page.query_selector('a.next-page')
            page_count = 1
            
            while next_button and page_count < 10:  # Limit to prevent infinite loops
                next_button.click()
                page.wait_for_load_state('networkidle')
                
                additional_rows = page.query_selector_all('table.permits-table tbody tr')
                for row in additional_rows:
                    permit_data = self._extract_permit_from_row(row)
                    if permit_data and self._is_restaurant_related(permit_data):
                        results.append(permit_data)
                
                next_button = page.query_selector('a.next-page')
                page_count += 1
                time.sleep(self.delay)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching for '{search_term}': {e}")
            return []
    
    def _extract_permit_from_row(self, row) -> Optional[Dict[str, Any]]:
        """Extract permit data from table row."""
        
        try:
            cells = row.query_selector_all('td')
            if len(cells) < 5:
                return None
            
            permit_data = {
                'source': 'hc_permits',
                'permit_id': cells[0].inner_text().strip(),
                'permit_type': cells[1].inner_text().strip(),
                'description': cells[2].inner_text().strip(),
                'address': cells[3].inner_text().strip(),
                'status': cells[4].inner_text().strip(),
                'application_date': self._parse_date(cells[5].inner_text().strip()) if len(cells) > 5 else None,
                'issued_date': self._parse_date(cells[6].inner_text().strip()) if len(cells) > 6 else None,
                'applicant': cells[7].inner_text().strip() if len(cells) > 7 else None,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract additional details from description
            description = permit_data['description'].lower()
            permit_data['business_type'] = self._extract_business_type(description)
            
            return permit_data
            
        except Exception as e:
            logger.warning(f"Error extracting permit data from row: {e}")
            return None
    
    def _fetch_permit_details(self, page: Page, permit_id: str) -> Optional[Dict[str, Any]]:
        """Fetch detailed permit information."""
        
        try:
            # Navigate to permit details page
            page.goto(f"{self.base_url}/permits/{permit_id}")
            page.wait_for_load_state('networkidle')
            
            # Extract detailed information
            details = {
                'permit_id': permit_id,
                'source': 'hc_permits_detail',
                'full_html': page.content(),  # Store HTML for later analysis
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract structured data from detail page
            detail_sections = page.query_selector_all('.permit-detail-section')
            for section in detail_sections:
                section_title = section.query_selector('h3')
                if section_title:
                    title = section_title.inner_text().strip().lower().replace(' ', '_')
                    content = section.query_selector('.section-content')
                    if content:
                        details[title] = content.inner_text().strip()
            
            # Extract milestone dates
            milestone_table = page.query_selector('table.milestones')
            if milestone_table:
                milestones = {}
                rows = milestone_table.query_selector_all('tbody tr')
                for row in rows:
                    cells = row.query_selector_all('td')
                    if len(cells) >= 2:
                        milestone_name = cells[0].inner_text().strip()
                        milestone_date = cells[1].inner_text().strip()
                        milestones[milestone_name.lower().replace(' ', '_')] = milestone_date
                details['milestones'] = milestones
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching details for permit {permit_id}: {e}")
            return None
    
    def _is_restaurant_related(self, permit_data: Dict[str, Any]) -> bool:
        """Check if permit is restaurant/food service related."""
        
        restaurant_indicators = [
            'restaurant', 'food service', 'kitchen', 'dining', 'cafe', 'bar',
            'food preparation', 'commercial kitchen', 'tenant finish',
            'build-out', 'renovation'
        ]
        
        text_to_check = ' '.join([
            permit_data.get('permit_type', ''),
            permit_data.get('description', ''),
            permit_data.get('business_type', '')
        ]).lower()
        
        return any(indicator in text_to_check for indicator in restaurant_indicators)
    
    def _extract_business_type(self, description: str) -> str:
        """Extract business type from permit description."""
        
        business_types = {
            'restaurant': ['restaurant', 'dining', 'eatery'],
            'fast_food': ['fast food', 'quick service', 'drive thru'],
            'cafe': ['cafe', 'coffee', 'bistro'],
            'bar': ['bar', 'tavern', 'pub', 'lounge'],
            'food_truck': ['food truck', 'mobile food'],
            'catering': ['catering', 'caterer'],
            'retail': ['retail', 'grocery', 'convenience']
        }
        
        for business_type, keywords in business_types.items():
            if any(keyword in description for keyword in keywords):
                return business_type
        
        return 'unknown'
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format."""
        
        if not date_str or date_str.strip() == '':
            return None
        
        # Common date formats
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d-%m-%Y']
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str.strip(), fmt)
                return parsed_date.isoformat()
            except ValueError:
                continue
        
        return date_str.strip()  # Return original if parsing fails
    
    def _deduplicate_permits(self, permits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate permits based on permit ID."""
        
        seen_ids = set()
        unique_permits = []
        
        for permit in permits:
            permit_id = permit.get('permit_id')
            if permit_id and permit_id not in seen_ids:
                seen_ids.add(permit_id)
                unique_permits.append(permit)
        
        return unique_permits


def search_restaurant_permits(days_back: int = 90) -> List[Dict[str, Any]]:
    """Search for restaurant-related permits in Harris County."""
    
    client = HarrisCountyPermitsClient()
    
    search_terms = [
        "restaurant tenant build-out",
        "food service renovation", 
        "commercial kitchen",
        "restaurant construction"
    ]
    
    date_from = datetime.now() - timedelta(days=days_back)
    permits = client.search_permits(search_terms=search_terms, date_from=date_from)
    
    return permits
