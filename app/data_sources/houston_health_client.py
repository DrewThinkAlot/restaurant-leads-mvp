"""Houston Health Department Food Service Facility Inspections API client (CKAN DataStore)."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator

from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class HoustonHealthClient(BaseAPIClient):
    """Client for Houston Health Department inspections via CKAN DataStore API."""
    
    def __init__(self):
        super().__init__(
            base_url="https://data.houstontx.gov/api/3/action",
            rate_limit_per_second=2.0,  # Be conservative with city API
            timeout=30
        )
        self.current_resource_id = None
    
    def _set_auth_headers(self):
        """CKAN doesn't require auth headers for public datasets."""
        pass
    
    def fetch_records(self, since: Optional[datetime] = None, 
                     limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Fetch Houston health inspection records from historical dataset."""
        
        logger.info("Fetching Houston health inspection records (historical data)")
        
        # Get available resource ID
        resource_id = self._get_current_resource_id()
        if not resource_id:
            logger.error("Could not resolve resource ID for Houston health data")
            return
        
        # Use simple datastore_search instead of SQL for better compatibility
        offset = 0
        batch_size = min(100, limit)  # Smaller batches for historical data
        total_fetched = 0
        
        while total_fetched < limit:
            try:
                params = {
                    "resource_id": resource_id,
                    "limit": batch_size,
                    "offset": offset
                }
                
                response_data = self.get("datastore_search", params)
                
                if not response_data.get("success"):
                    logger.warning(f"Houston health API response: {response_data}")
                    break
                
                records = response_data.get("result", {}).get("records", [])
                if not records:
                    break
                    
                for record in records:
                    if total_fetched >= limit:
                        break
                    
                    # Add metadata
                    record["_source"] = "houston_health"
                    record["_resource_id"] = resource_id
                    record["_fetched_at"] = datetime.utcnow().isoformat()
                    
                    yield self.normalize_record(record)
                    total_fetched += 1
                
                if len(records) < batch_size:
                    break
                
                offset += len(records)
                
            except Exception as e:
                logger.error(f"Error fetching Houston health records at offset {offset}: {e}")
                break
        
        logger.info(f"Fetched {total_fetched} Houston health inspection records")
    
    def _get_current_resource_id(self) -> Optional[str]:
        """Resolve available resource ID from historical datasets."""
        
        if self.current_resource_id:
            return self.current_resource_id
        
        try:
            # Get the specific known dataset
            package_id = "city-of-houston-health-and-human-services-food-service-facility-inspections"
            response = self.get("package_show", {"id": package_id})
            
            if not response.get("success"):
                logger.warning("Could not fetch Houston health package details")
                return None
            
            package = response.get("result", {})
            resources = package.get("resources", [])
            
            # Use the most recent fiscal year available (FY 15)
            for resource in resources:
                name = resource.get("name", "").lower()
                if "fiscal year 15" in name or "fy15" in name:
                    self.current_resource_id = resource["id"]
                    logger.info(f"Using Houston health FY15 resource: {self.current_resource_id}")
                    return self.current_resource_id
            
            # Fallback to last available resource
            if resources:
                self.current_resource_id = resources[-1]["id"]
                logger.info(f"Using last available Houston health resource: {self.current_resource_id}")
                return self.current_resource_id
            
        except Exception as e:
            logger.error(f"Error resolving Houston health resource ID: {e}")
        
        return None
    
    def _build_sql_query(self, since: Optional[datetime], limit: int) -> str:
        """Build SQL query for CKAN DataStore."""
        
        select_fields = [
            "business_name", "address", "city", "zip", 
            "inspection_date", "result", "permit_number"
        ]
        
        where_conditions = []
        
        # Date filter
        if since:
            date_str = since.strftime('%Y-%m-%d')
            where_conditions.append(f"inspection_date >= '{date_str}'")
        else:
            # Default to last 90 days
            cutoff = datetime.utcnow() - timedelta(days=90)
            date_str = cutoff.strftime('%Y-%m-%d')
            where_conditions.append(f"inspection_date >= '{date_str}'")
        
        # Only include restaurant-related inspections
        restaurant_keywords = [
            "RESTAURANT", "FOOD SERVICE", "KITCHEN", "CAFE", 
            "DINER", "BISTRO", "EATERY", "GRILL"
        ]
        
        business_conditions = []
        for keyword in restaurant_keywords:
            business_conditions.append(f"upper(business_name) LIKE '%{keyword}%'")
        
        if business_conditions:
            where_conditions.append(f"({' OR '.join(business_conditions)})")
        
        resource_placeholder = f'"{self.current_resource_id or "RESOURCE_ID"}"'
        
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM {resource_placeholder}
        """
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        
        query += " ORDER BY inspection_date DESC"
        
        return query
    
    def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Houston health inspection record to standard format."""
        
        normalized = {
            "source": "houston_health",
            "source_id": raw_record.get("permit_number") or f"health_{raw_record.get('_id', '')}",
            "source_url": f"https://data.houstontx.gov/dataset/food-service-facility-inspections",
            "raw_data": raw_record,
            "fetched_at": raw_record.get("_fetched_at"),
            
            # Venue details
            "venue_name": self._clean_text(raw_record.get("FacilityName") or raw_record.get("business_name", "")),
            "legal_name": None,  # Not available in health data
            "address": self._clean_text(raw_record.get("FacilityFullStreetAddress") or raw_record.get("address", "")),
            "city": self._clean_text(raw_record.get("FacilityCity") or raw_record.get("city", "Houston")),
            "state": "TX",
            "zip_code": self._clean_text(raw_record.get("FacilityZip") or raw_record.get("zip", "")),
            "county": "Harris",
            
            # Health inspection signals
            "inspection_date": self._parse_date(raw_record.get("inspection_date")),
            "inspection_result": raw_record.get("result"),
            "permit_number": raw_record.get("permit_number"),
            
            # Lead scoring signals
            "signal_strength": self._calculate_signal_strength(raw_record),
            "estimated_open_window": self._estimate_open_window(raw_record)
        }
        
        return normalized
    
    def _calculate_signal_strength(self, record: Dict[str, Any]) -> float:
        """Calculate lead strength score based on inspection data."""
        
        score = 0.0
        
        # Base score for health inspection activity
        score += 0.3
        
        # Recent inspection boost
        inspection_date = self._parse_date(record.get("inspection_date"))
        if inspection_date:
            try:
                days_ago = (datetime.utcnow() - datetime.fromisoformat(inspection_date)).days
                if days_ago <= 30:
                    score += 0.4  # Very recent activity
                elif days_ago <= 90:
                    score += 0.2  # Recent activity
            except:
                pass
        
        # Result-based scoring
        result = record.get("result", "").lower()
        if "satisfactory" in result or "pass" in result:
            score += 0.2  # Good inspection results suggest readiness
        elif "conditional" in result:
            score += 0.1  # Working toward compliance
        
        # Business name analysis
        business_name = record.get("business_name", "").lower()
        new_keywords = ["new", "coming soon", "opening", "grand opening"]
        if any(keyword in business_name for keyword in new_keywords):
            score += 0.3
        
        return min(score, 1.0)
    
    def _estimate_open_window(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate opening timeframe based on inspection data."""
        
        result = record.get("result", "").lower()
        
        if "satisfactory" in result or "pass" in result:
            # Passed inspection suggests imminent opening
            return {
                "min_days": 7,
                "max_days": 45,
                "confidence": 0.8
            }
        elif "conditional" in result:
            # Conditional pass suggests opening within 1-2 months
            return {
                "min_days": 30,
                "max_days": 60,
                "confidence": 0.6
            }
        else:
            # Other results suggest longer timeline
            return {
                "min_days": 60,
                "max_days": 120,
                "confidence": 0.4
            }
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text fields."""
        if not text:
            return ""
        return text.strip().title()
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format."""
        if not date_str:
            return None
        
        try:
            # Handle various date formats from CKAN
            if 'T' in str(date_str):
                dt = datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(str(date_str), '%Y-%m-%d')
            return dt.isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Could not parse date: {date_str}")
            return None
