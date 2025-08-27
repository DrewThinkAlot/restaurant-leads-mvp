"""Harris County Issued Permits API client (ArcGIS FeatureServer)."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator
from urllib.parse import quote_plus

from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class HarrisPermitsClient(BaseAPIClient):
    """Client for Harris County permits via ArcGIS FeatureServer."""
    
    def __init__(self):
        super().__init__(
            base_url="https://www.gis.hctx.net/arcgishcpid/rest/services/Permits/IssuedPermits/FeatureServer/0",
            rate_limit_per_second=1.0,  # Be conservative with county GIS
            timeout=45
        )
    
    def _set_auth_headers(self):
        """ArcGIS public endpoints don't require auth."""
        pass
    
    def fetch_records(self, since: Optional[datetime] = None, 
                     limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Fetch Harris County permit records."""
        
        logger.info("Fetching Harris County permit records")
        
        # Build ArcGIS query parameters
        where_clause = self._build_where_clause(since)
        
        params = {
            "where": where_clause,
            "outFields": "PERMITNUMBER,PERMITNAME,STATUS,ISSUEDDATE,STREETNUMBER,STREETNAME,APPTYPE,APPLICANTNAME",
            "orderByFields": "ISSUEDDATE DESC",
            "f": "json",
            "resultRecordCount": 2000,  # ArcGIS max per request
            "resultOffset": 0
        }
        
        offset = 0
        total_fetched = 0
        
        while total_fetched < limit:
            params["resultOffset"] = offset
            params["resultRecordCount"] = min(2000, limit - total_fetched)
            
            try:
                response_data = self.get("query", params)
                
                if not response_data.get("features"):
                    break
                
                features = response_data["features"]
                
                for feature in features:
                    if total_fetched >= limit:
                        break
                    
                    # Extract attributes from ArcGIS feature format
                    attributes = feature.get("attributes", {})
                    
                    # Add metadata
                    attributes["_source"] = "harris_permits"
                    attributes["_fetched_at"] = datetime.utcnow().isoformat()
                    
                    yield self.normalize_record(attributes)
                    total_fetched += 1
                
                if len(features) < params["resultRecordCount"]:
                    break
                
                offset += len(features)
                
            except Exception as e:
                logger.error(f"Error fetching Harris permits at offset {offset}: {e}")
                break
        
        logger.info(f"Fetched {total_fetched} Harris County permit records")
    
    def _build_where_clause(self, since: Optional[datetime]) -> str:
        """Build ArcGIS WHERE clause with date and keyword filters."""
        
        conditions = []
        
        # Date filter
        if since:
            # ArcGIS uses epoch time in milliseconds
            epoch_ms = int(since.timestamp() * 1000)
            conditions.append(f"ISSUEDDATE >= {epoch_ms}")
        else:
            # Default to last 90 days
            cutoff = datetime.utcnow() - timedelta(days=90)
            epoch_ms = int(cutoff.timestamp() * 1000)
            conditions.append(f"ISSUEDDATE >= {epoch_ms}")
        
        # Restaurant/food service keyword filters
        restaurant_keywords = [
            "RESTAURANT", "KITCHEN", "FOOD", "TENANT", "COMMERCIAL", 
            "FOOD SERVICE", "CAFE", "DINER", "BAR", "GRILL"
        ]
        
        keyword_conditions = []
        for keyword in restaurant_keywords:
            keyword_conditions.append(f"upper(PERMITNAME) LIKE '%{keyword}%'")
        
        if keyword_conditions:
            conditions.append(f"({' OR '.join(keyword_conditions)})")
        
        return " AND ".join(conditions) if conditions else "1=1"
    
    def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Harris County permit record to standard format."""
        
        # Handle address construction
        street_number = raw_record.get("STREETNUMBER", "")
        street_name = raw_record.get("STREETNAME", "")
        address = f"{street_number} {street_name}".strip()
        
        normalized = {
            "source": "harris_permits",
            "source_id": raw_record.get("PERMITNUMBER"),
            "source_url": "https://www.gis.hctx.net/arcgishcpid/rest/services/Permits/IssuedPermits/FeatureServer/0",
            "raw_data": raw_record,
            "fetched_at": raw_record.get("_fetched_at"),
            
            # Venue details (limited from permit data)
            "venue_name": self._extract_venue_name(raw_record),
            "legal_name": None,  # Not available in permit data
            "address": self._clean_text(address),
            "city": "Houston",  # Harris County permits are mostly Houston
            "state": "TX",
            "zip_code": None,  # Not available in this dataset
            "county": "Harris",
            
            # Permit-specific signals
            "permit_number": raw_record.get("PERMITNUMBER"),
            "permit_name": raw_record.get("PERMITNAME"),
            "permit_status": raw_record.get("STATUS"),
            "permit_type": raw_record.get("APPTYPE"),
            "issued_date": self._parse_arcgis_date(raw_record.get("ISSUEDDATE")),
            "applicant_name": self._clean_text(raw_record.get("APPLICANTNAME", "")),
            
            # Lead scoring signals
            "signal_strength": self._calculate_signal_strength(raw_record),
            "estimated_open_window": self._estimate_open_window(raw_record)
        }
        
        return normalized
    
    def _extract_venue_name(self, record: Dict[str, Any]) -> str:
        """Extract likely venue name from permit data."""
        
        # Try to extract business name from permit name or applicant
        permit_name = record.get("PERMITNAME", "").upper()
        applicant_name = record.get("APPLICANTNAME", "").upper()
        
        # Look for business names in permit description
        if "FOR" in permit_name:
            # e.g., "COMMERCIAL KITCHEN FOR ACME RESTAURANT"
            parts = permit_name.split("FOR")
            if len(parts) > 1:
                potential_name = parts[-1].strip()
                if len(potential_name) > 3:
                    return self._clean_text(potential_name)
        
        # Use applicant name if it looks like a business
        if applicant_name and ("LLC" in applicant_name or "INC" in applicant_name or 
                              "CORP" in applicant_name or "GROUP" in applicant_name):
            return self._clean_text(applicant_name)
        
        # Fallback to permit type description
        return self._clean_text(permit_name)
    
    def _calculate_signal_strength(self, record: Dict[str, Any]) -> float:
        """Calculate lead strength score based on permit data."""
        
        score = 0.0
        
        # Base score for permit activity
        score += 0.4
        
        # Recent permit boost
        issued_date = self._parse_arcgis_date(record.get("ISSUEDDATE"))
        if issued_date:
            try:
                days_ago = (datetime.utcnow() - datetime.fromisoformat(issued_date)).days
                if days_ago <= 30:
                    score += 0.4  # Very recent permit
                elif days_ago <= 90:
                    score += 0.2  # Recent permit
            except:
                pass
        
        # Permit type scoring
        permit_name = record.get("PERMITNAME", "").upper()
        if "RESTAURANT" in permit_name:
            score += 0.3
        elif "KITCHEN" in permit_name or "FOOD" in permit_name:
            score += 0.2
        elif "TENANT" in permit_name:
            score += 0.1  # Build-out permits are good signals
        
        # Status scoring
        status = record.get("STATUS", "").upper()
        if "APPROVED" in status or "ISSUED" in status:
            score += 0.2
        elif "PENDING" in status:
            score += 0.1
        
        return min(score, 1.0)
    
    def _estimate_open_window(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate opening timeframe based on permit type and status."""
        
        permit_name = record.get("PERMITNAME", "").upper()
        status = record.get("STATUS", "").upper()
        
        if "FOOD" in permit_name or "RESTAURANT" in permit_name:
            if "APPROVED" in status or "ISSUED" in status:
                # Food service permits suggest near-term opening
                return {
                    "min_days": 30,
                    "max_days": 90,
                    "confidence": 0.7
                }
            else:
                return {
                    "min_days": 60,
                    "max_days": 120,
                    "confidence": 0.5
                }
        elif "TENANT" in permit_name or "BUILD" in permit_name:
            # Build-out permits suggest longer timeline
            return {
                "min_days": 90,
                "max_days": 180,
                "confidence": 0.6
            }
        else:
            # General permits
            return {
                "min_days": 60,
                "max_days": 150,
                "confidence": 0.4
            }
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text fields."""
        if not text:
            return ""
        return text.strip().title()
    
    def _parse_arcgis_date(self, timestamp: int) -> Optional[str]:
        """Parse ArcGIS epoch timestamp to ISO format."""
        if not timestamp:
            return None
        
        try:
            # ArcGIS timestamps are in milliseconds
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.isoformat()
        except (ValueError, TypeError, OSError):
            logger.warning(f"Could not parse timestamp: {timestamp}")
            return None
