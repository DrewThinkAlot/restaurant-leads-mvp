"""Texas Comptroller CloudApps Public API client for entity enrichment."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator
from urllib.parse import quote_plus

from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class ComptrollerClient(BaseAPIClient):
    """Client for Texas Comptroller CloudApps Public API."""
    
    def __init__(self, api_key: str):
        super().__init__(
            base_url="https://api.cpa.texas.gov/public-data/franchise/accounts",
            api_key=api_key,
            rate_limit_per_second=1.0,  # Conservative rate limiting
            timeout=30
        )
    
    def _set_auth_headers(self):
        """Set API key header for Comptroller API."""
        if self.api_key:
            self.session.headers.update({
                'x-api-key': self.api_key
            })
    
    def fetch_records(self, since: Optional[datetime] = None, 
                     limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Fetch Comptroller records - used for entity enrichment, not bulk fetching."""
        
        # This client is primarily for enrichment lookups, not bulk fetching
        # Return empty generator as this API doesn't support date-based bulk queries
        logger.info("Comptroller client is for enrichment only, no bulk fetch available")
        return
        yield  # Make this a generator
    
    def search_by_name(self, name: str, city: str = None, zip_code: str = None) -> Dict[str, Any]:
        """Search for entities by name, city, and zip."""
        
        logger.info(f"Searching Comptroller for: {name}")
        
        params = {"name": name}
        if city:
            params["city"] = city
        if zip_code:
            params["zip"] = zip_code
        
        try:
            response_data = self.get("search", params)
            
            # Add metadata
            response_data["_source"] = "comptroller_search"
            response_data["_fetched_at"] = datetime.utcnow().isoformat()
            response_data["_query_params"] = params
            
            return self.normalize_search_result(response_data)
            
        except Exception as e:
            logger.error(f"Comptroller search failed for {name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "source": "comptroller_search",
                "fetched_at": datetime.utcnow().isoformat()
            }
    
    def lookup_by_taxpayer_number(self, taxpayer_number: str) -> Dict[str, Any]:
        """Lookup entity by taxpayer number."""
        
        logger.info(f"Looking up taxpayer: {taxpayer_number}")
        
        try:
            response_data = self.get("lookup", {"taxpayerNumber": taxpayer_number})
            
            # Add metadata
            response_data["_source"] = "comptroller_lookup"
            response_data["_fetched_at"] = datetime.utcnow().isoformat()
            response_data["_taxpayer_number"] = taxpayer_number
            
            return self.normalize_lookup_result(response_data)
            
        except Exception as e:
            logger.error(f"Comptroller lookup failed for {taxpayer_number}: {e}")
            return {
                "success": False,
                "error": str(e),
                "source": "comptroller_lookup",
                "fetched_at": datetime.utcnow().isoformat()
            }
    
    def enrich_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich candidate with Comptroller data."""
        
        venue_name = candidate.get("venue_name", "")
        legal_name = candidate.get("legal_name", "")
        city = candidate.get("city", "")
        zip_code = candidate.get("zip_code", "")
        
        enrichment_data = {
            "comptroller_match": None,
            "legal_entity_info": None,
            "registered_agent": None,
            "officers": [],
            "confidence": 0.0
        }
        
        # Try search by legal name first
        if legal_name:
            search_result = self.search_by_name(legal_name, city, zip_code)
            if search_result.get("success") and search_result.get("matches"):
                enrichment_data["comptroller_match"] = search_result
                enrichment_data["confidence"] = 0.8
        
        # Fallback to venue name search
        if not enrichment_data["comptroller_match"] and venue_name:
            search_result = self.search_by_name(venue_name, city, zip_code)
            if search_result.get("success") and search_result.get("matches"):
                enrichment_data["comptroller_match"] = search_result
                enrichment_data["confidence"] = 0.6
        
        # Extract structured data if we found a match
        if enrichment_data["comptroller_match"]:
            matches = enrichment_data["comptroller_match"].get("matches", [])
            if matches:
                best_match = matches[0]  # First match is usually best
                enrichment_data["legal_entity_info"] = {
                    "taxpayer_number": best_match.get("taxpayerNumber"),
                    "legal_name": best_match.get("legalName"),
                    "dba_name": best_match.get("dbaName"),
                    "entity_type": best_match.get("entityType"),
                    "status": best_match.get("status"),
                    "address": best_match.get("address")
                }
                
                enrichment_data["registered_agent"] = best_match.get("registeredAgent")
                enrichment_data["officers"] = best_match.get("officers", [])
        
        return enrichment_data
    
    def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Comptroller record - not used for bulk fetching."""
        return raw_record
    
    def normalize_search_result(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Comptroller search result."""
        
        normalized = {
            "success": True,
            "source": "comptroller_search",
            "fetched_at": raw_result.get("_fetched_at"),
            "query_params": raw_result.get("_query_params", {}),
            "matches": []
        }
        
        # Handle different response formats
        if isinstance(raw_result, list):
            # Direct list of results
            matches = raw_result
        elif isinstance(raw_result, dict):
            # Wrapped response
            matches = raw_result.get("results", raw_result.get("data", []))
        else:
            matches = []
        
        for match in matches:
            if isinstance(match, dict):
                normalized_match = {
                    "taxpayer_number": match.get("taxpayerNumber"),
                    "legal_name": self._clean_text(match.get("legalName", "")),
                    "dba_name": self._clean_text(match.get("dbaName", "")),
                    "entity_type": match.get("entityType"),
                    "status": match.get("status"),
                    "address": self._clean_address(match.get("address", {})),
                    "registered_agent": self._clean_text(match.get("registeredAgent", "")),
                    "officers": [self._clean_text(officer) for officer in match.get("officers", [])],
                    "formation_date": match.get("formationDate"),
                    "confidence_score": self._calculate_match_confidence(match)
                }
                normalized["matches"].append(normalized_match)
        
        # Sort matches by confidence
        normalized["matches"].sort(key=lambda x: x.get("confidence_score", 0), reverse=True)
        
        return normalized
    
    def normalize_lookup_result(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Comptroller lookup result."""
        
        if not raw_result or isinstance(raw_result, str):
            return {
                "success": False,
                "source": "comptroller_lookup",
                "error": "No data returned"
            }
        
        normalized = {
            "success": True,
            "source": "comptroller_lookup",
            "fetched_at": raw_result.get("_fetched_at"),
            "taxpayer_number": raw_result.get("_taxpayer_number"),
            "legal_name": self._clean_text(raw_result.get("legalName", "")),
            "dba_name": self._clean_text(raw_result.get("dbaName", "")),
            "entity_type": raw_result.get("entityType"),
            "status": raw_result.get("status"),
            "address": self._clean_address(raw_result.get("address", {})),
            "registered_agent": self._clean_text(raw_result.get("registeredAgent", "")),
            "officers": [self._clean_text(officer) for officer in raw_result.get("officers", [])],
            "formation_date": raw_result.get("formationDate"),
            "last_report_date": raw_result.get("lastReportDate")
        }
        
        return normalized
    
    def _calculate_match_confidence(self, match: Dict[str, Any]) -> float:
        """Calculate confidence score for entity match."""
        
        score = 0.0
        
        # Base score for having basic info
        if match.get("legalName"):
            score += 0.3
        
        # Active status boost
        status = match.get("status", "").lower()
        if "active" in status or "good standing" in status:
            score += 0.3
        elif "inactive" in status or "dissolved" in status:
            score -= 0.2
        
        # Entity type relevance
        entity_type = match.get("entityType", "").lower()
        if "llc" in entity_type or "corporation" in entity_type:
            score += 0.2
        
        # Address completeness
        address = match.get("address", {})
        if address and address.get("city") and address.get("zip"):
            score += 0.2
        
        return min(max(score, 0.0), 1.0)
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text fields."""
        if not text:
            return ""
        return text.strip().title()
    
    def _clean_address(self, address: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize address data."""
        if not address:
            return {}
        
        return {
            "street": self._clean_text(address.get("street", "")),
            "city": self._clean_text(address.get("city", "")),
            "state": address.get("state", "").upper()[:2],
            "zip": address.get("zip", "").strip()[:10]
        }
