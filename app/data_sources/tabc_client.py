"""TABC (Texas Alcoholic Beverage Commission) API client using Socrata/SODA."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator
from urllib.parse import quote_plus

from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class TABCClient(BaseAPIClient):
    """Client for TABC data via Texas Open Data (Socrata/SODA)."""
    
    PENDING_DATASET = "mxm5-tdpj"  # Pending Originals (Early Signal)
    ISSUED_DATASET = "7hf9-qc9f"   # Issued Licenses
    
    def __init__(self, app_token: Optional[str] = None):
        super().__init__(
            base_url="https://data.texas.gov/resource",
            api_key=app_token,
            rate_limit_per_second=5.0,  # Socrata allows higher rates with app token
            timeout=30
        )
    
    def _set_auth_headers(self):
        """Set Socrata app token header."""
        if self.api_key:
            self.session.headers.update({
                'X-App-Token': self.api_key
            })
    
    def fetch_records(self, since: Optional[datetime] = None, 
                     limit: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Fetch TABC records with pagination."""
        
        # Fetch both pending and issued records
        yield from self._fetch_pending_records(since, limit // 2)
        yield from self._fetch_issued_records(since, limit // 2)
    
    def _fetch_pending_records(self, since: Optional[datetime] = None, 
                              limit: int = 500) -> Generator[Dict[str, Any], None, None]:
        """Fetch pending original license applications."""
        
        logger.info("Fetching TABC pending applications")
        
        # Minimal SODA query - start with basic limit only
        params = {
            "$limit": min(50, limit // 2),  # Very small batches
            "$offset": 0
        }
        
        offset = 0
        total_fetched = 0
        
        while total_fetched < limit:
            params["$offset"] = offset
            params["$limit"] = min(500, limit - total_fetched)
            
            try:
                endpoint = f"{self.PENDING_DATASET}.json"
                response_data = self.get(endpoint, params)
                
                if not response_data:
                    break
                
                for record in response_data:
                    if total_fetched >= limit:
                        break
                    
                    # Add metadata
                    record["_source"] = "tabc_pending"
                    record["_dataset"] = self.PENDING_DATASET
                    record["_fetched_at"] = datetime.utcnow().isoformat()
                    
                    yield self.normalize_record(record)
                    total_fetched += 1
                
                if len(response_data) < params["$limit"]:
                    break
                
                offset += len(response_data)
                
            except Exception as e:
                logger.error(f"Error fetching TABC pending records at offset {offset}: {e}")
                # Fallback: try without where clause (date filter) once per loop
                try:
                    fallback_params = {k: v for k, v in params.items() if k != "$where"}
                    response_data = self.get(endpoint, fallback_params)
                    if not response_data:
                        break
                    for record in response_data:
                        if total_fetched >= limit:
                            break
                        record["_source"] = "tabc_pending"
                        record["_dataset"] = self.PENDING_DATASET
                        record["_fetched_at"] = datetime.utcnow().isoformat()
                        yield self.normalize_record(record)
                        total_fetched += 1
                    if len(response_data) < fallback_params["$limit"]:
                        break
                    offset += len(response_data)
                except Exception:
                    break
        
        logger.info(f"Fetched {total_fetched} TABC pending records")
    
    def _fetch_issued_records(self, since: Optional[datetime] = None, 
                             limit: int = 500) -> Generator[Dict[str, Any], None, None]:
        """Fetch issued licenses for transition tracking."""
        
        logger.info("Fetching TABC issued licenses")
        
        # Minimal SODA query - start with basic limit only
        params = {
            "$limit": min(50, limit // 2),  # Very small batches
            "$offset": 0
        }
        
        offset = 0
        total_fetched = 0
        
        while total_fetched < limit:
            params["$offset"] = offset
            params["$limit"] = min(500, limit - total_fetched)
            
            try:
                endpoint = f"{self.ISSUED_DATASET}.json"
                response_data = self.get(endpoint, params)
                
                if not response_data:
                    break
                
                for record in response_data:
                    if total_fetched >= limit:
                        break
                    
                    # Add metadata
                    record["_source"] = "tabc_issued"
                    record["_dataset"] = self.ISSUED_DATASET
                    record["_fetched_at"] = datetime.utcnow().isoformat()
                    
                    yield self.normalize_record(record)
                    total_fetched += 1
                
                if len(response_data) < params["$limit"]:
                    break
                
                offset += len(response_data)
                
            except Exception as e:
                logger.error(f"Error fetching TABC issued records at offset {offset}: {e}")
                # Fallback: try without where clause (date filter) once per loop
                try:
                    fallback_params = {k: v for k, v in params.items() if k != "$where"}
                    response_data = self.get(endpoint, fallback_params)
                    if not response_data:
                        break
                    for record in response_data:
                        if total_fetched >= limit:
                            break
                        record["_source"] = "tabc_issued"
                        record["_dataset"] = self.ISSUED_DATASET
                        record["_fetched_at"] = datetime.utcnow().isoformat()
                        yield self.normalize_record(record)
                        total_fetched += 1
                    if len(response_data) < fallback_params["$limit"]:
                        break
                    offset += len(response_data)
                except Exception:
                    break
        
        logger.info(f"Fetched {total_fetched} TABC issued records")
    
    def _build_where_clause(self, since: Optional[datetime], date_field: str) -> str:
        """Build SODA WHERE clause with date filter only (county schema varies)."""
        if since:
            date_str = since.strftime('%Y-%m-%d')
        else:
            cutoff = datetime.utcnow() - timedelta(days=30)
            date_str = cutoff.strftime('%Y-%m-%d')
        return f"{date_field} >= date '{date_str}'"
    
    def normalize_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize TABC record to standard format."""
        
        source = raw_record.get("_source", "tabc")
        
        # Common normalization
        normalized = {
            "source": source,
            "source_id": raw_record.get("aims_application_number") or raw_record.get("license_number"),
            "source_url": f"https://data.texas.gov/resource/{raw_record.get('_dataset')}.json",
            "raw_data": raw_record,
            "fetched_at": raw_record.get("_fetched_at"),
            
            # Venue details
            "venue_name": self._clean_text(raw_record.get("trade_name") or raw_record.get("tradename", "")),
            "legal_name": None,  # Not available in TABC data
            "address": self._clean_text(raw_record.get("address", "")),
            "city": self._clean_text(raw_record.get("city", "")),
            "state": "TX",
            "zip_code": self._clean_text(raw_record.get("zip", "")),
            "county": "Harris",
            
            # TABC-specific signals
            "license_type": raw_record.get("license_type"),
            "status": raw_record.get("status"),
            "owner_name": self._clean_text(raw_record.get("owner") or raw_record.get("owner_name") or raw_record.get("licensee_name", "")),
            
            # Dates
            "application_date": self._parse_date(raw_record.get("application_date")),
            "issue_date": self._parse_date(raw_record.get("issue_date")),
            
            # Lead scoring signals
            "signal_strength": self._calculate_signal_strength(raw_record, source),
            "estimated_open_window": self._estimate_open_window(raw_record, source)
        }
        
        return normalized
    
    def _calculate_signal_strength(self, record: Dict[str, Any], source: str) -> float:
        """Calculate lead strength score (0.0 to 1.0)."""
        
        score = 0.0
        
        # Source-based scoring
        if source == "tabc_pending":
            score += 0.8  # Pending applications are strong early signals
        elif source == "tabc_issued":
            score += 0.6  # Issued licenses indicate progress
        
        # License type scoring
        license_type = record.get("license_type", "").lower()
        if "restaurant" in license_type or "mixed beverage" in license_type:
            score += 0.2
        
        # Status scoring
        status = record.get("status", "").lower()
        if "approved" in status or "issued" in status:
            score += 0.1
        elif "pending" in status:
            score += 0.05
        
        return min(score, 1.0)
    
    def _estimate_open_window(self, record: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Estimate restaurant opening timeframe."""
        
        if source == "tabc_pending":
            # Pending applications suggest 2-6 months to opening
            return {
                "min_days": 60,
                "max_days": 180,
                "confidence": 0.7
            }
        elif source == "tabc_issued":
            # Issued licenses suggest 1-3 months to opening
            return {
                "min_days": 30,
                "max_days": 90,
                "confidence": 0.8
            }
        
        return {
            "min_days": 90,
            "max_days": 365,
            "confidence": 0.3
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
            # Handle common Socrata date formats
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Could not parse date: {date_str}")
            return None
