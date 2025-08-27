"""Main data source manager orchestrating all API clients."""

import csv
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

from .tabc_client import TABCClient
from .houston_health_client import HoustonHealthClient
from .harris_permits_client import HarrisPermitsClient
from .comptroller_client import ComptrollerClient
from .watermark_manager import WatermarkManager
from ..db import db_manager
from ..models import RawRecord

logger = logging.getLogger(__name__)


class DataSourceManager:
    """Manages all data source integrations with watermarks and normalization."""
    
    def __init__(self, tabc_app_token: str = None, comptroller_api_key: str = None):
        # Initialize clients
        self.tabc_client = TABCClient(tabc_app_token)
        self.houston_health_client = HoustonHealthClient()
        self.harris_permits_client = HarrisPermitsClient()
        self.comptroller_client = ComptrollerClient(comptroller_api_key) if comptroller_api_key else None
        
        # Initialize watermark manager
        self.watermark_manager = WatermarkManager()
        
        # Client registry
        self.clients = {
            "tabc": self.tabc_client,
            "houston_health": self.houston_health_client,
            "harris_permits": self.harris_permits_client
        }
        
        if self.comptroller_client:
            self.clients["comptroller"] = self.comptroller_client
    
    def fetch_all_sources(self, limit_per_source: int = 1000, 
                         parallel: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch data from all sources with watermark management."""
        
        logger.info("Starting data fetch from all sources")
        start_time = time.time()
        
        results = {}
        
        if parallel:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_source = {
                    executor.submit(self._fetch_source_with_watermark, source_name, client, limit_per_source): source_name
                    for source_name, client in self.clients.items()
                    if source_name != "comptroller"  # Comptroller is for enrichment only
                }
                
                for future in as_completed(future_to_source):
                    source_name = future_to_source[future]
                    try:
                        source_records = future.result()
                        results[source_name] = source_records
                        logger.info(f"Fetched {len(source_records)} records from {source_name}")
                    except Exception as e:
                        logger.error(f"Failed to fetch from {source_name}: {e}")
                        results[source_name] = []
        else:
            # Sequential execution
            for source_name, client in self.clients.items():
                if source_name == "comptroller":
                    continue  # Skip comptroller for bulk fetching
                
                try:
                    source_records = self._fetch_source_with_watermark(source_name, client, limit_per_source)
                    results[source_name] = source_records
                    logger.info(f"Fetched {len(source_records)} records from {source_name}")
                except Exception as e:
                    logger.error(f"Failed to fetch from {source_name}: {e}")
                    results[source_name] = []
        
        total_duration = time.time() - start_time
        total_records = sum(len(records) for records in results.values())
        
        logger.info(f"Fetched {total_records} total records in {total_duration:.1f}s")
        
        return results
    
    def _fetch_source_with_watermark(self, source_name: str, client, limit: int) -> List[Dict[str, Any]]:
        """Fetch data from a single source with watermark management."""
        
        start_time = time.time()
        
        # Get incremental window
        since = self.watermark_manager.get_incremental_window(source_name)
        logger.info(f"Fetching {source_name} since {since}")
        
        records = []
        latest_timestamp = None
        
        try:
            for record in client.fetch_records(since=since, limit=limit):
                records.append(record)
                
                # Track latest timestamp for watermark
                record_date = self._extract_record_date(record)
                if record_date and (not latest_timestamp or record_date > latest_timestamp):
                    latest_timestamp = record_date
            
            # Update watermark if we got records
            if records and latest_timestamp:
                self.watermark_manager.set_watermark(source_name, latest_timestamp)
            
            # Update fetch stats
            duration = time.time() - start_time
            self.watermark_manager.update_fetch_stats(source_name, len(records), duration)
            
            # Store raw records in database
            self._store_raw_records(records)
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching from {source_name}: {e}")
            raise
    
    def _extract_record_date(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract the most relevant date from a record for watermark purposes."""
        
        # Try various date fields
        date_fields = [
            "application_date", "issue_date", "issued_date", 
            "inspection_date", "fetched_at"
        ]
        
        for field in date_fields:
            date_str = record.get(field)
            if date_str:
                try:
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    continue
        
        return None
    
    def normalize_and_score_records(self, raw_results: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Normalize records and apply lead scoring."""
        
        logger.info("Normalizing and scoring records")
        
        all_normalized = []
        
        for source_name, records in raw_results.items():
            for record in records:
                # Add source-level scoring and metadata
                normalized = record.copy()
                normalized["normalized_at"] = datetime.utcnow().isoformat()
                normalized["source_priority"] = self._get_source_priority(source_name)
                
                # Apply cross-source lead scoring
                normalized["composite_lead_score"] = self._calculate_composite_score(record, source_name)
                
                all_normalized.append(normalized)
        
        # Sort by composite score
        all_normalized.sort(key=lambda x: x.get("composite_lead_score", 0), reverse=True)
        
        logger.info(f"Normalized {len(all_normalized)} records")
        
        return all_normalized
    
    def deduplicate_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate records across sources using address + name matching."""
        
        logger.info("Deduplicating records")
        
        seen_signatures = set()
        deduplicated = []
        duplicate_count = 0
        
        for record in records:
            signature = self._generate_record_signature(record)
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                deduplicated.append(record)
            else:
                duplicate_count += 1
                # Keep track of cross-source signals
                self._merge_duplicate_signals(deduplicated, record, signature)
        
        logger.info(f"Removed {duplicate_count} duplicates, kept {len(deduplicated)} unique records")
        
        return deduplicated
    
    def _generate_record_signature(self, record: Dict[str, Any]) -> str:
        """Generate unique signature for deduplication."""
        
        # Normalize address and name for matching
        venue_name = self._normalize_text(record.get("venue_name", ""))
        address = self._normalize_text(record.get("address", ""))
        
        # Create signature from key fields
        signature_parts = [venue_name, address]
        signature_string = "|".join(filter(None, signature_parts))
        
        return hashlib.md5(signature_string.encode()).hexdigest()
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for deduplication matching."""
        if not text:
            return ""
        
        # Remove common business suffixes, normalize spacing
        text = text.upper().strip()
        text = text.replace("LLC", "").replace("INC", "").replace("CORP", "")
        text = " ".join(text.split())  # Normalize whitespace
        
        return text
    
    def _merge_duplicate_signals(self, deduplicated: List[Dict[str, Any]], 
                                duplicate: Dict[str, Any], signature: str):
        """Merge signals from duplicate record into existing record."""
        
        # Find existing record with same signature
        for existing in deduplicated:
            if self._generate_record_signature(existing) == signature:
                # Merge source flags and boost composite score
                existing_sources = existing.get("cross_source_signals", [])
                duplicate_source = duplicate.get("source")
                
                if duplicate_source not in existing_sources:
                    existing_sources.append(duplicate_source)
                    existing["cross_source_signals"] = existing_sources
                    
                    # Boost score for cross-source validation
                    existing["composite_lead_score"] = min(
                        existing.get("composite_lead_score", 0) + 0.2, 1.0
                    )
                break
    
    def enrich_with_comptroller(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich top records with Comptroller data."""
        
        if not self.comptroller_client:
            logger.warning("Comptroller client not configured, skipping enrichment")
            return records
        
        logger.info("Enriching records with Comptroller data")
        
        # Only enrich top-scoring records to avoid rate limits
        top_records = [r for r in records if r.get("composite_lead_score", 0) >= 0.5][:50]
        
        for record in top_records:
            try:
                enrichment = self.comptroller_client.enrich_candidate(record)
                record["comptroller_enrichment"] = enrichment
                
                # Boost score if we found legal entity info
                if enrichment.get("legal_entity_info"):
                    record["composite_lead_score"] = min(
                        record.get("composite_lead_score", 0) + 0.1, 1.0
                    )
                
                time.sleep(1.1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Comptroller enrichment failed for record: {e}")
                continue
        
        logger.info(f"Enriched {len(top_records)} records with Comptroller data")
        
        return records
    
    def export_to_csv(self, records: List[Dict[str, Any]], 
                     output_path: str = "sales_ready_leads.csv") -> str:
        """Export sales-ready leads to CSV."""
        
        logger.info(f"Exporting {len(records)} records to CSV: {output_path}")
        
        # Define CSV columns for sales team
        csv_columns = [
            "venue_name", "address", "city", "state", "zip_code", 
            "owner_name", "composite_lead_score", "signal_strength",
            "estimated_min_days", "estimated_max_days", "confidence",
            "source", "cross_source_signals", "application_date", 
            "inspection_date", "issued_date", "permit_number",
            "license_type", "inspection_result", "permit_status"
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore')
            writer.writeheader()
            
            for record in records:
                # Flatten nested data for CSV
                csv_row = record.copy()
                
                # Handle estimated open window
                open_window = record.get("estimated_open_window", {})
                csv_row["estimated_min_days"] = open_window.get("min_days")
                csv_row["estimated_max_days"] = open_window.get("max_days")
                csv_row["confidence"] = open_window.get("confidence")
                
                # Handle cross-source signals
                csv_row["cross_source_signals"] = ",".join(record.get("cross_source_signals", []))
                
                writer.writerow(csv_row)
        
        logger.info(f"CSV export completed: {output_path}")
        return output_path
    
    def _store_raw_records(self, records: List[Dict[str, Any]]):
        """Store raw records in database for audit trail."""
        
        try:
            with db_manager.get_session() as session:
                for record in records:
                    raw_record = RawRecord(
                        source=record.get("source"),
                        source_id=record.get("source_id", ""),
                        url=record.get("source_url"),
                        raw_json=record.get("raw_data", record),
                        fetched_at=datetime.utcnow()
                    )
                    session.add(raw_record)
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Failed to store raw records: {e}")
    
    def _get_source_priority(self, source_name: str) -> float:
        """Get priority weight for different sources."""
        
        priorities = {
            "tabc": 1.0,  # Highest priority - official licensing
            "harris_permits": 0.8,  # High priority - construction signals  
            "houston_health": 0.6,  # Medium priority - operational signals
            "comptroller": 0.4  # Lower priority - entity information
        }
        
        return priorities.get(source_name, 0.5)
    
    def _calculate_composite_score(self, record: Dict[str, Any], source_name: str) -> float:
        """Calculate composite lead score across all signals."""
        
        score = 0.0
        
        # Base signal strength from source
        signal_strength = record.get("signal_strength", 0.0)
        source_priority = self._get_source_priority(source_name)
        
        score += signal_strength * source_priority
        
        # Recency boost
        record_date = self._extract_record_date(record)
        if record_date:
            days_ago = (datetime.utcnow() - record_date).days
            if days_ago <= 7:
                score += 0.3  # Very recent
            elif days_ago <= 30:
                score += 0.2  # Recent
            elif days_ago <= 90:
                score += 0.1  # Somewhat recent
        
        # Business name quality signals
        venue_name = record.get("venue_name", "").lower()
        quality_indicators = ["restaurant", "kitchen", "grill", "cafe", "bistro"]
        if any(indicator in venue_name for indicator in quality_indicators):
            score += 0.1
        
        # Status-based scoring
        status_fields = ["status", "permit_status", "inspection_result"]
        for field in status_fields:
            status_value = record.get(field)
            if status_value is None:
                continue
            status = str(status_value).lower()
            if "approved" in status or "issued" in status or "satisfactory" in status:
                score += 0.1
            elif "pending" in status:
                score += 0.05
        
        return min(score, 1.0)
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get summary of data source pipeline status."""
        
        watermark_status = self.watermark_manager.get_status_summary()
        
        return {
            "data_sources": {
                "total_configured": len(self.clients),
                "active_sources": [name for name in self.clients.keys() if name != "comptroller"],
                "enrichment_sources": ["comptroller"] if self.comptroller_client else []
            },
            "watermarks": watermark_status,
            "last_updated": datetime.utcnow().isoformat()
        }
    
    def close(self):
        """Close all client connections."""
        for client in self.clients.values():
            if hasattr(client, 'close'):
                client.close()
