from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, Optional
import json
import asyncio
import logging
from datetime import datetime

from ..tools.tabc_open_data import get_pending_restaurant_licenses
from ..tools.hc_permits import search_restaurant_permits  
from ..tools.hc_food_permits import search_food_permits_by_candidate
from ..tools.socrata_mcp import discover_and_query_datasets
from ..db import db_manager
from ..models import RawRecord
from ..llm import get_llm

logger = logging.getLogger(__name__)


class TABCTool(BaseTool):
    """Tool for TABC open data queries."""
    
    name: str = "tabc_query"
    description: str = "Query TABC open data for pending restaurant licenses in Harris County"
    
    def _run(self, county: str = "Harris", days_back: int = 90) -> str:
        """Execute TABC query."""
        try:
            records = get_pending_restaurant_licenses(county, days_back)
            return json.dumps(records, indent=2)
        except Exception as e:
            logger.error(f"TABC query failed: {e}")
            return json.dumps({"error": str(e), "records": []})


class HarrisCountyPermitsTool(BaseTool):
    """Tool for Harris County permits scraping."""
    
    name: str = "hc_permits_search"
    description: str = "Search Harris County ePermits for restaurant-related building permits"
    
    def _run(self, days_back: int = 90) -> str:
        """Execute Harris County permits search."""
        try:
            permits = search_restaurant_permits(days_back)
            return json.dumps(permits, indent=2)
        except Exception as e:
            logger.error(f"HC permits search failed: {e}")
            return json.dumps({"error": str(e), "permits": []})


class HCPHFoodTool(BaseTool):
    """Tool for HCPH food permits."""
    
    name: str = "hcph_food_search"  
    description: str = "Search Harris County Public Health for food service permits and plan reviews"
    
    def _run(self, search_terms: str = "restaurant,food service") -> str:
        """Execute HCPH food permits search."""
        try:
            # This is a simplified implementation - would need actual search logic
            # For now, return placeholder structure
            results = {
                "search_terms": search_terms.split(","),
                "results": [],
                "note": "HCPH search requires specific candidate data"
            }
            return json.dumps(results, indent=2)
        except Exception as e:
            logger.error(f"HCPH search failed: {e}")
            return json.dumps({"error": str(e), "results": []})


class SocrataMCPTool(BaseTool):
    """Tool for Socrata MCP discovery and querying."""
    
    name: str = "socrata_discover"
    description: str = "Discover and query Socrata datasets for restaurant licensing data"
    
    def _run(self, county: str = "Harris") -> str:
        """Execute Socrata discovery and querying."""
        try:
            # Run async function in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                results = loop.run_until_complete(discover_and_query_datasets(county))
                return json.dumps(results, indent=2)
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Socrata MCP query failed: {e}")
            return json.dumps({"error": str(e), "datasets": []})


class SignalScoutAgent:
    """Agent for discovering restaurant candidates from multiple sources."""
    
    def __init__(self):
        self.tools = [
            TABCTool(),
            HarrisCountyPermitsTool(),
            HCPHFoodTool(),
            SocrataMCPTool()
        ]
        
        self.agent = Agent(
            role="Signal Scout",
            goal="Discover restaurant candidates from all available data sources with high recall",
            backstory="""
            You are an expert data scout specializing in finding restaurant opening signals.
            You systematically search multiple government databases and permit systems to identify
            restaurants that are likely to open soon. You focus on Harris County, TX and look for
            signals like pending TABC licenses, building permits, health department plan reviews,
            and other regulatory milestones that indicate upcoming restaurant openings.
            """,
            tools=self.tools,
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def execute_discovery(self, max_candidates: int = 100) -> List[Dict[str, Any]]:
        """Execute discovery across all sources."""
        
        all_candidates = []
        
        # TABC pending licenses
        logger.info("Searching TABC data...")
        try:
            tabc_records = get_pending_restaurant_licenses("Harris", 90)
            for record in tabc_records:
                candidate = self._convert_tabc_to_candidate(record)
                if candidate:
                    all_candidates.append(candidate)
                    self._store_raw_record("tabc", record)
        except Exception as e:
            logger.error(f"TABC search failed: {e}")
        
        # Harris County permits
        logger.info("Searching Harris County permits...")
        try:
            hc_permits = search_restaurant_permits(90)
            for permit in hc_permits:
                candidate = self._convert_permit_to_candidate(permit)
                if candidate:
                    all_candidates.append(candidate)
                    self._store_raw_record("hc_permits", permit)
        except Exception as e:
            logger.error(f"HC permits search failed: {e}")
        
        # Socrata MCP discovery (optional)
        logger.info("Trying Socrata MCP discovery...")
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                socrata_results = loop.run_until_complete(discover_and_query_datasets("Harris"))
                for result in socrata_results:
                    candidate = self._convert_socrata_to_candidate(result)
                    if candidate:
                        all_candidates.append(candidate)
                        self._store_raw_record("socrata_mcp", result)
            finally:
                loop.close()
        except Exception as e:
            logger.warning(f"Socrata MCP discovery failed: {e}")
        
        # Deduplicate and limit
        unique_candidates = self._deduplicate_candidates(all_candidates)
        
        return unique_candidates[:max_candidates]
    
    def _convert_tabc_to_candidate(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert TABC record to candidate format."""
        
        try:
            return {
                "source": "tabc",
                "source_id": record.get("source_id"),
                "venue_name": record.get("trade_name") or record.get("business_name"),
                "legal_name": record.get("business_name"),
                "address": record.get("address"),
                "city": record.get("city"),
                "state": record.get("state", "TX"),
                "zip_code": record.get("zip_code"),
                "county": record.get("county", "Harris"),
                "phone": record.get("phone"),
                "source_flags": {
                    "tabc": record.get("status", "").lower().replace(" ", "_"),
                    "hc_permit": None,
                    "hc_health": None,
                    "houston_permit": None
                },
                "signals": {
                    "tabc_status": record.get("status"),
                    "tabc_dates": {
                        "application_date": record.get("application_date"),
                        "status_date": record.get("status_date")
                    },
                    "permit_types": [record.get("license_type")] if record.get("license_type") else []
                }
            }
        except Exception as e:
            logger.warning(f"Failed to convert TABC record: {e}")
            return None
    
    def _convert_permit_to_candidate(self, permit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert permit record to candidate format."""
        
        try:
            return {
                "source": "hc_permits", 
                "source_id": permit.get("permit_id"),
                "venue_name": permit.get("applicant") or "Unknown",
                "legal_name": permit.get("applicant"),
                "address": permit.get("address"),
                "city": self._extract_city_from_address(permit.get("address", "")),
                "state": "TX",
                "zip_code": self._extract_zip_from_address(permit.get("address", "")),
                "county": "Harris",
                "source_flags": {
                    "tabc": None,
                    "hc_permit": "found",
                    "hc_health": None,
                    "houston_permit": None
                },
                "signals": {
                    "permit_types": [permit.get("permit_type")],
                    "milestone_dates": {
                        "application_date": permit.get("application_date"),
                        "issued_date": permit.get("issued_date")
                    }
                }
            }
        except Exception as e:
            logger.warning(f"Failed to convert permit record: {e}")
            return None
    
    def _convert_socrata_to_candidate(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Socrata record to candidate format."""
        
        try:
            return {
                "source": "socrata",
                "source_id": record.get("id") or record.get("license_number"),
                "venue_name": record.get("business_name") or record.get("trade_name"),
                "legal_name": record.get("legal_name") or record.get("business_name"),
                "address": record.get("address") or record.get("business_address"),
                "city": record.get("city"),
                "state": record.get("state", "TX"),
                "zip_code": record.get("zip_code") or record.get("zip"),
                "county": record.get("county", "Harris"),
                "source_flags": {
                    "tabc": record.get("status", "").lower().replace(" ", "_") if "tabc" in record.get("dataset_source", {}).get("name", "").lower() else None,
                    "hc_permit": None,
                    "hc_health": None,
                    "houston_permit": None
                },
                "signals": {
                    "milestone_dates": {
                        "application_date": record.get("application_date"),
                        "status_date": record.get("status_date")
                    }
                }
            }
        except Exception as e:
            logger.warning(f"Failed to convert Socrata record: {e}")
            return None
    
    def _extract_city_from_address(self, address: str) -> str:
        """Extract city from full address."""
        if not address:
            return "Houston"  # Default
        
        # Simple heuristic - city is often after last comma
        parts = address.split(",")
        if len(parts) >= 2:
            return parts[-2].strip()
        
        return "Houston"
    
    def _extract_zip_from_address(self, address: str) -> Optional[str]:
        """Extract ZIP code from address."""
        import re
        if not address:
            return None
        
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
        return zip_match.group(1) if zip_match else None
    
    def _deduplicate_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove obvious duplicates."""
        
        unique_candidates = []
        seen_addresses = set()
        seen_names = set()
        
        for candidate in candidates:
            address = candidate.get("address", "").lower().strip()
            name = candidate.get("venue_name", "").lower().strip()
            
            # Simple deduplication by address or name
            key = f"{address}|{name}"
            
            if key not in seen_addresses and address and name:
                unique_candidates.append(candidate)
                seen_addresses.add(key)
                seen_names.add(name)
        
        return unique_candidates
    
    def _store_raw_record(self, source: str, data: Dict[str, Any]):
        """Store raw record in database."""
        
        try:
            with db_manager.get_session() as session:
                raw_record = RawRecord(
                    source=source,
                    source_id=str(data.get("source_id") or data.get("id", "")),
                    raw_json=data,
                    fetched_at=datetime.now()
                )
                session.add(raw_record)
                session.commit()
                
        except Exception as e:
            logger.warning(f"Failed to store raw record: {e}")
