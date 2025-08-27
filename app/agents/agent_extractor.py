from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, cast, Optional
import json
import logging
from datetime import datetime

from ..settings import settings
from ..llm import get_llm
from ..schemas import RestaurantCandidate
from ..tools.geocode_local import geocoder

logger = logging.getLogger(__name__)


class LLMExtractionTool(BaseTool):
    """Tool for LLM-based data extraction and normalization."""
    
    name: str = "llm_extract"
    description: str = "Extract and normalize restaurant data using LLM with strict JSON schema"
    
    def _run(self, raw_data: str, schema_description: str) -> str:
        """Execute LLM extraction."""
        try:
            llm = get_llm(temperature=0.1, max_tokens=500)
            
            prompt = f"""
            Extract restaurant information from the following raw data and return valid JSON only matching RestaurantCandidate schema.
            If unsure about any field, set nullable fields to null. Do not invent phone numbers, emails, or other contact info.
            
            Schema requirements:
            - candidate_id: generate a UUID
            - venue_name: string (required)
            - legal_name: string or null
            - address: string (required)
            - suite: string or null
            - city: string (required)
            - state: "TX"
            - zip: string (required)
            - county: "Harris"
            - phone: string or null (only if explicitly provided)
            - email: string or null (only if explicitly provided)
            - source_flags: object with tabc, hc_permit, hc_health, houston_permit fields
            
            Raw data:
            {raw_data}
            
            Return only valid JSON:
            """
            
            response = llm._call(prompt)
            
            return response
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return json.dumps({"error": str(e)})


class ExtractorAgent:
    """Agent for normalizing raw candidate data into structured format."""
    
    def __init__(self):
        self.tools = [LLMExtractionTool()]
        
        self.agent = Agent(
            role="Data Extractor",
            goal="Normalize messy raw data into clean, structured RestaurantCandidate JSON objects",
            backstory="""
            You are a meticulous data processing specialist who excels at extracting structured
            information from various raw data sources. You have deep expertise in parsing
            government records, permits, and business filings. You ensure data quality by
            following strict schemas and never inventing information that isn't explicitly
            provided in the source data.
            """,
            tools=cast(List[BaseTool], self.tools),
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def normalize_candidates(self, raw_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize raw candidates to RestaurantCandidate schema."""
        
        normalized = []
        
        for raw_candidate in raw_candidates:
            try:
                candidate = self._normalize_single_candidate(raw_candidate)
                if candidate and self._validate_candidate(candidate):
                    normalized.append(candidate)
                    
            except Exception as e:
                logger.warning(f"Failed to normalize candidate: {e}")
                continue
        
        # Deduplicate based on normalized address and name
        return self._deduplicate_normalized(normalized)
    
    def _normalize_single_candidate(self, raw_candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single raw candidate."""
        
        import uuid
        
        # Extract basic fields
        venue_name = self._clean_name(raw_candidate.get("venue_name") or "")
        legal_name = self._clean_name(raw_candidate.get("legal_name"))
        address = self._clean_address(raw_candidate.get("address") or "")
        
        if not venue_name or not address:
            return None
        
        # Parse address using local geocoder
        addr_components = geocoder.parse_address(address)
        
        # Build normalized candidate
        candidate = {
            "candidate_id": str(uuid.uuid4()),
            "venue_name": venue_name,
            "legal_name": legal_name if legal_name != venue_name else None,
            "address": addr_components.normalized or address,
            "suite": addr_components.suite,
            "city": addr_components.city or raw_candidate.get("city") or "Houston",
            "state": "TX",
            "zip": addr_components.zip_code or raw_candidate.get("zip_code") or "",
            "county": "Harris",
            "phone": self._clean_phone(raw_candidate.get("phone")),
            "email": self._clean_email(raw_candidate.get("email")),
            "source_flags": self._normalize_source_flags(raw_candidate.get("source_flags", {}))
        }
        
        return candidate
    
    def _clean_name(self, name: Optional[str]) -> Optional[str]:
        """Clean business name."""
        if not name:
            return None
        
        name = name.strip()
        if len(name) < 2:
            return None
        
        # Remove common data artifacts
        name = name.replace("  ", " ")
        name = name.replace("\n", " ")
        name = name.replace("\t", " ")
        
        return name.title() if name else None
    
    def _clean_address(self, address: Optional[str]) -> Optional[str]:
        """Clean address string."""
        if not address:
            return None
        
        address = address.strip()
        if len(address) < 10:  # Too short to be valid address
            return None
        
        # Remove data artifacts
        address = address.replace("\n", " ")
        address = address.replace("  ", " ")
        
        return address.title() if address else None
    
    def _clean_phone(self, phone: Optional[str]) -> Optional[str]:
        """Clean phone number."""
        if not phone:
            return None
        
        import re
        
        # Extract digits only
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        
        # Return original if can't normalize
        return phone.strip() if phone.strip() else None
    
    def _clean_email(self, email: Optional[str]) -> Optional[str]:
        """Clean email address."""
        if not email:
            return None
        
        email = email.strip().lower()
        
        # Basic email validation
        if '@' in email and '.' in email:
            return email
        
        return None
    
    def _normalize_source_flags(self, source_flags: Dict[str, Any]) -> Dict[str, str]:
        """Normalize source flags."""
        
        normalized = {
            "tabc": None,
            "hc_permit": None,
            "hc_health": None,
            "houston_permit": None
        }
        
        for key, value in source_flags.items():
            if key in normalized and value:
                normalized[key] = str(value).lower()
        
        return normalized
    
    def _validate_candidate(self, candidate: Dict[str, Any]) -> bool:
        """Validate candidate against schema requirements."""
        
        required_fields = ["candidate_id", "venue_name", "address", "city", "state", "zip", "county"]
        
        for field in required_fields:
            if not candidate.get(field):
                logger.warning(f"Candidate missing required field: {field}")
                return False
        
        # Validate Harris County
        if not geocoder.is_harris_county_address(candidate["address"]):
            logger.info(f"Candidate not in Harris County: {candidate['venue_name']}")
            return False
        
        return True
    
    def _deduplicate_normalized(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicates from normalized candidates."""
        
        from ..tools.geocode_local import calculate_business_name_similarity
        
        unique_candidates = []
        
        for candidate in candidates:
            is_duplicate = False
            
            for existing in unique_candidates:
                # Check address similarity
                addr_sim = geocoder.calculate_address_similarity(
                    candidate["address"], existing["address"]
                )
                
                # Check name similarity
                name_sim = calculate_business_name_similarity(
                    candidate["venue_name"], existing["venue_name"]
                )
                
                # Consider duplicate if high similarity on both
                if addr_sim > 0.8 and name_sim > 0.7:
                    is_duplicate = True
                    # Merge source flags
                    for flag_key, flag_value in candidate["source_flags"].items():
                        if flag_value and not existing["source_flags"].get(flag_key):
                            existing["source_flags"][flag_key] = flag_value
                    break
            
            if not is_duplicate:
                unique_candidates.append(candidate)
        
        return unique_candidates
    
    def extract_with_llm_fallback(self, problematic_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Use LLM for problematic data extraction."""
        
        try:
            llm = get_llm(temperature=0.1, max_tokens=800)
            
            with open("/Users/admin/CascadeProjects/restaurant-leads-mvp/app/prompts/extractor.md", "r") as f:
                prompt_template = f.read()
            
            prompt = prompt_template.format(raw_data=json.dumps(problematic_data, indent=2))
            
            result = llm._call(prompt)
            
            # Parse JSON response
            try:
                extracted = json.loads(result)
                return extracted
            except json.JSONDecodeError:
                # Try to extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', result, re.DOTALL)
                if json_match:
                    extracted = json.loads(json_match.group(0))
                    return extracted
                
            return None
            
        except Exception as e:
            logger.error(f"LLM extraction fallback failed: {e}")
            return None
