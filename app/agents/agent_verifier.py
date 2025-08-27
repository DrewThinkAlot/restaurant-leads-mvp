from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, cast
import json
import logging
from datetime import datetime, timedelta

from ..settings import settings
from ..llm import get_llm
from ..tools.geocode_local import geocoder

logger = logging.getLogger(__name__)


class QualityCheckTool(BaseTool):
    """Tool for automated quality checks on candidates."""
    
    name: str = "quality_check"
    description: str = "Perform comprehensive quality checks on candidate data"
    
    def _run(self, candidates_json: str) -> str:
        """Execute quality checks."""
        try:
            candidates = json.loads(candidates_json)
            results = []
            
            for candidate in candidates:
                quality_result = self._check_candidate_quality(candidate)
                results.append(quality_result)
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Quality check failed: {e}")
            return json.dumps({"error": str(e)})
    
    def _check_candidate_quality(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Check quality of individual candidate."""
        
        issues = []
        warnings = []
        score = 100.0
        
        # Required field checks
        required_fields = ["venue_name", "address", "city", "zip"]
        for field in required_fields:
            if not candidate.get(field):
                issues.append(f"Missing required field: {field}")
                score -= 20
        
        # Address validation
        address = candidate.get("address", "")
        if address:
            if not geocoder.is_harris_county_address(address):
                warnings.append("Address may not be in Harris County")
                score -= 5
            
            if len(address) < 15:
                warnings.append("Address appears too short/incomplete")
                score -= 10
        
        # ETA validation
        eta_result = candidate.get("eta_result", {})
        if eta_result:
            confidence = eta_result.get("confidence_0_1", 0)
            if confidence < 0.65:
                issues.append(f"ETA confidence too low: {confidence}")
                score -= 30
            
            eta_days = eta_result.get("eta_days", 0)
            if eta_days > 90:
                warnings.append(f"ETA beyond 90 days: {eta_days}")
                score -= 5
        
        # Data consistency checks
        venue_name = candidate.get("venue_name", "")
        legal_name = candidate.get("legal_name", "")
        
        if venue_name and legal_name and venue_name.lower() == legal_name.lower():
            warnings.append("Venue name identical to legal name")
        
        # Source flags validation
        source_flags = candidate.get("source_flags", {})
        active_sources = sum(1 for flag in source_flags.values() if flag)
        
        if active_sources == 0:
            issues.append("No active source flags")
            score -= 15
        
        return {
            "candidate_id": candidate.get("candidate_id"),
            "venue_name": candidate.get("venue_name"),
            "quality_score": max(0, score),
            "issues": issues,
            "warnings": warnings,
            "passed": len(issues) == 0 and score >= 60
        }


class VerifierAgent:
    """Agent for final quality verification and conflict resolution."""
    
    def __init__(self):
        self.tools = [QualityCheckTool()]
        
        self.agent = Agent(
            role="Quality Verifier",
            goal="Ensure data quality and resolve conflicts before lead creation",
            backstory="""
            You are a meticulous quality assurance specialist responsible for the final
            validation of restaurant lead candidates. You have expertise in data validation,
            conflict detection, and business rule enforcement. Your role is critical as you
            are the final gate before leads are sent to the sales team. You ensure only
            high-quality, actionable leads proceed to pitch generation.
            """,
            tools=cast(List[BaseTool], self.tools),
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def verify_candidates(self, candidates_with_eta: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Verify candidates and filter based on quality gates."""
        
        logger.info(f"Verifying {len(candidates_with_eta)} candidates")
        
        verified_candidates = []
        
        for candidate in candidates_with_eta:
            try:
                verification_result = self._verify_single_candidate(candidate)
                
                if verification_result["passed"]:
                    # Add verification metadata
                    candidate["verification"] = verification_result
                    verified_candidates.append(candidate)
                    
                    logger.info(f"Verified candidate: {candidate['venue_name']} "
                              f"(Quality score: {verification_result['quality_score']:.1f})")
                else:
                    logger.warning(f"Candidate failed verification: {candidate['venue_name']} "
                                 f"Issues: {verification_result['issues']}")
                    
            except Exception as e:
                logger.warning(f"Verification failed for {candidate.get('venue_name')}: {e}")
                continue
        
        logger.info(f"Verification complete: {len(verified_candidates)} passed quality gates")
        
        return verified_candidates
    
    def _verify_single_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Verify a single candidate."""
        
        verification = {
            "candidate_id": candidate.get("candidate_id"),
            "venue_name": candidate.get("venue_name"),
            "quality_score": 100.0,
            "issues": [],
            "warnings": [],
            "checks_performed": [],
            "passed": True
        }
        
        # Core data quality checks
        self._check_required_fields(candidate, verification)
        self._check_address_quality(candidate, verification)
        self._check_eta_validity(candidate, verification)
        self._check_source_consistency(candidate, verification)
        self._check_business_logic(candidate, verification)
        
        # Determine if candidate passes
        verification["passed"] = (
            len(verification["issues"]) == 0 and 
            verification["quality_score"] >= 60.0
        )
        
        return verification
    
    def _check_required_fields(self, candidate: Dict[str, Any], verification: Dict[str, Any]):
        """Check required field completeness."""
        
        verification["checks_performed"].append("required_fields")
        
        required_fields = {
            "candidate_id": "Candidate ID",
            "venue_name": "Venue name",
            "address": "Address",
            "city": "City",
            "zip": "ZIP code",
            "eta_result": "ETA prediction"
        }
        
        for field, display_name in required_fields.items():
            if not candidate.get(field):
                verification["issues"].append(f"Missing {display_name}")
                verification["quality_score"] -= 15
        
        # Check field quality
        venue_name = candidate.get("venue_name", "")
        if venue_name and len(venue_name.strip()) < 3:
            verification["issues"].append("Venue name too short")
            verification["quality_score"] -= 10
        
        address = candidate.get("address", "")
        if address and len(address.strip()) < 10:
            verification["issues"].append("Address too short/incomplete")
            verification["quality_score"] -= 10
    
    def _check_address_quality(self, candidate: Dict[str, Any], verification: Dict[str, Any]):
        """Check address quality and Harris County validation."""
        
        verification["checks_performed"].append("address_quality")
        
        address = candidate.get("address", "")
        if not address:
            return
        
        # Harris County validation
        if not geocoder.is_harris_county_address(address):
            verification["warnings"].append("Address may not be in Harris County")
            verification["quality_score"] -= 5
        
        # Address completeness
        addr_components = geocoder.parse_address(address)
        
        if not addr_components.street_number:
            verification["warnings"].append("Missing street number")
            verification["quality_score"] -= 5
        
        if not addr_components.zip_code:
            verification["warnings"].append("Missing ZIP code in address")
            verification["quality_score"] -= 3
    
    def _check_eta_validity(self, candidate: Dict[str, Any], verification: Dict[str, Any]):
        """Check ETA prediction validity."""
        
        verification["checks_performed"].append("eta_validity")
        
        eta_result = candidate.get("eta_result", {})
        if not eta_result:
            verification["issues"].append("Missing ETA prediction")
            verification["quality_score"] -= 25
            return
        
        # Confidence threshold
        confidence = eta_result.get("confidence_0_1", 0)
        if confidence < 0.65:
            verification["issues"].append(f"ETA confidence too low: {confidence:.2f}")
            verification["quality_score"] -= 30
        
        # ETA range validation
        eta_days = eta_result.get("eta_days", 0)
        if eta_days <= 0:
            verification["issues"].append("Invalid ETA days")
            verification["quality_score"] -= 20
        elif eta_days > 90:
            verification["warnings"].append(f"ETA beyond 90 days: {eta_days}")
            verification["quality_score"] -= 5
        
        # Date validation
        try:
            eta_start = eta_result.get("eta_start")
            eta_end = eta_result.get("eta_end")
            
            if eta_start:
                start_date = datetime.fromisoformat(eta_start.replace('Z', '+00:00'))
                today = datetime.now()
                
                if start_date < today:
                    verification["issues"].append("ETA start date is in the past")
                    verification["quality_score"] -= 15
                
                if start_date > today + timedelta(days=120):
                    verification["warnings"].append("ETA start date far in future")
                    verification["quality_score"] -= 5
            
        except Exception as e:
            verification["warnings"].append("Invalid ETA date format")
            verification["quality_score"] -= 5
    
    def _check_source_consistency(self, candidate: Dict[str, Any], verification: Dict[str, Any]):
        """Check consistency across data sources."""
        
        verification["checks_performed"].append("source_consistency")
        
        source_flags = candidate.get("source_flags", {})
        
        # Check for at least one active source
        active_sources = [flag for flag in source_flags.values() if flag]
        if not active_sources:
            verification["issues"].append("No active data sources")
            verification["quality_score"] -= 20
        
        # Flag conflicts (basic check)
        tabc_status = source_flags.get("tabc")
        hc_permit = source_flags.get("hc_permit")
        
        if tabc_status and "denied" in tabc_status.lower():
            verification["warnings"].append("TABC status indicates denial")
            verification["quality_score"] -= 10
        
        if hc_permit and "not_found" in hc_permit.lower():
            verification["warnings"].append("Harris County permit not found")
            verification["quality_score"] -= 5
    
    def _check_business_logic(self, candidate: Dict[str, Any], verification: Dict[str, Any]):
        """Apply business logic checks."""
        
        verification["checks_performed"].append("business_logic")
        
        # Check for obvious test/dummy data
        venue_name = candidate.get("venue_name", "").lower()
        test_indicators = ["test", "dummy", "sample", "example", "xxx"]
        
        if any(indicator in venue_name for indicator in test_indicators):
            verification["issues"].append("Appears to be test/dummy data")
            verification["quality_score"] -= 50
        
        # Check for reasonable business name
        if venue_name and len(venue_name.strip()) > 100:
            verification["warnings"].append("Venue name unusually long")
            verification["quality_score"] -= 3
        
        # Check phone format if present
        phone = candidate.get("phone", "")
        if phone:
            import re
            # Basic phone validation
            digits = re.sub(r'\D', '', phone)
            if len(digits) not in [10, 11]:
                verification["warnings"].append("Invalid phone number format")
                verification["quality_score"] -= 5
    
    def get_verification_summary(self, verified_candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get summary of verification results."""
        
        if not verified_candidates:
            return {
                "total_verified": 0,
                "avg_quality_score": 0,
                "issues_summary": {},
                "warnings_summary": {}
            }
        
        quality_scores = [c.get("verification", {}).get("quality_score", 0) for c in verified_candidates]
        
        # Aggregate issues and warnings
        all_issues = []
        all_warnings = []
        
        for candidate in verified_candidates:
            verification = candidate.get("verification", {})
            all_issues.extend(verification.get("issues", []))
            all_warnings.extend(verification.get("warnings", []))
        
        # Count occurrences
        issues_count = {}
        for issue in all_issues:
            issues_count[issue] = issues_count.get(issue, 0) + 1
        
        warnings_count = {}
        for warning in all_warnings:
            warnings_count[warning] = warnings_count.get(warning, 0) + 1
        
        return {
            "total_verified": len(verified_candidates),
            "avg_quality_score": sum(quality_scores) / len(quality_scores) if quality_scores else 0,
            "quality_score_distribution": {
                "excellent (90+)": len([s for s in quality_scores if s >= 90]),
                "good (75-89)": len([s for s in quality_scores if 75 <= s < 90]),
                "acceptable (60-74)": len([s for s in quality_scores if 60 <= s < 75]),
                "poor (<60)": len([s for s in quality_scores if s < 60])
            },
            "issues_summary": issues_count,
            "warnings_summary": warnings_count
        }
