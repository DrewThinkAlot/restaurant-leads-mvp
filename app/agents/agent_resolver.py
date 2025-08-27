from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, Tuple, cast
import json
import logging

from ..settings import settings
from ..llm import get_llm
from ..tools.geocode_local import geocoder, calculate_business_name_similarity
from ..schemas import MatchEvaluation

logger = logging.getLogger(__name__)


class LLMMatchingTool(BaseTool):
    """Tool for LLM-based entity matching when rules are inconclusive."""
    
    name: str = "llm_match_evaluation"
    description: str = "Use LLM to evaluate if two restaurant records represent the same entity"
    
    def _run(self, record1: str, record2: str) -> str:
        """Execute LLM matching evaluation."""
        try:
            llm = get_llm(temperature=0.1, max_tokens=300)
            
            import os
            prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "resolver.md")
            with open(prompt_path, "r") as f:
                prompt_template = f.read()
            
            prompt = prompt_template.format(record1=record1, record2=record2)
            
            response = llm._call(prompt)
            
            return response
            
        except Exception as e:
            logger.error(f"LLM matching failed: {e}")
            return json.dumps({
                "same_entity": False,
                "confidence_0_1": 0.0,
                "explanation": f"Error: {str(e)}"
            })


class ResolverAgent:
    """Agent for entity resolution and deduplication."""
    
    def __init__(self):
        self.tools = [LLMMatchingTool()]
        
        self.agent = Agent(
            role="Entity Resolver",
            goal="Identify and merge duplicate restaurant records using deterministic rules and LLM assistance",
            backstory="""
            You are an expert data analyst specialized in entity resolution and record linkage.
            You use deterministic matching rules first (exact matches, phone/email/address matching)
            and only consult LLM for ambiguous cases. You excel at identifying when multiple
            records represent the same business entity and merging them appropriately while
            preserving all valuable information from source systems.
            """,
            tools=cast(List[BaseTool], self.tools),
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def resolve_entities(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Resolve entities using hybrid rule-based + LLM approach."""
        
        logger.info(f"Resolving entities for {len(candidates)} candidates")
        
        # First pass: deterministic rules
        resolved_groups = self._apply_deterministic_rules(candidates)
        
        # Second pass: LLM for ambiguous cases
        final_resolved = self._apply_llm_matching(resolved_groups)
        
        # Third pass: merge grouped records
        merged_candidates = self._merge_resolved_groups(final_resolved)
        
        logger.info(f"Entity resolution complete: {len(merged_candidates)} unique entities")
        
        return merged_candidates
    
    def _apply_deterministic_rules(self, candidates: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Apply deterministic matching rules."""
        
        groups = []
        unmatched = candidates.copy()
        
        while unmatched:
            candidate = unmatched.pop(0)
            group = [candidate]
            
            # Find matches using deterministic rules
            remaining = []
            for other in unmatched:
                if self._is_deterministic_match(candidate, other):
                    group.append(other)
                else:
                    remaining.append(other)
            
            unmatched = remaining
            groups.append(group)
        
        return groups
    
    def _is_deterministic_match(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> bool:
        """Apply deterministic matching rules."""
        
        # Rule 1: Exact address match
        addr1 = record1.get("address", "").lower().strip()
        addr2 = record2.get("address", "").lower().strip()
        
        if addr1 and addr2 and addr1 == addr2:
            return True
        
        # Rule 2: Phone number match
        phone1 = record1.get("phone", "")
        phone2 = record2.get("phone", "")
        
        if phone1 and phone2:
            # Extract digits only for comparison
            import re
            digits1 = re.sub(r'\D', '', phone1)
            digits2 = re.sub(r'\D', '', phone2)
            
            if len(digits1) >= 10 and len(digits2) >= 10:
                if digits1[-10:] == digits2[-10:]:  # Compare last 10 digits
                    return True
        
        # Rule 3: Email match
        email1 = record1.get("email", "")
        email2 = record2.get("email", "")
        
        if email1 and email2 and email1.lower() == email2.lower():
            return True
        
        # Rule 4: High address similarity + name similarity
        addr_similarity = geocoder.calculate_address_similarity(
            record1.get("address", ""), record2.get("address", "")
        )
        
        name_similarity = calculate_business_name_similarity(
            record1.get("venue_name", ""), record2.get("venue_name", "")
        )
        
        if addr_similarity > 0.9 and name_similarity > 0.8:
            return True
        
        # Rule 5: Same venue name + very close address (suite difference)
        if name_similarity > 0.95 and addr_similarity > 0.7:
            # Check if addresses differ only by suite/unit
            addr1_base = self._extract_base_address(record1.get("address", ""))
            addr2_base = self._extract_base_address(record2.get("address", ""))
            
            if addr1_base and addr2_base and addr1_base.lower() == addr2_base.lower():
                return True
        
        return False
    
    def _extract_base_address(self, address: str) -> str:
        """Extract base address without suite/unit."""
        
        if not address:
            return ""
        
        import re
        
        # Remove suite/unit patterns
        suite_patterns = [
            r'\s+(suite|ste|unit|apt|apartment|#)\s+[a-z0-9\-]+',
            r'\s+#\s*[a-z0-9\-]+',
            r',\s+(suite|ste|unit|apt|apartment)\s+[a-z0-9\-]+'
        ]
        
        base_addr = address
        for pattern in suite_patterns:
            base_addr = re.sub(pattern, '', base_addr, flags=re.IGNORECASE)
        
        return base_addr.strip()
    
    def _apply_llm_matching(self, groups: List[List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
        """Apply LLM matching for potential cross-group matches."""
        
        # Find groups that might need LLM evaluation
        ambiguous_pairs = self._find_ambiguous_pairs(groups)
        
        final_groups = groups.copy()
        
        for group1_idx, group2_idx, record1, record2 in ambiguous_pairs:
            try:
                match_result = self._evaluate_with_llm(record1, record2)
                
                if match_result["same_entity"] and match_result["confidence_0_1"] > 0.7:
                    # Merge groups
                    group1 = final_groups[group1_idx]
                    group2 = final_groups[group2_idx]
                    
                    # Merge group2 into group1
                    group1.extend(group2)
                    
                    # Remove group2 (set to empty, will filter later)
                    final_groups[group2_idx] = []
                    
                    logger.info(f"LLM merged entities: {record1['venue_name']} & {record2['venue_name']}")
                    
            except Exception as e:
                logger.warning(f"LLM matching failed for pair: {e}")
                continue
        
        # Filter out empty groups
        return [group for group in final_groups if group]
    
    def _find_ambiguous_pairs(self, groups: List[List[Dict[str, Any]]]) -> List[Tuple[int, int, Dict[str, Any], Dict[str, Any]]]:
        """Find pairs that might need LLM evaluation."""
        
        ambiguous_pairs = []
        
        for i, group1 in enumerate(groups):
            for j, group2 in enumerate(groups[i+1:], i+1):
                # Check if groups might be related
                for record1 in group1:
                    for record2 in group2:
                        if self._is_ambiguous_pair(record1, record2):
                            ambiguous_pairs.append((i, j, record1, record2))
                            break
                    else:
                        continue
                    break
        
        return ambiguous_pairs
    
    def _is_ambiguous_pair(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> bool:
        """Check if pair needs LLM evaluation."""
        
        # Address similarity in medium range
        addr_sim = geocoder.calculate_address_similarity(
            record1.get("address", ""), record2.get("address", "")
        )
        
        # Name similarity in medium range
        name_sim = calculate_business_name_similarity(
            record1.get("venue_name", ""), record2.get("venue_name", "")
        )
        
        # Ambiguous if moderate similarity on either dimension
        if 0.4 < addr_sim < 0.9 and name_sim > 0.3:
            return True
        
        if 0.3 < name_sim < 0.8 and addr_sim > 0.4:
            return True
        
        # Same source flags might indicate same business
        flags1 = record1.get("source_flags", {})
        flags2 = record2.get("source_flags", {})
        
        common_flags = 0
        total_flags = 0
        
        for flag_key in ["tabc", "hc_permit", "hc_health"]:
            val1 = flags1.get(flag_key)
            val2 = flags2.get(flag_key)
            
            if val1 and val2:
                total_flags += 1
                if val1 == val2:
                    common_flags += 1
        
        if total_flags > 0 and common_flags / total_flags > 0.5:
            return True
        
        return False
    
    def _evaluate_with_llm(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> Dict[str, Any]:
        """Use LLM to evaluate if records match."""
        
        llm = get_llm(temperature=0.1, max_tokens=200)
        
        # Prepare simplified records for LLM
        simple_record1 = {
            "venue_name": record1.get("venue_name"),
            "legal_name": record1.get("legal_name"),
            "address": record1.get("address"),
            "phone": record1.get("phone"),
            "source_flags": record1.get("source_flags")
        }
        
        simple_record2 = {
            "venue_name": record2.get("venue_name"),
            "legal_name": record2.get("legal_name"),
            "address": record2.get("address"),
            "phone": record2.get("phone"),
            "source_flags": record2.get("source_flags")
        }
        
        prompt = f"""
        Evaluate if these two restaurant records represent the same business entity.
        Return JSON with: same_entity (boolean), confidence_0_1 (float), explanation (string under 30 words).
        
        Record 1: {json.dumps(simple_record1, indent=2)}
        
        Record 2: {json.dumps(simple_record2, indent=2)}
        
        Consider: similar names (including abbreviations), same/similar addresses, matching contact info, business type indicators.
        
        Return only JSON:
        """
        
        response = llm._call(prompt)
        
        try:
            result = json.loads(response)
            
            # Validate result structure
            if not isinstance(result.get("same_entity"), bool):
                result["same_entity"] = False
            
            if not isinstance(result.get("confidence_0_1"), (int, float)):
                result["confidence_0_1"] = 0.0
            
            if not result.get("explanation"):
                result["explanation"] = "No explanation provided"
            
            return result
            
        except json.JSONDecodeError:
            return {
                "same_entity": False,
                "confidence_0_1": 0.0,
                "explanation": "Failed to parse LLM response"
            }
    
    def _merge_resolved_groups(self, groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Merge records within each resolved group."""
        
        merged_candidates = []
        
        for group in groups:
            if not group:
                continue
            
            if len(group) == 1:
                merged_candidates.append(group[0])
            else:
                merged_record = self._merge_group_records(group)
                merged_candidates.append(merged_record)
        
        return merged_candidates
    
    def _merge_group_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple records representing the same entity."""
        
        # Use first record as base
        merged = records[0].copy()
        
        # Merge source flags from all records
        all_source_flags = {}
        for record in records:
            for flag_key, flag_value in record.get("source_flags", {}).items():
                if flag_value:
                    all_source_flags[flag_key] = flag_value
        
        merged["source_flags"] = all_source_flags
        
        # Choose best values for key fields
        for record in records[1:]:
            # Use longest/most complete venue name
            if len(record.get("venue_name", "")) > len(merged.get("venue_name", "")):
                merged["venue_name"] = record["venue_name"]
            
            # Use legal name if not present
            if not merged.get("legal_name") and record.get("legal_name"):
                merged["legal_name"] = record["legal_name"]
            
            # Use phone if not present
            if not merged.get("phone") and record.get("phone"):
                merged["phone"] = record["phone"]
            
            # Use email if not present
            if not merged.get("email") and record.get("email"):
                merged["email"] = record["email"]
            
            # Use most complete address
            if len(record.get("address", "")) > len(merged.get("address", "")):
                merged["address"] = record["address"]
        
        # Add metadata about merge
        merged["_merged_from"] = len(records)
        merged["_source_records"] = [r.get("candidate_id") for r in records]
        
        return merged
