from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, cast
import json
import logging
from datetime import datetime, timedelta

from ..settings import settings
from ..llm import get_llm
from ..rules import ETARulesEngine
from ..schemas import ETAResult

logger = logging.getLogger(__name__)


class LLMETAAdjustmentTool(BaseTool):
    """Tool for LLM-based ETA adjustment and rationale."""
    
    name: str = "llm_eta_adjust"
    description: str = "Use LLM to adjust ETA predictions based on milestone text and context"
    
    def _run(self, rule_result: str, milestone_text: str) -> str:
        """Execute LLM ETA adjustment."""
        try:
            llm = get_llm(temperature=0.2, max_tokens=400)
            
            import os
            prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "eta.md")
            with open(prompt_path, "r") as f:
                prompt_template = f.read()
            
            prompt = prompt_template.format(
                rule_result=rule_result,
                milestone_text=milestone_text
            )
            
            response = llm._call(prompt)
            
            return response
            
        except Exception as e:
            logger.error(f"LLM ETA adjustment failed: {e}")
            return rule_result  # Return original if LLM fails


class LLMBatchETAAdjustmentTool(BaseTool):
    """Tool for batch LLM-based ETA adjustment."""
    
    name: str = "llm_batch_eta_adjust"
    description: str = "Use LLM to adjust a batch of ETA predictions"

    def _run(self, batch_inputs: str) -> str:
        """Execute batch LLM ETA adjustment."""
        try:
            llm = get_llm(temperature=0.2, max_tokens=4096)  # Increased tokens for batch
            
            import os
            prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "eta_batch.md")
            with open(prompt_path, "r") as f:
                prompt_template = f.read()
            
            prompt = prompt_template.format(batch_inputs=batch_inputs)
            
            response = llm._call(prompt)
            
            return response
            
        except Exception as e:
            logger.error(f"LLM batch ETA adjustment failed: {e}")
            # On failure, return an empty JSON array string
            return json.dumps([])


class ETAAgent:
    """Agent for estimating restaurant opening dates using rules + LLM adjustment."""
    
    def __init__(self):
        self.rules_engine = ETARulesEngine()
        self.tools = [LLMETAAdjustmentTool(), LLMBatchETAAdjustmentTool()]
        
        self.agent = Agent(
            role="ETA Estimator",
            goal="Predict restaurant opening dates using deterministic rules enhanced by LLM insights",
            backstory="""
            You are a business intelligence analyst specializing in predicting restaurant opening timelines.
            You have deep expertise in regulatory processes, permitting workflows, and construction schedules.
            You apply proven deterministic rules first, then use contextual AI to refine predictions based on
            specific milestone language, seasonal factors, and market conditions. Your predictions directly
            impact sales team prioritization and resource allocation.
            """,
            tools=cast(List[BaseTool], self.tools),
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def estimate_opening_dates(self, resolved_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Estimate opening dates for qualified candidates."""
        
        logger.info(f"Estimating ETAs for {len(resolved_candidates)} candidates")
        
        # Batch process all candidates
        final_eta_results = self._estimate_batch_candidates(resolved_candidates)
        
        qualified_candidates = []
        for candidate, eta_result in zip(resolved_candidates, final_eta_results):
            if eta_result and self.rules_engine.should_create_lead(eta_result):
                candidate_with_eta = candidate.copy()
                candidate_with_eta["eta_result"] = {
                    "eta_start": eta_result.eta_start.isoformat(),
                    "eta_end": eta_result.eta_end.isoformat(),
                    "eta_days": eta_result.eta_days,
                    "confidence_0_1": eta_result.confidence_0_1,
                    "rationale_text": eta_result.rule_name,
                    "signals_considered": eta_result.signals_used
                }
                qualified_candidates.append(candidate_with_eta)
                
                logger.info(f"Qualified candidate: {candidate['venue_name']} "
                          f"(ETA: {eta_result.eta_days} days, "
                          f"Confidence: {eta_result.confidence_0_1:.2f})")
        
        logger.info(f"ETA estimation complete: {len(qualified_candidates)} qualified")
        
        return qualified_candidates

    def _estimate_batch_candidates(self, candidates: List[Dict[str, Any]]) -> List[Any]:
        """Estimate ETA for a batch of candidates."""
        
        # 1. Apply deterministic rules to all candidates
        rule_results = []
        for candidate in candidates:
            signals = self._extract_signals_data(candidate)
            rule_result = self.rules_engine.evaluate_candidate(candidate, signals)
            rule_results.append(rule_result)

        # 2. Identify candidates needing LLM adjustment
        llm_batch_inputs = []
        indices_for_llm = []
        for i, (candidate, rule_result) in enumerate(zip(candidates, rule_results)):
            if rule_result:
                signals = self._extract_signals_data(candidate)
                milestone_text = self._extract_milestone_text(candidate, signals)
                if milestone_text and len(milestone_text.strip()) > 20:
                    llm_batch_inputs.append({
                        "candidate_id": i,
                        "rule_result": rule_result.to_dict(),
                        "milestone_text": milestone_text
                    })
                    indices_for_llm.append(i)

        # 3. Apply LLM adjustment in a single batch call
        final_results = list(rule_results) # Start with rule results
        if llm_batch_inputs:
            adjusted_results = self._apply_batch_llm_adjustment(llm_batch_inputs, rule_results)
            for i, adjusted_result in adjusted_results.items():
                final_results[i] = adjusted_result

        return final_results
    
    def _estimate_single_candidate(self, candidate: Dict[str, Any]) -> Any:
        """Estimate ETA for a single candidate."""
        
        # Extract signals data
        signals = self._extract_signals_data(candidate)
        
        # Apply deterministic rules first
        rule_result = self.rules_engine.evaluate_candidate(candidate, signals)
        
        if not rule_result:
            return None
        
        # Get milestone text for LLM context
        milestone_text = self._extract_milestone_text(candidate, signals)
        
        # Apply LLM adjustment if milestone text provides additional context
        if milestone_text and len(milestone_text.strip()) > 20:
            adjusted_result = self._apply_llm_adjustment(rule_result, milestone_text)
            if adjusted_result:
                return adjusted_result
        
        return rule_result
    
    def _extract_signals_data(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Extract signals data from candidate."""
        
        signals = candidate.get("signals", {})
        source_flags = candidate.get("source_flags", {})
        
        # Build comprehensive signals dict
        signals_data = {
            "tabc_status": signals.get("tabc_status"),
            "tabc_dates": signals.get("tabc_dates", {}),
            "health_status": self._infer_health_status(source_flags, signals),
            "permit_types": signals.get("permit_types", []),
            "milestone_dates": signals.get("milestone_dates", {})
        }
        
        return signals_data
    
    def _infer_health_status(self, source_flags: Dict[str, Any], signals: Dict[str, Any]) -> str:
        """Infer health department status from available data."""
        
        hc_health_flag = source_flags.get("hc_health")
        
        if hc_health_flag:
            return hc_health_flag
        
        # Look for health-related permit types
        permit_types = signals.get("permit_types", [])
        
        for permit_type in permit_types:
            permit_lower = permit_type.lower()
            if "plan review" in permit_lower:
                if "approved" in permit_lower:
                    return "plan_review_approved"
                else:
                    return "plan_review_received"
            elif "food service" in permit_lower:
                return "food_service_permit"
        
        return "unknown"
    
    def _extract_milestone_text(self, candidate: Dict[str, Any], signals: Dict[str, Any]) -> str:
        """Extract milestone text for LLM context."""
        
        text_parts = []
        
        # TABC status and dates
        tabc_status = signals.get("tabc_status")
        if tabc_status:
            text_parts.append(f"TABC Status: {tabc_status}")
        
        tabc_dates = signals.get("tabc_dates", {})
        for date_key, date_value in tabc_dates.items():
            if date_value:
                text_parts.append(f"TABC {date_key}: {date_value}")
        
        # Permit types
        permit_types = signals.get("permit_types", [])
        for permit_type in permit_types:
            text_parts.append(f"Permit: {permit_type}")
        
        # Milestone dates
        milestone_dates = signals.get("milestone_dates", {})
        for milestone_key, milestone_date in milestone_dates.items():
            if milestone_date:
                text_parts.append(f"{milestone_key}: {milestone_date}")
        
        return "\n".join(text_parts)
    
    def _apply_batch_llm_adjustment(self, batch_inputs: List[Dict], original_rule_results: List) -> Dict[int, Any]:
        """Apply LLM adjustment to a batch of candidates."""
        try:
            batch_tool = LLMBatchETAAdjustmentTool()
            response_str = batch_tool._run(json.dumps(batch_inputs))
            
            adjusted_json = json.loads(response_str)
            
            adjusted_results = {}
            for item in adjusted_json:
                candidate_id = item.get("candidate_id")
                if candidate_id is None:
                    continue
                
                original_result = original_rule_results[candidate_id]
                if not original_result:
                    continue

                # Validate and create adjusted ETARuleResult
                adjusted_result = self._create_validated_adjusted_result(item, original_result)
                adjusted_results[candidate_id] = adjusted_result
                
                logger.info(f"LLM batch adjusted ETA for candidate {candidate_id}: "
                           f"{original_result.eta_days} -> {adjusted_result.eta_days} days, "
                           f"confidence: {original_result.confidence_0_1:.2f} -> {adjusted_result.confidence_0_1:.2f}")

            return adjusted_results

        except Exception as e:
            logger.warning(f"LLM batch adjustment failed, using rule results: {e}")
            return {}

    def _apply_llm_adjustment(self, rule_result, milestone_text: str):
        """Apply LLM adjustment to rule-based ETA."""
        
        try:
            llm = get_llm(temperature=0.2, max_tokens=400)
            
            rule_summary = {
                "eta_start": rule_result.eta_start.isoformat(),
                "eta_end": rule_result.eta_end.isoformat(),
                "eta_days": rule_result.eta_days,
                "confidence_0_1": rule_result.confidence_0_1,
                "rule_name": rule_result.rule_name
            }
            
            prompt = f"""
            Based on the rule-based ETA prediction and additional milestone information, 
            you may adjust the ETA by ±15 days and confidence by ±0.1.
            
            Current prediction: {json.dumps(rule_summary, indent=2)}
            
            Additional milestone information:
            {milestone_text}
            
            Consider factors like:
            - Recent milestone completions suggesting faster progress
            - Delays or complications mentioned in records
            - Seasonal construction patterns
            - Permit approval timelines
            
            Return JSON with adjusted ETAResult:
            {{
                "eta_start": "YYYY-MM-DD",
                "eta_end": "YYYY-MM-DD", 
                "eta_days": 45,
                "confidence_0_1": 0.75,
                "signals_considered": ["list", "of", "signals"],
                "rationale_text": "Brief explanation for adjustment"
            }}
            """
            
            response = llm._call(prompt)
            
            result_json = json.loads(response)
            
            # Validate adjustments are within allowed range
            original_days = rule_result.eta_days
            adjusted_days = result_json.get("eta_days", original_days)
            
            if abs(adjusted_days - original_days) > 15:
                adjusted_days = original_days + (15 if adjusted_days > original_days else -15)
            
            original_confidence = rule_result.confidence_0_1
            adjusted_confidence = result_json.get("confidence_0_1", original_confidence)
            
            if abs(adjusted_confidence - original_confidence) > 0.1:
                adjusted_confidence = original_confidence + (0.1 if adjusted_confidence > original_confidence else -0.1)
            
            # Ensure confidence is within bounds
            adjusted_confidence = max(0.0, min(1.0, adjusted_confidence))
            
            # Create adjusted result
            from ..rules import ETARuleResult
            
            adjusted_eta_start = rule_result.eta_start + timedelta(days=adjusted_days - rule_result.eta_days)
            adjusted_eta_end = rule_result.eta_end + timedelta(days=adjusted_days - rule_result.eta_days)
            
            adjusted_result = ETARuleResult(
                eta_start=adjusted_eta_start,
                eta_end=adjusted_eta_end,
                eta_days=int(adjusted_days),
                confidence_0_1=adjusted_confidence,
                rule_name=f"{rule_result.rule_name}_llm_adjusted",
                signals_used=result_json.get("signals_considered", rule_result.signals_used)
            )
            
            # Add rationale
            adjusted_result.rationale_text = result_json.get("rationale_text", "LLM adjustment applied")
            
            logger.info(f"LLM adjusted ETA: {rule_result.eta_days} -> {adjusted_days} days, "
                       f"confidence: {rule_result.confidence_0_1:.2f} -> {adjusted_confidence:.2f}")
            
            return adjusted_result
            
        except Exception as e:
            logger.warning(f"LLM adjustment failed, using rule result: {e}")
            return rule_result

    def _create_validated_adjusted_result(self, llm_output: Dict, original_result: Any) -> Any:
        """Validate LLM output and create an ETARuleResult."""
        
        # Validate adjustments are within allowed range
        original_days = original_result.eta_days
        adjusted_days = llm_output.get("eta_days", original_days)
        if abs(adjusted_days - original_days) > 15:
            adjusted_days = original_days + (15 if adjusted_days > original_days else -15)

        original_confidence = original_result.confidence_0_1
        adjusted_confidence = llm_output.get("confidence_0_1", original_confidence)
        if abs(adjusted_confidence - original_confidence) > 0.1:
            adjusted_confidence = original_confidence + (0.1 if adjusted_confidence > original_confidence else -0.1)
        
        # Ensure confidence is within bounds
        adjusted_confidence = max(0.0, min(1.0, adjusted_confidence))

        # Create adjusted result
        from ..rules import ETARuleResult
        
        day_delta = adjusted_days - original_result.eta_days
        adjusted_eta_start = original_result.eta_start + timedelta(days=day_delta)
        adjusted_eta_end = original_result.eta_end + timedelta(days=day_delta)
        
        adjusted_result = ETARuleResult(
            eta_start=adjusted_eta_start,
            eta_end=adjusted_eta_end,
            eta_days=int(adjusted_days),
            confidence_0_1=adjusted_confidence,
            rule_name=f"{original_result.rule_name}_llm_batch_adjusted",
            signals_used=llm_output.get("signals_considered", original_result.signals_used)
        )
        adjusted_result.rationale_text = llm_output.get("rationale_text", "LLM batch adjustment applied")
        
        return adjusted_result
    
    def get_eta_summary_stats(self, candidates_with_eta: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get summary statistics for ETA predictions."""
        
        if not candidates_with_eta:
            return {
                "total_candidates": 0,
                "avg_eta_days": 0,
                "avg_confidence": 0,
                "eta_distribution": {}
            }
        
        eta_days = [c["eta_result"]["eta_days"] for c in candidates_with_eta]
        confidences = [c["eta_result"]["confidence_0_1"] for c in candidates_with_eta]
        
        # ETA distribution buckets
        eta_buckets = {
            "0-30 days": len([d for d in eta_days if d <= 30]),
            "31-60 days": len([d for d in eta_days if 30 < d <= 60]),
            "61-90 days": len([d for d in eta_days if 60 < d <= 90]),
            "90+ days": len([d for d in eta_days if d > 90])
        }
        
        return {
            "total_candidates": len(candidates_with_eta),
            "avg_eta_days": sum(eta_days) / len(eta_days),
            "avg_confidence": sum(confidences) / len(confidences),
            "eta_distribution": eta_buckets,
            "high_confidence_count": len([c for c in confidences if c >= 0.75])
        }
