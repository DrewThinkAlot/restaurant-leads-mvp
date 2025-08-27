from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import json
import time

from .run_pipeline import PipelineRunner
from ..agents.crew import RestaurantLeadsCrew
from ..agents.agent_contact_finder import ContactFinderAgent
from ..settings import settings

logger = logging.getLogger(__name__)


class EnhancedPipelineRunner(PipelineRunner):
    """Enhanced pipeline that combines direct API calls with AI agent analysis."""

    def __init__(self):
        super().__init__()
        self.ai_crew = RestaurantLeadsCrew()

    def run_hybrid_pipeline(
        self,
        max_candidates: int = 100,
        harris_only: bool = True,
        use_stable_apis: bool = True,
        use_ai_enhancement: bool = True
    ) -> Dict[str, Any]:
        """
        Run hybrid pipeline combining direct API calls with AI enhancement.

        Args:
            max_candidates: Maximum candidates to process
            harris_only: Focus on Harris County only
            use_stable_apis: Use direct API calls for data fetching
            use_ai_enhancement: Apply AI agents for analysis and lead generation
        """

        logger.info("Starting hybrid pipeline...")
        start_time = time.time()

        try:
            # Step 1: Use current pipeline for reliable data fetching
            logger.info("Step 1: Fetching data with direct APIs...")
            api_result = self.run_complete_pipeline(
                max_candidates=max_candidates,
                harris_only=harris_only,
                use_stable_apis=use_stable_apis
            )

            if not api_result.get("execution_success", False):
                logger.warning("API pipeline failed, falling back to AI-only mode")
                return self._run_ai_only_pipeline(max_candidates, harris_only)

            candidates = api_result.get("candidates", [])
            logger.info(f"Fetched {len(candidates)} candidates via direct APIs")

            if not use_ai_enhancement:
                logger.info("AI enhancement disabled, returning API results")
                return api_result

            # Step 2: Convert candidates to AI agent format
            logger.info("Step 2: Converting candidates for AI analysis...")
            ai_candidates = self._convert_candidates_for_ai(candidates)

            # Step 3: Run AI analysis pipeline
            logger.info("Step 3: Running AI agent analysis...")
            ai_result = self._run_ai_analysis(ai_candidates, max_candidates)

            # Step 4: Merge API and AI results
            logger.info("Step 4: Merging API and AI results...")
            final_result = self._merge_results(api_result, ai_result, start_time)

            logger.info("Hybrid pipeline completed successfully")
            return final_result

        except Exception as e:
            logger.error(f"Hybrid pipeline failed: {e}")
            # Fallback to API-only pipeline
            logger.info("Falling back to API-only pipeline...")
            return self.run_complete_pipeline(
                max_candidates=max_candidates,
                harris_only=harris_only,
                use_stable_apis=use_stable_apis
            )

    def _convert_candidates_for_ai(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert database candidates to AI agent format."""
        ai_candidates = []

        for candidate_data in candidates:
            # Convert to the format expected by AI agents
            ai_candidate = {
                "candidate_id": str(candidate_data.get("candidate_id", "")),
                "venue_name": candidate_data.get("venue_name", ""),
                "legal_name": candidate_data.get("legal_name"),
                "address": candidate_data.get("address", ""),
                "city": candidate_data.get("city", ""),
                "state": candidate_data.get("state", ""),
                "zip": candidate_data.get("zip_code", ""),
                "phone": candidate_data.get("phone"),
                "email": candidate_data.get("email"),
                "source_flags": candidate_data.get("source_flags", {}),
                "signals": candidate_data.get("signals", {}),
                "first_seen": candidate_data.get("first_seen", "").isoformat() if candidate_data.get("first_seen") else None,
                "last_seen": candidate_data.get("last_seen", "").isoformat() if candidate_data.get("last_seen") else None
            }
            ai_candidates.append(ai_candidate)

        return ai_candidates

    def _run_ai_analysis(self, candidates: List[Dict[str, Any]], max_candidates: int) -> Dict[str, Any]:
        """Run AI agent analysis on candidates."""
        try:
            # Create a temporary crew with the candidates as context
            from crewai import Task

            # Use the existing AI crew but modify the first task to use our data
            scout_task = Task(
                description=f"""
                Process these {len(candidates)} restaurant candidates that were already gathered from APIs.
                Do not gather new data - analyze and enhance the provided candidates.

                Input candidates: {json.dumps(candidates[:max_candidates])}

                Focus on:
                1. Signal analysis and enrichment
                2. Entity resolution and deduplication
                3. Opening date estimation
                4. Quality verification
                5. Sales pitch generation

                Return enhanced candidate data with AI analysis.
                """,
                agent=self.ai_crew.extractor.agent,  # Start with extractor since we already have raw data
                expected_output="JSON array of enhanced candidates with AI analysis"
            )

            # Run the remaining AI pipeline
            self.ai_crew.crew.tasks = [scout_task]

            result = self.ai_crew.crew.kickoff()

            if isinstance(result, str):
                try:
                    enhanced_candidates = json.loads(result)
                except json.JSONDecodeError:
                    logger.error("Failed to parse AI result as JSON")
                    enhanced_candidates = candidates  # Return original if AI fails
            else:
                enhanced_candidates = result

            # Step 4: AI Enhancement Phase
            if use_ai_enhancement and self.vllm_available:
                print("🤖 Running AI enhancement...")
                
                # Skip Contact Discovery for now - ContactFinder disabled
                # ContactFinder agent disabled temporarily
                candidates_for_ai = candidates
                
                # Run RestaurantLeadsCrew
                crew = RestaurantLeadsCrew()
                ai_results = crew.run_pipeline(candidates_for_ai)
                
                # Merge AI results with API data
                enhanced_leads = self._merge_ai_results(candidates_for_ai, ai_results)
                return {
                    'leads': enhanced_leads,
                    'total_candidates': len(candidates),
                    'qualified_leads': len([l for l in enhanced_leads if l.get('confidence_0_1', 0) > 0.7]),
                    'api_success': True,
                    'ai_enhancement': True
                }

            return {
                "enhanced_candidates": enhanced_candidates if isinstance(enhanced_candidates, list) else [enhanced_candidates],
                "ai_success": True
            }

        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return {
                "enhanced_candidates": candidates,  # Return original candidates
                "ai_success": False,
                "error": str(e)
            }

    def _merge_results(
        self,
        api_result: Dict[str, Any],
        ai_result: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """Merge API results with AI enhancements."""
        enhanced_candidates = ai_result.get("enhanced_candidates", [])
        ai_success = ai_result.get("ai_success", False)

        # Use AI results if successful, otherwise fall back to API results
        if ai_success and enhanced_candidates:
            final_candidates = enhanced_candidates
            qualified_leads = len([c for c in enhanced_candidates
                                 if c.get("confidence_0_1", 0) >= 0.65])
        else:
            final_candidates = api_result.get("candidates", [])
            qualified_leads = api_result.get("qualified_leads", 0)

        return {
            "leads": final_candidates,
            "total_candidates": len(final_candidates),
            "qualified_leads": qualified_leads,
            "execution_time_seconds": time.time() - start_time,
            "pipeline_stages": {
                "raw_candidates": len(api_result.get("candidates", [])),
                "ai_enhanced": len(enhanced_candidates) if ai_success else 0,
                "final_leads": len(final_candidates)
            },
            "ai_enhancement": ai_success,
            "api_success": api_result.get("execution_success", False)
        }

    def _run_ai_only_pipeline(self, max_candidates: int, harris_only: bool) -> Dict[str, Any]:
        """Fallback to AI-only pipeline if API pipeline fails."""
        logger.info("Running AI-only pipeline...")
        return self.ai_crew.run_pipeline(max_candidates, harris_only)
