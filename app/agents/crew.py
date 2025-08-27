from crewai import Crew, Agent, Task, Process
from typing import List, Dict, Any, Optional
import json
import logging
from crewai import Crew, Agent, Task
from ..settings import settings
from ..llm import get_llm
from .agent_signal_scout import SignalScoutAgent
from .agent_extractor import ExtractorAgent
from .agent_resolver import ResolverAgent
from .agent_eta import ETAAgent
from .agent_verifier import VerifierAgent
from .agent_pitch import PitchAgent

logger = logging.getLogger(__name__)


class RestaurantLeadsCrew:
    """CrewAI-style orchestration for restaurant leads pipeline."""
    
    def __init__(self):
        self.signal_scout = SignalScoutAgent()
        self.extractor = ExtractorAgent()
        self.resolver = ResolverAgent()
        self.eta_estimator = ETAAgent()
        self.verifier = VerifierAgent()
        self.pitch_writer = PitchAgent()
        
        # Configure LLM for Ollama Turbo
        self.llm = get_llm()
        
        # Initialize crew with sequential process and custom LLM
        self.crew = Crew(
            agents=[
                self.signal_scout.agent,
                self.extractor.agent,
                self.resolver.agent,
                self.eta_estimator.agent,
                self.verifier.agent,
                self.pitch_writer.agent
            ],
            tasks=[],  # Will be set dynamically
            process=Process.sequential,
            verbose=True,
            manager_llm=self.llm  # Use Ollama Turbo for crew management
        )
    
    def run_pipeline(self, max_candidates: int = 100, harris_only: bool = True) -> Dict[str, Any]:
        """Execute the full pipeline with structured JSON flow."""
        
        logger.info(f"Starting pipeline with max_candidates={max_candidates}")
        
        # Task 1: Signal Scout - Find raw candidates
        scout_task = Task(
            description=f"""
            Gather restaurant candidates from all available sources (TABC, Harris County permits, HCPH).
            Focus on Harris County, TX with recent activity in last 90 days.
            Look for pending licenses, building permits, plan reviews, and other opening signals.
            Maximum candidates to collect: {max_candidates}
            Harris County only: {harris_only}
            
            Return JSON array of raw candidate data with source information.
            """,
            agent=self.signal_scout.agent,
            expected_output="JSON array of raw restaurant candidates with source flags and basic info"
        )
        
        # Task 2: Extractor - Normalize candidates
        extractor_task = Task(
            description="""
            Take the raw candidates from SignalScout and normalize them into clean RestaurantCandidate JSON objects.
            Ensure all fields match the schema exactly. Set nullable fields to null if data is missing.
            Do not invent phone numbers, emails, or other contact info.
            Deduplicate obvious duplicates based on name and address.
            
            Input: Raw candidate data from previous task
            Output: JSON array of normalized RestaurantCandidate objects
            """,
            agent=self.extractor.agent,
            expected_output="JSON array of normalized RestaurantCandidate objects",
            context=[scout_task]
        )
        
        # Task 3: Resolver - Entity resolution
        resolver_task = Task(
            description="""
            Perform entity resolution on the normalized candidates.
            Use deterministic rules first (exact address match, phone/email match).
            For ambiguous cases, use LLM with MatchEvaluation to determine if records represent the same entity.
            Merge records that represent the same restaurant.
            
            Input: Normalized candidates from extractor
            Output: JSON array of resolved candidates with duplicate merging
            """,
            agent=self.resolver.agent,
            expected_output="JSON array of entity-resolved candidates",
            context=[extractor_task]
        )
        
        # Task 4: ETA Estimator - Predict opening dates
        eta_task = Task(
            description="""
            Estimate opening dates for each candidate using deterministic rules first, then LLM adjustment.
            Apply the high-probability, medium-probability, and down-weighting rules.
            Allow LLM to adjust ETA by ±15 days and confidence by ±0.1 with rationale.
            Only candidates with confidence ≥ 0.65 and ETA within 60 days should proceed.
            
            Input: Resolved candidates with signals
            Output: JSON array of ETAResult objects for qualified candidates
            """,
            agent=self.eta_estimator.agent,
            expected_output="JSON array of ETAResult objects for qualified candidates",
            context=[resolver_task]
        )
        
        # Task 5: Verifier - Quality checks
        verifier_task = Task(
            description="""
            Perform final quality checks on candidates with ETA predictions.
            Verify data consistency, flag conflicts, ensure minimum confidence thresholds.
            Remove candidates that don't meet quality gates.
            
            Input: Candidates with ETA predictions
            Output: JSON array of verified candidates ready for lead creation
            """,
            agent=self.verifier.agent,
            expected_output="JSON array of verified, high-quality candidates",
            context=[eta_task]
        )
        
        # Task 6: Pitch Writer - Create sales-ready content
        pitch_task = Task(
            description="""
            Create sales-ready pitch content for verified candidates.
            Generate concise, value-focused pitch copy for POS sales reps.
            Include: how_to_pitch (1 sentence), pitch_text (≤120 words), sms_text (≤40 words).
            Mention upcoming opening window and concrete business value.
            
            Input: Verified candidates with ETA data
            Output: JSON array of LeadOutput objects with pitch content
            """,
            agent=self.pitch_writer.agent,
            expected_output="JSON array of LeadOutput objects with sales pitch content",
            context=[verifier_task]
        )
        
        # Execute crew with tasks
        self.crew.tasks = [scout_task, extractor_task, resolver_task, eta_task, verifier_task, pitch_task]
        
        try:
            result = self.crew.kickoff()
            
            # Parse final result as JSON
            if isinstance(result, str):
                try:
                    leads = json.loads(result)
                except json.JSONDecodeError:
                    logger.error("Failed to parse crew result as JSON")
                    leads = []
            else:
                leads = result
            
            return {
                "leads": leads if isinstance(leads, list) else [leads],
                "total_candidates": len(leads) if isinstance(leads, list) else 1,
                "qualified_leads": len([l for l in (leads if isinstance(leads, list) else [leads]) 
                                     if isinstance(l, dict) and l.get("confidence_0_1", 0) >= 0.65]),
                "execution_success": True
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Crew execution failed: {error_msg}")
            
            # Handle specific CrewAI errors
            if "'list' object has no attribute 'rstrip'" in error_msg:
                logger.error("CrewAI data processing error: List passed where string expected")
                logger.error("This may be caused by empty data or CrewAI library issue")
                return {
                    "leads": [],
                    "total_candidates": 0,
                    "qualified_leads": 0,
                    "execution_success": False,
                    "error": "CrewAI processing error: List object passed where string expected. This may indicate empty data sources or a CrewAI library compatibility issue."
                }
            
            return {
                "leads": [],
                "total_candidates": 0,
                "qualified_leads": 0,
                "execution_success": False,
                "error": error_msg
            }
    
    def get_crew_status(self) -> Dict[str, Any]:
        """Get status of crew execution."""
        
        return {
            "agents_count": len(self.crew.agents),
            "tasks_count": len(self.crew.tasks),
            "process": self.crew.process.name if hasattr(self.crew.process, 'name') else str(self.crew.process)
        }
