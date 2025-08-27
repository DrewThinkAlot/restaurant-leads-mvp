import logging
import time
from typing import Dict, Any, List
from datetime import datetime
import os

from ..agents.crew import RestaurantLeadsCrew
from ..agents.agent_signal_scout import SignalScoutAgent
from ..agents.agent_extractor import ExtractorAgent
from ..agents.agent_resolver import ResolverAgent
from ..agents.agent_contact_finder import ContactFinderAgent
from ..agents.agent_eta import ETAAgent
from ..agents.agent_verifier import VerifierAgent
from ..agents.agent_pitch import PitchAgent
from ..data_sources import DataSourceManager
from ..db import db_manager
from ..models import Candidate, Signal, ETAInference, Lead

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Orchestrates the complete restaurant leads pipeline."""
    
    def __init__(self):
        self.crew = RestaurantLeadsCrew()
        
        # Initialize individual agents for direct use
        self.signal_scout = SignalScoutAgent()
        self.extractor = ExtractorAgent()
        self.resolver = ResolverAgent()
        self.contact_finder = ContactFinderAgent()
        self.eta_estimator = ETAAgent()
        self.verifier = VerifierAgent()
        self.pitch_writer = PitchAgent()
        
        # Initialize new data source manager
        self.data_source_manager = DataSourceManager(
            tabc_app_token=os.getenv('TABC_APP_TOKEN'),
            comptroller_api_key=os.getenv('TX_COMPTROLLER_API_KEY')
        )
    
    def run_complete_pipeline(self, max_candidates: int = 100, harris_only: bool = True, 
                             use_stable_apis: bool = True) -> Dict[str, Any]:
        """Run the complete pipeline with option to use stable APIs or legacy scraping."""
        
        start_time = time.time()
        logger.info(f"Starting complete pipeline: max_candidates={max_candidates}, stable_apis={use_stable_apis}")
        
        try:
            if use_stable_apis:
                # Use new stable API data sources
                return self._run_api_pipeline(max_candidates)
            else:
                # Use legacy scraping approach
                return self._run_legacy_pipeline(max_candidates, harris_only)
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Pipeline failed after {execution_time:.1f}s: {e}")
            raise
    
    def _run_api_pipeline(self, max_candidates: int) -> Dict[str, Any]:
        """Run pipeline using stable API data sources."""
        
        start_time = time.time()
        
        # Step 1: Fetch data from all stable API sources
        logger.info("Step 1: Fetching data from stable API sources...")
        raw_results = self.data_source_manager.fetch_all_sources(
            limit_per_source=max_candidates // 3  # Distribute across sources
        )
        
        total_raw = sum(len(records) for records in raw_results.values())
        logger.info(f"Fetched {total_raw} raw records from {len(raw_results)} sources")
        
        if total_raw == 0:
            logger.warning("No records fetched from API sources")
            return self._empty_pipeline_result(start_time)
        
        # Step 2: Normalize and score records
        logger.info("Step 2: Normalizing and scoring records...")
        normalized_records = self.data_source_manager.normalize_and_score_records(raw_results)
        
        # Step 3: Deduplicate across sources
        logger.info("Step 3: Deduplicating records...")
        deduplicated_records = self.data_source_manager.deduplicate_records(normalized_records)
        
        # Step 4: Enrich top candidates with Comptroller data
        logger.info("Step 4: Enriching with Comptroller data...")
        enriched_records = self.data_source_manager.enrich_with_comptroller(deduplicated_records)
        
        # Step 5: Convert to pipeline format and run through existing agents
        logger.info("Step 5: Converting to candidate format...")
        candidates = self._convert_api_records_to_candidates(enriched_records[:max_candidates])
        
        # Store candidates in database
        self._store_candidates_in_db(candidates)
        
        # Step 6: Run through existing pipeline stages
        logger.info("Step 6: Processing through pipeline agents...")
        
        # Entity resolution
        resolved_candidates = self.resolver.resolve_entities(candidates)
        logger.info(f"Resolved to {len(resolved_candidates)} unique entities")
        
        # Contact finding (enhance with existing data) - DISABLED
        # candidates_with_contacts = self.contact_finder.find_contacts(resolved_candidates)
        # logger.info(f"Found contacts for {len([c for c in candidates_with_contacts if 'contacts' in c])} candidates")
        logger.info("Contact finding disabled - skipping contact enrichment")
        candidates_with_contacts = resolved_candidates  # Skip contact finding
        
        # ETA estimation
        candidates_with_eta = self.eta_estimator.estimate_opening_dates(candidates_with_contacts)
        logger.info(f"Generated ETA for {len(candidates_with_eta)} candidates")
        
        # Store ETA inferences
        self._store_eta_inferences(candidates_with_eta)
        
        # Quality verification
        verified_candidates = self.verifier.verify_candidates(candidates_with_eta)
        logger.info(f"Verified {len(verified_candidates)} high-quality candidates")
        
        # Pitch generation
        final_leads = self.pitch_writer.create_pitch_content(verified_candidates)
        logger.info(f"Generated {len(final_leads)} sales-ready leads")
        
        # Step 7: Export CSV for sales team
        csv_path = self.data_source_manager.export_to_csv(
            enriched_records[:len(final_leads)], 
            f"sales_leads_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        )
        
        execution_time = time.time() - start_time
        
        # Prepare response
        response = {
            "leads": final_leads,
            "total_candidates": total_raw,
            "qualified_leads": len(final_leads),
            "execution_time_seconds": execution_time,
            "csv_export_path": csv_path,
            "data_source_summary": self.data_source_manager.get_pipeline_summary(),
            "pipeline_stages": {
                "raw_records": total_raw,
                "normalized_records": len(normalized_records),
                "deduplicated_records": len(deduplicated_records),
                "enriched_records": len(enriched_records),
                "pipeline_candidates": len(candidates),
                "resolved_candidates": len(resolved_candidates),
                "contacts_found": len([c for c in candidates_with_contacts if 'contacts' in c]),
                "eta_qualified": len(candidates_with_eta),
                "verified_leads": len(verified_candidates),
                "final_leads": len(final_leads)
            }
        }
        
        logger.info(f"API pipeline completed successfully in {execution_time:.1f}s")
        return response
    
    def _run_legacy_pipeline(self, max_candidates: int, harris_only: bool) -> Dict[str, Any]:
        """Run legacy pipeline using existing scraping approach."""
        
        start_time = time.time()
        
        # Step 1: Signal Scout - Discover candidates
        logger.info("Step 1: Discovering candidates...")
        raw_candidates = self.signal_scout.execute_discovery(max_candidates)
        logger.info(f"Discovered {len(raw_candidates)} raw candidates")
        
        if not raw_candidates:
            logger.warning("No candidates discovered, ending pipeline")
            return self._empty_pipeline_result(start_time)
        
        # Step 2: Extractor - Normalize candidates
        logger.info("Step 2: Normalizing candidates...")
        normalized_candidates = self.extractor.normalize_candidates(raw_candidates)
        logger.info(f"Normalized {len(normalized_candidates)} candidates")
        
        # Store candidates in database
        self._store_candidates_in_db(normalized_candidates)
        
        # Step 3: Resolver - Entity resolution
        logger.info("Step 3: Resolving entities...")
        resolved_candidates = self.resolver.resolve_entities(normalized_candidates)
        logger.info(f"Resolved to {len(resolved_candidates)} unique entities")
        
        # Step 3.5: Contact Finder - Identify decision makers - DISABLED
        # candidates_with_contacts = self.contact_finder.find_contacts(resolved_candidates)
        # logger.info(f"Found contacts for {len([c for c in candidates_with_contacts if 'contacts' in c])} candidates")
        logger.info("Contact finding disabled - skipping contact enrichment")
        candidates_with_contacts = resolved_candidates  # Skip contact finding
        
        # Step 4: ETA Estimator - Predict opening dates
        logger.info("Step 4: Estimating opening dates...")
        candidates_with_eta = self.eta_estimator.estimate_opening_dates(candidates_with_contacts)
        logger.info(f"Generated ETA for {len(candidates_with_eta)} candidates")
        
        # Store ETA inferences in database
        self._store_eta_inferences(candidates_with_eta)
        
        # Step 5: Verifier - Quality checks
        logger.info("Step 5: Verifying candidate quality...")
        verified_candidates = self.verifier.verify_candidates(candidates_with_eta)
        logger.info(f"Verified {len(verified_candidates)} high-quality candidates")
        
        # Step 6: Pitch Writer - Generate sales content
        logger.info("Step 6: Creating pitch content...")
        final_leads = self.pitch_writer.create_pitch_content(verified_candidates)
        logger.info(f"Generated {len(final_leads)} sales-ready leads")
        
        execution_time = time.time() - start_time
        
        # Prepare response
        response = {
            "leads": final_leads,
            "total_candidates": len(raw_candidates),
            "qualified_leads": len(final_leads),
            "execution_time_seconds": execution_time,
            "pipeline_stages": {
                "raw_candidates": len(raw_candidates),
                "normalized_candidates": len(normalized_candidates),
                "resolved_candidates": len(resolved_candidates),
                "contacts_found": len([c for c in candidates_with_contacts if 'contacts' in c]),
                "eta_qualified": len(candidates_with_eta),
                "verified_leads": len(verified_candidates),
                "final_leads": len(final_leads)
            }
        }
        
        logger.info(f"Legacy pipeline completed successfully in {execution_time:.1f}s")
        return response
    
    def _convert_api_records_to_candidates(self, api_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert normalized API records to candidate format for existing pipeline."""
        
        candidates = []
        
        for record in api_records:
            candidate = {
                "candidate_id": f"api_{record.get('source')}_{record.get('source_id', '')}",
                "venue_name": record.get("venue_name", ""),
                "legal_name": record.get("legal_name"),
                "address": record.get("address", ""),
                "suite": record.get("suite"),
                "city": record.get("city", ""),
                "state": record.get("state", "TX"),
                "zip": record.get("zip_code", ""),
                "phone": record.get("phone"),
                "email": record.get("email"),
                "source_flags": {
                    "primary_source": record.get("source"),
                    "cross_source_signals": record.get("cross_source_signals", []),
                    "composite_lead_score": record.get("composite_lead_score", 0.0),
                    "api_sourced": True
                },
                "signals": {
                    "signal_strength": record.get("signal_strength", 0.0),
                    "estimated_open_window": record.get("estimated_open_window", {}),
                    "license_type": record.get("license_type"),
                    "permit_status": record.get("permit_status"),
                    "inspection_result": record.get("inspection_result"),
                    "application_date": record.get("application_date"),
                    "inspection_date": record.get("inspection_date"),
                    "issued_date": record.get("issued_date")
                }
            }
            
            # Add enriched data if available
            if record.get("comptroller_enrichment"):
                enrichment = record["comptroller_enrichment"]
                if enrichment.get("legal_entity_info"):
                    entity_info = enrichment["legal_entity_info"]
                    candidate["legal_name"] = entity_info.get("legal_name") or candidate["legal_name"]
                    candidate["source_flags"]["taxpayer_number"] = entity_info.get("taxpayer_number")
                    candidate["source_flags"]["entity_type"] = entity_info.get("entity_type")
            
            candidates.append(candidate)
        
        return candidates
    
    def _empty_pipeline_result(self, start_time: float) -> Dict[str, Any]:
        """Return empty pipeline result structure."""
        return {
            "leads": [],
            "total_candidates": 0,
            "qualified_leads": 0,
            "execution_time_seconds": time.time() - start_time,
            "pipeline_stages": {
                "raw_candidates": 0,
                "normalized_candidates": 0,
                "resolved_candidates": 0,
                "eta_qualified": 0,
                "verified_leads": 0,
                "final_leads": 0
            }
        }

    def _store_candidates_in_db(self, candidates: List[Dict[str, Any]]):
        """Store normalized candidates in database."""
        
        try:
            with db_manager.get_session() as session:
                for candidate_data in candidates:
                    # Check if candidate already exists by venue name and address
                    existing = session.query(Candidate).filter(
                        Candidate.venue_name == candidate_data["venue_name"],
                        Candidate.address == candidate_data["address"],
                        Candidate.city == candidate_data["city"]
                    ).first()
                    
                    if existing:
                        # For now, skip updating existing candidates to avoid session issues
                        logger.info(f"Skipping existing candidate: {candidate_data['venue_name']} at {candidate_data['address']}")
                        continue
                    else:
                        # Create new candidate
                        candidate = Candidate(
                            # candidate_id will be auto-generated by SQLAlchemy
                            venue_name=candidate_data["venue_name"],
                            legal_name=candidate_data.get("legal_name"),
                            address=candidate_data["address"],
                            suite=candidate_data.get("suite"),
                            city=candidate_data["city"],
                            state=candidate_data["state"],
                            zip_code=candidate_data["zip"],
                            phone=candidate_data.get("phone"),
                            email=candidate_data.get("email"),
                            source_flags=candidate_data.get("source_flags", {}),
                            first_seen=datetime.now(),
                            last_seen=datetime.now()
                        )
                        session.add(candidate)
                        session.flush()  # Ensure candidate_id is available for signals
                    
                    # Store signals if present
                    signals_data = candidate_data.get("signals", {})
                    if signals_data:
                        signal = Signal(
                            candidate_id=candidate_data["candidate_id"],
                            tabc_status=signals_data.get("tabc_status"),
                            tabc_dates=signals_data.get("tabc_dates", {}),
                            health_status=signals_data.get("health_status"),
                            permit_types=signals_data.get("permit_types", []),
                            milestone_dates=signals_data.get("milestone_dates", {})
                        )
                        session.add(signal)
                
                session.commit()
                logger.info(f"Stored {len(candidates)} candidates in database")
                
        except Exception as e:
            logger.error(f"Failed to store candidates: {e}")
            raise
    
    def _store_eta_inferences(self, candidates_with_eta: List[Dict[str, Any]]):
        """Store ETA inferences in database."""
        
        try:
            with db_manager.get_session() as session:
                for candidate in candidates_with_eta:
                    eta_result = candidate.get("eta_result", {})
                    
                    if eta_result:
                        eta_inference = ETAInference(
                            candidate_id=candidate["candidate_id"],
                            eta_start=datetime.fromisoformat(eta_result["eta_start"]),
                            eta_end=datetime.fromisoformat(eta_result["eta_end"]),
                            eta_days=eta_result["eta_days"],
                            confidence_0_1=eta_result["confidence_0_1"],
                            rationale_text=eta_result.get("rationale_text", "")
                        )
                        session.add(eta_inference)
                
                session.commit()
                logger.info(f"Stored {len(candidates_with_eta)} ETA inferences")
                
        except Exception as e:
            logger.error(f"Failed to store ETA inferences: {e}")
            raise
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and statistics."""
        
        try:
            with db_manager.get_session() as session:
                # Count records by type
                total_candidates = session.query(Candidate).count()
                total_signals = session.query(Signal).count()
                total_eta_inferences = session.query(ETAInference).count()
                total_leads = session.query(Lead).count()
                
                # Recent activity
                from datetime import timedelta
                recent_cutoff = datetime.now() - timedelta(hours=24)
                
                recent_candidates = session.query(Candidate).filter(
                    Candidate.first_seen >= recent_cutoff
                ).count()
                
                recent_leads = session.query(Lead).filter(
                    Lead.created_at >= recent_cutoff
                ).count()
                
                return {
                    "database_stats": {
                        "total_candidates": total_candidates,
                        "total_signals": total_signals,
                        "total_eta_inferences": total_eta_inferences,
                        "total_leads": total_leads
                    },
                    "recent_activity_24h": {
                        "new_candidates": recent_candidates,
                        "new_leads": recent_leads
                    },
                    "crew_status": self.crew.get_crew_status(),
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def run_incremental_update(self, days_back: int = 1, use_stable_apis: bool = True) -> Dict[str, Any]:
        """Run incremental pipeline update for recent data."""
        
        logger.info(f"Running incremental update for last {days_back} days")
        
        try:
            # Run with smaller candidate limit for incremental updates
            result = self.run_complete_pipeline(
                max_candidates=200, 
                harris_only=True,
                use_stable_apis=use_stable_apis
            )
            
            logger.info(f"Incremental update complete: {result['qualified_leads']} new leads")
            return result
            
        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            raise
    
    def get_data_source_status(self) -> Dict[str, Any]:
        """Get status of data source integrations."""
        
        try:
            return self.data_source_manager.get_pipeline_summary()
        except Exception as e:
            logger.error(f"Failed to get data source status: {e}")
            return {"error": str(e)}
    
    def __del__(self):
        """Cleanup resources."""
        if hasattr(self, 'data_source_manager'):
            self.data_source_manager.close()
