#!/usr/bin/env python3
"""
Simple pipeline test that generates leads without LLM dependencies.
This tests the core data ingestion and basic lead generation functionality.
"""

from datetime import datetime, timedelta
from app.data_sources.manager import DataSourceManager
from app.db import db_manager
from app.models import Candidate, Lead, ETAInference


def create_simple_leads():
    """Create simple leads from candidates without LLM processing."""
    
    print("=== SIMPLE PIPELINE TEST ===")
    
    # Step 1: Fetch and process data
    print("1. Fetching and processing data...")
    manager = DataSourceManager()
    
    raw_results = manager.fetch_all_sources(limit_per_source=5)
    total_raw = sum(len(records) for records in raw_results.values())
    print(f"   Raw records: {total_raw}")
    
    if total_raw == 0:
        print("❌ No raw data available")
        return
    
    normalized = manager.normalize_and_score_records(raw_results)
    deduplicated = manager.deduplicate_records(normalized)
    quality_records = [r for r in deduplicated if r.get('venue_name') and r.get('address')]
    
    print(f"   Quality records: {len(quality_records)}")
    
    # Step 2: Convert to candidates
    print("2. Converting to candidates...")
    from app.pipelines.run_pipeline import PipelineRunner
    runner = PipelineRunner()
    
    candidates = runner._convert_api_records_to_candidates(quality_records)
    runner._store_candidates_in_db(candidates)
    
    # Step 3: Generate simple ETA estimates and leads
    print("3. Generating simple leads...")
    
    with db_manager.get_session() as session:
        db_candidates = session.query(Candidate).all()
        leads_created = 0
        
        for candidate in db_candidates:
            # Skip if lead already exists
            existing_lead = session.query(Lead).filter(
                Lead.candidate_id == candidate.candidate_id
            ).first()
            
            if existing_lead:
                continue
            
            # Create simple ETA estimate
            source_flags = candidate.source_flags or {}
            source = source_flags.get('primary_source', '')
            
            if 'tabc' in source:
                # TABC signals suggest 30-90 days
                eta_start = datetime.now() + timedelta(days=30)
                eta_end = datetime.now() + timedelta(days=90)
                confidence = 0.7
            elif 'health' in source:
                # Health inspections suggest 7-45 days
                eta_start = datetime.now() + timedelta(days=7)
                eta_end = datetime.now() + timedelta(days=45)
                confidence = 0.6
            else:
                # Default estimate
                eta_start = datetime.now() + timedelta(days=60)
                eta_end = datetime.now() + timedelta(days=120)
                confidence = 0.5
            
            # Create ETA inference
            eta_inference = ETAInference(
                candidate_id=candidate.candidate_id,
                eta_start=eta_start,
                eta_end=eta_end,
                eta_days=(eta_end - eta_start).days,
                confidence_0_1=confidence,
                rationale_text=f"Simple rule-based estimate for {source} source"
            )
            session.add(eta_inference)
            
            # Create simple pitch content
            venue_name = candidate.venue_name
            address = f"{candidate.address}, {candidate.city}"
            
            if eta_start.month == eta_end.month:
                eta_window = f"{eta_start.strftime('%b %d')} – {eta_end.strftime('%d')}"
            else:
                eta_window = f"{eta_start.strftime('%b %d')} – {eta_end.strftime('%b %d')}"
            
            pitch_text = f"""Hi! I see {venue_name} is opening soon at {address}. 
We help new restaurants get set up with POS systems and payment processing. 
Our team specializes in restaurant launches - we can have you ready to accept 
payments on day one with competitive rates. Would you like to chat about 
your payment processing needs? Expected opening: {eta_window}."""
            
            how_to_pitch = f"New restaurant opening at {address}. Contact about POS setup before {eta_window} opening."
            
            sms_text = f"{venue_name} opening {eta_window}? We help new restaurants with POS setup. Quick chat?"
            
            # Create lead
            lead = Lead(
                candidate_id=candidate.candidate_id,
                status="new",
                pitch_text=pitch_text,
                how_to_pitch=how_to_pitch,
                sms_text=sms_text
            )
            session.add(lead)
            leads_created += 1
        
        session.commit()
        
        print(f"   Created {leads_created} new leads")
    
    # Step 4: Display results
    print("4. Pipeline Results:")
    with db_manager.get_session() as session:
        total_candidates = session.query(Candidate).count()
        total_leads = session.query(Lead).count()
        total_eta_inferences = session.query(ETAInference).count()
        
        print(f"   Candidates: {total_candidates}")
        print(f"   ETA Inferences: {total_eta_inferences}")
        print(f"   Leads: {total_leads}")
        
        # Show sample leads
        leads = session.query(Lead).join(Candidate).limit(3).all()
        print(f"\n   Sample Leads:")
        for i, lead in enumerate(leads, 1):
            print(f"   {i}. {lead.candidate.venue_name}")
            print(f"      How to pitch: {lead.how_to_pitch}")
            print(f"      SMS: {lead.sms_text}")
            print()
    
    print("✅ Simple pipeline completed successfully!")
    return True


if __name__ == "__main__":
    create_simple_leads()