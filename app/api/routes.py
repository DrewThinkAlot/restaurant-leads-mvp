from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
import time
import logging

from ..db import get_db
from ..schemas import (
    PipelineRequest, PipelineResponse, LeadOutput, 
    CandidateCreate, LeadCreate, HealthCheck
)
from ..models import Candidate, Lead, Signal, ETAInference, Contact
from ..pipelines.enhanced_pipeline import EnhancedPipelineRunner

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/run_pipeline", response_model=PipelineResponse)
async def run_pipeline(
    request: PipelineRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Execute the full restaurant leads pipeline."""
    
    start_time = time.time()
    
    try:
        logger.info(f"Starting pipeline with max_candidates={request.max_candidates}")
        
        # Initialize enhanced pipeline runner
        runner = EnhancedPipelineRunner()
        
        # Execute enhanced pipeline (defaults: use_stable_apis=True, use_ai_enhancement=True)
        result = runner.run_hybrid_pipeline(
            max_candidates=request.max_candidates,
            harris_only=request.harris_only
        )
        
        execution_time = time.time() - start_time
        
        # Store leads in database
        stored_leads = []
        for lead_data in result["leads"]:
            try:
                # Create candidate record if not exists
                candidate = db.query(Candidate).filter(
                    Candidate.candidate_id == lead_data["candidate_id"]
                ).first()
                
                if not candidate:
                    # This shouldn't happen if pipeline worked correctly
                    logger.warning(f"Candidate {lead_data['candidate_id']} not found in DB")
                    continue
                
                # Create lead record
                lead = Lead(
                    candidate_id=lead_data["candidate_id"],
                    status="new",
                    pitch_text=lead_data["pitch_text"],
                    how_to_pitch=lead_data["how_to_pitch"],
                    sms_text=lead_data.get("sms_text")
                )
                
                db.add(lead)
                stored_leads.append(lead_data)
                
            except Exception as e:
                logger.error(f"Failed to store lead: {e}")
                continue
        
        db.commit()
        
        response = PipelineResponse(
            leads=stored_leads,
            total_candidates=result.get("total_candidates", 0),
            qualified_leads=len(stored_leads),
            execution_time_seconds=execution_time
        )
        
        logger.info(f"Pipeline completed: {len(stored_leads)} leads generated in {execution_time:.1f}s")
        
        return response
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


@router.get("/leads", response_model=List[LeadOutput])
async def get_leads(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get leads with optional filtering."""
    
    try:
        # Build query
        query = db.query(Lead).join(Candidate)
        
        if status:
            query = query.filter(Lead.status == status)
        
        # Get leads with pagination
        leads = query.order_by(Lead.created_at.desc()).offset(offset).limit(limit).all()
        
        # Convert to LeadOutput format
        lead_outputs = []
        for lead in leads:
            # Get ETA information
            eta_inference = db.query(ETAInference).filter(
                ETAInference.candidate_id == lead.candidate_id
            ).order_by(ETAInference.created_at.desc()).first()
            
            eta_window = "Next 60 days"
            confidence = 0.0
            
            if eta_inference:
                # Format ETA window
                start_date = eta_inference.eta_start
                end_date = eta_inference.eta_end
                
                if start_date and end_date:
                    if start_date.month == end_date.month:
                        eta_window = f"{start_date.strftime('%b %d')} – {end_date.strftime('%d')}"
                    else:
                        eta_window = f"{start_date.strftime('%b %d')} – {end_date.strftime('%b %d')}"
                
                confidence = eta_inference.confidence_0_1
            
            lead_output = LeadOutput(
                lead_id=lead.lead_id,
                candidate_id=lead.candidate_id,
                venue_name=lead.candidate.venue_name,
                entity_name=lead.candidate.legal_name,
                address=lead.candidate.address,
                phone=lead.candidate.phone,
                eta_window=eta_window,
                confidence_0_1=confidence,
                how_to_pitch=lead.how_to_pitch or "",
                pitch_text=lead.pitch_text or "",
                sms_text=lead.sms_text
            )
            
            lead_outputs.append(lead_output)
        
        return lead_outputs
        
    except Exception as e:
        logger.error(f"Failed to retrieve leads: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve leads: {str(e)}")


@router.get("/leads/{lead_id}", response_model=LeadOutput)
async def get_lead(lead_id: str, db: Session = Depends(get_db)):
    """Get a specific lead by ID."""
    
    try:
        lead = db.query(Lead).join(Candidate).filter(Lead.lead_id == lead_id).first()
        
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")
        
        # Get ETA information
        eta_inference = db.query(ETAInference).filter(
            ETAInference.candidate_id == lead.candidate_id
        ).order_by(ETAInference.created_at.desc()).first()
        
        eta_window = "Next 60 days"
        confidence = 0.0
        
        if eta_inference:
            start_date = eta_inference.eta_start
            end_date = eta_inference.eta_end
            
            if start_date and end_date:
                if start_date.month == end_date.month:
                    eta_window = f"{start_date.strftime('%b %d')} – {end_date.strftime('%d')}"
                else:
                    eta_window = f"{start_date.strftime('%b %d')} – {end_date.strftime('%b %d')}"
            
            confidence = eta_inference.confidence_0_1
        
        lead_output = LeadOutput(
            lead_id=lead.lead_id,
            candidate_id=lead.candidate_id,
            venue_name=lead.candidate.venue_name,
            entity_name=lead.candidate.legal_name,
            address=lead.candidate.address,
            phone=lead.candidate.phone,
            eta_window=eta_window,
            confidence_0_1=confidence,
            how_to_pitch=lead.how_to_pitch or "",
            pitch_text=lead.pitch_text or "",
            sms_text=lead.sms_text
        )
        
        return lead_output
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve lead {lead_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve lead: {str(e)}")


@router.put("/leads/{lead_id}/status")
async def update_lead_status(
    lead_id: str, 
    status: str,
    db: Session = Depends(get_db)
):
    """Update lead status."""
    
    valid_statuses = ["new", "verified", "sent"]
    
    if status not in valid_statuses:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
        )
    
    try:
        lead = db.query(Lead).filter(Lead.lead_id == lead_id).first()
        
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")
        
        old_status = lead.status
        lead.status = status
        db.commit()
        
        logger.info(f"Updated lead {lead_id} status: {old_status} -> {status}")
        
        return {"message": f"Lead status updated to {status}", "lead_id": lead_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update lead status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update lead status: {str(e)}")


@router.get("/contacts")
async def get_contacts(
    candidate_id: Optional[str] = None,
    min_confidence: float = 0.0,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get contacts with optional filtering by candidate."""
    
    try:
        # Build query
        query = db.query(Contact).join(Candidate)
        
        if candidate_id:
            query = query.filter(Contact.candidate_id == candidate_id)
        
        if min_confidence > 0:
            query = query.filter(Contact.confidence_0_1 >= min_confidence)
        
        # Get contacts with pagination
        contacts = query.order_by(Contact.confidence_0_1.desc(), Contact.created_at.desc()).offset(offset).limit(limit).all()
        
        # Convert to response format
        contact_data = []
        for contact in contacts:
            contact_dict = {
                "id": str(contact.id),
                "candidate_id": str(contact.candidate_id),
                "venue_name": contact.candidate.venue_name,
                "full_name": contact.full_name,
                "role": contact.role,
                "email": contact.email,
                "phone": contact.phone,
                "source": contact.source,
                "source_url": contact.source_url,
                "provenance_text": contact.provenance_text,
                "confidence_0_1": contact.confidence_0_1,
                "contactability": contact.contactability,
                "notes": contact.notes,
                "created_at": contact.created_at.isoformat()
            }
            contact_data.append(contact_dict)
        
        return {
            "contacts": contact_data,
            "total_found": len(contact_data),
            "filters": {
                "candidate_id": candidate_id,
                "min_confidence": min_confidence
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to retrieve contacts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve contacts: {str(e)}")


@router.get("/candidates", response_model=List[dict])
async def get_candidates(
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get restaurant candidates."""
    
    try:
        candidates = db.query(Candidate).order_by(
            Candidate.last_seen.desc()
        ).offset(offset).limit(limit).all()
        
        candidate_data = []
        for candidate in candidates:
            # Get latest signals
            signals = db.query(Signal).filter(
                Signal.candidate_id == candidate.candidate_id
            ).order_by(Signal.created_at.desc()).first()
            
            candidate_dict = {
                "candidate_id": str(candidate.candidate_id),
                "venue_name": candidate.venue_name,
                "legal_name": candidate.legal_name,
                "address": candidate.address,
                "city": candidate.city,
                "state": candidate.state,
                "zip_code": candidate.zip_code,
                "phone": candidate.phone,
                "email": candidate.email,
                "source_flags": candidate.source_flags,
                "first_seen": candidate.first_seen.isoformat() if candidate.first_seen else None,
                "last_seen": candidate.last_seen.isoformat() if candidate.last_seen else None,
                "signals": {
                    "tabc_status": signals.tabc_status if signals else None,
                    "health_status": signals.health_status if signals else None,
                    "permit_types": signals.permit_types if signals else []
                } if signals else {}
            }
            
            candidate_data.append(candidate_dict)
        
        return candidate_data
        
    except Exception as e:
        logger.error(f"Failed to retrieve candidates: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve candidates: {str(e)}")


@router.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get pipeline statistics."""
    
    try:
        # Count totals
        total_candidates = db.query(Candidate).count()
        total_leads = db.query(Lead).count()
        
        # Count by status
        lead_status_counts = {}
        for status in ["new", "verified", "sent"]:
            count = db.query(Lead).filter(Lead.status == status).count()
            lead_status_counts[status] = count
        
        # Recent activity
        from datetime import datetime, timedelta
        recent_cutoff = datetime.now() - timedelta(days=7)
        
        recent_candidates = db.query(Candidate).filter(
            Candidate.first_seen >= recent_cutoff
        ).count()
        
        recent_leads = db.query(Lead).filter(
            Lead.created_at >= recent_cutoff
        ).count()
        
        # ETA distribution
        eta_inferences = db.query(ETAInference).all()
        
        eta_distribution = {
            "0-30 days": 0,
            "31-60 days": 0,
            "61-90 days": 0,
            "90+ days": 0
        }
        
        avg_confidence = 0
        if eta_inferences:
            confidences = []
            for eta in eta_inferences:
                confidences.append(eta.confidence_0_1)
                
                if eta.eta_days <= 30:
                    eta_distribution["0-30 days"] += 1
                elif eta.eta_days <= 60:
                    eta_distribution["31-60 days"] += 1
                elif eta.eta_days <= 90:
                    eta_distribution["61-90 days"] += 1
                else:
                    eta_distribution["90+ days"] += 1
            
            avg_confidence = sum(confidences) / len(confidences)
        
        return {
            "total_candidates": total_candidates,
            "total_leads": total_leads,
            "lead_status_counts": lead_status_counts,
            "recent_activity": {
                "new_candidates_7d": recent_candidates,
                "new_leads_7d": recent_leads
            },
            "eta_distribution": eta_distribution,
            "avg_confidence": round(avg_confidence, 2)
        }
        
    except Exception as e:
        logger.error(f"Failed to retrieve stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve stats: {str(e)}")


@router.post("/test_pipeline")
async def test_pipeline_components():
    """Test individual pipeline components."""
    
    try:
        from ..agents.agent_signal_scout import SignalScoutAgent
        from ..tools.tabc_open_data import tabc_client
        
        test_results = {
            "tabc_connection": False,
            "database_connection": False,
            "model_connection": False,
            "agents_initialized": False
        }
        
        # Test TABC connection
        try:
            datasets = tabc_client.discover_datasets()
            test_results["tabc_connection"] = len(datasets) > 0
        except Exception as e:
            logger.warning(f"TABC test failed: {e}")
        
        # Test database connection
        try:
            from ..db import db_manager
            with db_manager.get_session() as session:
                session.execute("SELECT 1")
                test_results["database_connection"] = True
        except Exception as e:
            logger.warning(f"Database test failed: {e}")
        
        # Test model connection
        try:
            import requests
            from ..settings import settings
            response = requests.get(f"{settings.vllm_base_url}/health", timeout=10)
            test_results["model_connection"] = response.status_code == 200
        except Exception as e:
            logger.warning(f"Model server test failed: {e}")
        
        # Test agent initialization
        try:
            scout = SignalScoutAgent()
            test_results["agents_initialized"] = scout.agent is not None
        except Exception as e:
            logger.warning(f"Agent test failed: {e}")
        
        return {
            "test_results": test_results,
            "overall_status": "healthy" if all(test_results.values()) else "degraded"
        }
        
    except Exception as e:
        logger.error(f"Component test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Component test failed: {str(e)}")
