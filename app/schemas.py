from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID


class RestaurantCandidate(BaseModel):
    """JSON schema for normalized restaurant candidates."""
    candidate_id: UUID
    venue_name: str
    legal_name: Optional[str] = None
    address: str
    suite: Optional[str] = None
    city: str
    state: str = "TX"
    zip: str = Field(..., alias="zip_code")
    county: str = "Harris"
    phone: Optional[str] = None
    email: Optional[str] = None
    source_flags: Dict[str, Optional[str]] = Field(default_factory=dict)
    
    class Config:
        allow_population_by_field_name = True


class SourceFlags(BaseModel):
    """Source flags for tracking data sources."""
    tabc: Optional[str] = None  # pending|active|none|null
    hc_permit: Optional[str] = None  # found|not_found|null
    hc_health: Optional[str] = None  # plan_review|approved|unknown|null
    houston_permit: Optional[str] = None  # found|not_found|null


class MatchEvaluation(BaseModel):
    """Entity resolver evaluation result."""
    same_entity: bool
    confidence_0_1: float = Field(..., ge=0.0, le=1.0)
    explanation: str


class ETAResult(BaseModel):
    """ETA estimation result."""
    eta_start: str  # YYYY-MM-DD format
    eta_end: str  # YYYY-MM-DD format
    eta_days: int
    confidence_0_1: float = Field(..., ge=0.0, le=1.0)
    signals_considered: List[str]
    rationale_text: str


class LeadOutput(BaseModel):
    """Final lead output for sales team."""
    lead_id: UUID
    candidate_id: UUID
    venue_name: str
    entity_name: Optional[str] = None
    address: str
    phone: Optional[str] = None
    eta_window: str  # Human readable like "Aug 15 – Sep 15"
    confidence_0_1: float = Field(..., ge=0.0, le=1.0)
    how_to_pitch: str
    pitch_text: str
    sms_text: Optional[str] = None


class PipelineRequest(BaseModel):
    """Pipeline execution request."""
    max_candidates: int = Field(default=100, le=500)
    harris_only: bool = True


class PipelineResponse(BaseModel):
    """Pipeline execution response."""
    leads: List[LeadOutput]
    total_candidates: int
    qualified_leads: int
    execution_time_seconds: float


class SignalData(BaseModel):
    """Signal data for a candidate."""
    tabc_status: Optional[str] = None
    tabc_dates: Dict[str, Any] = Field(default_factory=dict)
    health_status: Optional[str] = None
    permit_types: List[str] = Field(default_factory=list)
    milestone_dates: Dict[str, str] = Field(default_factory=dict)


class RawRecordInput(BaseModel):
    """Input for raw record creation."""
    source: str
    source_id: str
    url: Optional[str] = None
    raw_json: Dict[str, Any]


class CandidateCreate(BaseModel):
    """Schema for creating new candidates."""
    venue_name: str
    legal_name: Optional[str] = None
    address: str
    suite: Optional[str] = None
    city: str
    state: str = "TX"
    zip_code: str
    county: str = "Harris"
    phone: Optional[str] = None
    email: Optional[str] = None
    source_flags: Dict[str, Optional[str]] = Field(default_factory=dict)


class LeadCreate(BaseModel):
    """Schema for creating new leads."""
    candidate_id: UUID
    status: str = "new"
    pitch_text: str
    how_to_pitch: str
    sms_text: Optional[str] = None


class HealthCheck(BaseModel):
    """Health check response."""
    status: str
    database: str
    model_server: Optional[str] = None
    timestamp: datetime
