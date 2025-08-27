from sqlalchemy import Column, String, DateTime, Float, Text, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, VARCHAR
from datetime import datetime
import uuid
import json

Base = declarative_base()


class UUID(TypeDecorator):
    """Platform-independent UUID type."""
    impl = VARCHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(VARCHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif isinstance(value, uuid.UUID):
            return str(value)
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            try:
                return uuid.UUID(value)
            except (ValueError, TypeError):
                # If it's not a valid UUID, generate a new one
                return uuid.uuid4()


class RawRecord(Base):
    """Raw data records from various sources."""
    __tablename__ = "raw_records"
    
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    source = Column(String(50), nullable=False, index=True)
    source_id = Column(String(200), nullable=True, index=True)
    url = Column(String(500))
    raw_json = Column(JSON)
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)


class Candidate(Base):
    """Normalized restaurant candidates."""
    __tablename__ = "candidates"
    
    candidate_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    venue_name = Column(String(200), nullable=False, index=True)
    legal_name = Column(String(200), index=True)
    address = Column(String(300), nullable=False, index=True)
    suite = Column(String(50))
    city = Column(String(100), nullable=False, index=True)
    state = Column(String(2), default="TX")
    zip_code = Column(String(10))
    county = Column(String(50), default="Harris")
    phone = Column(String(20))
    email = Column(String(200))
    source_flags = Column(JSON, default=dict)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    signals = relationship("Signal", back_populates="candidate")
    eta_inferences = relationship("ETAInference", back_populates="candidate")
    contacts = relationship("Contact", back_populates="candidate")
    leads = relationship("Lead", back_populates="candidate")


class Signal(Base):
    """Signals and milestones for candidates."""
    __tablename__ = "signals"
    
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    candidate_id = Column(UUID, ForeignKey("candidates.candidate_id"), nullable=False)
    tabc_status = Column(String(50))
    tabc_dates = Column(JSON, default=dict)
    health_status = Column(String(50))
    permit_types = Column(JSON, default=list)
    milestone_dates = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    candidate = relationship("Candidate", back_populates="signals")


class ETAInference(Base):
    """ETA predictions for restaurant openings."""
    __tablename__ = "eta_inferences"
    
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    candidate_id = Column(UUID, ForeignKey("candidates.candidate_id"), nullable=False)
    eta_start = Column(DateTime, nullable=False)
    eta_end = Column(DateTime, nullable=False)
    eta_days = Column(Float, nullable=False)
    confidence_0_1 = Column(Float, nullable=False)
    rationale_text = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    candidate = relationship("Candidate", back_populates="eta_inferences")


class Contact(Base):
    """Decision-maker contacts for venues."""
    __tablename__ = "contacts"
    
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    candidate_id = Column(UUID, ForeignKey("candidates.candidate_id"), nullable=False)
    full_name = Column(String(200), nullable=False)
    role = Column(String(50))  # owner|managing_member|operating_partner|gm|unknown
    email = Column(String(200))
    phone = Column(String(20))
    source = Column(String(50), nullable=False)  # tabc|comptroller|permit|site|pattern
    source_url = Column(String(500), nullable=False)
    provenance_text = Column(Text, nullable=False)
    confidence_0_1 = Column(Float, nullable=False)
    contactability = Column(JSON, default=dict)  # ok_to_email/call/sms flags
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    candidate = relationship("Candidate", back_populates="contacts")


class Lead(Base):
    """Qualified leads ready for sales outreach."""
    __tablename__ = "leads"
    
    lead_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    candidate_id = Column(UUID, ForeignKey("candidates.candidate_id"), nullable=False)
    status = Column(String(20), default="new", index=True)  # new|verified|sent
    pitch_text = Column(Text)
    how_to_pitch = Column(Text)
    sms_text = Column(String(160))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    candidate = relationship("Candidate", back_populates="leads")
