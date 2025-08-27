from sqlalchemy import create_engine, Column, String, DateTime, Float, Text, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.pool import StaticPool
from contextlib import contextmanager
from typing import Generator
from datetime import datetime

from .models import Base
from .settings import settings


class DatabaseManager:
    """Database connection and session management with performance optimizations."""
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or settings.db_url
        
        # Configure engine based on database type with optimized connection pooling
        if self.db_url.startswith("sqlite"):
            self.engine = create_engine(
                self.db_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                echo=settings.env == "dev"
            )
        else:
            self.engine = create_engine(
                self.db_url, 
                echo=settings.env == "dev",
                pool_size=10,  # Connection pool size
                max_overflow=20,  # Max overflow connections
                pool_recycle=3600  # Recycle connections every hour
            )
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(bind=self.engine)
    
    def create_indexes(self):
        """Create performance indexes on existing tables."""
        with self.engine.connect() as conn:
            # Create indexes for better query performance
            indexes = [
                # Candidate table indexes
                "CREATE INDEX IF NOT EXISTS idx_candidates_zip_code ON candidates(zip_code);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_county ON candidates(county);", 
                "CREATE INDEX IF NOT EXISTS idx_candidates_phone ON candidates(phone);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_email ON candidates(email);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_first_seen ON candidates(first_seen);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_last_seen ON candidates(last_seen);",
                
                # Contact table indexes
                "CREATE INDEX IF NOT EXISTS idx_contacts_full_name ON contacts(full_name);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_role ON contacts(role);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_confidence ON contacts(confidence_0_1);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_created_at ON contacts(created_at);",
                
                # ETA Inference table indexes
                "CREATE INDEX IF NOT EXISTS idx_eta_inferences_eta_days ON eta_inferences(eta_days);",
                "CREATE INDEX IF NOT EXISTS idx_eta_inferences_confidence ON eta_inferences(confidence_0_1);",
                "CREATE INDEX IF NOT EXISTS idx_eta_inferences_created_at ON eta_inferences(created_at);",
                
                # Lead table indexes
                "CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at);",
                "CREATE INDEX IF NOT EXISTS idx_leads_updated_at ON leads(updated_at);",
            ]
            
            for index_sql in indexes:
                try:
                    conn.execute(index_sql)
                    conn.commit()
                except Exception as e:
                    print(f"Warning: Could not create index: {e}")
                    conn.rollback()
    
    def drop_tables(self):
        """Drop all database tables."""
        Base.metadata.drop_all(bind=self.engine)
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_session_sync(self) -> Session:
        """Get database session for dependency injection."""
        return self.SessionLocal()


# Global database manager
db_manager = DatabaseManager()


def get_db() -> Generator[Session, None, None]:
    """Dependency for FastAPI to get database session."""
    session = db_manager.get_session_sync()
    try:
        yield session
    finally:
        session.close()


def init_db():
    """Initialize database tables and apply performance optimizations."""
    db_manager.create_tables()
    db_manager.create_indexes()
    print("✅ Database initialized with performance optimizations")


def optimize_db():
    """Apply database optimizations to existing database."""
    db_manager.create_indexes()
    print("✅ Database performance optimizations applied")
