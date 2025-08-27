from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from contextlib import contextmanager
from typing import Generator

from .models import Base
from .settings import settings


class DatabaseManager:
    """Database connection and session management."""
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or settings.db_url
        
        # Configure engine based on database type
        if self.db_url.startswith("sqlite"):
            self.engine = create_engine(
                self.db_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                echo=settings.env == "dev"
            )
        else:
            self.engine = create_engine(self.db_url, echo=settings.env == "dev")
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(bind=self.engine)
    
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
    """Initialize database tables."""
    db_manager.create_tables()
