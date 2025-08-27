from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from datetime import datetime

from .settings import settings
from .db import init_db
from .api.routes import router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Initializing database...")
    init_db()
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Restaurant Opening Leads MVP",
    description="AI-powered pipeline for predicting restaurant openings in Harris County, TX",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Restaurant Opening Leads MVP",
        "version": "1.0.0",
        "environment": settings.env,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    from .schemas import HealthCheck
    from .db import db_manager
    
    # Check database connectivity
    try:
        with db_manager.get_session() as session:
            session.execute("SELECT 1")
            db_status = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    # Check model server connectivity (optional)
    model_status = None
    try:
        import requests
        response = requests.get(f"{settings.vllm_base_url}/health", timeout=5)
        if response.status_code == 200:
            model_status = "healthy"
        else:
            model_status = f"http_{response.status_code}"
    except Exception as e:
        model_status = f"error: {str(e)}"
    
    return HealthCheck(
        status="healthy" if db_status == "healthy" else "degraded",
        database=db_status,
        model_server=model_status,
        timestamp=datetime.now()
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8080,
        reload=settings.env == "dev"
    )
