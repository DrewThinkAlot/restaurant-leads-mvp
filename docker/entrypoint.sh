#!/bin/bash
set -e

# Wait for model server to be available
wait_for_model_server() {
    echo "Waiting for model server at $VLLM_BASE_URL..."
    
    for i in {1..30}; do
        if curl -s --fail "$VLLM_BASE_URL/health" > /dev/null 2>&1; then
            echo "Model server is ready!"
            return 0
        fi
        echo "Waiting for model server... ($i/30)"
        sleep 2
    done
    
    echo "Warning: Model server not available at $VLLM_BASE_URL"
    echo "Continuing without model server checks..."
}

# Initialize database
init_database() {
    echo "Initializing database..."
    python3 -c "from app.db import init_db; init_db()"
    echo "Database initialized successfully"
}

# Main execution based on command
case "${1:-api}" in
    api)
        echo "Starting Restaurant Leads MVP API..."
        wait_for_model_server
        init_database
        exec uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1
        ;;
    
    pipeline)
        echo "Running pipeline once..."
        wait_for_model_server
        init_database
        python3 -c "
from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
import json

runner = EnhancedPipelineRunner()
result = runner.run_hybrid_pipeline(max_candidates=50)
print(json.dumps(result, indent=2))
"
        ;;
    
    worker)
        echo "Starting background worker..."
        wait_for_model_server
        init_database
        # Could implement Celery or similar here
        exec python3 -c "
import time
import logging
from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

runner = EnhancedPipelineRunner()

while True:
    try:
        logger.info('Running enhanced hybrid pipeline update...')
        result = runner.run_hybrid_pipeline(max_candidates=200)
        logger.info(f'Update complete: {result[\"qualified_leads\"]} leads')
    except Exception as e:
        logger.error(f'Pipeline update failed: {e}')
    
    # Wait 1 hour between runs
    time.sleep(3600)
"
        ;;
    
    test)
        echo "Running tests..."
        init_database
        exec python3 -m pytest app/tests/ -v
        ;;
    
    shell)
        echo "Starting interactive shell..."
        init_database
        exec python3 -c "
from app.db import db_manager
from app.models import *
from app.pipelines.run_pipeline import PipelineRunner
print('Available: db_manager, models (Candidate, Lead, etc.), PipelineRunner')
import IPython; IPython.embed()
"
        ;;
    
    *)
        echo "Usage: $0 [api|pipeline|worker|test|shell]"
        echo "  api     - Start FastAPI server (default)"
        echo "  pipeline - Run pipeline once and exit"
        echo "  worker  - Start background pipeline worker"
        echo "  test    - Run test suite"
        echo "  shell   - Interactive Python shell"
        exit 1
        ;;
esac
