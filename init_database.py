#!/usr/bin/env python3
"""Initialize database for restaurant leads MVP."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db import init_db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        logger.info("Initializing database...")
        init_db()
        logger.info("Database initialization completed successfully!")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
