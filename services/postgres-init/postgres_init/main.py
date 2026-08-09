#!/usr/bin/env python3
"""Database Infrastructure Initialization Script.

This script:
1. Waits for PostgreSQL to become reachable
2. Imports shared SQLAlchemy models so metadata is registered
3. Creates shared database tables if they do not exist
4. Exits with code 1 if initialization fails (Fail Fast)
"""

import logging
import os
import sys
import time

from sqlalchemy.exc import SQLAlchemyError

from k8s_observability.persistence import Base, build_engine, test_connection
from k8s_observability.persistence.sqlachemy.tables import * # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def ensure_database_ready(
    database_url: str,
    max_retries: int = 30,
    retry_delay: float = 2.0,
) -> bool:
    """Wait until PostgreSQL is reachable."""
    logger.info("Building SQLAlchemy engine...")
    engine = build_engine(database_url)

    for attempt in range(max_retries):
        try:
            if test_connection(engine):
                logger.info("✓ PostgreSQL connection successful")
                return True
        except Exception as e:
            logger.warning(
                f"Database connection failed "
                f"(attempt {attempt + 1}/{max_retries}): {e}"
            )

        if attempt < max_retries - 1:
            time.sleep(retry_delay)

    return False


def ensure_schema_exists(database_url: str) -> bool:
    """Create all shared tables if they do not already exist."""
    try:
        logger.info("Creating shared database schema...")
        engine = build_engine(database_url)
        Base.metadata.create_all(bind=engine)
        logger.info("✓ Shared database schema initialized")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to create schema: {e}", exc_info=True)
        return False


def main() -> int:
    """Initialize shared database infrastructure."""
    try:
        database_url = os.environ["DATABASE_URL"]
        logger.info("Database init starting...")
        logger.info("Database URL detected")

        if not ensure_database_ready(
            database_url=database_url,
            max_retries=30,
            retry_delay=2.0,
        ):
            logger.error("❌ PostgreSQL did not become ready in time")
            return 1

        if not ensure_schema_exists(database_url):
            logger.error("❌ Failed to initialize shared database schema")
            return 1

        logger.info("✅ Database infrastructure initialization complete")
        return 0

    except KeyError:
        logger.error("❌ DATABASE_URL environment variable is required")
        return 1
    except Exception as e:
        logger.error(f"❌ Error during database initialization: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
