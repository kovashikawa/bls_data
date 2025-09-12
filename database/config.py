# bls_data/database/config.py

import os
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from bls_logging.config import get_logger

Base = declarative_base()
log = get_logger(__name__)


class DatabaseConfig:
    def __init__(self, database_url: str = None):
        if database_url:
            self.database_url = database_url
        else:
            # Build database URL from environment variables
            db_user = os.getenv("DB_USER", "postgres")
            db_pass = os.getenv("DB_PASS", "password")
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME", "bls_data")

            self.database_url = (
                f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            )
        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,  # Set to True for SQL debugging
        )
        self.SessionLocal = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)

    def check_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
