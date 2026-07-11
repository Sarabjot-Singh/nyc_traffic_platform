import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()


# Lightweight wrapper around SQLAlchemy for database access in the pipeline.
class Database:
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{os.getenv('POSTGRES_USER')}:"
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:"
            f"{os.getenv('POSTGRES_PORT')}/"
            f"{os.getenv('POSTGRES_DB')}",
            pool_pre_ping=True,
        )

    def execute(self, query: str, params: dict | None = None):
        # Run a SQL statement and return the underlying result handle.
        """
        Execute INSERT, UPDATE, DELETE or DDL statements.
        """
        try:
            with self.engine.begin() as conn:
                rs = conn.execute(text(query), params or {})
                return rs
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def fetch_one(self, query: str, params: dict | None = None):
        # Fetch a single row from the database.
        """
        Fetch a single row.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchone()
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def fetch_all(self, query: str, params: dict | None = None):
        # Fetch all matching rows from the database.
        """
        Fetch all rows.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def test_connection(self):
        # Check whether the configured database connection is available.
        """
        Returns True if connection succeeds.
        """
        try:
            with self.engine.connect():
                return True
        except SQLAlchemyError:
            return False