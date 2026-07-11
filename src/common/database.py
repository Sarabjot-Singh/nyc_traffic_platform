import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()


# Lightweight wrapper around SQLAlchemy for database access in the pipeline.
class Database:
    """Simple database wrapper for executing SQL statements through SQLAlchemy."""

    def __init__(self):
        """Create a SQLAlchemy engine using the configured PostgreSQL connection values."""
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
        """Execute a SQL statement and return the result handle.

        Args:
            query: SQL text to execute.
            params: Optional bind parameters for the query.

        Returns:
            SQLAlchemy result object for the executed statement.
        """
        # Run a SQL statement and return the underlying result handle.
        try:
            with self.engine.begin() as conn:
                rs = conn.execute(text(query), params or {})
                return rs
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def fetch_one(self, query: str, params: dict | None = None):
        """Fetch a single row from the database.

        Args:
            query: SQL text to execute.
            params: Optional bind parameters for the query.

        Returns:
            A single database row or None if no row is found.
        """
        # Fetch a single row from the database.
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchone()
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def fetch_all(self, query: str, params: dict | None = None):
        """Fetch all rows returned by a SQL query.

        Args:
            query: SQL text to execute.
            params: Optional bind parameters for the query.

        Returns:
            A list-like result containing all matching rows.
        """
        # Fetch all matching rows from the database.
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            raise Exception(f"Database Error: {e}")

    def test_connection(self):
        """Check whether the configured database connection is available.

        Returns:
            bool: True if the connection succeeds, otherwise False.
        """
        # Check whether the configured database connection is available.
        try:
            with self.engine.connect():
                return True
        except SQLAlchemyError:
            return False