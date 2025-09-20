"""Database connection management."""

import psycopg
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """PostgreSQL database connection manager."""
    
    def __init__(self, connection_string: str):
        """Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self._connection: Optional[psycopg.Connection] = None
    
    @contextmanager
    def get_connection(self):
        """Get database connection as context manager."""
        connection = None
        try:
            connection = psycopg.connect(self.connection_string)
            yield connection
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_server_version(self) -> Optional[str]:
        """Get PostgreSQL server version.
        
        Returns:
            Server version string or None if failed
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    result = cur.fetchone()
                    return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get server version: {e}")
            return None
    
    def get_available_schemas(self) -> list[str]:
        """Get list of available schemas.
        
        Returns:
            List of schema names
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        ORDER BY schema_name
                    """)
                    results = cur.fetchall()
                    return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Failed to get schemas: {e}")
            return []

