"""Database service for managing dsa_production connections and operations."""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from contextlib import contextmanager

from ..db.connection import DatabaseConnection
from ..config import AppConfig
from ..credentials.manager import CredentialsManager, DatabaseCredentials
from ..connector import SourceConnection
from ..exporters.normalized_postgres_exporter import NormalizedPostgreSQLExporter

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for managing dsa_production database operations."""
    
    def __init__(self, config: AppConfig):
        """Initialize database service.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self._production_connection: Optional[DatabaseConnection] = None
        self._credentials_manager: Optional[CredentialsManager] = None
        self._postgres_exporter: Optional[NormalizedPostgreSQLExporter] = None
    
    def get_production_connection(self) -> DatabaseConnection:
        """Get connection to dsa_production database.
        
        Returns:
            DatabaseConnection instance for dsa_production
        """
        if self._production_connection is None:
            connection_string = self.config.database.get_connection_string()
            self._production_connection = DatabaseConnection(connection_string)
            
            # Test connection
            if not self._production_connection.test_connection():
                raise ConnectionError("Failed to connect to dsa_production database")
            
            logger.info("Connected to dsa_production database")
        
        return self._production_connection
    
    @contextmanager
    def get_production_connection_with_schema(self):
        """Get production database connection with dsa_production schema in search path."""
        with self.get_production_connection().get_connection() as conn:
            # Set search path to include dsa_production schema
            with conn.cursor() as cur:
                cur.execute("SET search_path TO dsa_production, public")
                # Commit the search path change
                conn.commit()
            yield conn
    
    def get_credentials_manager(self) -> CredentialsManager:
        """Get credentials manager instance.
        
        Returns:
            CredentialsManager instance
        """
        if self._credentials_manager is None:
            production_connection = self.get_production_connection()
            self._credentials_manager = CredentialsManager(production_connection)
        
        return self._credentials_manager
    
    def get_credentials(self, connection_id: str = "test") -> Optional[DatabaseCredentials]:
        """Fetch credentials for a specific connection ID.
        
        Args:
            connection_id: Connection identifier
            
        Returns:
            DatabaseCredentials object or None if not found
        """
        try:
            credentials_manager = self.get_credentials_manager()
            return credentials_manager.get_credentials(connection_id)
        except Exception as e:
            logger.error(f"Failed to get credentials for {connection_id}: {e}")
            return None
    
    def create_source_connection(self, connection_id: str = "test") -> Optional[SourceConnection]:
        """Create a source connection from stored credentials.
        
        Args:
            connection_id: Connection identifier
            
        Returns:
            SourceConnection object or None if failed
        """
        credentials = self.get_credentials(connection_id)
        if not credentials:
            logger.error(f"No credentials found for connection_id: {connection_id}")
            return None
        
        # Build connection string based on source type
        if credentials.source_type.lower() == "postgresql":
            connection_string = f"postgresql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
        else:
            logger.error(f"Unsupported source type: {credentials.source_type}")
            return None
        
        return SourceConnection(
            source_type=credentials.source_type,
            connection_string=connection_string,
            credentials={
                'host': credentials.host,
                'port': credentials.port,
                'database_name': credentials.database_name,
                'username': credentials.username,
                'password': credentials.password,
                'ssl_mode': credentials.ssl_mode,
                'connection_name': connection_id  # Pass the actual connection_id
            }
        )
    
    def get_postgres_exporter(self) -> NormalizedPostgreSQLExporter:
        """Get normalized PostgreSQL exporter instance.
        
        Returns:
            NormalizedPostgreSQLExporter instance
        """
        if self._postgres_exporter is None:
            production_connection = self.get_production_connection()
            self._postgres_exporter = NormalizedPostgreSQLExporter(self.config, production_connection)
        
        return self._postgres_exporter
    
    def export_metadata(self, schemas: List[Any], sync_id: Optional[str] = None) -> Dict[str, Any]:
        """Export normalized metadata to PostgreSQL database.
        
        Args:
            schemas: List of normalized schema metadata objects
            sync_id: Optional sync ID for tracking
            
        Returns:
            Dictionary with export results
        """
        try:
            exporter = self.get_postgres_exporter()
            actual_sync_id = exporter.export_metadata(schemas, sync_id)
            
            return {
                'success': True,
                'sync_id': actual_sync_id,
                'message': f"PostgreSQL export: sync_id {actual_sync_id}"
            }
        except Exception as e:
            logger.error(f"PostgreSQL metadata export failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': f"PostgreSQL export failed: {e}"
            }
    
    def export_quality_metrics(self, metrics: Dict[str, List[Any]], sync_id: Optional[str] = None, 
                              connection_name: str = "test-connection", 
                              connector_name: str = "postgres", 
                              tenant_id: str = "default") -> Dict[str, Any]:
        """Export quality metrics to PostgreSQL database.
        
        Args:
            metrics: Dictionary of quality metrics
            sync_id: Optional sync ID for tracking
            connection_name: Name of the connection
            connector_name: Name of the connector
            tenant_id: Tenant identifier
            
        Returns:
            Dictionary with export results
        """
        try:
            exporter = self.get_postgres_exporter()
            actual_sync_id = exporter.export_quality_metrics(metrics, sync_id, connection_name, connector_name, tenant_id)
            
            return {
                'success': True,
                'sync_id': actual_sync_id,
                'message': f"PostgreSQL export: sync_id {actual_sync_id}"
            }
        except Exception as e:
            logger.error(f"PostgreSQL quality metrics export failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': f"PostgreSQL export failed: {e}"
            }
    
    def get_latest_metadata_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the latest metadata extraction run.
        
        Returns:
            Dictionary with latest run information or None
        """
        try:
            exporter = self.get_postgres_exporter()
            return exporter.get_latest_metadata_run()
        except Exception as e:
            logger.error(f"Failed to get latest metadata run: {e}")
            return None
    
    def get_latest_quality_metrics_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the latest quality metrics extraction run.
        
        Returns:
            Dictionary with latest run information or None
        """
        try:
            exporter = self.get_postgres_exporter()
            return exporter.get_latest_quality_metrics_run()
        except Exception as e:
            logger.error(f"Failed to get latest quality metrics run: {e}")
            return None
    
    def cleanup_old_metadata(self, days: int = 30) -> int:
        """Clean up old metadata records.
        
        Args:
            days: Number of days to keep metadata
            
        Returns:
            Number of records deleted
        """
        try:
            exporter = self.get_postgres_exporter()
            return exporter.cleanup_old_metadata(days)
        except Exception as e:
            logger.error(f"Failed to cleanup old metadata: {e}")
            return 0
    
    def test_connection(self) -> bool:
        """Test the dsa_production database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection = self.get_production_connection()
            return connection.test_connection()
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the dsa_production connection.
        
        Returns:
            Dictionary with connection information
        """
        try:
            connection = self.get_production_connection()
            return {
                'database': 'dsa_production',
                'host': self.config.database.host,
                'port': self.config.database.port,
                'status': 'connected' if self.test_connection() else 'disconnected',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'database': 'dsa_production',
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
