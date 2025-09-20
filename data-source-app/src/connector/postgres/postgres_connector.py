"""PostgreSQL connector implementation."""

import logging
from typing import Dict, List, Any, Optional

from ..base_connector import BaseConnector, SourceConnection
from .postgres_source import PostgreSQLSource
from ...models.normalized_models import NormalizedSchema, TableQualityMetrics
from ...db.connection import DatabaseConnection
from ...config import AppConfig

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL connector implementation."""
    
    def __init__(self, connection: SourceConnection, config: AppConfig, sync_id: str = ""):
        """Initialize PostgreSQL connector.
        
        Args:
            connection: Source connection information
            config: Application configuration
            sync_id: Unique sync identifier for this extraction run
        """
        super().__init__(connection, config, sync_id)
        self.db_connection = DatabaseConnection(connection.connection_string)
        
        self.source = PostgreSQLSource(
            self.db_connection, 
            config, 
            connection_name=connection.credentials.get('connection_name', 'test-connection'),
            sync_id=sync_id
        )
    
    def extract_metadata(self, target_schemas: Optional[List[str]] = None) -> List[NormalizedSchema]:
        """Extract metadata for all schemas or specified schemas.
        
        Args:
            target_schemas: List of schema names to extract. If None, uses config schemas.
            
        Returns:
            List of schema metadata objects
        """
        return self.source.extract_all_metadata(target_schemas)
    
    def extract_quality_metrics(self, schemas: List[str]) -> Dict[str, List[TableQualityMetrics]]:
        """Extract quality metrics for all tables in specified schemas.
        
        Args:
            schemas: List of schema names
            
        Returns:
            Dictionary mapping schema names to lists of table metrics
        """
        return self.source.extract_all_quality_metrics(schemas)
    
    def get_available_schemas(self) -> List[str]:
        """Get list of available schemas in the source.
        
        Returns:
            List of schema names
        """
        return self.db_connection.get_available_schemas()
    
    def test_connection(self) -> bool:
        """Test the connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        return self.db_connection.test_connection()
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the connection.
        
        Returns:
            Dictionary with connection information
        """
        try:
            server_version = self.db_connection.get_server_version()
            return {
                'source_type': self.connection.source_type,
                'host': self.connection.credentials.get('host'),
                'port': self.connection.credentials.get('port'),
                'database': self.connection.credentials.get('database_name'),
                'username': self.connection.credentials.get('username'),
                'server_version': server_version,
                'connection_status': 'connected' if self.test_connection() else 'disconnected'
            }
        except Exception as e:
            logger.error(f"Failed to get connection info: {e}")
            return {
                'source_type': self.connection.source_type,
                'connection_status': 'error',
                'error': str(e)
            }
