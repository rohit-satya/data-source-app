"""Factory for creating source-specific connectors."""

import logging
from typing import Optional

from .base_connector import BaseConnector, SourceConnection
from .postgres.postgres_connector import PostgreSQLConnector
from ..config import AppConfig

logger = logging.getLogger(__name__)


class ConnectorFactory:
    """Factory for creating source-specific connectors."""
    
    _connectors = {
        'postgresql': PostgreSQLConnector,
    }
    
    @classmethod
    def create_connector(cls, connection: SourceConnection, config: AppConfig) -> Optional[BaseConnector]:
        """Create a connector for the specified source type.
        
        Args:
            connection: Source connection information
            config: Application configuration
            
        Returns:
            Connector instance or None if source type not supported
        """
        source_type = connection.source_type.lower()
        
        if source_type not in cls._connectors:
            logger.error(f"Unsupported source type: {source_type}")
            return None
        
        connector_class = cls._connectors[source_type]
        return connector_class(connection, config)
    
    @classmethod
    def get_supported_source_types(cls) -> list:
        """Get list of supported source types.
        
        Returns:
            List of supported source type strings
        """
        return list(cls._connectors.keys())
    
    @classmethod
    def register_connector(cls, source_type: str, connector_class: type):
        """Register a new connector type.
        
        Args:
            source_type: Source type identifier
            connector_class: Connector class that implements BaseConnector
        """
        if not issubclass(connector_class, BaseConnector):
            raise ValueError(f"Connector class must inherit from BaseConnector")
        
        cls._connectors[source_type.lower()] = connector_class
        logger.info(f"Registered connector for source type: {source_type}")
