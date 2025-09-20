"""Base classes for data source connectors."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from ..config import AppConfig
from ..models.normalized_models import NormalizedSchema


@dataclass
class SourceConnection:
    """Generic source connection information."""
    source_type: str
    connection_string: str
    credentials: Dict[str, Any]


class BaseConnector(ABC):
    """Abstract base class for data source connectors."""
    
    def __init__(self, connection: SourceConnection, config: AppConfig, sync_id: str = ""):
        """Initialize connector.
        
        Args:
            connection: Source connection information
            config: Application configuration
            sync_id: Unique sync identifier for this extraction run
        """
        self.connection = connection
        self.config = config
        self.sync_id = sync_id
    
    @abstractmethod
    def extract_metadata(self, target_schemas: Optional[List[str]] = None) -> List[NormalizedSchema]:
        """Extract metadata for all schemas or specified schemas.
        
        Args:
            target_schemas: List of schema names to extract. If None, uses config schemas.
            
        Returns:
            List of schema metadata objects
        """
        pass
    
    @abstractmethod
    def extract_quality_metrics(self, schemas: List[str]) -> Dict[str, List[Any]]:
        """Extract quality metrics for all tables in specified schemas.
        
        Args:
            schemas: List of schema names
            
        Returns:
            Dictionary mapping schema names to lists of table metrics
        """
        pass
    
    @abstractmethod
    def get_available_schemas(self) -> List[str]:
        """Get list of available schemas in the source.
        
        Returns:
            List of schema names
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test the connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the connection.
        
        Returns:
            Dictionary with connection information
        """
        pass
