"""Normalized data models for metadata extraction."""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class NormalizedColumn:
    """Normalized column metadata following the new structure."""
    typeName: str = "Column"
    status: str = "ACTIVE"
    name: str = ""
    connectionName: str = ""
    tenantId: str = "default"
    lastSyncRun: str = ""
    lastSyncRunAt: int = 0
    connectorName: str = ""
    attributes: Dict[str, Any] = None
    customAttributes: Dict[str, Any] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}
        if self.customAttributes is None:
            self.customAttributes = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "typeName": self.typeName,
            "status": self.status,
            "name": self.name,
            "connectionName": self.connectionName,
            "tenantId": self.tenantId,
            "lastSyncRun": self.lastSyncRun,
            "lastSyncRunAt": self.lastSyncRunAt,
            "connectorName": self.connectorName,
            "attributes": self.attributes,
            "customAttributes": self.customAttributes
        }


@dataclass
class NormalizedTable:
    """Normalized table metadata following the new structure."""
    typeName: str = "Table"
    status: str = "ACTIVE"
    name: str = ""
    connectionName: str = ""
    tenantId: str = "default"
    lastSyncRun: str = ""
    lastSyncRunAt: int = 0
    connectorName: str = ""
    attributes: Dict[str, Any] = None
    customAttributes: Dict[str, Any] = None
    columns: List[NormalizedColumn] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}
        if self.customAttributes is None:
            self.customAttributes = {}
        if self.columns is None:
            self.columns = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        result = {
            "typeName": self.typeName,
            "status": self.status,
            "name": self.name,
            "connectionName": self.connectionName,
            "tenantId": self.tenantId,
            "lastSyncRun": self.lastSyncRun,
            "lastSyncRunAt": self.lastSyncRunAt,
            "connectorName": self.connectorName,
            "attributes": self.attributes,
            "customAttributes": self.customAttributes
        }
        
        # Include columns if they exist
        if self.columns:
            result['columns'] = [column.to_dict() for column in self.columns]
        
        return result


@dataclass
class NormalizedSchema:
    """Normalized schema metadata following the new structure."""
    typeName: str = "Schema"
    status: str = "ACTIVE"
    name: str = ""
    connectionName: str = ""
    tenantId: str = "default"
    lastSyncRun: str = ""
    lastSyncRunAt: int = 0
    connectorName: str = ""
    attributes: Dict[str, Any] = None
    customAttributes: Dict[str, Any] = None
    tables: List[NormalizedTable] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}
        if self.customAttributes is None:
            self.customAttributes = {}
        if self.tables is None:
            self.tables = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        result = {
            "typeName": self.typeName,
            "status": self.status,
            "name": self.name,
            "connectionName": self.connectionName,
            "tenantId": self.tenantId,
            "lastSyncRun": self.lastSyncRun,
            "lastSyncRunAt": self.lastSyncRunAt,
            "connectorName": self.connectorName,
            "attributes": self.attributes,
            "customAttributes": self.customAttributes
        }
        
        # Include tables if they exist
        if self.tables:
            result['tables'] = [table.to_dict() for table in self.tables]
        
        return result


@dataclass
class NormalizedDatabase:
    """Normalized database metadata following the new structure."""
    typeName: str = "Database"
    status: str = "ACTIVE"
    name: str = ""
    connectionName: str = ""
    tenantId: str = "default"
    lastSyncRun: str = ""
    lastSyncRunAt: int = 0
    connectorName: str = ""
    attributes: Dict[str, Any] = None
    customAttributes: Dict[str, Any] = None
    schemas: List[NormalizedSchema] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}
        if self.customAttributes is None:
            self.customAttributes = {}
        if self.schemas is None:
            self.schemas = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "typeName": self.typeName,
            "status": self.status,
            "attributes": self.attributes,
            "customAttributes": self.customAttributes
        }


@dataclass
class ColumnQualityMetrics:
    """Quality metrics for a specific column."""
    column_name: str
    total_count: int
    non_null_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    top_values: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.top_values is None:
            self.top_values = []


@dataclass
class TableQualityMetrics:
    """Quality metrics for a specific table."""
    schema_name: str
    table_name: str
    row_count: int
    column_metrics: List[ColumnQualityMetrics] = None

    def __post_init__(self):
        if self.column_metrics is None:
            self.column_metrics = []
