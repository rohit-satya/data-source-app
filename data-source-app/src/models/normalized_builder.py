"""Builder utility for creating normalized entities."""

import time
from typing import Dict, Any, Optional
from datetime import datetime

from .normalized_models import (
    NormalizedColumn, 
    NormalizedTable, 
    NormalizedSchema, 
    NormalizedDatabase
)


class NormalizedEntityBuilder:
    """Builder for creating normalized entities with proper qualified names and attributes."""
    
    def __init__(self, connection_name: str = "test-connection", tenant_id: str = "default", 
                 connector_name: str = "postgres", sync_id: str = ""):
        """Initialize the builder.
        
        Args:
            connection_name: Name of the connection
            tenant_id: Tenant identifier
            connector_name: Name of the connector
            sync_id: Unique sync identifier for this extraction run
        """
        self.connection_name = connection_name
        self.tenant_id = tenant_id
        self.connector_name = connector_name
        self.sync_id = sync_id
        self.sync_timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
    
    def _build_qualified_name(self, *parts: str) -> str:
        """Build qualified name from parts."""
        return "/".join([self.tenant_id, self.connector_name, *parts])
    
    def create_database(self, database_name: str) -> NormalizedDatabase:
        """Create a normalized database entity.
        
        Args:
            database_name: Name of the database
            
        Returns:
            NormalizedDatabase entity
        """
        qualified_name = self._build_qualified_name(database_name)
        connection_qualified_name = self._build_qualified_name()
        
        return NormalizedDatabase(
            name=database_name,
            connectionName=self.connection_name,
            tenantId=self.tenant_id,
            lastSyncRun=self.sync_id,
            lastSyncRunAt=self.sync_timestamp,
            connectorName=self.connector_name,
            attributes={
                "qualifiedName": qualified_name,
                "connectionQualifiedName": connection_qualified_name,
                "databaseName": database_name,
                "databaseQualifiedName": qualified_name
            },
            customAttributes={}
        )
    
    def create_schema(self, database_name: str, schema_name: str) -> NormalizedSchema:
        """Create a normalized schema entity.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            
        Returns:
            NormalizedSchema entity
        """
        qualified_name = self._build_qualified_name(database_name, schema_name)
        connection_qualified_name = self._build_qualified_name()
        database_qualified_name = self._build_qualified_name(database_name)
        
        return NormalizedSchema(
            name=schema_name,
            connectionName=self.connection_name,
            tenantId=self.tenant_id,
            lastSyncRun=self.sync_id,
            lastSyncRunAt=self.sync_timestamp,
            connectorName=self.connector_name,
            attributes={
                "qualifiedName": qualified_name,
                "connectionQualifiedName": connection_qualified_name,
                "databaseName": database_name,
                "databaseQualifiedName": database_qualified_name,
                "schemaName": schema_name,
                "schemaQualifiedName": qualified_name,
                "database": {
                    "typeName": "Database",
                    "attributes": {},
                    "uniqueAttributes": {
                        "qualifiedName": database_qualified_name
                    }
                }
            },
            customAttributes={}
        )
    
    def create_table(self, database_name: str, schema_name: str, table_name: str, 
                    table_type: str = "BASE TABLE") -> NormalizedTable:
        """Create a normalized table entity.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table
            table_type: Type of the table
            
        Returns:
            NormalizedTable entity
        """
        qualified_name = self._build_qualified_name(database_name, schema_name, table_name)
        connection_qualified_name = self._build_qualified_name()
        database_qualified_name = self._build_qualified_name(database_name)
        schema_qualified_name = self._build_qualified_name(database_name, schema_name)
        
        return NormalizedTable(
            name=table_name,
            connectionName=self.connection_name,
            tenantId=self.tenant_id,
            lastSyncRun=self.sync_id,
            lastSyncRunAt=self.sync_timestamp,
            connectorName=self.connector_name,
            attributes={
                "qualifiedName": qualified_name,
                "connectionQualifiedName": connection_qualified_name,
                "databaseName": database_name,
                "databaseQualifiedName": database_qualified_name,
                "schemaName": schema_name,
                "schemaQualifiedName": schema_qualified_name
            },
            customAttributes={
                "table_type": table_type,
                "is_insertable_into": "YES",
                "is_typed": "NO",
                "self_referencing_col_name": ""
            }
        )
    
    def create_column(self, database_name: str, schema_name: str, table_name: str, 
                     column_name: str, data_type: str, is_nullable: bool = True,
                     ordinal_position: int = 1, **kwargs) -> NormalizedColumn:
        """Create a normalized column entity.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table
            column_name: Name of the column
            data_type: Data type of the column
            is_nullable: Whether the column is nullable
            ordinal_position: Position of the column
            **kwargs: Additional column attributes
            
        Returns:
            NormalizedColumn entity
        """
        qualified_name = self._build_qualified_name(database_name, schema_name, table_name, column_name)
        connection_qualified_name = self._build_qualified_name()
        database_qualified_name = self._build_qualified_name(database_name)
        schema_qualified_name = self._build_qualified_name(database_name, schema_name)
        table_qualified_name = self._build_qualified_name(database_name, schema_name, table_name)
        
        # Build custom attributes from kwargs
        custom_attributes = {
            "ordinal_position": ordinal_position,
            "is_self_referencing": "NO",
            "type_name": data_type,
            "is_generated": "NEVER",
            "is_identity": "NO",
            "identity_cycle": "NO"
        }
        custom_attributes.update(kwargs)
        
        return NormalizedColumn(
            name=column_name,
            connectionName=self.connection_name,
            tenantId=self.tenant_id,
            lastSyncRun=self.sync_id,
            lastSyncRunAt=self.sync_timestamp,
            connectorName=self.connector_name,
            attributes={
                "qualifiedName": qualified_name,
                "connectionQualifiedName": connection_qualified_name,
                "databaseName": database_name,
                "databaseQualifiedName": database_qualified_name,
                "schemaName": schema_name,
                "schemaQualifiedName": schema_qualified_name,
                "tableName": table_name,
                "tableQualifiedName": table_qualified_name,
                "dataType": data_type,
                "isNullable": is_nullable,
                "order": ordinal_position
            },
            customAttributes=custom_attributes
        )
