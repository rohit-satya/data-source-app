# Adding New Data Source Connectors

This document explains how to add support for new data source types using the connector package architecture.

## Architecture Overview

The application now uses a connector-based architecture that separates the connection logic from the data extraction logic:

- **Base Connector** (`BaseConnector`): Abstract interface for all data source connectors
- **Source Implementation** (`{source}_source.py`): Contains the actual metadata and quality metrics extraction logic
- **Connector Implementation** (`{source}_connector.py`): Wraps the source implementation and provides the connector interface
- **Factory Pattern** (`ConnectorFactory`): Creates appropriate connectors based on source type

## Directory Structure

```
src/connector/
├── __init__.py
├── base_connector.py          # Base classes and interfaces
├── connector_factory.py       # Factory for creating connectors
└── postgres/                  # PostgreSQL connector package
    ├── __init__.py
    ├── postgres_connector.py  # PostgreSQL connector implementation
    └── postgres_source.py     # PostgreSQL source implementation
```

## Steps to Add a New Source Type

### 1. Create Source Package Directory

Create a new directory for your source type:
```bash
mkdir src/connector/{source_type}
```

### 2. Create Source Implementation

Create `src/connector/{source_type}/{source_type}_source.py`:

```python
"""MySQL source implementation for metadata and quality metrics extraction."""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from ...config import AppConfig

logger = logging.getLogger(__name__)

# Define your data structures
@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    is_nullable: bool
    # ... other fields

@dataclass
class TableMetadata:
    name: str
    schema: str
    columns: List[ColumnMetadata]
    # ... other fields

@dataclass
class SchemaMetadata:
    name: str
    tables: List[TableMetadata]

@dataclass
class ColumnQualityMetrics:
    column_name: str
    total_count: int
    null_percentage: float
    # ... other fields

@dataclass
class TableQualityMetrics:
    schema_name: str
    table_name: str
    column_metrics: List[ColumnQualityMetrics]

class MySQLSource:
    """MySQL source implementation for metadata and quality metrics extraction."""
    
    def __init__(self, connection, config: AppConfig):
        """Initialize MySQL source.
        
        Args:
            connection: Database connection instance
            config: Application configuration
        """
        self.connection = connection
        self.config = config
    
    def extract_all_metadata(self, target_schemas: Optional[List[str]] = None) -> List[SchemaMetadata]:
        """Extract metadata for all schemas or specified schemas.
        
        This is where you implement the actual metadata extraction logic
        specific to your database type.
        """
        # Your implementation here
        pass
    
    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """Extract metadata for a specific schema."""
        # Your implementation here
        pass
    
    def extract_all_quality_metrics(self, schemas: List[str]) -> Dict[str, List[TableQualityMetrics]]:
        """Extract quality metrics for all tables in specified schemas."""
        # Your implementation here
        pass
    
    def extract_table_quality_metrics(self, schema_name: str, table_name: str) -> TableQualityMetrics:
        """Extract quality metrics for a specific table."""
        # Your implementation here
        pass
```

### 3. Create Connector Implementation

Create `src/connector/{source_type}/{source_type}_connector.py`:

```python
"""MySQL connector implementation."""

import logging
from typing import Dict, List, Any, Optional

from ..base_connector import BaseConnector, SourceConnection
from .mysql_source import MySQLSource, SchemaMetadata, TableQualityMetrics
from ...config import AppConfig

logger = logging.getLogger(__name__)

class MySQLConnector(BaseConnector):
    """MySQL connector implementation."""
    
    def __init__(self, connection: SourceConnection, config: AppConfig):
        """Initialize MySQL connector."""
        super().__init__(connection, config)
        # Initialize your database connection here
        # self.db_connection = MySQLConnection(connection.connection_string)
        self.source = MySQLSource(self.db_connection, config)
    
    def extract_metadata(self, target_schemas: Optional[List[str]] = None) -> List[SchemaMetadata]:
        """Extract metadata for all schemas or specified schemas."""
        return self.source.extract_all_metadata(target_schemas)
    
    def extract_quality_metrics(self, schemas: List[str]) -> Dict[str, List[TableQualityMetrics]]:
        """Extract quality metrics for all tables in specified schemas."""
        return self.source.extract_all_quality_metrics(schemas)
    
    def get_available_schemas(self) -> List[str]:
        """Get list of available schemas in the source."""
        # Your implementation here
        pass
    
    def test_connection(self) -> bool:
        """Test the connection to the data source."""
        # Your implementation here
        pass
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the connection."""
        # Your implementation here
        pass
```

### 4. Create Package Init File

Create `src/connector/{source_type}/__init__.py`:

```python
"""MySQL connector package."""

from .mysql_connector import MySQLConnector
from .mysql_source import MySQLSource

__all__ = ['MySQLConnector', 'MySQLSource']
```

### 5. Register with Factory

Update `src/connector/connector_factory.py`:

```python
from .mysql.mysql_connector import MySQLConnector

class ConnectorFactory:
    _connectors = {
        'postgresql': PostgreSQLConnector,
        'mysql': MySQLConnector,  # Add your new connector
    }
```

### 6. Update Connection String Building

In `src/app.py`, update the `get_source_connection` function:

```python
# Build connection string based on source type
if credentials.source_type.lower() == "postgresql":
    connection_string = f"postgresql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
elif credentials.source_type.lower() == "mysql":
    connection_string = f"mysql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
else:
    console.print(f"❌ Unsupported source type: {credentials.source_type}", style="red")
    return None
```

### 7. Test Your Implementation

1. Add credentials for your new source type:
   ```bash
   python -m src.app credentials-add --connection-id mysql-test --host localhost --port 3306 --database testdb --username root --password password
   ```

2. Test metadata extraction:
   ```bash
   python -m src.app scan --connection-id mysql-test
   ```

3. Test quality metrics extraction:
   ```bash
   python -m src.app quality-metrics --connection-id mysql-test
   ```

## Key Benefits of This Architecture

### Separation of Concerns
- **Connector**: Handles connection management and provides the interface
- **Source**: Contains the actual data extraction logic
- **Factory**: Manages connector creation and registration

### User-Defined Extraction Logic
Users can implement their own extraction logic by:
1. Creating a new source class that implements the extraction methods
2. Creating a connector that wraps the source
3. Registering the connector with the factory

### Easy Extension
- Add new source types by creating new packages
- Each source type is self-contained
- No changes needed to the main application logic

### Consistent Interface
- All connectors implement the same `BaseConnector` interface
- Application code doesn't need to know about specific source types
- Easy to add new methods to the base interface

## Example: Adding SQL Server Support

1. Create `src/connector/sqlserver/` directory
2. Implement `SQLServerSource` with extraction logic
3. Implement `SQLServerConnector` that wraps the source
4. Register with `ConnectorFactory`
5. Update connection string building in `app.py`

The key is that all the actual metadata and quality metrics extraction logic goes in the `{source}_source.py` file, while the connector just provides the interface and manages the connection.
