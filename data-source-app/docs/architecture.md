# Data Source App Architecture

## Overview

The Data Source App is a comprehensive metadata extraction and analysis platform designed to extract, store, and analyze database metadata from various data sources. The application follows a modular, connector-based architecture that supports multiple database types and provides rich metadata analysis capabilities.

## Core Components

### 1. Scan Command
**Purpose**: Extracts comprehensive metadata from database schemas

**Architecture Flow**:
```
CLI Command → AppConfig → ConnectorFactory → Source Connector → Metadata Extraction → Normalized Export
```

**Key Components**:
- **CLI Interface** (`src/app.py`): Typer-based command-line interface
- **Configuration Management** (`src/config.py`): YAML-based configuration with environment variable support
- **Connector Factory** (`src/connector/connector_factory.py`): Creates appropriate connectors based on source type
- **Source Connectors** (`src/connector/{source}/`): Database-specific extraction logic
- **Normalized Builder** (`src/models/normalized_builder.py`): Creates standardized metadata entities
- **Exporters** (`src/exporters/`): Multiple export formats (JSON, CSV, PostgreSQL)

**Data Flow**:
1. **Connection Setup**: Loads credentials from `dsa_production.credentials` table
2. **Schema Discovery**: Identifies target schemas to scan
3. **Metadata Extraction**: Extracts schemas, tables, columns, constraints, indexes
4. **Normalization**: Converts to standardized entity format
5. **Export**: Stores in `dsa_production` schema with normalized tables

**Database Schema**:
- `normalized_schemas`: Schema-level metadata
- `normalized_tables`: Table-level metadata with relationships
- `normalized_columns`: Column-level metadata with constraints

### 2. Quality Metrics Command
**Purpose**: Analyzes data quality characteristics of database tables and columns

**Architecture Flow**:
```
CLI Command → AppConfig → ConnectorFactory → Quality Metrics Extraction → Database Storage
```

**Key Components**:
- **Quality Metrics Extractor** (`src/extractor/quality_metrics.py`): Core quality analysis logic
- **Table Quality Metrics**: Row counts, column counts, data quality indicators
- **Column Quality Metrics**: Null percentages, distinct counts, top values
- **Quality Scoring**: Automated quality scoring based on data characteristics

**Metrics Collected**:
- **Table Level**: Row count, column count, quality indicators
- **Column Level**: 
  - Total count and null count/percentage
  - Distinct count and distinct percentage
  - Top-K most frequent values
  - Data quality scoring

**Database Schema**:
- `quality_metrics_runs`: Tracks quality extraction runs
- `table_quality_metrics`: Table-level quality data
- `column_quality_metrics`: Column-level quality data
- `column_top_values`: Most frequent values per column

### 3. Incremental Diff Command
**Purpose**: Compares metadata between two sync runs to identify changes

**Architecture Flow**:
```
CLI Command → IncrementalDiffService → Metadata Comparison → Diff Storage
```

**Key Components**:
- **Incremental Diff Service** (`src/services/incremental_diff_service.py`): Core diff logic
- **Diff Calculation**: Compares assets between sync runs
- **Change Detection**: Identifies added, removed, modified, and unchanged entities
- **Diff Storage**: Stores differences in dedicated tables

**Diff Types**:
- **Schema Diffs**: Schema-level changes
- **Table Diffs**: Table structure and metadata changes
- **Column Diffs**: Column definition and constraint changes

**Database Schema**:
- `diff_sync_runs`: Tracks diff operations
- `incremental_diff_schema`: Schema-level differences
- `incremental_diff_table`: Table-level differences
- `incremental_diff_column`: Column-level differences
- `diff_summary`: Aggregated diff statistics

### 4. Frontend
**Purpose**: Web-based interface for viewing and analyzing metadata

**Architecture Flow**:
```
Flask App → Database Queries → Template Rendering → Web Interface
```

**Key Components**:
- **Flask Application** (`frontend/app.py`): Web server and routing
- **Database Views**: Pre-built views for latest metadata
- **Template Engine**: Jinja2-based HTML templates
- **Metadata Display**: Structured display of schemas, tables, and columns

**Features**:
- **Connection Selection**: Choose connection to analyze
- **Schema Overview**: High-level schema information
- **Table Details**: Detailed table metadata with relationships
- **Column Analysis**: Column-level details with quality metrics
- **Real-time Data**: Queries latest metadata from database

**Database Views**:
- `latest_schema_metadata`: Most recent schema data
- `latest_table_metadata`: Most recent table data
- `latest_column_metadata`: Most recent column data
- `latest_column_quality_metrics`: Latest quality metrics

## Data Architecture

### Normalized Entity Model
The application uses a standardized entity model for consistent metadata representation:

**Schema Entity**:
- Basic info: name, connection, tenant, sync details
- Attributes: qualified names, database info
- Custom attributes: owner, type, timestamps

**Table Entity**:
- Basic info: name, connection, tenant, sync details
- Attributes: qualified names, schema info
- Custom attributes: type, constraints, relationships, partitioning

**Column Entity**:
- Basic info: name, connection, tenant, sync details
- Attributes: qualified names, data type, constraints
- Custom attributes: position, defaults, constraints, indexing

### Database Schema Design

**Production Schema** (`dsa_production`):
- **Normalized Tables**: Store metadata in standardized format
- **Quality Metrics**: Store data quality analysis results
- **Sync Tracking**: Track extraction runs and status
- **Credentials**: Secure credential storage
- **Diff Tables**: Store incremental change data

**Key Design Principles**:
- **No Unique Constraints**: Allows multiple entities with same names from different contexts
- **JSONB Storage**: Flexible attribute storage using PostgreSQL JSONB
- **Audit Trail**: Complete tracking of sync runs and changes
- **Performance**: Optimized indexes for common queries

## Technology Stack

### Backend
- **Python 3.10+**: Core application language
- **Typer**: CLI framework
- **Rich**: Enhanced terminal output
- **Psycopg**: PostgreSQL database adapter
- **PyYAML**: Configuration management
- **Dataclasses**: Data modeling

### Frontend
- **Flask**: Web framework
- **Jinja2**: Template engine
- **Bootstrap**: UI framework
- **PostgreSQL**: Data storage

### Database
- **PostgreSQL**: Primary database
- **JSONB**: Flexible data storage
- **Views**: Pre-computed metadata views
- **Functions**: Database-level utilities

## Security Features

### Credential Management
- **Encrypted Storage**: Passwords stored encrypted in database
- **Connection Pooling**: Secure connection management
- **Environment Variables**: Support for external credential sources
- **Access Control**: Database-level permissions

### Data Protection
- **No Sensitive Data**: Only metadata is extracted, not actual data
- **Secure Connections**: SSL support for database connections
- **Audit Logging**: Complete tracking of all operations

## Scalability Features

### Modular Architecture
- **Connector Pattern**: Easy addition of new data sources
- **Factory Pattern**: Dynamic connector creation
- **Service Layer**: Separated business logic
- **Export Layer**: Multiple output formats

### Performance Optimizations
- **Connection Pooling**: Efficient database connections
- **Batch Processing**: Bulk operations for large datasets
- **Indexing**: Optimized database queries
- **Sampling**: Configurable sampling for large tables

### Monitoring and Observability
- **Structured Logging**: Comprehensive logging with levels
- **Progress Tracking**: Real-time progress indicators
- **Error Handling**: Graceful error handling and reporting
- **Status Tracking**: Sync run status and statistics

## Deployment Architecture

### Single-Node Deployment
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Client    │    │  Flask Frontend │    │   PostgreSQL    │
│                 │    │                 │    │                 │
│  - scan         │───▶│  - Web UI       │───▶│  - dsa_prod     │
│  - quality      │    │  - API          │    │  - credentials  │
│  - incremental  │    │  - Views        │    │  - metadata     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Multi-Node Deployment
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │  App Instances  │    │   Database      │
│                 │    │                 │    │   Cluster       │
│  - nginx        │───▶│  - Multiple     │───▶│  - Primary      │
│  - SSL          │    │  - Load Sharing │    │  - Replicas     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Future Enhancements

### Planned Features
- **Real-time Monitoring**: Live metadata change detection
- **API Layer**: RESTful API for programmatic access
- **Advanced Analytics**: Machine learning-based quality insights
- **Multi-tenant Support**: Enhanced tenant isolation
- **Cloud Integration**: Native cloud database support

### Extensibility
- **Plugin System**: Custom connector plugins
- **Custom Metrics**: User-defined quality metrics
- **Integration APIs**: Third-party tool integration
- **Custom Exporters**: Additional output formats

This architecture provides a robust, scalable, and maintainable foundation for comprehensive database metadata management and analysis.
