# Data Source App - Thought Process

## Application Design Philosophy

### Choice of CLI Architecture
**Decision**: Command-line interface (CLI) over web-based or GUI application

**Rationale**:
- **Zero Server Setup**: Testers don't need to run additional servers or services
- **Environment Agnostic**: Works across different operating systems and environments
- **Scriptable**: Can be easily integrated into CI/CD pipelines and automated workflows
- **Resource Efficient**: Minimal resource requirements, no persistent server processes
- **Developer Friendly**: Familiar interface for data engineers and developers

**Key Benefits**:
- Testers can quickly set up connections with their credentials
- Run metadata extraction and quality metrics analysis with simple commands
- Execute incremental diff analysis to track changes between sync runs
- All operations are stateless and can be run on-demand

### Frontend Strategy
**Decision**: Separate Flask-based web frontend for visualization

**Rationale**:
- **Separation of Concerns**: CLI handles data extraction, web UI handles visualization
- **Flexible Deployment**: Can run CLI and frontend independently
- **User Experience**: Provides intuitive dashboard for non-technical users
- **Real-time Data**: Direct database queries for live metadata display

**Implementation**:
- **Dashboard View**: Shows all connections with last sync status and sync counts
- **Metadata Browser**: Click-through interface for schemas → tables → columns
- **Quality Metrics**: Visual representation of data quality scores and statistics
- **Independent Operation**: Runs on separate port (5001) without affecting CLI

## Setup and Configuration Strategy

### Environment Agnostic Design
**Goal**: Make setup as simple as possible for testers

**Implementation**:
- **Remote Database**: All data stored in remote PostgreSQL instance (Aiven)
- **Pre-configured Connection**: Application points to remote `dsa_production` database
- **Minimal Dependencies**: Only Python packages from `requirements.txt`
- **No Local Database**: Eliminates need for local PostgreSQL installation
- **Configuration-driven**: All settings in `config.yml` file

**Tester Workflow**:
1. Install Python dependencies: `pip install -r requirements.txt`
2. Configure credentials: `python -m src.app credentials-add`
3. Run extractions: `python -m src.app scan --connection-id production`
4. View results: `python frontend/web_app.py` (separate terminal)

### Why PostgreSQL
**Decision**: PostgreSQL as the primary and currently supported data source

**Rationale**:
- **Industry Standard**: Most commonly used structured database
- **Rich Metadata**: Comprehensive system catalogs for metadata extraction
- **Easy Setup**: Simple connection and database setup process
- **Free Tier Available**: Aiven provides free PostgreSQL instances
- **Extensible**: Well-documented extension points for custom metadata

**Technical Benefits**:
- **System Catalogs**: `information_schema` and `pg_catalog` provide rich metadata
- **JSONB Support**: Native JSON support for flexible metadata storage
- **Partitioning**: Built-in table partitioning for large datasets
- **Foreign Keys**: Comprehensive relationship tracking
- **Indexes**: Detailed index metadata and performance information

## Core Functionality Design

### Incremental Diff Analysis
**Challenge**: Unable to implement true incremental extraction due to complexity

**Solution**: Post-extraction diff analysis between sync runs

**Implementation**:
- **Two-Phase Approach**: Extract full metadata, then compare between runs
- **Change Detection**: Identify added, removed, and modified assets
- **Structured Storage**: Store diff results in dedicated tables
- **Summary Views**: Pre-built views for easy change analysis

**Benefits**:
- **Change Tracking**: Customers can see what changed between syncs
- **Audit Trail**: Historical record of metadata changes
- **Impact Analysis**: Understand effects of schema modifications
- **Compliance**: Track data lineage and governance changes

### Connection and Credentials Management
**Goal**: Secure, flexible credential handling for multiple connections

**Implementation**:
- **Encrypted Storage**: Passwords encrypted using Fernet (AES-128) with PBKDF2 key derivation
- **Multiple Connections**: Support for multiple database connections per instance
- **Connection Testing**: Validate connections before storing credentials
- **Environment Support**: Flexible configuration for different environments

**Security Features**:
- **Master Key**: Configurable encryption key in `config.yml`
- **Key Derivation**: PBKDF2 with 100,000 iterations and fixed salt
- **Authenticated Encryption**: Fernet provides both encryption and integrity
- **No Plain Text**: Passwords never stored in plain text

### Metadata Selection Strategy
**Goal**: Extract metadata that provides real business value

**Table-level Metadata**:
- **Partitioning**: Parent/child partition relationships for data organization
- **Foreign Relationships**: Table dependencies and data lineage
- **Constraints**: Primary keys, foreign keys, unique constraints
- **Indexes**: Performance optimization metadata
- **Storage**: Tablespace and storage characteristics

**Column-level Metadata**:
- **Data Types**: Comprehensive type information including precision/scale
- **Constraints**: NOT NULL, CHECK constraints, defaults
- **Keys**: Primary key, foreign key, unique key identification
- **Indexing**: Column-level index information
- **Quality Indicators**: Nullability, distinctness, data classification

**Business Value**:
- **Data Governance**: Understand data structure and relationships
- **Performance**: Identify optimization opportunities
- **Compliance**: Track data lineage and dependencies
- **Documentation**: Automatic generation of data dictionaries

## Extensibility and Architecture

### Connector-based Architecture
**Goal**: Make adding new data sources as easy as possible

**Implementation**:
- **Abstract Base Classes**: Common interfaces for all connectors
- **Factory Pattern**: Automatic connector selection based on source type
- **Source-specific Logic**: Each connector handles its own extraction logic
- **Shared Components**: Common utilities for metadata processing

**Developer Experience**:
- **Minimal Code**: Only need to implement source-specific extraction
- **Consistent Interface**: All connectors follow the same patterns
- **Reusable Components**: Common functionality is abstracted out
- **Clear Documentation**: Step-by-step guide for adding new connectors

### Export Format Flexibility
**Goal**: Support multiple output formats for different use cases

**Supported Formats**:
- **PostgreSQL**: Persistent storage in normalized tables
- **JSON**: Human-readable format for debugging and integration
- **CSV**: Spreadsheet-compatible format for analysis
- **All**: Generate all formats in single run

**Use Cases**:
- **Development**: JSON format for debugging and testing
- **Production**: PostgreSQL format for persistent storage
- **Analysis**: CSV format for data analysis tools
- **Integration**: JSON format for API consumption

## Technical Architecture Decisions

### Normalized Entity Model
**Decision**: Standardized entity structure following industry patterns

**Structure**:
- **Top-level Attributes**: Common fields (name, connection, tenant, sync details)
- **Attributes Map**: Source-specific metadata in structured format
- **Custom Attributes**: Extensible metadata for additional context
- **Qualified Names**: Hierarchical naming for unique identification

**Benefits**:
- **Consistency**: Uniform structure across all data sources
- **Extensibility**: Easy to add new metadata fields
- **Integration**: Compatible with data catalog tools
- **Queryability**: Structured format enables complex queries

### Database Schema Design
**Decision**: Separate production schema (`dsa_production`) for metadata storage

**Schema Organization**:
- **Normalized Tables**: `normalized_schemas`, `normalized_tables`, `normalized_columns`
- **Quality Metrics**: Dedicated tables for data quality analysis
- **Sync Tracking**: `sync_runs` table for extraction history
- **Credentials**: Encrypted credential storage
- **Diff Tables**: Incremental change tracking

**Design Principles**:
- **Separation**: Metadata storage separate from source data
- **Normalization**: Structured, queryable format
- **History**: Track changes over time
- **Security**: Encrypted sensitive data storage

### Quality Metrics Strategy
**Goal**: Provide actionable data quality insights

**Metrics Collected**:
- **Row Counts**: Total and sampled row counts
- **Null Analysis**: Null counts and percentages
- **Distinctness**: Unique value analysis
- **Top Values**: Most frequent values and frequencies
- **Quality Scoring**: Overall quality score (0-100)

**Business Value**:
- **Data Quality**: Identify data quality issues
- **Performance**: Understand data distribution
- **Governance**: Track data quality over time
- **Compliance**: Meet data quality requirements

## Future Considerations

### Scalability
- **Incremental Extraction**: True incremental extraction for large datasets
- **Parallel Processing**: Multi-threaded extraction for performance
- **Caching**: Intelligent caching for frequently accessed metadata
- **Streaming**: Real-time metadata updates

### Integration
- **API Layer**: REST API for programmatic access
- **Webhooks**: Event-driven updates for external systems
- **Data Catalogs**: Integration with enterprise data catalogs
- **Monitoring**: Integration with monitoring and alerting systems

### Advanced Features
- **Data Lineage**: Comprehensive data lineage tracking
- **Impact Analysis**: Understand effects of schema changes
- **Automated Testing**: Data quality testing and validation
- **Compliance**: Automated compliance checking and reporting

## Summary

The Data Source App represents a comprehensive approach to metadata extraction and analysis, designed with simplicity, security, and extensibility in mind. The CLI-first approach ensures easy adoption, while the modular architecture allows for future growth and integration. The focus on real-world metadata needs and secure credential handling makes it suitable for both development and production environments.

The application successfully balances technical complexity with user experience, providing powerful metadata extraction capabilities while maintaining a simple, intuitive interface for both technical and non-technical users.
