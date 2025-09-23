# Data Source App

A comprehensive Python application for extracting metadata and quality metrics from PostgreSQL databases with advanced features for data governance, change tracking, and web-based visualization. This tool helps data teams understand their database structure, relationships, and data quality characteristics.

## Features

### Core Metadata Extraction
- Extract schemas, tables, and columns with detailed information
- Capture data types, nullability, defaults, and constraints
- Extract primary keys, foreign keys, unique constraints, and indexes
- Support for various PostgreSQL data types including JSONB, arrays, and custom types
- **Normalized Entity Model**: Structured metadata following industry-standard data catalog patterns

### Advanced Metadata Features
- **Partition Information**: Extract parent/child partition relationships
- **Foreign Key Relationships**: Track table dependencies and relationships
- **Index Analysis**: Comprehensive index metadata including columns and properties
- **Tablespace Information**: Database storage and performance metadata
- **Custom Attributes**: Extensible metadata storage for additional context

### Quality Metrics & Analysis
- Row count analysis with sampling for large tables
- Null count and percentage calculations
- Distinct value analysis
- Top-K value frequency analysis
- Data quality scoring and summary statistics
- **Comprehensive Quality Views**: Pre-built views for easy quality analysis

### Change Tracking & Diff Analysis
- **Incremental Diff**: Compare metadata between sync runs
- **Change Detection**: Identify added, removed, and modified assets
- **Historical Tracking**: Track metadata changes over time
- **Diff Summary Views**: Easy-to-query change summaries

### Credentials Management
- **Secure Storage**: Encrypted password storage with configurable keys
- **Multiple Connections**: Manage multiple database connections
- **Connection Testing**: Validate connections before use
- **Environment Support**: Flexible configuration for different environments

### Web Frontend
- **Interactive Web Interface**: Browse metadata in a user-friendly web application
- **Real-time Data**: Live data from your database
- **Connection Management**: View and manage database connections
- **Metadata Navigation**: Easy browsing of schemas, tables, and columns

### Export Capabilities
- **PostgreSQL Storage**: Store metadata directly in PostgreSQL database for structured querying
- **JSON export** for programmatic consumption
- **Combined exports** with both metadata and quality metrics
- **Configurable output** directories and database schemas

### Command Line Interface
- Easy-to-use CLI with Typer
- Support for specific schema scanning or full database analysis
- Configurable via YAML files
- Rich console output with progress indicators
- **Connection-based Operations**: Work with named connections instead of direct DSNs

## Installation

### Prerequisites
- Python 3.11 or higher
- pip package manager

### Setup

**Note**: The application connects to a pre-configured remote database which is pre-configured with:
- `dsa_production` schema with all necessary tables
- Sample data for testing
No local database setup is required.

1. Clone the repository:
```bash
git clone https://github.com/rohit-satya/data-source-app.git
cd data-source-app/data-source-app
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

### Basic Usage

1. **Set up credentials for your database:**
```bash
python -m src.app credentials-add \
  --connection-id production \
  --host your-db-host \
  --port 5432 \
  --database your-database \
  --username your-username \
  --password your-password
```

2. **Extract metadata using your connection:**
```bash
python -m src.app scan --connection-id production --format postgres
```

3. **Extract quality metrics:**
```bash
python -m src.app quality-metrics --connection-id production --format postgres
```

4. **Run incremental diff to track changes (requires at least 2 scans):**
```bash
python -m src.app incremental-diff --connection-id production --format postgres
```

5. **Start the web frontend:**
```bash
cd frontend
python web_app.py
# Open http://localhost:5001 in your browser
```

### Configuration

The application uses a YAML configuration file (`config.yml`) with the following structure:

```yaml
# Database connection
database:
  dsn: "postgresql://user:password@host:port/database"
  # Alternative: individual parameters
  # host: localhost
  # port: 5432
  # database: postgres
  # user: postgres
  # password: password

# Target schemas (empty = all schemas)
schemas:
  - public
  - dsa_ecommerce

# Quality metrics configuration
metrics:
  enabled: true
  sample_limit: 10000
  top_k_values: 10
  include_null_counts: true
  include_distinct_counts: true

# Output configuration
output:
  json_dir: "./output/json"
  create_dirs: true
  # PostgreSQL export configuration
  postgres:
    enabled: true
    schema: "dsa_production"
    cleanup_days: 30

# Lineage configuration
lineage:
  enabled: true
  extract_foreign_keys: true
  parse_view_dependencies: true

# Encryption configuration
encryption:
  # Master key for password encryption (base64 encoded)
  master_key: "your-base64-encoded-encryption-key"
```

## Command Reference

### Core Commands

#### `scan`
Extract metadata from specified schemas using a named connection.

**Options:**
- `--connection-id`: Connection ID to use (required)
- `--config, -c`: Path to configuration file (default: config.yml)
- `--schema, -s`: Specific schema to scan
- `--format, -f`: Output format (json, postgres, all) (default: postgres)
- `--verbose, -v`: Enable verbose logging
- `--log-file`: Log file path

**Examples:**
```bash
# Scan using named connection
python -m src.app scan --connection-id production --format postgres

# Scan specific schema
python -m src.app scan --connection-id production --schema public --format postgres
```

#### `quality-metrics`
Extract quality metrics from the database using a named connection.

**Options:**
- `--connection-id`: Connection ID to use (required)
- `--config, -c`: Path to configuration file
- `--schema, -s`: Specific schema to analyze
- `--format, -f`: Output format (json, postgres)
- `--verbose, -v`: Enable verbose logging

**Examples:**
```bash
# Analyze quality metrics
python -m src.app quality-metrics --connection-id production --format postgres
```

#### `incremental-diff`
Compare metadata between the last two sync runs to identify changes.

**Options:**
- `--connection-id`: Connection ID to analyze (required)
- `--format, -f`: Output format (postgres, json) (default: postgres)
- `--config, -c`: Path to configuration file
- `--verbose, -v`: Enable verbose logging

**Examples:**
```bash
# Run incremental diff analysis
python -m src.app incremental-diff --connection-id production --format postgres
```

### Credentials Management

#### `credentials-add`
Add new database connection credentials.

**Options:**
- `--connection-id`: Unique identifier for the connection (required)
- `--host`: Database host (required)
- `--port`: Database port (required)
- `--database`: Database name (required)
- `--username`: Username (required)
- `--password`: Password (required)
- `--ssl-mode`: SSL mode (default: prefer)
- `--description`: Optional description
- `--config, -c`: Path to configuration file

**Examples:**
```bash
# Add production database credentials
python -m src.app credentials-add \
  --connection-id production \
  --host db.example.com \
  --port 5432 \
  --database mydb \
  --username admin \
  --password secret123
```

#### `credentials-list`
List all stored credentials.

**Options:**
- `--config, -c`: Path to configuration file

#### `credentials-delete`
Delete credentials for a connection.

**Options:**
- `--connection-id`: Connection ID to delete (required)
- `--config, -c`: Path to configuration file


## Output Formats

### JSON Output
The JSON export provides structured metadata in the `output/json` directory with separate files for each entity type:

- `metadata_YYYYMMDD_HHMMSS_column.json` - Column metadata
- `metadata_YYYYMMDD_HHMMSS_table.json` - Table metadata  
- `metadata_YYYYMMDD_HHMMSS_schema.json` - Schema metadata

### PostgreSQL Storage

The PostgreSQL storage option stores metadata directly in the database using a structured schema called `dsa_production`. This provides several advantages:

#### Database Schema
The metadata is stored in the following tables:

**Core Metadata Tables:**
- **`sync_runs`** - Tracks sync runs with timestamps and statistics
- **`normalized_schemas`** - Schema-level metadata with normalized structure
- **`normalized_tables`** - Table-level metadata with normalized structure
- **`normalized_columns`** - Column-level metadata with normalized structure

**Quality Metrics Tables:**
- **`quality_metrics_runs`** - Quality metrics extraction runs
- **`table_quality_metrics`** - Table-level quality metrics
- **`column_quality_metrics`** - Column-level quality metrics
- **`column_top_values`** - Most frequent values for each column

**Change Tracking Tables:**
- **`diff_sync_runs`** - Tracks incremental diff operations
- **`incremental_diff_schema`** - Schema-level changes
- **`incremental_diff_table`** - Table-level changes
- **`incremental_diff_column`** - Column-level changes

**Credentials Management:**
- **`credentials`** - Encrypted database connection credentials

**Views for Easy Querying:**
- **`latest_schema_metadata`** - Latest schema metadata
- **`latest_table_metadata`** - Latest table metadata
- **`latest_column_metadata`** - Latest column metadata
- **`latest_quality_metrics_summary`** - Latest quality metrics
- **`diff_summary`** - Incremental diff summaries
- **`active_credentials`** - Active database connections

#### Benefits
- **Structured Querying**: Use SQL to query metadata with joins and filters
- **Historical Tracking**: Track metadata changes over time
- **Performance**: Better performance for large datasets
- **Integration**: Easy integration with existing database tools


#### Example Queries

```sql
-- Get latest metadata for all tables
SELECT 
    name as table_name,
    attributes->>'schemaName' as schema_name,
    attributes->>'tableType' as table_type,
    custom_attributes->>'created_at' as created_at
FROM dsa_production.latest_table_metadata
ORDER BY schema_name, table_name;

-- Find columns with high null percentages
SELECT 
    schema_name,
    table_name,
    column_name,
    null_percentage
FROM dsa_production.latest_column_quality_metrics 
WHERE null_percentage > 50
ORDER BY null_percentage DESC;

-- Get recent changes using incremental diff
SELECT 
    schema_name,
    table_name,
    change_type,
    COUNT(*) as change_count
FROM dsa_production.diff_summary ds
JOIN dsa_production.incremental_diff_table idt ON ds.diff_sync_id = idt.diff_sync_id
WHERE ds.connection_id = 'production'
GROUP BY schema_name, table_name, change_type;

-- Get metadata summary by connection
SELECT 
    connection_name,
    connector_name,
    COUNT(DISTINCT sync_id) as sync_count,
    MAX(sync_timestamp) as latest_sync
FROM dsa_production.sync_runs 
WHERE status = 'completed'
GROUP BY connection_name, connector_name
ORDER BY latest_sync DESC;
```

## Web Frontend

The application includes a web-based frontend for easy metadata browsing and visualization.

### Starting the Frontend

```bash
# Navigate to frontend directory
cd frontend

# Start the Flask web application
python web_app.py

# Open http://localhost:5001 in your browser
```

### Frontend Features

- **Connection Management**: View and manage database connections
- **Metadata Browser**: Navigate through schemas, tables, and columns
- **Real-time Data**: Live data from your database
- **Quality Metrics**: View data quality metrics and statistics
- **Change Tracking**: Browse incremental diff results
- **Responsive Design**: Works on desktop and mobile devices

### Frontend Routes

- **`/`** - Home page with connection list
- **`/metadata/<connection_id>`** - Metadata browser for specific connection
- **`/api/connections`** - API endpoint for connection list
- **`/api/metadata/<connection_id>`** - API endpoint for metadata

## Quality Metrics

The application provides comprehensive data quality analysis:

### Column-level Metrics
- **Total Count**: Number of rows analyzed
- **Null Count**: Number of null values
- **Null Percentage**: Percentage of null values
- **Distinct Count**: Number of unique values
- **Distinct Percentage**: Percentage of unique values
- **Top Values**: Most frequent values with counts

### Table-level Metrics
- **Row Count**: Total number of rows
- **Column Count**: Number of columns
- **Quality Indicators**: Flags for high null rates, low distinctness

### Quality Scoring
The application calculates an overall quality score (0-100) based on:
- High null percentage columns (penalty: 30 points)
- Low distinct percentage columns (penalty: 20 points)

### Features Demonstrated
- Various data types (VARCHAR, INTEGER, DECIMAL, JSONB, etc.)
- Foreign key relationships
- Check constraints
- Unique constraints
- Indexes for performance
- Triggers for automatic updates
- Views for data aggregation
- Comments with tags
- Sample data with realistic values

## Development

### Project Structure
```
data-source-app/
├── README.md
├── config.yml              # Main configuration file
├── requirements.txt        # Python dependencies
├── pyproject.toml         # Project configuration
├── docs/                  # Documentation
│   ├── architecture.md
│   ├── setup.pdf
│   └── usage.pdf
├── frontend/              # Web frontend
│   ├── web_app.py         # Flask web application
│   ├── app.py             # Command-line frontend
│   ├── templates/         # HTML templates
│   └── README.md
├── scripts/               # Setup and utility scripts
│   ├── create_normalized_schema.sql
│   └── create_incremental_diff_schema.sql
├── src/                   # Main application code
│   ├── app.py             # CLI entry point
│   ├── config.py          # Configuration management
│   ├── connector/         # Database connectors
│   │   └── postgres/
│   ├── credentials/       # Credentials management
│   ├── db/                # Database connection and queries
│   ├── exporters/         # Export functionality
│   ├── extractor/         # Legacy metadata extraction
│   ├── models/            # Data models
│   ├── services/          # Business logic services
│   └── utils/             # Utility functions
├── tests/                 # Test files
└── output/                # Generated output files
    └── json/
```

## Architecture

### Normalized Entity Model

The application uses a normalized entity model that follows industry-standard data catalog patterns:

- **Schema Entities**: Top-level database schemas
- **Table Entities**: Tables within schemas
- **Column Entities**: Columns within tables

Each entity includes:
- **Core Attributes**: `typeName`, `name`, `connectionName`, `tenantId`, `lastSyncRun`, `lastSyncRunAt`, `connectorName`
- **Attributes**: Qualified names, data types, constraints, and metadata
- **Custom Attributes**: Extensible JSONB fields for additional context

### Key Components

- **Connectors**: Database-specific extraction logic (PostgreSQL)
- **Services**: Business logic for metadata processing and diff analysis
- **Models**: Normalized entity builders and data structures
- **Exporters**: Multiple output formats (PostgreSQL, JSON)
- **Frontend**: Web and command-line interfaces

### Security Features

- **Encrypted Credentials**: Passwords stored with AES-128 encryption using configurable master keys
- **Connection Testing**: Validate connections before use
