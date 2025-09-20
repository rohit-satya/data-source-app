# PostgreSQL Metadata App

A comprehensive Python application for extracting metadata and quality metrics from PostgreSQL databases. This tool helps data teams understand their database structure, relationships, and data quality characteristics.

## Features

### Schema Metadata
- Extract schemas, tables, and columns with detailed information
- Capture data types, nullability, defaults, and constraints
- Extract primary keys, foreign keys, unique constraints, and indexes
- Support for various PostgreSQL data types including JSONB, arrays, and custom types

### Business Context
- Extract table and column descriptions from PostgreSQL comments
- Parse tags from comments using `[tags: tag1,tag2]` format
- Support for YAML metadata files for additional business context
- Hierarchical tag organization for better data governance

### Quality Metrics
- Row count analysis with sampling for large tables
- Null count and percentage calculations
- Distinct value analysis
- Top-K value frequency analysis
- Data quality scoring and summary statistics

### Export Capabilities
- **PostgreSQL Storage**: Store metadata directly in PostgreSQL database for structured querying
- **JSON export** for programmatic consumption
- **CSV export** for spreadsheet analysis
- **Combined exports** with both metadata and quality metrics
- **Configurable output** directories and database schemas

### Command Line Interface
- Easy-to-use CLI with Typer
- Support for specific schema scanning or full database analysis
- Configurable via YAML files
- Rich console output with progress indicators

## Installation

### Prerequisites
- Python 3.11 or higher
- PostgreSQL database access
- pip package manager

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd postgres-metadata-app
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your database connection in `config.yml`:
```yaml
database:
  dsn: "postgresql://username:password@localhost:5432/database_name"
```

4. (Optional) Load sample data for testing:
```bash
./scripts/populate_sample.sh
```

## Quick Start

### Basic Usage

1. **Load sample data and create production schema:**
```bash
python -m src.app populate-sample
```

2. **Extract metadata and store in PostgreSQL:**
```bash
python -m src.app scan --schema dsa_ecommerce --format postgres
```

3. **Extract metadata for all schemas:**
```bash
python -m src.app scan-all --format postgres
```

4. **Extract quality metrics and store in PostgreSQL:**
```bash
python -m src.app quality-metrics --schema dsa_ecommerce --format postgres
```

5. **Check status of latest extractions:**
```bash
python -m src.app status
```

6. **Clean up old metadata:**
```bash
python -m src.app cleanup --days 30
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
  csv_dir: "./output/csv"
  create_dirs: true
  # PostgreSQL export configuration
  postgres:
    enabled: true
    schema: "dsa_production"
    cleanup_days: 30

# Business context configuration
business_context:
  extract_comments: true
  parse_tags: true
  metadata_table: null
  metadata_yaml: "./sample_data/sample_metadata.yml"

# Lineage configuration
lineage:
  enabled: true
  extract_foreign_keys: true
  parse_view_dependencies: true
```

## Command Reference

### `scan`
Extract metadata from specified schemas.

**Options:**
- `--config, -c`: Path to configuration file (default: config.yml)
- `--schema, -s`: Specific schema to scan
- `--format, -f`: Output format (json, csv, postgres, all) (default: postgres)
- `--verbose, -v`: Enable verbose logging
- `--log-file`: Log file path

**Examples:**
```bash
# Scan public schema and store in PostgreSQL
python -m src.app scan --schema public --format postgres

# Scan all schemas and export to all formats
python -m src.app scan --format all
```

### `scan-all`
Extract metadata from all available schemas.

**Options:**
- `--config, -c`: Path to configuration file
- `--format, -f`: Output format (json, csv, both)
- `--verbose, -v`: Enable verbose logging
- `--log-file`: Log file path

### `quality-metrics`
Extract quality metrics from the database.

**Options:**
- `--config, -c`: Path to configuration file
- `--schema, -s`: Specific schema to analyze
- `--format, -f`: Output format (json, csv, both)
- `--verbose, -v`: Enable verbose logging
- `--log-file`: Log file path

**Examples:**
```bash
# Analyze quality metrics for ecommerce schema
python -m src.app quality-metrics --schema ecommerce --format csv
```

### `populate-sample`
Load sample data into the database for testing.

**Options:**
- `--config, -c`: Path to configuration file
- `--verbose, -v`: Enable verbose logging

## Output Formats

### JSON Output
The JSON export provides structured metadata in a hierarchical format:

```json
{
  "export_info": {
    "timestamp": "2024-01-15T10:30:00",
    "version": "1.0.0",
    "total_schemas": 2
  },
  "schemas": [
    {
      "name": "public",
      "owner": "postgres",
      "tables": [
        {
          "name": "users",
          "schema": "public",
          "table_type": "BASE TABLE",
          "comment": "User accounts table [tags: core,user]",
          "tags": ["core", "user"],
          "columns": [...],
          "constraints": [...],
          "indexes": [...]
        }
      ]
    }
  ]
}
```

### CSV Output
The CSV export creates separate files for different metadata types:
- `metadata_schemas.csv` - Schema information
- `metadata_tables.csv` - Table information
- `metadata_columns.csv` - Column details
- `metadata_constraints.csv` - Constraint information
- `metadata_indexes.csv` - Index details

### PostgreSQL Storage

The PostgreSQL storage option stores metadata directly in the database using a structured schema called `dsa_production`. This provides several advantages:

#### Database Schema
The metadata is stored in the following tables:

- **`metadata_extraction_runs`** - Tracks extraction runs with timestamps and statistics
- **`schemas_metadata`** - Schema-level metadata (name, owner, table count)
- **`tables_metadata`** - Table-level metadata (name, type, comments, tags)
- **`columns_metadata`** - Column-level metadata (type, nullability, constraints)
- **`constraints_metadata`** - Constraint information (PK, FK, unique, check)
- **`indexes_metadata`** - Index definitions and properties
- **`quality_metrics_runs`** - Quality metrics extraction runs
- **`table_quality_metrics`** - Table-level quality metrics
- **`column_quality_metrics`** - Column-level quality metrics
- **`column_top_values`** - Most frequent values for each column

#### Benefits
- **Structured Querying**: Use SQL to query metadata with joins and filters
- **Historical Tracking**: Track metadata changes over time
- **Performance**: Better performance for large datasets
- **Integration**: Easy integration with existing database tools
- **Cleanup**: Built-in cleanup functionality for old metadata

#### Example Queries

```sql
-- Get all tables with their column counts
SELECT 
    schema_name, 
    table_name, 
    column_count,
    comment
FROM dsa_production.tables_metadata 
WHERE run_id = (SELECT MAX(run_id) FROM dsa_production.metadata_extraction_runs)
ORDER BY schema_name, table_name;

-- Find columns with high null percentages
SELECT 
    schema_name,
    table_name,
    column_name,
    null_percentage
FROM dsa_production.column_quality_metrics 
WHERE null_percentage > 50
ORDER BY null_percentage DESC;

-- Get quality score by schema
SELECT 
    schema_name,
    AVG(overall_quality_score) as avg_quality_score
FROM dsa_production.quality_metrics_runs 
WHERE status = 'completed'
GROUP BY schema_name;
```

#### New Commands

- **`status`** - Show status of latest metadata and quality metrics extractions
- **`cleanup`** - Clean up old metadata (default: 30 days)

## Business Context and Tags

### Comment-based Tags
The application can parse tags from PostgreSQL comments using the format:
```sql
COMMENT ON TABLE customers IS 'Customer information table [tags: core,user]';
COMMENT ON COLUMN customers.email IS 'Customer email address [tags: contact,unique]';
```

### YAML Metadata
Additional business context can be provided via YAML files:

```yaml
ecommerce:
  customers:
    description: "Customer master data with personal and financial information"
    tags: ["master-data", "customer", "financial"]
    columns:
      email:
        description: "Primary contact email address"
        tags: ["pii", "contact", "unique", "business-key"]
```

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

## Sample Data

The application includes comprehensive sample data for testing:

### E-commerce Schema
- **customers**: Customer master data with personal information
- **categories**: Product category hierarchy
- **products**: Product catalog with inventory tracking
- **orders**: Customer orders with financial information
- **order_items**: Order line items
- **reviews**: Product reviews and ratings

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
postgres-metadata-app/
├── README.md
├── docs/
├── sample_data/
│   ├── sample_schema.sql
│   └── sample_metadata.yml
├── scripts/
│   └── populate_sample.sh
├── src/
│   ├── app.py              # CLI entry point
│   ├── config.py           # Configuration management
│   ├── db/                 # Database connection and queries
│   ├── extractor/          # Metadata and quality extraction
│   ├── exporters/          # JSON and CSV exporters
│   └── utils.py            # Utility functions
├── tests/
├── requirements.txt
└── pyproject.toml
```

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify connection parameters in config.yml
   - Check if PostgreSQL is running
   - Ensure user has appropriate permissions

2. **Permission Denied**
   - Ensure user has SELECT permissions on target schemas
   - For quality metrics, user needs access to pg_stat_user_tables

3. **Memory Issues with Large Tables**
   - Reduce sample_limit in configuration
   - Process schemas individually
   - Use database-level sampling

4. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Ensure Python 3.11+ is installed

### Logging

Enable verbose logging for debugging:
```bash
python -m src.app scan --verbose --log-file debug.log
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review existing GitHub issues
3. Create a new issue with detailed information
4. Include relevant logs and configuration details
