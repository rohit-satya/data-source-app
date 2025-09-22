# Incremental Diff Feature

This document describes the incremental diff feature that compares metadata between the last two sync runs for a connection.

## Overview

The incremental diff feature allows you to:
- Compare metadata between two consecutive sync runs
- Track changes in schemas, tables, and columns
- Store detailed differences in dedicated database tables
- Monitor diff operations through a summary view

## Setup

### 1. Create Database Schema

Run the setup script to create the required tables:

```bash
./scripts/setup_incremental_diff.sh
```

This creates the following tables:
- `dsa_production.diff_sync_runs` - Tracks diff operations
- `dsa_production.incremental_diff_schema` - Schema-level changes
- `dsa_production.incremental_diff_table` - Table-level changes  
- `dsa_production.incremental_diff_column` - Column-level changes
- `dsa_production.diff_summary` - Summary view for easy querying

### 2. Prerequisites

- At least 2 sync runs must exist for the connection
- The connection must have been used with the `scan` command previously

## Usage

### Basic Command

```bash
python -m src.app incremental-diff --connection-id <connection_id>
```

### Command Options

```bash
python -m src.app incremental-diff [OPTIONS]

Options:
  --connection-id TEXT    Connection ID to compare [required]
  --format TEXT          Output format (postgres, json) [default: postgres]
  --config TEXT          Path to configuration file [default: config.yml]
  --verbose              Enable verbose logging
  --log-file TEXT        Log file path
  --help                 Show help message
```

### Example

```bash
# Compare last two sync runs for 'aiven' connection
python -m src.app incremental-diff --connection-id aiven --verbose

# Compare with JSON output format
python -m src.app incremental-diff --connection-id aiven --format json
```

## How It Works

### 1. Process Flow

1. **Identify Sync Runs**: Finds the last 2 sync runs for the specified connection
2. **Create Diff Record**: Creates a new `diff_sync_runs` record with unique UUID
3. **Compare Assets**: Compares schemas, tables, and columns between the two runs
4. **Store Differences**: Saves detailed differences to respective diff tables
5. **Update Status**: Marks the diff operation as completed

### 2. Change Detection

The system detects the following types of changes:

- **Added**: Asset exists in newer run but not in older run
- **Removed**: Asset exists in older run but not in newer run  
- **Modified**: Asset exists in both runs but with different attributes
- **Unchanged**: Asset exists in both runs with identical attributes

### 3. Comparison Logic

For each asset type (schema, table, column), the system compares:
- `attributes` - Core metadata attributes
- `custom_attributes` - Custom metadata attributes
- `created_at` and `updated_at` timestamps

## Database Schema

### diff_sync_runs Table

Tracks each diff operation:

```sql
CREATE TABLE dsa_production.diff_sync_runs (
    diff_sync_id UUID PRIMARY KEY,
    connection_id TEXT NOT NULL,
    sync_run_1_id UUID NOT NULL,  -- Older sync run
    sync_run_2_id UUID NOT NULL,  -- Newer sync run
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL DEFAULT 'running',
    total_schemas_changed INTEGER DEFAULT 0,
    total_tables_changed INTEGER DEFAULT 0,
    total_columns_changed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### incremental_diff_* Tables

Store detailed differences for each asset type:

```sql
-- Schema differences
CREATE TABLE dsa_production.incremental_diff_schema (
    diff_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    diff_sync_id UUID NOT NULL,
    schema_name TEXT NOT NULL,
    change_type TEXT NOT NULL,  -- 'added', 'removed', 'modified'
    sync_run_1_data JSONB,     -- Data from older run
    sync_run_2_data JSONB,     -- Data from newer run
    differences JSONB,         -- Detailed differences
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table differences (similar structure)
CREATE TABLE dsa_production.incremental_diff_table (...);

-- Column differences (similar structure)  
CREATE TABLE dsa_production.incremental_diff_column (...);
```

## Querying Results

### View All Diff Operations

```sql
SELECT * FROM dsa_production.diff_summary 
ORDER BY started_at DESC;
```

### View Specific Diff Details

```sql
-- Get diff summary for a specific operation
SELECT * FROM dsa_production.diff_summary 
WHERE diff_sync_id = 'your-diff-sync-id';

-- View schema changes
SELECT * FROM dsa_production.incremental_diff_schema 
WHERE diff_sync_id = 'your-diff-sync-id';

-- View table changes
SELECT * FROM dsa_production.incremental_diff_table 
WHERE diff_sync_id = 'your-diff-sync-id';

-- View column changes
SELECT * FROM dsa_production.incremental_diff_column 
WHERE diff_sync_id = 'your-diff-sync-id';
```

### Find Recent Changes

```sql
-- Find all changes in the last 24 hours
SELECT * FROM dsa_production.diff_summary 
WHERE started_at > NOW() - INTERVAL '24 hours';

-- Find all modified columns
SELECT * FROM dsa_production.incremental_diff_column 
WHERE change_type = 'modified'
ORDER BY created_at DESC;
```

## Output Format

### Console Output

The command provides rich console output including:
- Progress indicators
- Summary of changes found
- Diff sync ID for tracking
- Helpful SQL queries for further investigation

### Example Output

```
🔍 Starting incremental diff for connection: aiven
📊 Output format: postgres
⠋ Running incremental diff...

🎉 Incremental diff completed successfully!
🆔 Diff Sync ID: 123e4567-e89b-12d3-a456-426614174000
🔌 Connection: aiven
📊 Sync Run 1 (older): 456e7890-e89b-12d3-a456-426614174001
📊 Sync Run 2 (newer): 789e0123-e89b-12d3-a456-426614174002

┏━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Asset Type  ┃ Changes ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━┩
│ Schemas     │ 0       │
│ Tables      │ 2       │
│ Columns     │ 5       │
│ Total       │ 7       │
└─────────────┴─────────┘

💡 View detailed changes in dsa_production.incremental_diff_* tables
💡 Query diff summary: SELECT * FROM dsa_production.diff_summary WHERE diff_sync_id = '123e4567-e89b-12d3-a456-426614174000'
```

## Error Handling

### Common Errors

1. **Not enough sync runs**: Need at least 2 sync runs for comparison
2. **Connection not found**: Connection ID doesn't exist
3. **Database connection issues**: Problems connecting to dsa_production database

### Error States

- `running` - Diff operation in progress
- `completed` - Diff operation completed successfully
- `failed` - Diff operation failed with error

## Integration

### With Existing Commands

The incremental diff feature integrates with existing commands:

1. **scan** - Creates sync runs that can be compared
2. **quality_metrics** - Creates sync runs that can be compared
3. **status** - Shows latest sync run information

### Workflow Integration

```bash
# 1. Run initial sync
python -m src.app scan --connection-id aiven

# 2. Make changes to source database
# (add/remove/modify tables, columns, etc.)

# 3. Run another sync
python -m src.app scan --connection-id aiven

# 4. Compare the changes
python -m src.app incremental-diff --connection-id aiven
```

## Performance Considerations

- Large databases may take longer to process
- Consider running during off-peak hours
- Monitor database performance during diff operations
- Use indexes for better query performance

## Troubleshooting

### Check Sync Runs

```sql
-- Verify sync runs exist for connection
SELECT sync_id, extraction_timestamp, status 
FROM dsa_production.sync_runs 
WHERE connection_id = 'your-connection-id' 
ORDER BY extraction_timestamp DESC;
```

### Check Diff Status

```sql
-- Check diff operation status
SELECT * FROM dsa_production.diff_sync_runs 
WHERE connection_id = 'your-connection-id' 
ORDER BY started_at DESC;
```

### Debug Issues

Enable verbose logging to see detailed processing:

```bash
python -m src.app incremental-diff --connection-id aiven --verbose --log-file diff.log
```
