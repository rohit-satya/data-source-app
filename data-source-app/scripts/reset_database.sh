#!/bin/bash

# Reset database script for normalized schema
# This script drops old tables and creates new normalized schema

set -e

echo "🔄 Resetting database for normalized schema..."

# Get the database connection string from config
DB_DSN=$(python -c "
import yaml
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    print(config['database']['dsn'])
")

echo "📊 Database DSN: $DB_DSN"

# Drop old tables
echo "🗑️  Dropping old tables..."
psql "$DB_DSN" -f scripts/drop_old_tables.sql

# Create new normalized schema
echo "🏗️  Creating normalized schema..."
psql "$DB_DSN" -f scripts/create_normalized_schema.sql

echo "✅ Database reset completed successfully!"
echo "📋 New tables created:"
echo "   - sync_runs"
echo "   - normalized_schemas"
echo "   - normalized_tables"
echo "   - normalized_columns"
echo "   - quality_metrics_runs"
echo "   - table_quality_metrics"
echo "   - column_quality_metrics"
echo "   - column_top_values"
