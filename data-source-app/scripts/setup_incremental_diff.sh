#!/bin/bash
# Script to set up incremental diff schema

echo "🔧 Setting up incremental diff schema..."

# Check if config.yml exists
if [ ! -f "config.yml" ]; then
    echo "❌ config.yml not found. Please create it first."
    exit 1
fi

# Run the SQL script
echo "📄 Creating incremental diff tables..."
psql -f scripts/create_incremental_diff_schema.sql

if [ $? -eq 0 ]; then
    echo "✅ Incremental diff schema created successfully!"
    echo ""
    echo "📋 Created tables:"
    echo "  - dsa_production.diff_sync_runs"
    echo "  - dsa_production.incremental_diff_schema"
    echo "  - dsa_production.incremental_diff_table"
    echo "  - dsa_production.incremental_diff_column"
    echo "  - dsa_production.diff_summary (view)"
    echo ""
    echo "💡 You can now run: python -m src.app incremental-diff --connection-id <connection_id>"
else
    echo "❌ Failed to create incremental diff schema"
    exit 1
fi
