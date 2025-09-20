#!/usr/bin/env python3
"""
Demo script for PostgreSQL metadata storage functionality.

This script demonstrates how to use the new PostgreSQL export functionality
to store metadata directly in a PostgreSQL database instead of JSON/CSV files.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.app import app
from rich.console import Console

console = Console()

def main():
    """Run the demo."""
    console.print("🚀 PostgreSQL Metadata Storage Demo", style="bold blue")
    console.print("=" * 50)
    
    console.print("\n📋 Available Commands:")
    console.print("1. populate-sample  - Load sample data and create production schema")
    console.print("2. scan            - Extract metadata and store in PostgreSQL")
    console.print("3. quality-metrics - Extract quality metrics and store in PostgreSQL")
    console.print("4. status          - Show status of latest extractions")
    console.print("5. cleanup         - Clean up old metadata")
    
    console.print("\n🔧 Example Usage:")
    console.print("python demo_postgres.py populate-sample")
    console.print("python demo_postgres.py scan --format postgres")
    console.print("python demo_postgres.py quality-metrics --format postgres")
    console.print("python demo_postgres.py status")
    console.print("python demo_postgres.py cleanup --days 7")
    
    console.print("\n📊 Database Schema:")
    console.print("The metadata is stored in the 'dsa_production' schema with tables:")
    console.print("- metadata_extraction_runs    - Tracks extraction runs")
    console.print("- schemas_metadata           - Schema-level metadata")
    console.print("- tables_metadata            - Table-level metadata")
    console.print("- columns_metadata           - Column-level metadata")
    console.print("- constraints_metadata       - Constraint metadata")
    console.print("- indexes_metadata           - Index metadata")
    console.print("- quality_metrics_runs       - Quality metrics runs")
    console.print("- table_quality_metrics      - Table quality metrics")
    console.print("- column_quality_metrics     - Column quality metrics")
    console.print("- column_top_values          - Most frequent values")
    
    console.print("\n🎯 Benefits of PostgreSQL Storage:")
    console.print("✅ Structured data storage with proper relationships")
    console.print("✅ Easy querying with SQL")
    console.print("✅ Historical tracking of metadata changes")
    console.print("✅ Built-in cleanup functionality")
    console.print("✅ Better performance for large datasets")
    console.print("✅ Integration with existing database tools")
    
    # Run the app with command line arguments
    if len(sys.argv) > 1:
        app()

if __name__ == "__main__":
    main()
