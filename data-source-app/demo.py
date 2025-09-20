#!/usr/bin/env python3
"""Demo script for PostgreSQL metadata app."""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import AppConfig
from src.db.connection import DatabaseConnection
from src.extractor.metadata_extractor import MetadataExtractor
from src.extractor.quality_metrics import QualityMetricsExtractor
from src.exporters.json_exporter import JSONExporter
from src.exporters.csv_exporter import CSVExporter


def demo_metadata_extraction():
    """Demonstrate metadata extraction functionality."""
    print("🚀 PostgreSQL Metadata App Demo")
    print("=" * 50)
    
    # Load configuration
    try:
        config = AppConfig.from_file("config.yml")
        config.load_environment_variables()
        print("✅ Configuration loaded")
    except FileNotFoundError:
        print("❌ Configuration file not found. Please create config.yml")
        return
    
    # Test database connection
    try:
        db_connection = DatabaseConnection(config.database.get_connection_string())
        if not db_connection.test_connection():
            print("❌ Database connection failed")
            return
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return
    
    # Extract metadata
    print("\n📊 Extracting metadata...")
    try:
        extractor = MetadataExtractor(db_connection, config)
        schemas = extractor.extract_all_metadata()
        
        print(f"✅ Extracted metadata for {len(schemas)} schemas")
        
        # Display summary
        total_tables = sum(len(schema.tables) for schema in schemas)
        total_columns = sum(
            len(table.columns) for schema in schemas for table in schema.tables
        )
        
        print(f"   - Total tables: {total_tables}")
        print(f"   - Total columns: {total_columns}")
        
        # Show some examples
        for schema in schemas[:2]:  # Show first 2 schemas
            print(f"\n📁 Schema: {schema.name}")
            for table in schema.tables[:3]:  # Show first 3 tables
                print(f"   📋 Table: {table.name} ({len(table.columns)} columns)")
                if table.tags:
                    print(f"      Tags: {', '.join(table.tags)}")
                for column in table.columns[:2]:  # Show first 2 columns
                    print(f"      🔹 {column.name} ({column.data_type})")
                    if column.tags:
                        print(f"         Tags: {', '.join(column.tags)}")
        
    except Exception as e:
        print(f"❌ Metadata extraction error: {e}")
        return
    
    # Extract quality metrics
    print("\n📈 Extracting quality metrics...")
    try:
        metrics_extractor = QualityMetricsExtractor(db_connection, config)
        metrics = metrics_extractor.extract_all_metrics([s.name for s in schemas])
        
        total_tables_analyzed = sum(len(table_list) for table_list in metrics.values())
        print(f"✅ Analyzed quality metrics for {total_tables_analyzed} tables")
        
        # Show quality summary
        for schema_name, table_metrics in list(metrics.items())[:2]:
            print(f"\n📊 Schema: {schema_name}")
            for table in table_metrics[:2]:
                print(f"   📋 Table: {table.table_name} ({table.row_count} rows)")
                high_null = sum(1 for col in table.column_metrics if col.null_percentage > 50)
                print(f"      High null columns: {high_null}")
        
    except Exception as e:
        print(f"❌ Quality metrics extraction error: {e}")
        return
    
    # Export results
    print("\n📁 Exporting results...")
    try:
        # Ensure output directories exist
        os.makedirs("output/json", exist_ok=True)
        os.makedirs("output/csv", exist_ok=True)
        
        # Export JSON
        json_exporter = JSONExporter(config)
        json_file = json_exporter.export_metadata(schemas, "demo_metadata.json")
        print(f"✅ JSON export: {json_file}")
        
        # Export CSV
        csv_exporter = CSVExporter(config)
        csv_files = csv_exporter.export_metadata(schemas, "demo_metadata")
        print(f"✅ CSV exports: {len(csv_files)} files")
        
        # Export quality metrics
        json_metrics_file = json_exporter.export_quality_metrics(metrics, "demo_quality_metrics.json")
        print(f"✅ Quality metrics JSON: {json_metrics_file}")
        
    except Exception as e:
        print(f"❌ Export error: {e}")
        return
    
    print("\n🎉 Demo completed successfully!")
    print("\nNext steps:")
    print("1. Check the output/ directory for exported files")
    print("2. Review the JSON files for detailed metadata")
    print("3. Use the CSV files for spreadsheet analysis")
    print("4. Run 'python -m src.app --help' for more options")


if __name__ == "__main__":
    demo_metadata_extraction()
