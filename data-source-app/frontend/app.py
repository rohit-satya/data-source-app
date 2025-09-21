#!/usr/bin/env python3
"""
Simple Frontend for Metadata Display
Displays latest metadata for a given connection_id without touching existing components.
"""

import os
import sys
import json
import psycopg
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import yaml

# Add the src directory to the path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import AppConfig

@dataclass
class MetadataDisplay:
    """Data class for displaying metadata in the frontend."""
    connection_id: str
    sync_id: str
    sync_timestamp: datetime
    connector_name: str
    schemas: List[Dict[str, Any]]
    tables: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]

class MetadataFrontend:
    """Simple frontend for displaying metadata."""
    
    def __init__(self, config_file: str = "config.yml"):
        """Initialize the frontend with configuration."""
        self.config = AppConfig.from_file(config_file)
        self.config.load_environment_variables()
        self.connection_string = self.config.database.get_connection_string()
    
    def get_latest_sync_run(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest sync run for a given connection_id."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET search_path TO dsa_production, public;")
                    cur.execute("""
                        SELECT 
                            sync_id, 
                            sync_timestamp, 
                            connector_name, 
                            connection_name,
                            status
                        FROM sync_runs 
                        WHERE connection_name = %s 
                        AND status = 'completed'
                        ORDER BY sync_timestamp DESC 
                        LIMIT 1
                    """, (connection_id,))
                    
                    result = cur.fetchone()
                    if result:
                        return {
                            'sync_id': result[0],
                            'sync_timestamp': result[1],
                            'connector_name': result[2],
                            'connection_name': result[3],
                            'status': result[4]
                        }
                    return None
        except Exception as e:
            print(f"Error getting latest sync run: {e}")
            return None
    
    def get_latest_metadata(self, sync_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get latest metadata for a given sync_id."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET search_path TO dsa_production, public;")
                    
                    # Get schemas
                    cur.execute("""
                        SELECT 
                            type_name, status, name, connection_name, tenant_id,
                            last_sync_run, last_sync_run_at, connector_name,
                            attributes, custom_attributes
                        FROM normalized_schemas 
                        WHERE sync_id = %s
                        ORDER BY name
                    """, (sync_id,))
                    
                    schemas = []
                    for row in cur.fetchall():
                        schemas.append({
                            'type_name': row[0],
                            'status': row[1],
                            'name': row[2],
                            'connection_name': row[3],
                            'tenant_id': row[4],
                            'last_sync_run': row[5],
                            'last_sync_run_at': row[6],
                            'connector_name': row[7],
                            'attributes': row[8],
                            'custom_attributes': row[9]
                        })
                    
                    # Get tables
                    cur.execute("""
                        SELECT 
                            type_name, status, name, connection_name, tenant_id,
                            last_sync_run, last_sync_run_at, connector_name,
                            attributes, custom_attributes
                        FROM normalized_tables 
                        WHERE sync_id = %s
                        ORDER BY attributes->>'schemaName', name
                    """, (sync_id,))
                    
                    tables = []
                    for row in cur.fetchall():
                        tables.append({
                            'type_name': row[0],
                            'status': row[1],
                            'name': row[2],
                            'connection_name': row[3],
                            'tenant_id': row[4],
                            'last_sync_run': row[5],
                            'last_sync_run_at': row[6],
                            'connector_name': row[7],
                            'attributes': row[8],
                            'custom_attributes': row[9]
                        })
                    
                    # Get columns
                    cur.execute("""
                        SELECT 
                            type_name, status, name, connection_name, tenant_id,
                            last_sync_run, last_sync_run_at, connector_name,
                            attributes, custom_attributes
                        FROM normalized_columns 
                        WHERE sync_id = %s
                        ORDER BY attributes->>'schemaName', attributes->>'tableName', name
                    """, (sync_id,))
                    
                    columns = []
                    for row in cur.fetchall():
                        columns.append({
                            'type_name': row[0],
                            'status': row[1],
                            'name': row[2],
                            'connection_name': row[3],
                            'tenant_id': row[4],
                            'last_sync_run': row[5],
                            'last_sync_run_at': row[6],
                            'connector_name': row[7],
                            'attributes': row[8],
                            'custom_attributes': row[9]
                        })
                    
                    return {
                        'schemas': schemas,
                        'tables': tables,
                        'columns': columns
                    }
        except Exception as e:
            print(f"Error getting latest metadata: {e}")
            return {'schemas': [], 'tables': [], 'columns': []}
    
    def get_metadata_display(self, connection_id: str) -> Optional[MetadataDisplay]:
        """Get complete metadata display for a connection_id."""
        # Get latest sync run
        sync_run = self.get_latest_sync_run(connection_id)
        if not sync_run:
            return None
        
        # Get metadata for the sync run
        metadata = self.get_latest_metadata(sync_run['sync_id'])
        
        return MetadataDisplay(
            connection_id=connection_id,
            sync_id=sync_run['sync_id'],
            sync_timestamp=sync_run['sync_timestamp'],
            connector_name=sync_run['connector_name'],
            schemas=metadata['schemas'],
            tables=metadata['tables'],
            columns=metadata['columns']
        )
    
    def display_metadata(self, connection_id: str):
        """Display metadata in a formatted way."""
        metadata_display = self.get_metadata_display(connection_id)
        
        if not metadata_display:
            print(f"❌ No metadata found for connection_id: {connection_id}")
            return
        
        print("=" * 80)
        print(f"📊 METADATA DISPLAY FOR CONNECTION: {connection_id}")
        print("=" * 80)
        print(f"🆔 Sync ID: {metadata_display.sync_id}")
        print(f"⏰ Sync Timestamp: {metadata_display.sync_timestamp}")
        print(f"🔌 Connector: {metadata_display.connector_name}")
        print(f"📈 Summary: {len(metadata_display.schemas)} schemas, {len(metadata_display.tables)} tables, {len(metadata_display.columns)} columns")
        print()
        
        # Display schemas
        if metadata_display.schemas:
            print("📁 SCHEMAS")
            print("-" * 40)
            for schema in metadata_display.schemas:
                print(f"  • {schema['name']}")
                if schema['custom_attributes']:
                    print(f"    Custom Attributes: {json.dumps(schema['custom_attributes'], indent=6)}")
            print()
        
        # Display tables grouped by schema
        if metadata_display.tables:
            print("📋 TABLES")
            print("-" * 40)
            current_schema = None
            for table in metadata_display.tables:
                schema_name = table['attributes'].get('schemaName', 'unknown')
                if schema_name != current_schema:
                    print(f"  📁 Schema: {schema_name}")
                    current_schema = schema_name
                
                print(f"    • {table['name']}")
                if table['custom_attributes']:
                    table_type = table['custom_attributes'].get('table_type', '')
                    if table_type:
                        print(f"      Type: {table_type}")
                
                # Show columns for this table
                table_columns = [col for col in metadata_display.columns 
                               if col['attributes'].get('tableName') == table['name'] 
                               and col['attributes'].get('schemaName') == schema_name]
                
                if table_columns:
                    print(f"      Columns ({len(table_columns)}):")
                    print(f"        {'Name':<20} {'Type':<15} {'Nullable':<10} {'Order':<5} {'Comment'}")
                    print(f"        {'-'*20} {'-'*15} {'-'*10} {'-'*5} {'-'*30}")
                    for col in table_columns:
                        data_type = col['attributes'].get('dataType', 'unknown')
                        is_nullable = col['attributes'].get('isNullable', False)
                        nullable_str = "NULL" if is_nullable else "NOT NULL"
                        order = col['attributes'].get('order', col['custom_attributes'].get('ordinal_position', ''))
                        comment = col['custom_attributes'].get('comment', '') if col['custom_attributes'] else ''
                        comment = str(comment)[:30] + '...' if len(str(comment)) > 30 else str(comment)
                        print(f"        {col['name']:<20} {data_type:<15} {nullable_str:<10} {str(order):<5} {comment}")
            print()
        
        # Display all columns overview
        if metadata_display.columns:
            print("📋 ALL COLUMNS OVERVIEW")
            print("-" * 40)
            print(f"{'Schema':<15} {'Table':<15} {'Column':<20} {'Type':<15} {'Nullable':<10} {'Order':<5} {'Comment'}")
            print(f"{'-'*15} {'-'*15} {'-'*20} {'-'*15} {'-'*10} {'-'*5} {'-'*30}")
            
            for col in metadata_display.columns:
                schema_name = col['attributes'].get('schemaName', 'unknown')
                # Extract table name from qualified name
                qualified_name = col['attributes'].get('qualifiedName', '')
                table_name = 'unknown'
                if qualified_name and '/' in qualified_name:
                    parts = qualified_name.split('/')
                    if len(parts) >= 4:  # tenant/connector/timestamp/database/schema/table/column
                        table_name = parts[-2] if len(parts) > 2 else 'unknown'
                
                data_type = col['attributes'].get('dataType', 'unknown')
                is_nullable = col['attributes'].get('isNullable', False)
                nullable_str = "NULL" if is_nullable else "NOT NULL"
                order = col['attributes'].get('order', col['custom_attributes'].get('ordinal_position', ''))
                comment = col['custom_attributes'].get('comment', '') if col['custom_attributes'] else ''
                comment = str(comment)[:30] + '...' if len(str(comment)) > 30 else str(comment)
                print(f"{schema_name:<15} {table_name:<15} {col['name']:<20} {data_type:<15} {nullable_str:<10} {str(order):<5} {comment}")
            print()
        
        # Display summary statistics
        print("📊 SUMMARY STATISTICS")
        print("-" * 40)
        print(f"Total Schemas: {len(metadata_display.schemas)}")
        print(f"Total Tables: {len(metadata_display.tables)}")
        print(f"Total Columns: {len(metadata_display.columns)}")
        
        # Count by schema
        schema_counts = {}
        for table in metadata_display.tables:
            schema_name = table['attributes'].get('schemaName', 'unknown')
            schema_counts[schema_name] = schema_counts.get(schema_name, 0) + 1
        
        if schema_counts:
            print("\nTables by Schema:")
            for schema, count in schema_counts.items():
                print(f"  {schema}: {count} tables")
        
        print("=" * 80)

def main():
    """Main function to run the frontend."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Metadata Frontend Display')
    parser.add_argument('connection_id', help='Connection ID to display metadata for')
    parser.add_argument('--config', '-c', default='config.yml', help='Configuration file path')
    
    args = parser.parse_args()
    
    try:
        frontend = MetadataFrontend(args.config)
        frontend.display_metadata(args.connection_id)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
