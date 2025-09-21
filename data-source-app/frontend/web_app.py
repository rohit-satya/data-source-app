#!/usr/bin/env python3
"""
Web Frontend for Metadata Display
A simple Flask web application to display metadata in a browser.
"""

import os
import sys
import json
import psycopg
from datetime import datetime
from typing import Dict, List, Any, Optional
from flask import Flask, render_template, request, jsonify
import yaml

# Add the src directory to the path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import AppConfig

app = Flask(__name__)

class MetadataWebFrontend:
    """Web frontend for displaying metadata."""
    
    def __init__(self, config_file: str = "config.yml"):
        """Initialize the web frontend with configuration."""
        self.config = AppConfig.from_file(config_file)
        self.config.load_environment_variables()
        self.connection_string = self.config.database.get_connection_string()
    
    def get_available_connections(self) -> List[Dict[str, Any]]:
        """Get list of available connection IDs."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET search_path TO dsa_production, public;")
                    cur.execute("""
                        SELECT DISTINCT 
                            connection_name,
                            connector_name,
                            MAX(sync_timestamp) as latest_sync,
                            COUNT(*) as sync_count
                        FROM sync_runs 
                        WHERE status = 'completed'
                        GROUP BY connection_name, connector_name
                        ORDER BY latest_sync DESC
                    """)
                    
                    connections = []
                    for row in cur.fetchall():
                        connections.append({
                            'connection_id': row[0],
                            'connector_name': row[1],
                            'latest_sync': row[2],
                            'sync_count': row[3]
                        })
                    return connections
        except Exception as e:
            print(f"Error getting available connections: {e}")
            return []
    
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
                            'sync_id': str(result[0]),
                            'sync_timestamp': result[1].isoformat(),
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
                            'last_sync_run': str(row[5]),
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
                            'last_sync_run': str(row[5]),
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
                            'last_sync_run': str(row[5]),
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

# Initialize the web frontend
web_frontend = MetadataWebFrontend()

@app.route('/')
def index():
    """Main page showing available connections."""
    connections = web_frontend.get_available_connections()
    return render_template('index.html', connections=connections)

@app.route('/metadata/<connection_id>')
def metadata(connection_id):
    """Display metadata for a specific connection."""
    sync_run = web_frontend.get_latest_sync_run(connection_id)
    if not sync_run:
        return render_template('error.html', 
                             error=f"No metadata found for connection: {connection_id}")
    
    metadata = web_frontend.get_latest_metadata(sync_run['sync_id'])
    
    return render_template('metadata.html', 
                         connection_id=connection_id,
                         sync_run=sync_run,
                         metadata=metadata)

@app.route('/api/connections')
def api_connections():
    """API endpoint to get available connections."""
    connections = web_frontend.get_available_connections()
    return jsonify(connections)

@app.route('/api/metadata/<connection_id>')
def api_metadata(connection_id):
    """API endpoint to get metadata for a connection."""
    sync_run = web_frontend.get_latest_sync_run(connection_id)
    if not sync_run:
        return jsonify({'error': f'No metadata found for connection: {connection_id}'}), 404
    
    metadata = web_frontend.get_latest_metadata(sync_run['sync_id'])
    
    return jsonify({
        'connection_id': connection_id,
        'sync_run': sync_run,
        'metadata': metadata
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
