"""Service for handling incremental diff operations between sync runs."""

import uuid
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from ..services.database_service import DatabaseService
from ..config import AppConfig


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class DiffResult:
    """Result of a diff operation."""
    change_type: str  # 'added', 'removed', 'modified', 'unchanged'
    differences: Dict[str, Any]
    sync_run_1_data: Optional[Dict[str, Any]] = None
    sync_run_2_data: Optional[Dict[str, Any]] = None


class IncrementalDiffService:
    """Service for calculating incremental differences between sync runs."""
    
    def __init__(self, config: AppConfig):
        """Initialize the incremental diff service.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.db_service = DatabaseService(config)
        self.logger = logging.getLogger(__name__)
    
    def create_diff_sync_run(
        self, 
        connection_id: str, 
        sync_run_1_id: str, 
        sync_run_2_id: str
    ) -> str:
        """Create a new diff sync run record.
        
        Args:
            connection_id: Connection identifier
            sync_run_1_id: First sync run ID (older)
            sync_run_2_id: Second sync run ID (newer)
            
        Returns:
            Diff sync ID
        """
        diff_sync_id = str(uuid.uuid4())
        
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO diff_sync_runs 
                    (diff_sync_id, connection_id, sync_run_1_id, sync_run_2_id, status)
                    VALUES (%s, %s, %s, %s, 'running')
                """, (diff_sync_id, connection_id, sync_run_1_id, sync_run_2_id))
                conn.commit()
        
        self.logger.info(f"Created diff sync run: {diff_sync_id}")
        return diff_sync_id
    
    def get_last_two_sync_runs(self, connection_id: str) -> Optional[Tuple[str, str]]:
        """Get the last two sync run IDs for a connection.
        
        Args:
            connection_id: Connection identifier
            
        Returns:
            Tuple of (older_sync_id, newer_sync_id) or None if not enough runs
        """
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sync_id 
                    FROM sync_runs 
                    WHERE connection_name = %s 
                    ORDER BY sync_timestamp DESC 
                    LIMIT 2
                """, (connection_id,))
                
                results = cur.fetchall()
                if len(results) < 2:
                    return None
                
                return (results[1][0], results[0][0])  # older, newer
    
    def calculate_schema_diff(
        self, 
        diff_sync_id: str, 
        sync_run_1_id: str, 
        sync_run_2_id: str
    ) -> int:
        """Calculate differences between schemas in two sync runs.
        
        Args:
            diff_sync_id: Diff sync run ID
            sync_run_1_id: First sync run ID (older)
            sync_run_2_id: Second sync run ID (newer)
            
        Returns:
            Number of schema changes
        """
        changes_count = 0
        
        # Get schemas from both sync runs
        schemas_1 = self._get_schemas_for_sync_run(sync_run_1_id)
        schemas_2 = self._get_schemas_for_sync_run(sync_run_2_id)
        
        # Create lookup dictionaries
        schemas_1_dict = {s['name']: s for s in schemas_1}
        schemas_2_dict = {s['name']: s for s in schemas_2}
        
        # Find all unique schema names
        all_schema_names = set(schemas_1_dict.keys()) | set(schemas_2_dict.keys())
        
        for schema_name in all_schema_names:
            schema_1 = schemas_1_dict.get(schema_name)
            schema_2 = schemas_2_dict.get(schema_name)
            
            diff_result = self._calculate_asset_diff(schema_1, schema_2)
            
            if diff_result.change_type != 'unchanged':
                self._save_schema_diff(
                    diff_sync_id, schema_name, diff_result, 
                    schema_1, schema_2
                )
                changes_count += 1
        
        return changes_count
    
    def calculate_table_diff(
        self, 
        diff_sync_id: str, 
        sync_run_1_id: str, 
        sync_run_2_id: str
    ) -> int:
        """Calculate differences between tables in two sync runs.
        
        Args:
            diff_sync_id: Diff sync run ID
            sync_run_1_id: First sync run ID (older)
            sync_run_2_id: Second sync run ID (newer)
            
        Returns:
            Number of table changes
        """
        changes_count = 0
        
        # Get tables from both sync runs
        tables_1 = self._get_tables_for_sync_run(sync_run_1_id)
        tables_2 = self._get_tables_for_sync_run(sync_run_2_id)
        
        # Create lookup dictionaries
        tables_1_dict = {(t['schema_name'], t['name']): t for t in tables_1}
        tables_2_dict = {(t['schema_name'], t['name']): t for t in tables_2}
        
        # Find all unique table keys
        all_table_keys = set(tables_1_dict.keys()) | set(tables_2_dict.keys())
        
        for schema_name, table_name in all_table_keys:
            table_1 = tables_1_dict.get((schema_name, table_name))
            table_2 = tables_2_dict.get((schema_name, table_name))
            
            diff_result = self._calculate_asset_diff(table_1, table_2)
            
            if diff_result.change_type != 'unchanged':
                self._save_table_diff(
                    diff_sync_id, schema_name, table_name, diff_result,
                    table_1, table_2
                )
                changes_count += 1
        
        return changes_count
    
    def calculate_column_diff(
        self, 
        diff_sync_id: str, 
        sync_run_1_id: str, 
        sync_run_2_id: str
    ) -> int:
        """Calculate differences between columns in two sync runs.
        
        Args:
            diff_sync_id: Diff sync run ID
            sync_run_1_id: First sync run ID (older)
            sync_run_2_id: Second sync run ID (newer)
            
        Returns:
            Number of column changes
        """
        changes_count = 0
        
        # Get columns from both sync runs
        columns_1 = self._get_columns_for_sync_run(sync_run_1_id)
        columns_2 = self._get_columns_for_sync_run(sync_run_2_id)
        
        # Create lookup dictionaries
        columns_1_dict = {(c['schema_name'], c['table_name'], c['name']): c for c in columns_1}
        columns_2_dict = {(c['schema_name'], c['table_name'], c['name']): c for c in columns_2}
        
        # Find all unique column keys
        all_column_keys = set(columns_1_dict.keys()) | set(columns_2_dict.keys())
        
        for schema_name, table_name, column_name in all_column_keys:
            column_1 = columns_1_dict.get((schema_name, table_name, column_name))
            column_2 = columns_2_dict.get((schema_name, table_name, column_name))
            
            diff_result = self._calculate_asset_diff(column_1, column_2)
            
            if diff_result.change_type != 'unchanged':
                self._save_column_diff(
                    diff_sync_id, schema_name, table_name, column_name, diff_result,
                    column_1, column_2
                )
                changes_count += 1
        
        return changes_count
    
    def _get_schemas_for_sync_run(self, sync_id: str) -> List[Dict[str, Any]]:
        """Get schemas for a specific sync run."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        name,
                        attributes,
                        custom_attributes,
                        created_at
                    FROM normalized_schemas 
                    WHERE sync_id = %s
                """, (sync_id,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
    
    def _get_tables_for_sync_run(self, sync_id: str) -> List[Dict[str, Any]]:
        """Get tables for a specific sync run."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        t.attributes->>'schemaName' as schema_name,
                        t.name,
                        t.attributes,
                        t.custom_attributes,
                        t.created_at
                    FROM normalized_tables t
                    WHERE t.sync_id = %s
                """, (sync_id,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
    
    def _get_columns_for_sync_run(self, sync_id: str) -> List[Dict[str, Any]]:
        """Get columns for a specific sync run."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        c.attributes->>'schemaName' as schema_name,
                        COALESCE(c.attributes->>'tableName', 'unknown') as table_name,
                        c.name,
                        c.attributes,
                        c.custom_attributes,
                        c.created_at
                    FROM normalized_columns c
                    WHERE c.sync_id = %s
                """, (sync_id,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
    
    def _calculate_asset_diff(
        self, 
        asset_1: Optional[Dict[str, Any]], 
        asset_2: Optional[Dict[str, Any]]
    ) -> DiffResult:
        """Calculate differences between two assets.
        
        Args:
            asset_1: First asset (older)
            asset_2: Second asset (newer)
            
        Returns:
            DiffResult with change type and differences
        """
        if asset_1 is None and asset_2 is None:
            return DiffResult('unchanged', {})
        
        if asset_1 is None:
            return DiffResult('added', {}, None, asset_2)
        
        if asset_2 is None:
            return DiffResult('removed', {}, asset_1, None)
        
        # Compare attributes and custom_attributes
        differences = {}
        
        # Compare attributes
        attrs_1 = asset_1.get('attributes', {}) or {}
        attrs_2 = asset_2.get('attributes', {}) or {}
        
        for key in set(attrs_1.keys()) | set(attrs_2.keys()):
            val_1 = attrs_1.get(key)
            val_2 = attrs_2.get(key)
            
            if val_1 != val_2:
                differences[f'attributes.{key}'] = {
                    'old': val_1,
                    'new': val_2
                }
        
        # Compare custom_attributes
        custom_attrs_1 = asset_1.get('custom_attributes', {}) or {}
        custom_attrs_2 = asset_2.get('custom_attributes', {}) or {}
        
        for key in set(custom_attrs_1.keys()) | set(custom_attrs_2.keys()):
            val_1 = custom_attrs_1.get(key)
            val_2 = custom_attrs_2.get(key)
            
            if val_1 != val_2:
                differences[f'custom_attributes.{key}'] = {
                    'old': val_1,
                    'new': val_2
                }
        
        if differences:
            return DiffResult('modified', differences, asset_1, asset_2)
        else:
            return DiffResult('unchanged', {}, asset_1, asset_2)
    
    def _save_schema_diff(
        self, 
        diff_sync_id: str, 
        schema_name: str, 
        diff_result: DiffResult,
        schema_1: Optional[Dict[str, Any]], 
        schema_2: Optional[Dict[str, Any]]
    ):
        """Save schema diff to database."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO incremental_diff_schema 
                    (diff_sync_id, schema_name, change_type, sync_run_1_data, sync_run_2_data, differences)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    diff_sync_id, schema_name, diff_result.change_type,
                    json.dumps(schema_1, cls=DateTimeEncoder) if schema_1 else None,
                    json.dumps(schema_2, cls=DateTimeEncoder) if schema_2 else None,
                    json.dumps(diff_result.differences, cls=DateTimeEncoder)
                ))
                conn.commit()
    
    def _save_table_diff(
        self, 
        diff_sync_id: str, 
        schema_name: str, 
        table_name: str, 
        diff_result: DiffResult,
        table_1: Optional[Dict[str, Any]], 
        table_2: Optional[Dict[str, Any]]
    ):
        """Save table diff to database."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO incremental_diff_table 
                    (diff_sync_id, schema_name, table_name, change_type, sync_run_1_data, sync_run_2_data, differences)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    diff_sync_id, schema_name, table_name, diff_result.change_type,
                    json.dumps(table_1, cls=DateTimeEncoder) if table_1 else None,
                    json.dumps(table_2, cls=DateTimeEncoder) if table_2 else None,
                    json.dumps(diff_result.differences, cls=DateTimeEncoder)
                ))
                conn.commit()
    
    def _save_column_diff(
        self, 
        diff_sync_id: str, 
        schema_name: str, 
        table_name: str, 
        column_name: str, 
        diff_result: DiffResult,
        column_1: Optional[Dict[str, Any]], 
        column_2: Optional[Dict[str, Any]]
    ):
        """Save column diff to database."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO incremental_diff_column 
                    (diff_sync_id, schema_name, table_name, column_name, change_type, sync_run_1_data, sync_run_2_data, differences)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    diff_sync_id, schema_name, table_name, column_name, diff_result.change_type,
                    json.dumps(column_1, cls=DateTimeEncoder) if column_1 else None,
                    json.dumps(column_2, cls=DateTimeEncoder) if column_2 else None,
                    json.dumps(diff_result.differences, cls=DateTimeEncoder)
                ))
                conn.commit()
    
    def update_diff_sync_run_status(
        self, 
        diff_sync_id: str, 
        status: str, 
        total_schemas_changed: int = 0,
        total_tables_changed: int = 0,
        total_columns_changed: int = 0,
        error_message: Optional[str] = None
    ):
        """Update the status of a diff sync run."""
        with self.db_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                if status == 'completed':
                    cur.execute("""
                        UPDATE diff_sync_runs 
                        SET status = %s, completed_at = NOW(), 
                            total_schemas_changed = %s, total_tables_changed = %s, total_columns_changed = %s
                        WHERE diff_sync_id = %s
                    """, (status, total_schemas_changed, total_tables_changed, total_columns_changed, diff_sync_id))
                else:
                    cur.execute("""
                        UPDATE diff_sync_runs 
                        SET status = %s, error_message = %s
                        WHERE diff_sync_id = %s
                    """, (status, error_message, diff_sync_id))
                conn.commit()
    
    def run_incremental_diff(
        self, 
        connection_id: str, 
        output_format: str = "postgres"
    ) -> Dict[str, Any]:
        """Run the complete incremental diff process.
        
        Args:
            connection_id: Connection identifier
            output_format: Output format (not used for now, but kept for consistency)
            
        Returns:
            Dictionary with diff results
        """
        self.logger.info(f"Starting incremental diff for connection: {connection_id}")
        
        try:
            # Get last two sync runs
            sync_runs = self.get_last_two_sync_runs(connection_id)
            if not sync_runs:
                return {
                    "success": False,
                    "error": f"Not enough sync runs found for connection: {connection_id}. Need at least 2 runs."
                }
            
            sync_run_1_id, sync_run_2_id = sync_runs
            self.logger.info(f"Comparing sync runs: {sync_run_1_id} (older) vs {sync_run_2_id} (newer)")
            
            # Create diff sync run record
            diff_sync_id = self.create_diff_sync_run(connection_id, sync_run_1_id, sync_run_2_id)
            
            # Calculate differences
            schemas_changed = self.calculate_schema_diff(diff_sync_id, sync_run_1_id, sync_run_2_id)
            tables_changed = self.calculate_table_diff(diff_sync_id, sync_run_1_id, sync_run_2_id)
            columns_changed = self.calculate_column_diff(diff_sync_id, sync_run_1_id, sync_run_2_id)
            
            # Update status
            self.update_diff_sync_run_status(
                diff_sync_id, 'completed', 
                schemas_changed, tables_changed, columns_changed
            )
            
            result = {
                "success": True,
                "diff_sync_id": diff_sync_id,
                "connection_id": connection_id,
                "sync_run_1_id": sync_run_1_id,
                "sync_run_2_id": sync_run_2_id,
                "schemas_changed": schemas_changed,
                "tables_changed": tables_changed,
                "columns_changed": columns_changed,
                "total_changes": schemas_changed + tables_changed + columns_changed
            }
            
            self.logger.info(f"Incremental diff completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Incremental diff failed: {e}")
            if 'diff_sync_id' in locals():
                self.update_diff_sync_run_status(diff_sync_id, 'failed', error_message=str(e))
            
            return {
                "success": False,
                "error": str(e),
                "connection_id": connection_id
            }
