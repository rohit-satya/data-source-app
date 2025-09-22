"""Normalized PostgreSQL export functionality for metadata and quality metrics."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
import json

from ..models.normalized_models import NormalizedSchema, NormalizedTable, NormalizedColumn
from ..extractor.quality_metrics import TableQualityMetrics, ColumnQualityMetrics
from ..config import AppConfig
from ..db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class NormalizedPostgreSQLExporter:
    """Export normalized metadata and quality metrics to PostgreSQL database."""
    
    def __init__(self, config: AppConfig, db_connection: DatabaseConnection):
        """Initialize normalized PostgreSQL exporter.
        
        Args:
            config: Application configuration
            db_connection: Database connection instance
        """
        self.config = config
        self.db_connection = db_connection
        self.production_schema = "dsa_production"
    
    def export_metadata(self, schemas: List[NormalizedSchema], 
                       sync_id: Optional[str] = None) -> str:
        """Export normalized metadata to PostgreSQL database.
        
        Args:
            schemas: List of normalized schema metadata objects
            sync_id: Optional sync ID for tracking
            
        Returns:
            Sync ID of the extraction
        """
        start_time = time.time()
        
        # Use the production connection with schema set
        from ..services.database_service import DatabaseService
        database_service = DatabaseService(self.config)
        
        with database_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                try:
                    # Debug: Check search path
                    cur.execute("SELECT current_schemas(true)")
                    schemas_debug = cur.fetchone()
                    logger.info(f"Current search path: {schemas_debug[0]}")
                    
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Create or get sync ID
                    if sync_id is None:
                        sync_id = self._create_sync_run(cur, schemas)
                    else:
                        # Ensure sync_id exists in sync_runs table
                        self._ensure_sync_run_exists(cur, sync_id, schemas)
                    
                    # Export normalized schemas
                    self._export_normalized_schemas(cur, sync_id, schemas)
                    
                    # Export normalized tables
                    self._export_normalized_tables(cur, sync_id, schemas)
                    
                    # Export normalized columns
                    self._export_normalized_columns(cur, sync_id, schemas)
                    
                    # Calculate duration
                    duration = time.time() - start_time
                    self._finalize_sync_run(cur, sync_id, duration, 'completed')
                    
                    # Commit transaction
                    cur.execute("COMMIT")
                    
                    logger.info(f"Normalized metadata exported to PostgreSQL with sync_id: {sync_id}")
                    return sync_id
                    
                except Exception as e:
                    # Rollback on error
                    cur.execute("ROLLBACK")
                    self._finalize_sync_run(cur, sync_id, 0, 'failed', str(e))
                    raise e
    
    def export_quality_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], 
                              sync_id: Optional[str] = None, 
                              connection_name: str = "test-connection",
                              connector_name: str = "postgres",
                              tenant_id: str = "default") -> str:
        """Export quality metrics to PostgreSQL database.
        
        Args:
            metrics: Dictionary of schema metrics
            sync_id: Optional sync ID for tracking
            connection_name: Name of the connection
            connector_name: Name of the connector
            tenant_id: Tenant identifier
            
        Returns:
            Sync ID of the extraction
        """
        start_time = time.time()
        
        # Use the production connection with schema set
        from ..services.database_service import DatabaseService
        database_service = DatabaseService(self.config)
        
        with database_service.get_production_connection_with_schema() as conn:
            with conn.cursor() as cur:
                try:
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Create or get sync ID
                    if sync_id is None:
                        sync_id = self._create_quality_metrics_run(cur, metrics, connection_name, connector_name, tenant_id)
                    else:
                        # Ensure sync_id exists in quality_metrics_runs table
                        self._ensure_quality_metrics_run_exists(cur, sync_id, metrics, connection_name, connector_name, tenant_id)
                    
                    # Calculate totals
                    total_tables = sum(len(table_metrics) for table_metrics in metrics.values())
                    total_columns = sum(
                        len(table.column_metrics) for table_metrics in metrics.values() 
                        for table in table_metrics
                    )
                    
                    # Update run with totals
                    self._update_quality_metrics_run(cur, sync_id, total_tables, total_columns)
                    
                    # Export table quality metrics
                    self._export_table_quality_metrics(cur, sync_id, metrics)
                    
                    # Export column quality metrics
                    self._export_column_quality_metrics(cur, sync_id, metrics)
                    
                    # Calculate duration
                    duration = time.time() - start_time
                    self._finalize_quality_metrics_run(cur, sync_id, duration, 'completed')
                    
                    # Commit transaction
                    cur.execute("COMMIT")
                    
                    logger.info(f"Quality metrics exported to PostgreSQL with sync_id: {sync_id}")
                    return sync_id
                    
                except Exception as e:
                    # Rollback on error
                    cur.execute("ROLLBACK")
                    self._finalize_quality_metrics_run(cur, sync_id, 0, 'failed', str(e))
                    raise e
    
    def _create_sync_run(self, cur, schemas: List[NormalizedSchema]) -> str:
        """Create a new sync run record."""
        if not schemas:
            raise ValueError("No schemas provided for sync run")
        
        # Get sync_id from the first schema (all should have the same sync_id)
        sync_id = schemas[0].lastSyncRun
        connector_name = schemas[0].connectorName
        connection_name = schemas[0].connectionName
        tenant_id = schemas[0].tenantId
        
        cur.execute("""
            INSERT INTO sync_runs (sync_id, connector_name, connection_name, tenant_id, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (sync_id) DO NOTHING
        """, (sync_id, connector_name, connection_name, tenant_id))
        
        return sync_id
    
    def _ensure_sync_run_exists(self, cur, sync_id: str, schemas: List[NormalizedSchema]):
        """Ensure sync_id exists in sync_runs table."""
        if not schemas:
            raise ValueError("No schemas provided for sync run")
        
        # Get metadata from the first schema (all should have the same metadata)
        connector_name = schemas[0].connectorName
        connection_name = schemas[0].connectionName
        tenant_id = schemas[0].tenantId
        
        cur.execute("""
            INSERT INTO sync_runs (sync_id, connector_name, connection_name, tenant_id, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (sync_id) DO NOTHING
        """, (sync_id, connector_name, connection_name, tenant_id))
    
    def _export_normalized_schemas(self, cur, sync_id: str, schemas: List[NormalizedSchema]):
        """Export normalized schemas to database."""
        for schema in schemas:
            cur.execute("""
                INSERT INTO normalized_schemas (
                    sync_id, type_name, status, name, connection_name, tenant_id,
                    last_sync_run, last_sync_run_at, connector_name,
                    attributes, custom_attributes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            """, (
                sync_id, schema.typeName, schema.status, schema.name,
                schema.connectionName, schema.tenantId, schema.lastSyncRun,
                schema.lastSyncRunAt, schema.connectorName,
                json.dumps(schema.attributes), json.dumps(schema.customAttributes)
            ))
    
    def _export_normalized_tables(self, cur, sync_id: str, schemas: List[NormalizedSchema]):
        """Export normalized tables to database."""
        for schema in schemas:
            for table in schema.tables:
                cur.execute("""
                    INSERT INTO normalized_tables (
                        sync_id, type_name, status, name, connection_name, tenant_id,
                        last_sync_run, last_sync_run_at, connector_name,
                        attributes, custom_attributes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                """, (
                    sync_id, table.typeName, table.status, table.name,
                    table.connectionName, table.tenantId, table.lastSyncRun,
                    table.lastSyncRunAt, table.connectorName,
                    json.dumps(table.attributes), json.dumps(table.customAttributes)
                ))
    
    def _export_normalized_columns(self, cur, sync_id: str, schemas: List[NormalizedSchema]):
        """Export normalized columns to database."""
        for schema in schemas:
            for table in schema.tables:
                for column in table.columns:
                    cur.execute("""
                        INSERT INTO normalized_columns (
                            sync_id, type_name, status, name, connection_name, tenant_id,
                            last_sync_run, last_sync_run_at, connector_name,
                            attributes, custom_attributes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                    """, (
                        sync_id, column.typeName, column.status, column.name,
                        column.connectionName, column.tenantId, column.lastSyncRun,
                        column.lastSyncRunAt, column.connectorName,
                        json.dumps(column.attributes), json.dumps(column.customAttributes)
                    ))
    
    def _finalize_sync_run(self, cur, sync_id: str, duration: float, status: str, error_message: str = None):
        """Finalize sync run with duration and status."""
        cur.execute("""
            UPDATE sync_runs 
            SET status = %s, error_message = %s
            WHERE sync_id = %s
        """, (status, error_message, sync_id))
    
    def _create_quality_metrics_run(self, cur, metrics: Dict[str, List[TableQualityMetrics]], 
                                   connection_name: str = "test-connection", 
                                   connector_name: str = "postgres", 
                                   tenant_id: str = "default") -> str:
        """Create a new quality metrics run record."""
        # Generate a new sync_id for quality metrics
        import uuid
        sync_id = str(uuid.uuid4())
        
        target_schemas = list(metrics.keys())
        
        # First ensure sync_id exists in sync_runs table
        cur.execute("""
            INSERT INTO sync_runs (sync_id, connector_name, connection_name, tenant_id, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (sync_id) DO NOTHING
        """, (sync_id, connector_name, connection_name, tenant_id))
        
        cur.execute("""
            INSERT INTO quality_metrics_runs (sync_id, target_schemas, total_tables, total_columns, status)
            VALUES (%s, %s, 0, 0, 'running')
        """, (sync_id, target_schemas))
        
        return sync_id
    
    def _update_quality_metrics_run(self, cur, sync_id: str, total_tables: int, total_columns: int):
        """Update quality metrics run with totals."""
        cur.execute("""
            UPDATE quality_metrics_runs 
            SET total_tables = %s, total_columns = %s
            WHERE sync_id = %s
        """, (total_tables, total_columns, sync_id))
    
    def _ensure_quality_metrics_run_exists(self, cur, sync_id: str, metrics: Dict[str, List[TableQualityMetrics]], 
                                          connection_name: str = "test-connection", 
                                          connector_name: str = "postgres", 
                                          tenant_id: str = "default"):
        """Ensure sync_id exists in quality_metrics_runs table."""
        target_schemas = list(metrics.keys())
        
        # First ensure sync_id exists in sync_runs table
        cur.execute("""
            INSERT INTO sync_runs (sync_id, connector_name, connection_name, tenant_id, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (sync_id) DO NOTHING
        """, (sync_id, connector_name, connection_name, tenant_id))
        
        # Then insert into quality_metrics_runs
        cur.execute("""
            INSERT INTO quality_metrics_runs (sync_id, target_schemas, total_tables, total_columns, status)
            VALUES (%s, %s, 0, 0, 'running')
            ON CONFLICT (sync_id) DO NOTHING
        """, (sync_id, target_schemas))
    
    def _export_table_quality_metrics(self, cur, sync_id: str, metrics: Dict[str, List[TableQualityMetrics]]):
        """Export table quality metrics."""
        # Get run_id from sync_id
        cur.execute("SELECT run_id FROM quality_metrics_runs WHERE sync_id = %s", (sync_id,))
        result = cur.fetchone()
        if not result:
            raise ValueError(f"No quality metrics run found for sync_id: {sync_id}")
        run_id = result[0]
        
        for schema_name, table_metrics in metrics.items():
            for table_metric in table_metrics:
                cur.execute("""
                    INSERT INTO table_quality_metrics (run_id, schema_name, table_name, row_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id, schema_name, table_name) DO UPDATE SET
                        row_count = EXCLUDED.row_count
                """, (run_id, table_metric.schema_name, table_metric.table_name, table_metric.row_count))
    
    def _export_column_quality_metrics(self, cur, sync_id: str, metrics: Dict[str, List[TableQualityMetrics]]):
        """Export column quality metrics."""
        # Get run_id from sync_id
        cur.execute("SELECT run_id FROM quality_metrics_runs WHERE sync_id = %s", (sync_id,))
        result = cur.fetchone()
        if not result:
            raise ValueError(f"No quality metrics run found for sync_id: {sync_id}")
        run_id = result[0]
        
        for schema_name, table_metrics in metrics.items():
            for table_metric in table_metrics:
                for column_metric in table_metric.column_metrics:
                    cur.execute("""
                        INSERT INTO column_quality_metrics (
                            run_id, schema_name, table_name, column_name,
                            total_count, non_null_count, null_count, null_percentage,
                            distinct_count, distinct_percentage
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, schema_name, table_name, column_name) DO UPDATE SET
                            total_count = EXCLUDED.total_count,
                            non_null_count = EXCLUDED.non_null_count,
                            null_count = EXCLUDED.null_count,
                            null_percentage = EXCLUDED.null_percentage,
                            distinct_count = EXCLUDED.distinct_count,
                            distinct_percentage = EXCLUDED.distinct_percentage
                    """, (
                        run_id, schema_name, table_metric.table_name, column_metric.column_name,
                        column_metric.total_count, column_metric.non_null_count, column_metric.null_count,
                        column_metric.null_percentage, column_metric.distinct_count, column_metric.distinct_percentage
                    ))
                    
                    # Export top values
                    if column_metric.top_values:
                        # Get the metric_id for this column
                        cur.execute("""
                            SELECT metric_id FROM column_quality_metrics 
                            WHERE run_id = %s AND schema_name = %s AND table_name = %s AND column_name = %s
                        """, (run_id, schema_name, table_metric.table_name, column_metric.column_name))
                        metric_result = cur.fetchone()
                        if metric_result:
                            metric_id = metric_result[0]
                            for top_value in column_metric.top_values:
                                value_text = str(top_value['value'])
                                frequency = top_value['frequency']
                                # Calculate percentage (frequency / total_count * 100)
                                percentage = (frequency / column_metric.total_count * 100) if column_metric.total_count > 0 else 0
                                cur.execute("""
                                    INSERT INTO column_top_values (metric_id, value_text, frequency, percentage)
                                    VALUES (%s, %s, %s, %s)
                                """, (metric_id, value_text, frequency, percentage))
    
    def _finalize_quality_metrics_run(self, cur, sync_id: str, duration: float, status: str, error_message: str = None):
        """Finalize quality metrics run with duration and status."""
        cur.execute("""
            UPDATE quality_metrics_runs 
            SET extraction_duration_seconds = %s, status = %s, error_message = %s
            WHERE sync_id = %s
        """, (duration, status, error_message, sync_id))
    
    def get_latest_metadata_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the latest metadata extraction run."""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sync_id, sync_timestamp, connector_name, connection_name,
                        tenant_id, status, created_at
                    FROM sync_runs 
                    WHERE status = 'completed'
                    ORDER BY sync_timestamp DESC 
                    LIMIT 1
                """)
                result = cur.fetchone()
                
                if result:
                    return {
                        'sync_id': result[0],
                        'sync_timestamp': result[1],
                        'connector_name': result[2],
                        'connection_name': result[3],
                        'tenant_id': result[4],
                        'status': result[5],
                        'created_at': result[6]
                    }
                return None
    
    def get_latest_quality_metrics_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the latest quality metrics extraction run."""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sync_id, extraction_timestamp, total_tables, total_columns,
                        extraction_duration_seconds, status, created_at
                    FROM quality_metrics_runs 
                    WHERE status = 'completed'
                    ORDER BY extraction_timestamp DESC 
                    LIMIT 1
                """)
                result = cur.fetchone()
                
                if result:
                    return {
                        'sync_id': result[0],
                        'extraction_timestamp': result[1],
                        'total_tables': result[2],
                        'total_columns': result[3],
                        'extraction_duration_seconds': result[4],
                        'status': result[5],
                        'created_at': result[6]
                    }
                return None
    
    def cleanup_old_metadata(self, days: int = 30) -> int:
        """Clean up old metadata records.
        
        Args:
            days: Number of days to keep metadata
            
        Returns:
            Number of records deleted
        """
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                # Delete old sync runs and cascade to related tables
                cur.execute("""
                    DELETE FROM sync_runs 
                    WHERE created_at < NOW() - INTERVAL '%s days'
                """, (days,))
                
                deleted_count = cur.rowcount
                logger.info(f"Cleaned up {deleted_count} old metadata records")
                return deleted_count
