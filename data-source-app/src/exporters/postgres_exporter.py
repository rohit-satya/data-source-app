"""PostgreSQL export functionality for metadata and quality metrics."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time

from ..extractor.metadata_extractor import SchemaMetadata, TableMetadata, ColumnMetadata
from ..extractor.quality_metrics import TableQualityMetrics, ColumnQualityMetrics
from ..config import AppConfig
from ..db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class PostgreSQLExporter:
    """Export metadata and quality metrics to PostgreSQL database."""
    
    def __init__(self, config: AppConfig, db_connection: DatabaseConnection):
        """Initialize PostgreSQL exporter.
        
        Args:
            config: Application configuration
            db_connection: Database connection instance
        """
        self.config = config
        self.db_connection = db_connection
        self.production_schema = "dsa_production"
    
    def export_metadata(self, schemas: List[SchemaMetadata], 
                       run_id: Optional[int] = None) -> int:
        """Export metadata to PostgreSQL database.
        
        Args:
            schemas: List of schema metadata objects
            run_id: Optional run ID for tracking
            
        Returns:
            Run ID of the extraction
        """
        start_time = time.time()
        
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Create or get run ID
                    if run_id is None:
                        run_id = self._create_metadata_run(cur, schemas)
                    
                    # Calculate totals
                    total_tables = sum(len(schema.tables) for schema in schemas)
                    total_columns = sum(
                        len(table.columns) for schema in schemas for table in schema.tables
                    )
                    total_constraints = sum(
                        len(table.constraints) for schema in schemas for table in schema.tables
                    )
                    total_indexes = sum(
                        len(table.indexes) for schema in schemas for table in schema.tables
                    )
                    
                    # Update run with totals
                    self._update_metadata_run(cur, run_id, total_tables, total_columns, 
                                            total_constraints, total_indexes)
                    
                    # Export schemas
                    self._export_schemas(cur, run_id, schemas)
                    
                    # Export tables
                    self._export_tables(cur, run_id, schemas)
                    
                    # Export columns
                    self._export_columns(cur, run_id, schemas)
                    
                    # Export constraints
                    self._export_constraints(cur, run_id, schemas)
                    
                    # Export indexes
                    self._export_indexes(cur, run_id, schemas)
                    
                    # Calculate duration
                    duration = time.time() - start_time
                    self._finalize_metadata_run(cur, run_id, duration, 'completed')
                    
                    # Commit transaction
                    cur.execute("COMMIT")
                    
                    logger.info(f"Metadata exported to PostgreSQL with run_id: {run_id}")
                    return run_id
                    
                except Exception as e:
                    # Rollback on error
                    cur.execute("ROLLBACK")
                    self._finalize_metadata_run(cur, run_id, 0, 'failed', str(e))
                    raise e
    
    def export_quality_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], 
                              run_id: Optional[int] = None) -> int:
        """Export quality metrics to PostgreSQL database.
        
        Args:
            metrics: Dictionary of schema metrics
            run_id: Optional run ID for tracking
            
        Returns:
            Run ID of the metrics extraction
        """
        start_time = time.time()
        
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Create or get run ID
                    if run_id is None:
                        run_id = self._create_quality_metrics_run(cur, metrics)
                    
                    # Calculate totals
                    total_tables = sum(len(table_list) for table_list in metrics.values())
                    total_columns = sum(
                        len(table.column_metrics) for table_list in metrics.values() 
                        for table in table_list
                    )
                    
                    # Calculate quality metrics
                    high_null_columns = sum(
                        sum(1 for col in table.column_metrics if col.null_percentage > 50)
                        for table_list in metrics.values() for table in table_list
                    )
                    low_distinct_columns = sum(
                        sum(1 for col in table.column_metrics 
                            if col.distinct_percentage < 10 and col.total_count > 100)
                        for table_list in metrics.values() for table in table_list
                    )
                    
                    # Calculate overall quality score
                    if total_columns > 0:
                        quality_score = 100 - (high_null_columns / total_columns * 30) - (low_distinct_columns / total_columns * 20)
                        quality_score = max(0, min(100, round(quality_score, 1)))
                    else:
                        quality_score = 100.0
                    
                    # Update run with totals
                    self._update_quality_metrics_run(cur, run_id, total_tables, total_columns,
                                                   high_null_columns, low_distinct_columns, quality_score)
                    
                    # Export table metrics
                    self._export_table_metrics(cur, run_id, metrics)
                    
                    # Export column metrics
                    self._export_column_metrics(cur, run_id, metrics)
                    
                    # Export top values
                    self._export_top_values(cur, run_id, metrics)
                    
                    # Calculate duration
                    duration = time.time() - start_time
                    self._finalize_quality_metrics_run(cur, run_id, duration, 'completed')
                    
                    # Commit transaction
                    cur.execute("COMMIT")
                    
                    logger.info(f"Quality metrics exported to PostgreSQL with run_id: {run_id}")
                    return run_id
                    
                except Exception as e:
                    # Rollback on error
                    cur.execute("ROLLBACK")
                    self._finalize_quality_metrics_run(cur, run_id, 0, 'failed', str(e))
                    raise e
    
    def _create_metadata_run(self, cur, schemas: List[SchemaMetadata]) -> int:
        """Create a new metadata extraction run record."""
        target_schemas = [schema.name for schema in schemas]
        
        cur.execute(f"""
            INSERT INTO {self.production_schema}.metadata_extraction_runs 
            (target_schemas, total_schemas, total_tables, total_columns, 
             total_constraints, total_indexes, status)
            VALUES (%s, %s, 0, 0, 0, 0, 'running')
            RETURNING run_id
        """, (target_schemas, len(schemas)))
        
        return cur.fetchone()[0]
    
    def _update_metadata_run(self, cur, run_id: int, total_tables: int, 
                           total_columns: int, total_constraints: int, total_indexes: int):
        """Update metadata run with calculated totals."""
        cur.execute(f"""
            UPDATE {self.production_schema}.metadata_extraction_runs 
            SET total_tables = %s, total_columns = %s, 
                total_constraints = %s, total_indexes = %s
            WHERE run_id = %s
        """, (total_tables, total_columns, total_constraints, total_indexes, run_id))
    
    def _finalize_metadata_run(self, cur, run_id: int, duration: float, 
                              status: str, error_message: Optional[str] = None):
        """Finalize metadata run with duration and status."""
        cur.execute(f"""
            UPDATE {self.production_schema}.metadata_extraction_runs 
            SET extraction_duration_seconds = %s, status = %s, error_message = %s
            WHERE run_id = %s
        """, (duration, status, error_message, run_id))
    
    def _create_quality_metrics_run(self, cur, metrics: Dict[str, List[TableQualityMetrics]]) -> int:
        """Create a new quality metrics extraction run record."""
        target_schemas = list(metrics.keys())
        
        cur.execute(f"""
            INSERT INTO {self.production_schema}.quality_metrics_runs 
            (target_schemas, total_tables, total_columns, 
             high_null_columns, low_distinct_columns, status)
            VALUES (%s, 0, 0, 0, 0, 'running')
            RETURNING metrics_run_id
        """, (target_schemas,))
        
        return cur.fetchone()[0]
    
    def _update_quality_metrics_run(self, cur, run_id: int, total_tables: int, 
                                   total_columns: int, high_null_columns: int, 
                                   low_distinct_columns: int, quality_score: float):
        """Update quality metrics run with calculated totals."""
        cur.execute(f"""
            UPDATE {self.production_schema}.quality_metrics_runs 
            SET total_tables = %s, total_columns = %s, 
                high_null_columns = %s, low_distinct_columns = %s,
                overall_quality_score = %s
            WHERE metrics_run_id = %s
        """, (total_tables, total_columns, high_null_columns, 
              low_distinct_columns, quality_score, run_id))
    
    def _finalize_quality_metrics_run(self, cur, run_id: int, duration: float, 
                                     status: str, error_message: Optional[str] = None):
        """Finalize quality metrics run with duration and status."""
        cur.execute(f"""
            UPDATE {self.production_schema}.quality_metrics_runs 
            SET extraction_duration_seconds = %s, status = %s, error_message = %s
            WHERE metrics_run_id = %s
        """, (duration, status, error_message, run_id))
    
    def _export_schemas(self, cur, run_id: int, schemas: List[SchemaMetadata]):
        """Export schema metadata."""
        for schema in schemas:
            cur.execute(f"""
                INSERT INTO {self.production_schema}.schemas_metadata 
                (run_id, schema_name, owner, table_count)
                VALUES (%s, %s, %s, %s)
            """, (run_id, schema.name, schema.owner, len(schema.tables)))
    
    def _export_tables(self, cur, run_id: int, schemas: List[SchemaMetadata]):
        """Export table metadata."""
        for schema in schemas:
            for table in schema.tables:
                cur.execute(f"""
                    INSERT INTO {self.production_schema}.tables_metadata 
                    (run_id, schema_name, table_name, table_type, comment, tags,
                     column_count, constraint_count, index_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (run_id, table.schema, table.name, table.table_type, 
                      table.comment, table.tags, len(table.columns),
                      len(table.constraints), len(table.indexes)))
    
    def _export_columns(self, cur, run_id: int, schemas: List[SchemaMetadata]):
        """Export column metadata."""
        for schema in schemas:
            for table in schema.tables:
                for column in table.columns:
                    cur.execute(f"""
                        INSERT INTO {self.production_schema}.columns_metadata 
                        (run_id, schema_name, table_name, column_name, position,
                         data_type, is_nullable, default_value, max_length,
                         precision_value, scale_value, comment, tags)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (run_id, table.schema, table.name, column.name, column.position,
                          column.data_type, column.is_nullable, column.default_value,
                          column.max_length, column.precision, column.scale,
                          column.comment, column.tags))
    
    def _export_constraints(self, cur, run_id: int, schemas: List[SchemaMetadata]):
        """Export constraint metadata."""
        for schema in schemas:
            for table in schema.tables:
                for constraint in table.constraints:
                    cur.execute(f"""
                        INSERT INTO {self.production_schema}.constraints_metadata 
                        (run_id, schema_name, table_name, constraint_name, constraint_type,
                         columns, referenced_schema, referenced_table, referenced_columns)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (run_id, table.schema, table.name, constraint.name, constraint.type,
                          constraint.columns, constraint.referenced_schema,
                          constraint.referenced_table, constraint.referenced_columns))
    
    def _export_indexes(self, cur, run_id: int, schemas: List[SchemaMetadata]):
        """Export index metadata."""
        for schema in schemas:
            for table in schema.tables:
                for index in table.indexes:
                    cur.execute(f"""
                        INSERT INTO {self.production_schema}.indexes_metadata 
                        (run_id, schema_name, table_name, index_name, definition,
                         columns, is_unique, is_primary)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (run_id, table.schema, table.name, index.name, index.definition,
                          index.columns, index.is_unique, index.is_primary))
    
    def _export_table_metrics(self, cur, run_id: int, metrics: Dict[str, List[TableQualityMetrics]]):
        """Export table quality metrics."""
        for schema_name, table_metrics_list in metrics.items():
            for table_metrics in table_metrics_list:
                high_null_count = sum(1 for col in table_metrics.column_metrics 
                                    if col.null_percentage > 50)
                low_distinct_count = sum(1 for col in table_metrics.column_metrics 
                                       if col.distinct_percentage < 10 and col.total_count > 100)
                
                cur.execute(f"""
                    INSERT INTO {self.production_schema}.table_quality_metrics 
                    (metrics_run_id, schema_name, table_name, row_count, column_count,
                     high_null_columns, low_distinct_columns)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (run_id, table_metrics.schema_name, table_metrics.table_name,
                      table_metrics.row_count, len(table_metrics.column_metrics),
                      high_null_count, low_distinct_count))
    
    def _export_column_metrics(self, cur, run_id: int, metrics: Dict[str, List[TableQualityMetrics]]):
        """Export column quality metrics."""
        for schema_name, table_metrics_list in metrics.items():
            for table_metrics in table_metrics_list:
                for col_metrics in table_metrics.column_metrics:
                    cur.execute(f"""
                        INSERT INTO {self.production_schema}.column_quality_metrics 
                        (metrics_run_id, schema_name, table_name, column_name,
                         total_count, non_null_count, null_count, null_percentage,
                         distinct_count, distinct_percentage)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (run_id, table_metrics.schema_name, table_metrics.table_name,
                          col_metrics.column_name, col_metrics.total_count,
                          col_metrics.non_null_count, col_metrics.null_count,
                          col_metrics.null_percentage, col_metrics.distinct_count,
                          col_metrics.distinct_percentage))
    
    def _export_top_values(self, cur, run_id: int, metrics: Dict[str, List[TableQualityMetrics]]):
        """Export top values for columns."""
        for schema_name, table_metrics_list in metrics.items():
            for table_metrics in table_metrics_list:
                for col_metrics in table_metrics.column_metrics:
                    for top_value in col_metrics.top_values:
                        cur.execute(f"""
                            INSERT INTO {self.production_schema}.column_top_values 
                            (metrics_run_id, schema_name, table_name, column_name,
                             value_text, frequency)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (run_id, table_metrics.schema_name, table_metrics.table_name,
                              col_metrics.column_name, str(top_value['value']),
                              top_value['frequency']))
    
    def get_latest_metadata_run(self) -> Optional[Dict[str, Any]]:
        """Get the latest metadata extraction run."""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT run_id, extraction_timestamp, target_schemas, total_schemas,
                           total_tables, total_columns, total_constraints, total_indexes,
                           extraction_duration_seconds, status, error_message
                    FROM {self.production_schema}.metadata_extraction_runs
                    WHERE status = 'completed'
                    ORDER BY run_id DESC
                    LIMIT 1
                """)
                
                result = cur.fetchone()
                if result:
                    return {
                        'run_id': result[0],
                        'extraction_timestamp': result[1],
                        'target_schemas': result[2],
                        'total_schemas': result[3],
                        'total_tables': result[4],
                        'total_columns': result[5],
                        'total_constraints': result[6],
                        'total_indexes': result[7],
                        'extraction_duration_seconds': result[8],
                        'status': result[9],
                        'error_message': result[10]
                    }
                return None
    
    def get_latest_quality_metrics_run(self) -> Optional[Dict[str, Any]]:
        """Get the latest quality metrics extraction run."""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT metrics_run_id, extraction_timestamp, target_schemas,
                           total_tables, total_columns, high_null_columns, low_distinct_columns,
                           overall_quality_score, extraction_duration_seconds, status, error_message
                    FROM {self.production_schema}.quality_metrics_runs
                    WHERE status = 'completed'
                    ORDER BY metrics_run_id DESC
                    LIMIT 1
                """)
                
                result = cur.fetchone()
                if result:
                    return {
                        'metrics_run_id': result[0],
                        'extraction_timestamp': result[1],
                        'target_schemas': result[2],
                        'total_tables': result[3],
                        'total_columns': result[4],
                        'high_null_columns': result[5],
                        'low_distinct_columns': result[6],
                        'overall_quality_score': result[7],
                        'extraction_duration_seconds': result[8],
                        'status': result[9],
                        'error_message': result[10]
                    }
                return None
    
    def cleanup_old_metadata(self, days_to_keep: int = 30) -> int:
        """Clean up old metadata using the database function."""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {self.production_schema}.cleanup_old_metadata(%s)", (days_to_keep,))
                return cur.fetchone()[0]
