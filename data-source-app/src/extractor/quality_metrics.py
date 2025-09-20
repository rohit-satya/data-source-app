"""Quality metrics extraction from PostgreSQL database."""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd

from ..db.connection import DatabaseConnection
from ..db.queries import MetadataQueries
from ..config import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class ColumnQualityMetrics:
    """Quality metrics for a specific column."""
    column_name: str
    total_count: int
    non_null_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    top_values: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.top_values is None:
            self.top_values = []


@dataclass
class TableQualityMetrics:
    """Quality metrics for a specific table."""
    schema_name: str
    table_name: str
    row_count: int
    column_metrics: List[ColumnQualityMetrics] = None

    def __post_init__(self):
        if self.column_metrics is None:
            self.column_metrics = []


class QualityMetricsExtractor:
    """Extract quality metrics from PostgreSQL database."""
    
    def __init__(self, db_connection: DatabaseConnection, config: AppConfig):
        """Initialize quality metrics extractor.
        
        Args:
            db_connection: Database connection instance
            config: Application configuration
        """
        self.db_connection = db_connection
        self.config = config
        self.queries = MetadataQueries()
    
    def extract_table_metrics(self, schema_name: str, table_name: str) -> TableQualityMetrics:
        """Extract quality metrics for a specific table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Table quality metrics object
        """
        logger.info(f"Extracting quality metrics for {schema_name}.{table_name}")
        
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                # Get table row count
                row_count = self._get_table_row_count(schema_name, table_name, cur)
                
                table_metrics = TableQualityMetrics(
                    schema_name=schema_name,
                    table_name=table_name,
                    row_count=row_count
                )
                
                # Get column metrics
                if self.config.metrics.enabled:
                    table_metrics.column_metrics = self._extract_column_metrics(
                        schema_name, table_name, cur
                    )
        
        return table_metrics
    
    def extract_all_metrics(self, schemas: List[str]) -> Dict[str, List[TableQualityMetrics]]:
        """Extract quality metrics for all tables in specified schemas.
        
        Args:
            schemas: List of schema names
            
        Returns:
            Dictionary mapping schema names to lists of table metrics
        """
        all_metrics = {}
        
        for schema_name in schemas:
            logger.info(f"Extracting quality metrics for schema: {schema_name}")
            
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get all tables in schema
                    cur.execute(self.queries.get_tables(schema_name), (schema_name,))
                    tables = cur.fetchall()
                    
                    schema_metrics = []
                    for table_info in tables:
                        table_name = table_info[0]
                        table_type = table_info[1]
                        
                        # Skip views for now (can be added later)
                        if table_type != 'BASE TABLE':
                            continue
                        
                        try:
                            table_metrics = self.extract_table_metrics(schema_name, table_name)
                            schema_metrics.append(table_metrics)
                        except Exception as e:
                            logger.error(f"Failed to extract metrics for {schema_name}.{table_name}: {e}")
                            continue
                    
                    all_metrics[schema_name] = schema_metrics
        
        return all_metrics
    
    def _get_table_row_count(self, schema_name: str, table_name: str, cur) -> int:
        """Get row count for a table."""
        try:
            cur.execute(self.queries.get_table_row_count(schema_name, table_name))
            result = cur.fetchone()
            
            if result and result[0]:  # row_count
                return result[0]
            else:
                # Fallback to COUNT(*) if stats are not available
                cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
            return 0
    
    def _extract_column_metrics(self, schema_name: str, table_name: str, cur) -> List[ColumnQualityMetrics]:
        """Extract quality metrics for all columns in a table."""
        # Get column information
        cur.execute(self.queries.get_columns(schema_name, table_name), 
                   (schema_name, table_name))
        columns = cur.fetchall()
        
        column_metrics = []
        for col_data in columns:
            column_name = col_data[0]
            data_type = col_data[4]
            
            try:
                metrics = self._extract_single_column_metrics(
                    schema_name, table_name, column_name, data_type, cur
                )
                column_metrics.append(metrics)
            except Exception as e:
                logger.error(f"Failed to extract metrics for column {column_name}: {e}")
                continue
        
        return column_metrics
    
    def _extract_single_column_metrics(self, schema_name: str, table_name: str, 
                                     column_name: str, data_type: str, cur) -> ColumnQualityMetrics:
        """Extract quality metrics for a single column."""
        # Get basic statistics
        cur.execute(self.queries.get_column_stats(schema_name, table_name, column_name, 
                                                self.config.metrics.sample_limit))
        stats_result = cur.fetchone()
        
        if not stats_result:
            raise ValueError(f"No statistics available for column {column_name}")
        
        total_count = stats_result[0]
        non_null_count = stats_result[1]
        null_count = stats_result[2]
        distinct_count = stats_result[3]
        
        # Calculate percentages
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        distinct_percentage = (distinct_count / total_count * 100) if total_count > 0 else 0
        
        # Get top values
        top_values = []
        if self.config.metrics.top_k_values > 0:
            try:
                top_values = self._get_top_values(schema_name, table_name, column_name, cur)
            except Exception as e:
                logger.warning(f"Could not get top values for {column_name}: {e}")
        
        return ColumnQualityMetrics(
            column_name=column_name,
            total_count=total_count,
            non_null_count=non_null_count,
            null_count=null_count,
            null_percentage=round(null_percentage, 2),
            distinct_count=distinct_count,
            distinct_percentage=round(distinct_percentage, 2),
            top_values=top_values
        )
    
    def _get_top_values(self, schema_name: str, table_name: str, column_name: str, cur) -> List[Dict[str, Any]]:
        """Get top values for a column."""
        cur.execute(self.queries.get_top_values(schema_name, table_name, column_name, 
                                              self.config.metrics.top_k_values))
        results = cur.fetchall()
        
        top_values = []
        for row in results:
            value = row[0]
            frequency = row[1]
            
            # Convert value to appropriate type for JSON serialization
            if isinstance(value, (int, float, str, bool)) or value is None:
                json_value = value
            else:
                json_value = str(value)
            
            top_values.append({
                'value': json_value,
                'frequency': frequency
            })
        
        return top_values
    
    def get_data_quality_summary(self, metrics: Dict[str, List[TableQualityMetrics]]) -> Dict[str, Any]:
        """Generate a data quality summary from metrics.
        
        Args:
            metrics: Dictionary of schema metrics
            
        Returns:
            Summary statistics
        """
        total_tables = 0
        total_columns = 0
        high_null_columns = 0
        low_distinct_columns = 0
        
        for schema_metrics in metrics.values():
            for table_metrics in schema_metrics:
                total_tables += 1
                total_columns += len(table_metrics.column_metrics)
                
                for col_metrics in table_metrics.column_metrics:
                    # Flag columns with high null percentage
                    if col_metrics.null_percentage > 50:
                        high_null_columns += 1
                    
                    # Flag columns with low distinct percentage (potential duplicates)
                    if col_metrics.distinct_percentage < 10 and col_metrics.total_count > 100:
                        low_distinct_columns += 1
        
        return {
            'total_tables': total_tables,
            'total_columns': total_columns,
            'high_null_columns': high_null_columns,
            'low_distinct_columns': low_distinct_columns,
            'quality_score': self._calculate_quality_score(
                total_columns, high_null_columns, low_distinct_columns
            )
        }
    
    def _calculate_quality_score(self, total_columns: int, high_null_columns: int, 
                                low_distinct_columns: int) -> float:
        """Calculate overall data quality score (0-100)."""
        if total_columns == 0:
            return 100.0
        
        # Penalize high null columns and low distinct columns
        null_penalty = (high_null_columns / total_columns) * 30
        distinct_penalty = (low_distinct_columns / total_columns) * 20
        
        quality_score = 100 - null_penalty - distinct_penalty
        return max(0, min(100, round(quality_score, 1)))

