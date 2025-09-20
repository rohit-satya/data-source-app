"""CSV export functionality for metadata and quality metrics."""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd

from ..extractor.metadata_extractor import SchemaMetadata, TableMetadata, ColumnMetadata
from ..extractor.quality_metrics import TableQualityMetrics, ColumnQualityMetrics
from ..config import AppConfig

logger = logging.getLogger(__name__)


class CSVExporter:
    """Export metadata and quality metrics to CSV format."""
    
    def __init__(self, config: AppConfig):
        """Initialize CSV exporter.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.output_dir = Path(config.output.csv_dir)
        
        if config.output.create_dirs:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_metadata(self, schemas: List[SchemaMetadata], 
                       prefix: Optional[str] = None) -> List[str]:
        """Export metadata to CSV files.
        
        Args:
            schemas: List of schema metadata objects
            prefix: Optional prefix for filenames
            
        Returns:
            List of exported file paths
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = prefix or f"metadata_{timestamp}"
        
        exported_files = []
        
        # Export schemas
        schemas_file = self._export_schemas(schemas, f"{prefix}_schemas.csv")
        exported_files.append(schemas_file)
        
        # Export tables
        tables_file = self._export_tables(schemas, f"{prefix}_tables.csv")
        exported_files.append(tables_file)
        
        # Export columns
        columns_file = self._export_columns(schemas, f"{prefix}_columns.csv")
        exported_files.append(columns_file)
        
        # Export constraints
        constraints_file = self._export_constraints(schemas, f"{prefix}_constraints.csv")
        exported_files.append(constraints_file)
        
        # Export indexes
        indexes_file = self._export_indexes(schemas, f"{prefix}_indexes.csv")
        exported_files.append(indexes_file)
        
        logger.info(f"Metadata exported to {len(exported_files)} CSV files")
        return exported_files
    
    def export_quality_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], 
                              prefix: Optional[str] = None) -> List[str]:
        """Export quality metrics to CSV files.
        
        Args:
            metrics: Dictionary of schema metrics
            prefix: Optional prefix for filenames
            
        Returns:
            List of exported file paths
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = prefix or f"quality_metrics_{timestamp}"
        
        exported_files = []
        
        # Export table metrics
        tables_file = self._export_table_metrics(metrics, f"{prefix}_tables.csv")
        exported_files.append(tables_file)
        
        # Export column metrics
        columns_file = self._export_column_metrics(metrics, f"{prefix}_columns.csv")
        exported_files.append(columns_file)
        
        # Export top values
        top_values_file = self._export_top_values(metrics, f"{prefix}_top_values.csv")
        exported_files.append(top_values_file)
        
        logger.info(f"Quality metrics exported to {len(exported_files)} CSV files")
        return exported_files
    
    def _export_schemas(self, schemas: List[SchemaMetadata], filename: str) -> str:
        """Export schema information to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['schema_name', 'owner', 'table_count'])
            
            for schema in schemas:
                writer.writerow([
                    schema.name,
                    schema.owner,
                    len(schema.tables)
                ])
        
        return str(filepath)
    
    def _export_tables(self, schemas: List[SchemaMetadata], filename: str) -> str:
        """Export table information to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'table_type', 'comment', 
                'tags', 'column_count', 'constraint_count', 'index_count'
            ])
            
            for schema in schemas:
                for table in schema.tables:
                    writer.writerow([
                        table.schema,
                        table.name,
                        table.table_type,
                        table.comment or '',
                        ';'.join(table.tags),
                        len(table.columns),
                        len(table.constraints),
                        len(table.indexes)
                    ])
        
        return str(filepath)
    
    def _export_columns(self, schemas: List[SchemaMetadata], filename: str) -> str:
        """Export column information to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'column_name', 'position',
                'data_type', 'is_nullable', 'default_value', 'max_length',
                'precision', 'scale', 'comment', 'tags'
            ])
            
            for schema in schemas:
                for table in schema.tables:
                    for column in table.columns:
                        writer.writerow([
                            table.schema,
                            table.name,
                            column.name,
                            column.position,
                            column.data_type,
                            column.is_nullable,
                            column.default_value or '',
                            column.max_length or '',
                            column.precision or '',
                            column.scale or '',
                            column.comment or '',
                            ';'.join(column.tags)
                        ])
        
        return str(filepath)
    
    def _export_constraints(self, schemas: List[SchemaMetadata], filename: str) -> str:
        """Export constraint information to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'constraint_name', 'constraint_type',
                'columns', 'referenced_schema', 'referenced_table', 'referenced_columns'
            ])
            
            for schema in schemas:
                for table in schema.tables:
                    for constraint in table.constraints:
                        writer.writerow([
                            table.schema,
                            table.name,
                            constraint.name,
                            constraint.type,
                            ';'.join(constraint.columns),
                            constraint.referenced_schema or '',
                            constraint.referenced_table or '',
                            ';'.join(constraint.referenced_columns) if constraint.referenced_columns else ''
                        ])
        
        return str(filepath)
    
    def _export_indexes(self, schemas: List[SchemaMetadata], filename: str) -> str:
        """Export index information to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'index_name', 'definition',
                'columns', 'is_unique', 'is_primary'
            ])
            
            for schema in schemas:
                for table in schema.tables:
                    for index in table.indexes:
                        writer.writerow([
                            table.schema,
                            table.name,
                            index.name,
                            index.definition,
                            ';'.join(index.columns),
                            index.is_unique,
                            index.is_primary
                        ])
        
        return str(filepath)
    
    def _export_table_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], filename: str) -> str:
        """Export table quality metrics to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'row_count', 'column_count',
                'high_null_columns', 'low_distinct_columns'
            ])
            
            for schema_name, table_metrics_list in metrics.items():
                for table_metrics in table_metrics_list:
                    high_null_count = sum(1 for col in table_metrics.column_metrics 
                                        if col.null_percentage > 50)
                    low_distinct_count = sum(1 for col in table_metrics.column_metrics 
                                           if col.distinct_percentage < 10 and col.total_count > 100)
                    
                    writer.writerow([
                        table_metrics.schema_name,
                        table_metrics.table_name,
                        table_metrics.row_count,
                        len(table_metrics.column_metrics),
                        high_null_count,
                        low_distinct_count
                    ])
        
        return str(filepath)
    
    def _export_column_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], filename: str) -> str:
        """Export column quality metrics to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'column_name', 'total_count',
                'non_null_count', 'null_count', 'null_percentage',
                'distinct_count', 'distinct_percentage'
            ])
            
            for schema_name, table_metrics_list in metrics.items():
                for table_metrics in table_metrics_list:
                    for col_metrics in table_metrics.column_metrics:
                        writer.writerow([
                            table_metrics.schema_name,
                            table_metrics.table_name,
                            col_metrics.column_name,
                            col_metrics.total_count,
                            col_metrics.non_null_count,
                            col_metrics.null_count,
                            col_metrics.null_percentage,
                            col_metrics.distinct_count,
                            col_metrics.distinct_percentage
                        ])
        
        return str(filepath)
    
    def _export_top_values(self, metrics: Dict[str, List[TableQualityMetrics]], filename: str) -> str:
        """Export top values to CSV."""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'schema_name', 'table_name', 'column_name', 'value', 'frequency'
            ])
            
            for schema_name, table_metrics_list in metrics.items():
                for table_metrics in table_metrics_list:
                    for col_metrics in table_metrics.column_metrics:
                        for top_value in col_metrics.top_values:
                            writer.writerow([
                                table_metrics.schema_name,
                                table_metrics.table_name,
                                col_metrics.column_name,
                                str(top_value['value']),
                                top_value['frequency']
                            ])
        
        return str(filepath)

