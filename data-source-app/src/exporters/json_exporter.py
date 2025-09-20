"""JSON export functionality for metadata and quality metrics."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..models.normalized_models import NormalizedSchema, NormalizedTable, NormalizedColumn, TableQualityMetrics, ColumnQualityMetrics
from ..config import AppConfig

logger = logging.getLogger(__name__)


class JSONExporter:
    """Export metadata and quality metrics to JSON format."""
    
    def __init__(self, config: AppConfig):
        """Initialize JSON exporter.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.output_dir = Path(config.output.json_dir)
        
        if config.output.create_dirs:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_metadata(self, schemas: List[NormalizedSchema], 
                       filename: Optional[str] = None) -> str:
        """Export metadata to separate JSON files for schema, table, and column.
        
        Args:
            schemas: List of schema metadata objects
            filename: Optional custom filename prefix
            
        Returns:
            Path to the exported files (comma-separated)
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"metadata_{timestamp}"
        
        # Create separate files for each entity type
        schema_file = self.output_dir / f"{filename}_schema.json"
        table_file = self.output_dir / f"{filename}_table.json"
        column_file = self.output_dir / f"{filename}_column.json"
        
        # Export schemas
        with open(schema_file, 'w', encoding='utf-8') as f:
            for schema in schemas:
                schema_dict = self._schema_to_dict(schema)
                # Remove tables from schema dict for schema file
                if 'tables' in schema_dict:
                    del schema_dict['tables']
                f.write(json.dumps(schema_dict, ensure_ascii=False, default=str) + '\n')
        
        # Export tables
        with open(table_file, 'w', encoding='utf-8') as f:
            for schema in schemas:
                for table in schema.tables:
                    table_dict = self._table_to_dict(table)
                    # Remove columns from table dict for table file
                    if 'columns' in table_dict:
                        del table_dict['columns']
                    f.write(json.dumps(table_dict, ensure_ascii=False, default=str) + '\n')
        
        # Export columns
        with open(column_file, 'w', encoding='utf-8') as f:
            for schema in schemas:
                for table in schema.tables:
                    for column in table.columns:
                        column_dict = self._column_to_dict(column)
                        f.write(json.dumps(column_dict, ensure_ascii=False, default=str) + '\n')
        
        logger.info(f"Metadata exported to: {schema_file}, {table_file}, {column_file}")
        return f"{schema_file},{table_file},{column_file}"
    
    def export_quality_metrics(self, metrics: Dict[str, List[TableQualityMetrics]], 
                              filename: Optional[str] = None) -> str:
        """Export quality metrics to JSON file.
        
        Args:
            metrics: Dictionary of schema metrics
            filename: Optional custom filename
            
        Returns:
            Path to the exported file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quality_metrics_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        # Convert metrics to dictionary
        metrics_dict = {
            'export_info': {
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0',
                'total_schemas': len(metrics)
            },
            'schemas': {}
        }
        
        for schema_name, table_metrics in metrics.items():
            metrics_dict['schemas'][schema_name] = {
                'tables': [self._table_metrics_to_dict(table) for table in table_metrics]
            }
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metrics_dict, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Quality metrics exported to: {filepath}")
        return str(filepath)
    
    def export_combined(self, schemas: List[NormalizedSchema], 
                       metrics: Dict[str, List[TableQualityMetrics]], 
                       filename: Optional[str] = None) -> str:
        """Export both metadata and quality metrics to a single JSON file.
        
        Args:
            schemas: List of schema metadata objects
            metrics: Dictionary of schema metrics
            filename: Optional custom filename
            
        Returns:
            Path to the exported file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"combined_export_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        # Create combined export
        combined_dict = {
            'export_info': {
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0',
                'type': 'combined',
                'total_schemas': len(schemas)
            },
            'metadata': {
                'schemas': [self._schema_to_dict(schema) for schema in schemas]
            },
            'quality_metrics': {
                'schemas': {}
            }
        }
        
        # Add quality metrics
        for schema_name, table_metrics in metrics.items():
            combined_dict['quality_metrics']['schemas'][schema_name] = {
                'tables': [self._table_metrics_to_dict(table) for table in table_metrics]
            }
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(combined_dict, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Combined export saved to: {filepath}")
        return str(filepath)
    
    def _schema_to_dict(self, schema: NormalizedSchema) -> Dict[str, Any]:
        """Convert NormalizedSchema to dictionary."""
        return schema.to_dict()
    
    def _table_to_dict(self, table: NormalizedTable) -> Dict[str, Any]:
        """Convert NormalizedTable to dictionary."""
        return table.to_dict()
    
    def _column_to_dict(self, column: NormalizedColumn) -> Dict[str, Any]:
        """Convert NormalizedColumn to dictionary."""
        return column.to_dict()
    
    def _constraint_to_dict(self, constraint) -> Dict[str, Any]:
        """Convert ConstraintMetadata to dictionary."""
        return {
            'name': constraint.name,
            'type': constraint.type,
            'columns': constraint.columns,
            'referenced_table': constraint.referenced_table,
            'referenced_schema': constraint.referenced_schema,
            'referenced_columns': constraint.referenced_columns
        }
    
    def _index_to_dict(self, index) -> Dict[str, Any]:
        """Convert IndexMetadata to dictionary."""
        return {
            'name': index.name,
            'definition': index.definition,
            'columns': index.columns,
            'is_unique': index.is_unique,
            'is_primary': index.is_primary
        }
    
    def _table_metrics_to_dict(self, table_metrics: TableQualityMetrics) -> Dict[str, Any]:
        """Convert TableQualityMetrics to dictionary."""
        return {
            'schema_name': table_metrics.schema_name,
            'table_name': table_metrics.table_name,
            'row_count': table_metrics.row_count,
            'column_metrics': [self._column_metrics_to_dict(col) for col in table_metrics.column_metrics]
        }
    
    def _column_metrics_to_dict(self, col_metrics: ColumnQualityMetrics) -> Dict[str, Any]:
        """Convert ColumnQualityMetrics to dictionary."""
        return {
            'column_name': col_metrics.column_name,
            'total_count': col_metrics.total_count,
            'non_null_count': col_metrics.non_null_count,
            'null_count': col_metrics.null_count,
            'null_percentage': col_metrics.null_percentage,
            'distinct_count': col_metrics.distinct_count,
            'distinct_percentage': col_metrics.distinct_percentage,
            'top_values': col_metrics.top_values
        }
