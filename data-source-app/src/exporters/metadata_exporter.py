"""Metadata exporter that handles exporting metadata to different formats."""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

from .json_exporter import JSONExporter
from .csv_exporter import CSVExporter
from ..config import AppConfig
from ..services.database_service import DatabaseService

logger = logging.getLogger(__name__)


class MetadataExporter:
    """Handles exporting metadata to different formats, agnostic of the source connector."""
    
    def __init__(self, config: AppConfig, database_service: Optional[DatabaseService] = None):
        """Initialize metadata exporter.
        
        Args:
            config: Application configuration
            database_service: Optional database service for PostgreSQL export
        """
        self.config = config
        self.database_service = database_service or DatabaseService(config)
        
        # Initialize format-specific exporters
        self.json_exporter = JSONExporter(config)
        self.csv_exporter = CSVExporter(config)
    
    def export_metadata(self, schemas: List[Any], output_format: str) -> Dict[str, Any]:
        """Export metadata in the specified format.
        
        Args:
            schemas: List of schema metadata objects
            output_format: Output format (json, csv, postgres, all)
            
        Returns:
            Dictionary with export results
        """
        results = {}
        
        if output_format in ["postgres", "all"]:
            try:
                result = self.database_service.export_metadata(schemas)
                results['postgres'] = result
            except Exception as e:
                logger.error(f"PostgreSQL export failed: {e}")
                results['postgres'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"PostgreSQL export failed: {e}"
                }
        
        if output_format in ["json", "all"]:
            try:
                json_file = self.json_exporter.export_metadata(schemas)
                results['json'] = {
                    'success': True,
                    'file': json_file,
                    'message': f"JSON export: {json_file}"
                }
            except Exception as e:
                logger.error(f"JSON export failed: {e}")
                results['json'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"JSON export failed: {e}"
                }
        
        if output_format in ["csv", "all"]:
            try:
                csv_files = self.csv_exporter.export_metadata(schemas)
                results['csv'] = {
                    'success': True,
                    'files': csv_files,
                    'count': len(csv_files),
                    'message': f"CSV exports: {len(csv_files)} files"
                }
            except Exception as e:
                logger.error(f"CSV export failed: {e}")
                results['csv'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"CSV export failed: {e}"
                }
        
        return results
    
    def export_quality_metrics(self, metrics: Dict[str, List[Any]], output_format: str) -> Dict[str, Any]:
        """Export quality metrics in the specified format.
        
        Args:
            metrics: Dictionary of quality metrics
            output_format: Output format (json, csv, postgres, all)
            
        Returns:
            Dictionary with export results
        """
        results = {}
        
        if output_format in ["postgres", "all"]:
            try:
                result = self.database_service.export_quality_metrics(metrics)
                results['postgres'] = result
            except Exception as e:
                logger.error(f"PostgreSQL export failed: {e}")
                results['postgres'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"PostgreSQL export failed: {e}"
                }
        
        if output_format in ["json", "all"]:
            try:
                json_file = self.json_exporter.export_quality_metrics(metrics)
                results['json'] = {
                    'success': True,
                    'file': json_file,
                    'message': f"JSON export: {json_file}"
                }
            except Exception as e:
                logger.error(f"JSON export failed: {e}")
                results['json'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"JSON export failed: {e}"
                }
        
        if output_format in ["csv", "all"]:
            try:
                csv_files = self.csv_exporter.export_quality_metrics(metrics)
                results['csv'] = {
                    'success': True,
                    'files': csv_files,
                    'count': len(csv_files),
                    'message': f"CSV exports: {len(csv_files)} files"
                }
            except Exception as e:
                logger.error(f"CSV export failed: {e}")
                results['csv'] = {
                    'success': False,
                    'error': str(e),
                    'message': f"CSV export failed: {e}"
                }
        
        return results
