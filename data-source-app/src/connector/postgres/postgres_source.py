"""PostgreSQL source implementation for metadata and quality metrics extraction."""

import re
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import yaml
import pandas as pd

from ...db.connection import DatabaseConnection
from ...db.queries import MetadataQueries
from ...config import AppConfig
from ...models.normalized_models import NormalizedColumn, NormalizedTable, NormalizedSchema, ColumnQualityMetrics, TableQualityMetrics
from ...models.normalized_builder import NormalizedEntityBuilder

logger = logging.getLogger(__name__)


# Old dataclass definitions removed - now using normalized models


class PostgreSQLSource:
    """PostgreSQL source implementation for metadata and quality metrics extraction."""
    
    def __init__(self, db_connection: DatabaseConnection, config: AppConfig, 
                 connection_name: str = "test-connection", sync_id: str = ""):
        """Initialize PostgreSQL source.
        
        Args:
            db_connection: Database connection instance
            config: Application configuration
            connection_name: Name of the connection
            sync_id: Unique sync identifier for this extraction run
        """
        self.db_connection = db_connection
        self.config = config
        self.queries = MetadataQueries()
        self._metadata_yaml: Optional[Dict] = None
        
        # Initialize normalized entity builder
        self.builder = NormalizedEntityBuilder(
            connection_name=connection_name,
            sync_id=sync_id
        )
    
    def extract_all_metadata(self, target_schemas: Optional[List[str]] = None) -> List[NormalizedSchema]:
        """Extract metadata for all schemas or specified schemas.
        
        Args:
            target_schemas: List of schema names to extract. If None, uses config schemas.
            
        Returns:
            List of normalized schema metadata objects
        """
        if target_schemas is None:
            target_schemas = self.config.schemas
        
        # If no schemas specified, get all available schemas
        if not target_schemas:
            target_schemas = self.db_connection.get_available_schemas()
        
        logger.info(f"Extracting metadata for schemas: {target_schemas}")
        
        # Get database name from connection string
        database_name = self._get_database_name_from_connection()
        
        schemas = []
        for schema_name in target_schemas:
            try:
                schema_metadata = self.extract_schema_metadata(database_name, schema_name)
                schemas.append(schema_metadata)
            except Exception as e:
                logger.error(f"Failed to extract metadata for schema {schema_name}: {e}")
                continue
        
        return schemas
    
    def _get_database_name_from_connection(self) -> str:
        """Extract database name from connection string."""
        try:
            # Parse connection string to get database name
            # Format: postgresql://user:pass@host:port/database
            connection_string = self.db_connection.connection_string
            if '/' in connection_string:
                return connection_string.split('/')[-1]
            return "unknown"
        except Exception:
            return "unknown"
    
    def extract_schema_metadata(self, database_name: str, schema_name: str) -> NormalizedSchema:
        """Extract metadata for a specific schema.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            
        Returns:
            Normalized schema metadata object
        """
        logger.info(f"Extracting metadata for schema: {schema_name}")
        
        # Create normalized schema
        schema_metadata = self.builder.create_schema(database_name, schema_name)
        
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                # Get tables
                cur.execute(self.queries.get_tables(schema_name), (schema_name,))
                tables = cur.fetchall()
                
                for table_info in tables:
                    table_name = table_info[0]
                    table_type = table_info[1]
                    
                    try:
                        table_metadata = self._extract_table_metadata(
                            database_name, schema_name, table_name, table_type, conn
                        )
                        schema_metadata.tables.append(table_metadata)
                    except Exception as e:
                        logger.error(f"Failed to extract metadata for table {schema_name}.{table_name}: {e}")
                        continue
        
        return schema_metadata
    
    def _collect_table_metadata(self, schema_name: str, table_name: str, cur) -> Dict[str, Any]:
        """Collect table metadata including constraints, indexes, partitions, and tablespace."""
        metadata = {
            'has_foreign_keys': False,
            'is_partitioned': False,
            'has_indexes': False,
            'has_primary_key': False
        }
        
        try:
            # Check for foreign keys
            cur.execute(self.queries.get_table_constraints(schema_name, table_name), (schema_name, table_name))
            constraints = cur.fetchall()
            
            for constraint in constraints:
                constraint_type = constraint[1]
                if constraint_type == 'FOREIGN KEY':
                    metadata['has_foreign_keys'] = True
                elif constraint_type == 'PRIMARY KEY':
                    metadata['has_primary_key'] = True
            
            # Check for indexes
            cur.execute(self.queries.get_table_indexes(schema_name, table_name), (schema_name, table_name))
            indexes = cur.fetchall()
            if indexes:
                metadata['has_indexes'] = True
            
            # Check for partition info
            cur.execute(self.queries.get_table_partition_info(schema_name, table_name), (schema_name, table_name))
            partition_info = cur.fetchone()
            if partition_info and partition_info[1] == 'p':  # 'p' means partitioned table
                metadata['is_partitioned'] = True
            
            # Get tablespace (only if not null)
            cur.execute(self.queries.get_table_tablespace(schema_name, table_name), (schema_name, table_name))
            tablespace_result = cur.fetchone()
            if tablespace_result and tablespace_result[0]:
                metadata['tablespace'] = tablespace_result[0]
            
            # Get partition relationships (only if they exist)
            cur.execute(self.queries.get_table_partition_relationships(schema_name, table_name), 
                       (schema_name, table_name, schema_name, table_name))
            partition_relationships = cur.fetchall()
            
            if partition_relationships:
                partitioned_from = []
                partitioned_to = []
                
                for rel in partition_relationships:
                    rel_name = rel[0]
                    rel_schema = rel[1]
                    rel_kind = rel[2]
                    is_parent = rel[3]
                    
                    full_name = f"{rel_schema}.{rel_name}" if rel_schema != schema_name else rel_name
                    
                    if is_parent:
                        # This table is partitioned from the related table
                        partitioned_from.append(full_name)
                    else:
                        # This table has partitions (the related table is a partition)
                        partitioned_to.append(full_name)
                
                if partitioned_from:
                    metadata['partitioned_from'] = partitioned_from
                if partitioned_to:
                    metadata['partitioned_to'] = partitioned_to
            
            # Get foreign key relationships (only if they exist)
            cur.execute(self.queries.get_table_foreign_relationships(schema_name, table_name), 
                       (schema_name, table_name))
            foreign_relationships = cur.fetchall()
            
            if foreign_relationships:
                foreign_tables = []
                for rel in foreign_relationships:
                    foreign_schema = rel[0]
                    foreign_table = rel[1]
                    full_name = f"{foreign_schema}.{foreign_table}" if foreign_schema != schema_name else foreign_table
                    foreign_tables.append(full_name)
                
                if foreign_tables:
                    metadata['foreign_relationships'] = foreign_tables
                
        except Exception as e:
            logger.warning(f"Could not collect table metadata for {schema_name}.{table_name}: {e}")
        
        return metadata
    
    def _collect_column_metadata(self, schema_name: str, table_name: str, cur) -> Dict[str, Dict[str, Any]]:
        """Collect column metadata including constraints and indexes."""
        column_metadata = {}
        
        try:
            # Get constraints for all columns
            cur.execute(self.queries.get_table_constraints(schema_name, table_name), (schema_name, table_name))
            constraints = cur.fetchall()
            
            for constraint in constraints:
                column_name = constraint[2]  # column_name
                constraint_type = constraint[1]  # constraint_type
                
                if column_name not in column_metadata:
                    column_metadata[column_name] = {
                        'is_primary_key': False,
                        'is_unique': False,
                        'is_foreign_key': False,
                        'is_indexed': False
                    }
                
                if constraint_type == 'PRIMARY KEY':
                    column_metadata[column_name]['is_primary_key'] = True
                elif constraint_type == 'UNIQUE':
                    column_metadata[column_name]['is_unique'] = True
                elif constraint_type == 'FOREIGN KEY':
                    column_metadata[column_name]['is_foreign_key'] = True
            
            # Get indexes for all columns
            cur.execute(self.queries.get_table_indexes(schema_name, table_name), (schema_name, table_name))
            indexes = cur.fetchall()
            
            for index in indexes:
                column_name = index[1]  # column_name
                if column_name not in column_metadata:
                    column_metadata[column_name] = {
                        'is_primary_key': False,
                        'is_unique': False,
                        'is_foreign_key': False,
                        'is_indexed': False
                    }
                column_metadata[column_name]['is_indexed'] = True
                
        except Exception as e:
            logger.warning(f"Could not collect column metadata for {schema_name}.{table_name}: {e}")
        
        return column_metadata
    
    def _extract_table_metadata(self, database_name: str, schema_name: str, table_name: str, 
                               table_type: str, conn) -> NormalizedTable:
        """Extract metadata for a specific table."""
        with conn.cursor() as cur:
            # Collect table metadata
            table_metadata_info = self._collect_table_metadata(schema_name, table_name, cur)
            
            # Create normalized table with metadata
            table_metadata = self.builder.create_table(
                database_name, 
                schema_name, 
                table_name, 
                table_type,
                **table_metadata_info
            )
            
            # Get table comment
            table_comment = None
            if self.config.business_context.extract_comments:
                cur.execute(self.queries.get_table_comments(schema_name, table_name), 
                           (schema_name, table_name))
                result = cur.fetchone()
                if result and result[0]:
                    table_comment = result[0]
            
            # Add comment to custom attributes
            if table_comment:
                table_metadata.customAttributes["comment"] = table_comment
            
            # Parse tags from comment
            tags = []
            if table_comment and self.config.business_context.parse_tags:
                tags = self._parse_tags_from_comment(table_comment)
            
            # Add tags from YAML metadata
            yaml_tags = self._get_tags_from_yaml(schema_name, table_name)
            tags.extend(yaml_tags)
            
            if tags:
                table_metadata.customAttributes["tags"] = list(set(tags))
            
            # Extract columns
            table_metadata.columns = self._extract_columns(database_name, schema_name, table_name, cur)
            
            return table_metadata
    
    def _extract_columns(self, database_name: str, schema_name: str, table_name: str, cur) -> List[NormalizedColumn]:
        """Extract column metadata."""
        cur.execute(self.queries.get_columns(schema_name, table_name), 
                   (schema_name, table_name))
        columns_data = cur.fetchall()
        
        # Get column comments
        column_comments = {}
        if self.config.business_context.extract_comments:
            cur.execute(self.queries.get_column_comments(schema_name, table_name), 
                       (schema_name, table_name))
            comments_data = cur.fetchall()
            column_comments = {row[0]: row[1] for row in comments_data if row[1]}
        
        # Collect column metadata (constraints, indexes)
        column_metadata = self._collect_column_metadata(schema_name, table_name, cur)
        
        columns = []
        for col_data in columns_data:
            column_name = col_data[0]
            comment = column_comments.get(column_name)
            tags = []
            
            if comment and self.config.business_context.parse_tags:
                tags = self._parse_tags_from_comment(comment)
            
            # Add tags from YAML metadata
            yaml_tags = self._get_tags_from_yaml(schema_name, table_name, column_name)
            tags.extend(yaml_tags)
            
            # Get column-specific metadata
            col_meta = column_metadata.get(column_name, {})
            
            # Create normalized column with metadata
            column = self.builder.create_column(
                database_name=database_name,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                data_type=col_data[4],
                is_nullable=col_data[3] == 'YES',
                ordinal_position=col_data[1],
                column_default=col_data[2],
                comment=comment,
                tags=list(set(tags)) if tags else [],
                is_primary_key=col_meta.get('is_primary_key', False),
                is_unique=col_meta.get('is_unique', False),
                is_foreign_key=col_meta.get('is_foreign_key', False),
                is_indexed=col_meta.get('is_indexed', False)
            )
            columns.append(column)
        
        return columns
    
    def extract_all_quality_metrics(self, schemas: List[str]) -> Dict[str, List[TableQualityMetrics]]:
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
                            table_metrics = self.extract_table_quality_metrics(schema_name, table_name)
                            schema_metrics.append(table_metrics)
                        except Exception as e:
                            logger.error(f"Failed to extract metrics for {schema_name}.{table_name}: {e}")
                            continue
                    
                    all_metrics[schema_name] = schema_metrics
        
        return all_metrics
    
    def extract_table_quality_metrics(self, schema_name: str, table_name: str) -> TableQualityMetrics:
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
                    table_metrics.column_metrics = self._extract_column_quality_metrics(
                        schema_name, table_name, cur
                    )
        
        return table_metrics
    
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
    
    def _extract_column_quality_metrics(self, schema_name: str, table_name: str, cur) -> List[ColumnQualityMetrics]:
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
                metrics = self._extract_single_column_quality_metrics(
                    schema_name, table_name, column_name, data_type, cur
                )
                column_metrics.append(metrics)
            except Exception as e:
                logger.error(f"Failed to extract metrics for column {column_name}: {e}")
                continue
        
        return column_metrics
    
    def _extract_single_column_quality_metrics(self, schema_name: str, table_name: str, 
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
    
    def _parse_tags_from_comment(self, comment: str) -> List[str]:
        """Parse tags from comment text.
        
        Expected format: "Description text [tags: tag1,tag2,tag3]"
        """
        if not comment:
            return []
        
        # Look for [tags: ...] pattern
        tag_pattern = r'\[tags:\s*([^\]]+)\]'
        match = re.search(tag_pattern, comment, re.IGNORECASE)
        
        if match:
            tags_str = match.group(1).strip()
            tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
            return tags
        
        return []
    
    def _get_tags_from_yaml(self, schema_name: str, table_name: str, 
                           column_name: Optional[str] = None) -> List[str]:
        """Get tags from YAML metadata file."""
        if not self.config.business_context.metadata_yaml:
            return []
        
        if self._metadata_yaml is None:
            self._load_metadata_yaml()
        
        if not self._metadata_yaml:
            return []
        
        # Navigate to the specific location in YAML
        try:
            schema_data = self._metadata_yaml.get(schema_name, {})
            table_data = schema_data.get(table_name, {})
            
            if column_name:
                column_data = table_data.get('columns', {}).get(column_name, {})
                return column_data.get('tags', [])
            else:
                return table_data.get('tags', [])
        except Exception as e:
            logger.warning(f"Error reading YAML metadata: {e}")
            return []
    
    def _load_metadata_yaml(self):
        """Load YAML metadata file."""
        yaml_path = Path(self.config.business_context.metadata_yaml)
        if not yaml_path.exists():
            logger.warning(f"Metadata YAML file not found: {yaml_path}")
            return
        
        try:
            with open(yaml_path, 'r') as f:
                self._metadata_yaml = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load metadata YAML: {e}")
            self._metadata_yaml = {}
