"""Metadata extraction from PostgreSQL database."""

import re
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import yaml

from ..db.connection import DatabaseConnection
from ..db.queries import MetadataQueries
from ..config import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Column metadata information."""
    name: str
    position: int
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class ConstraintMetadata:
    """Constraint metadata information."""
    name: str
    type: str  # PRIMARY KEY, FOREIGN KEY, UNIQUE, etc.
    columns: List[str]
    referenced_table: Optional[str] = None
    referenced_schema: Optional[str] = None
    referenced_columns: Optional[List[str]] = None


@dataclass
class IndexMetadata:
    """Index metadata information."""
    name: str
    definition: str
    columns: List[str]
    is_unique: bool
    is_primary: bool


@dataclass
class TableMetadata:
    """Table metadata information."""
    name: str
    schema: str
    table_type: str
    comment: Optional[str] = None
    tags: List[str] = None
    columns: List[ColumnMetadata] = None
    constraints: List[ConstraintMetadata] = None
    indexes: List[IndexMetadata] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.columns is None:
            self.columns = []
        if self.constraints is None:
            self.constraints = []
        if self.indexes is None:
            self.indexes = []


@dataclass
class SchemaMetadata:
    """Schema metadata information."""
    name: str
    owner: str
    tables: List[TableMetadata] = None

    def __post_init__(self):
        if self.tables is None:
            self.tables = []


class MetadataExtractor:
    """Extract metadata from PostgreSQL database."""
    
    def __init__(self, db_connection: DatabaseConnection, config: AppConfig):
        """Initialize metadata extractor.
        
        Args:
            db_connection: Database connection instance
            config: Application configuration
        """
        self.db_connection = db_connection
        self.config = config
        self.queries = MetadataQueries()
        self._metadata_yaml: Optional[Dict] = None
    
    def extract_all_metadata(self, target_schemas: Optional[List[str]] = None) -> List[SchemaMetadata]:
        """Extract metadata for all schemas or specified schemas.
        
        Args:
            target_schemas: List of schema names to extract. If None, uses config schemas.
            
        Returns:
            List of schema metadata objects
        """
        if target_schemas is None:
            target_schemas = self.config.schemas
        
        # If no schemas specified, get all available schemas
        if not target_schemas:
            target_schemas = self.db_connection.get_available_schemas()
        
        logger.info(f"Extracting metadata for schemas: {target_schemas}")
        
        schemas = []
        for schema_name in target_schemas:
            try:
                schema_metadata = self.extract_schema_metadata(schema_name)
                schemas.append(schema_metadata)
            except Exception as e:
                logger.error(f"Failed to extract metadata for schema {schema_name}: {e}")
                continue
        
        return schemas
    
    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """Extract metadata for a specific schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            Schema metadata object
        """
        logger.info(f"Extracting metadata for schema: {schema_name}")
        logger.info(f"new logschema: {schema_name}")
        
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                # Get schema info
                cur.execute(self.queries.get_schemas())
                schemas = cur.fetchall()
                schema_info = next((s for s in schemas if s[0] == schema_name), None)
                
                if not schema_info:
                    raise ValueError(f"Schema {schema_name} not found")
                
                schema_metadata = SchemaMetadata(
                    name=schema_info[0],
                    owner=schema_info[1]
                )
                
                # Get tables
                cur.execute(self.queries.get_tables(schema_name), (schema_name,))
                tables = cur.fetchall()
                
                for table_info in tables:
                    table_name = table_info[0]
                    table_type = table_info[1]
                    
                    try:
                        table_metadata = self._extract_table_metadata(
                            schema_name, table_name, table_type, conn
                        )
                        schema_metadata.tables.append(table_metadata)
                    except Exception as e:
                        logger.error(f"Failed to extract metadata for table {schema_name}.{table_name}: {e}")
                        continue
        
        return schema_metadata
    
    def _extract_table_metadata(self, schema_name: str, table_name: str, 
                               table_type: str, conn) -> TableMetadata:
        """Extract metadata for a specific table."""
        with conn.cursor() as cur:
            # Get table comment
            table_comment = None
            if self.config.business_context.extract_comments:
                cur.execute(self.queries.get_table_comments(schema_name, table_name), 
                           (schema_name, table_name))
                result = cur.fetchone()
                if result and result[0]:
                    table_comment = result[0]
            
            # Parse tags from comment
            tags = []
            if table_comment and self.config.business_context.parse_tags:
                tags = self._parse_tags_from_comment(table_comment)
            
            # Add tags from YAML metadata
            yaml_tags = self._get_tags_from_yaml(schema_name, table_name)
            tags.extend(yaml_tags)
            
            table_metadata = TableMetadata(
                name=table_name,
                schema=schema_name,
                table_type=table_type,
                comment=table_comment,
                tags=list(set(tags))  # Remove duplicates
            )
            
            # Extract columns
            table_metadata.columns = self._extract_columns(schema_name, table_name, cur)
            
            # Extract constraints
            table_metadata.constraints = self._extract_constraints(schema_name, table_name, cur)
            
            # Extract indexes
            table_metadata.indexes = self._extract_indexes(schema_name, table_name, cur)
            
            return table_metadata
    
    def _extract_columns(self, schema_name: str, table_name: str, cur) -> List[ColumnMetadata]:
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
            
            column = ColumnMetadata(
                name=column_name,
                position=col_data[1],
                data_type=col_data[4],
                is_nullable=col_data[3] == 'YES',
                default_value=col_data[2],
                max_length=col_data[5],
                precision=col_data[6],
                scale=col_data[7],
                comment=comment,
                tags=list(set(tags))
            )
            columns.append(column)
        
        return columns
    
    def _extract_constraints(self, schema_name: str, table_name: str, cur) -> List[ConstraintMetadata]:
        """Extract constraint metadata."""
        constraints = []
        
        # Primary keys
        cur.execute(self.queries.get_primary_keys(schema_name, table_name), 
                   (schema_name, table_name))
        pk_data = cur.fetchall()
        if pk_data:
            pk_columns = [row[0] for row in pk_data]
            constraints.append(ConstraintMetadata(
                name=f"pk_{table_name}",
                type="PRIMARY KEY",
                columns=pk_columns
            ))
        
        # Foreign keys
        cur.execute(self.queries.get_foreign_keys(schema_name, table_name), 
                   (schema_name, table_name))
        fk_data = cur.fetchall()
        for fk_row in fk_data:
            constraints.append(ConstraintMetadata(
                name=fk_row[4],  # constraint_name
                type="FOREIGN KEY",
                columns=[fk_row[0]],  # column_name
                referenced_table=fk_row[2],  # foreign_table_name
                referenced_schema=fk_row[1],  # foreign_table_schema
                referenced_columns=[fk_row[3]]  # foreign_column_name
            ))
        
        # Unique constraints
        cur.execute(self.queries.get_unique_constraints(schema_name, table_name), 
                   (schema_name, table_name))
        unique_data = cur.fetchall()
        unique_groups = {}
        for unique_row in unique_data:
            constraint_name = unique_row[0]
            column_name = unique_row[1]
            if constraint_name not in unique_groups:
                unique_groups[constraint_name] = []
            unique_groups[constraint_name].append(column_name)
        
        for constraint_name, columns in unique_groups.items():
            constraints.append(ConstraintMetadata(
                name=constraint_name,
                type="UNIQUE",
                columns=columns
            ))
        
        return constraints
    
    def _extract_indexes(self, schema_name: str, table_name: str, cur) -> List[IndexMetadata]:
        """Extract index metadata."""
        cur.execute(self.queries.get_indexes(schema_name, table_name), 
                   (schema_name, table_name))
        index_data = cur.fetchall()
        
        index_groups = {}
        for idx_row in index_data:
            index_name = idx_row[0]
            if index_name not in index_groups:
                index_groups[index_name] = {
                    'definition': idx_row[1],
                    'columns': [],
                    'is_unique': idx_row[3],
                    'is_primary': idx_row[4]
                }
            index_groups[index_name]['columns'].append(idx_row[2])
        
        indexes = []
        for index_name, index_info in index_groups.items():
            indexes.append(IndexMetadata(
                name=index_name,
                definition=index_info['definition'],
                columns=index_info['columns'],
                is_unique=index_info['is_unique'],
                is_primary=index_info['is_primary']
            ))
        
        return indexes
    
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
