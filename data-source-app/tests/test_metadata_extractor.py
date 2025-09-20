"""Tests for metadata extraction functionality."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.extractor.metadata_extractor import (
    MetadataExtractor, ColumnMetadata, TableMetadata, SchemaMetadata,
    ConstraintMetadata, IndexMetadata
)


class TestColumnMetadata:
    """Test ColumnMetadata class."""
    
    def test_column_metadata_creation(self):
        """Test creating column metadata."""
        column = ColumnMetadata(
            name="test_column",
            position=1,
            data_type="VARCHAR",
            is_nullable=True,
            default_value="'default'",
            max_length=100,
            comment="Test column",
            tags=["test", "column"]
        )
        
        assert column.name == "test_column"
        assert column.position == 1
        assert column.data_type == "VARCHAR"
        assert column.is_nullable is True
        assert column.default_value == "'default'"
        assert column.max_length == 100
        assert column.comment == "Test column"
        assert column.tags == ["test", "column"]
    
    def test_column_metadata_defaults(self):
        """Test column metadata with default values."""
        column = ColumnMetadata(
            name="test_column",
            position=1,
            data_type="INTEGER",
            is_nullable=False
        )
        
        assert column.default_value is None
        assert column.max_length is None
        assert column.precision is None
        assert column.scale is None
        assert column.comment is None
        assert column.tags == []


class TestTableMetadata:
    """Test TableMetadata class."""
    
    def test_table_metadata_creation(self):
        """Test creating table metadata."""
        table = TableMetadata(
            name="test_table",
            schema="public",
            table_type="BASE TABLE",
            comment="Test table",
            tags=["test", "table"]
        )
        
        assert table.name == "test_table"
        assert table.schema == "public"
        assert table.table_type == "BASE TABLE"
        assert table.comment == "Test table"
        assert table.tags == ["test", "table"]
        assert table.columns == []
        assert table.constraints == []
        assert table.indexes == []
    
    def test_table_metadata_defaults(self):
        """Test table metadata with default values."""
        table = TableMetadata(
            name="test_table",
            schema="public",
            table_type="BASE TABLE"
        )
        
        assert table.comment is None
        assert table.tags == []
        assert table.columns == []
        assert table.constraints == []
        assert table.indexes == []


class TestSchemaMetadata:
    """Test SchemaMetadata class."""
    
    def test_schema_metadata_creation(self):
        """Test creating schema metadata."""
        schema = SchemaMetadata(
            name="test_schema",
            owner="postgres"
        )
        
        assert schema.name == "test_schema"
        assert schema.owner == "postgres"
        assert schema.tables == []
    
    def test_schema_metadata_defaults(self):
        """Test schema metadata with default values."""
        schema = SchemaMetadata(name="test_schema", owner="postgres")
        assert schema.tables == []


class TestConstraintMetadata:
    """Test ConstraintMetadata class."""
    
    def test_primary_key_constraint(self):
        """Test primary key constraint metadata."""
        constraint = ConstraintMetadata(
            name="pk_test",
            type="PRIMARY KEY",
            columns=["id"]
        )
        
        assert constraint.name == "pk_test"
        assert constraint.type == "PRIMARY KEY"
        assert constraint.columns == ["id"]
        assert constraint.referenced_table is None
        assert constraint.referenced_schema is None
        assert constraint.referenced_columns is None
    
    def test_foreign_key_constraint(self):
        """Test foreign key constraint metadata."""
        constraint = ConstraintMetadata(
            name="fk_test",
            type="FOREIGN KEY",
            columns=["user_id"],
            referenced_table="users",
            referenced_schema="public",
            referenced_columns=["id"]
        )
        
        assert constraint.name == "fk_test"
        assert constraint.type == "FOREIGN KEY"
        assert constraint.columns == ["user_id"]
        assert constraint.referenced_table == "users"
        assert constraint.referenced_schema == "public"
        assert constraint.referenced_columns == ["id"]


class TestIndexMetadata:
    """Test IndexMetadata class."""
    
    def test_index_metadata_creation(self):
        """Test creating index metadata."""
        index = IndexMetadata(
            name="idx_test",
            definition="CREATE INDEX idx_test ON test_table (id)",
            columns=["id"],
            is_unique=False,
            is_primary=False
        )
        
        assert index.name == "idx_test"
        assert index.definition == "CREATE INDEX idx_test ON test_table (id)"
        assert index.columns == ["id"]
        assert index.is_unique is False
        assert index.is_primary is False


class TestMetadataExtractor:
    """Test MetadataExtractor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_connection = Mock()
        self.mock_config = Mock()
        self.extractor = MetadataExtractor(self.mock_db_connection, self.mock_config)
    
    def test_parse_tags_from_comment(self):
        """Test parsing tags from comments."""
        # Test with tags
        comment = "Test table [tags: test,table,important]"
        tags = self.extractor._parse_tags_from_comment(comment)
        assert tags == ["test", "table", "important"]
        
        # Test without tags
        comment = "Test table without tags"
        tags = self.extractor._parse_tags_from_comment(comment)
        assert tags == []
        
        # Test with empty comment
        tags = self.extractor._parse_tags_from_comment("")
        assert tags == []
        
        # Test with None comment
        tags = self.extractor._parse_tags_from_comment(None)
        assert tags == []
    
    def test_parse_tags_case_insensitive(self):
        """Test that tag parsing is case insensitive."""
        comment = "Test table [TAGS: Test,Table,Important]"
        tags = self.extractor._parse_tags_from_comment(comment)
        assert tags == ["Test", "Table", "Important"]
    
    def test_parse_tags_with_spaces(self):
        """Test parsing tags with various spacing."""
        comment = "Test table [tags: test, table , important ]"
        tags = self.extractor._parse_tags_from_comment(comment)
        assert tags == ["test", "table", "important"]
    
    @patch('src.extractor.metadata_extractor.Path')
    @patch('src.extractor.metadata_extractor.yaml')
    def test_load_metadata_yaml(self, mock_yaml, mock_path):
        """Test loading YAML metadata file."""
        # Mock file existence
        mock_path.return_value.exists.return_value = True
        
        # Mock YAML content
        mock_yaml_data = {
            'public': {
                'users': {
                    'tags': ['user', 'master'],
                    'columns': {
                        'email': {
                            'tags': ['pii', 'contact']
                        }
                    }
                }
            }
        }
        mock_yaml.safe_load.return_value = mock_yaml_data
        
        # Mock file opening
        with patch('builtins.open', mock_open()) as mock_file:
            self.extractor._load_metadata_yaml()
            
            # Verify YAML was loaded
            mock_yaml.safe_load.assert_called_once()
            assert self.extractor._metadata_yaml == mock_yaml_data
    
    def test_get_tags_from_yaml_table_level(self):
        """Test getting tags from YAML at table level."""
        self.extractor._metadata_yaml = {
            'public': {
                'users': {
                    'tags': ['user', 'master']
                }
            }
        }
        
        tags = self.extractor._get_tags_from_yaml('public', 'users')
        assert tags == ['user', 'master']
    
    def test_get_tags_from_yaml_column_level(self):
        """Test getting tags from YAML at column level."""
        self.extractor._metadata_yaml = {
            'public': {
                'users': {
                    'columns': {
                        'email': {
                            'tags': ['pii', 'contact']
                        }
                    }
                }
            }
        }
        
        tags = self.extractor._get_tags_from_yaml('public', 'users', 'email')
        assert tags == ['pii', 'contact']
    
    def test_get_tags_from_yaml_not_found(self):
        """Test getting tags when YAML data not found."""
        self.extractor._metadata_yaml = {}
        
        tags = self.extractor._get_tags_from_yaml('public', 'users')
        assert tags == []
        
        tags = self.extractor._get_tags_from_yaml('public', 'users', 'email')
        assert tags == []
    
    def test_get_tags_from_yaml_none_metadata(self):
        """Test getting tags when metadata YAML is None."""
        self.extractor._metadata_yaml = None
        
        tags = self.extractor._get_tags_from_yaml('public', 'users')
        assert tags == []


def mock_open():
    """Mock open function for testing."""
    return MagicMock()

