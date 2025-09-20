"""Tests for configuration management."""

import pytest
import tempfile
import os
from pathlib import Path
from src.config import AppConfig, DatabaseConfig, MetricsConfig, OutputConfig


class TestDatabaseConfig:
    """Test DatabaseConfig class."""
    
    def test_dsn_connection_string(self):
        """Test DSN connection string generation."""
        config = DatabaseConfig(dsn="postgresql://user:pass@host:5432/db")
        assert config.get_connection_string() == "postgresql://user:pass@host:5432/db"
    
    def test_individual_parameters(self):
        """Test individual parameter connection string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        expected = "postgresql://testuser:testpass@localhost:5432/testdb"
        assert config.get_connection_string() == expected
    
    def test_missing_parameters(self):
        """Test error when required parameters are missing."""
        config = DatabaseConfig(host="localhost")
        with pytest.raises(ValueError):
            config.get_connection_string()


class TestAppConfig:
    """Test AppConfig class."""
    
    def test_from_file(self):
        """Test loading configuration from YAML file."""
        config_data = """
database:
  dsn: "postgresql://user:pass@host:5432/db"
schemas:
  - public
  - test
metrics:
  enabled: true
  sample_limit: 5000
output:
  json_dir: "./test_output"
business_context:
  extract_comments: true
  parse_tags: true
lineage:
  enabled: true
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write(config_data)
            f.flush()
            
            try:
                config = AppConfig.from_file(f.name)
                
                assert config.database.dsn == "postgresql://user:pass@host:5432/db"
                assert config.schemas == ["public", "test"]
                assert config.metrics.enabled is True
                assert config.metrics.sample_limit == 5000
                assert config.output.json_dir == "./test_output"
                assert config.business_context.extract_comments is True
                assert config.lineage.enabled is True
            finally:
                os.unlink(f.name)
    
    def test_file_not_found(self):
        """Test error when config file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            AppConfig.from_file("nonexistent.yml")
    
    def test_load_environment_variables(self):
        """Test loading configuration from environment variables."""
        # Set environment variables
        os.environ['POSTGRES_HOST'] = 'envhost'
        os.environ['POSTGRES_PORT'] = '5433'
        os.environ['POSTGRES_DB'] = 'envdb'
        os.environ['POSTGRES_USER'] = 'envuser'
        os.environ['POSTGRES_PASSWORD'] = 'envpass'
        
        try:
            config = AppConfig(
                database=DatabaseConfig(),
                schemas=[],
                metrics=MetricsConfig(),
                output=OutputConfig(),
                business_context=None,
                lineage=None
            )
            config.load_environment_variables()
            
            assert config.database.host == 'envhost'
            assert config.database.port == 5433
            assert config.database.database == 'envdb'
            assert config.database.user == 'envuser'
            assert config.database.password == 'envpass'
        finally:
            # Clean up environment variables
            for key in ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']:
                if key in os.environ:
                    del os.environ[key]


class TestMetricsConfig:
    """Test MetricsConfig class."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = MetricsConfig()
        
        assert config.enabled is True
        assert config.sample_limit == 10000
        assert config.top_k_values == 10
        assert config.include_null_counts is True
        assert config.include_distinct_counts is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = MetricsConfig(
            enabled=False,
            sample_limit=5000,
            top_k_values=5,
            include_null_counts=False,
            include_distinct_counts=False
        )
        
        assert config.enabled is False
        assert config.sample_limit == 5000
        assert config.top_k_values == 5
        assert config.include_null_counts is False
        assert config.include_distinct_counts is False


class TestOutputConfig:
    """Test OutputConfig class."""
    
    def test_default_values(self):
        """Test default output configuration values."""
        config = OutputConfig()
        
        assert config.json_dir == "./output/json"
        assert config.csv_dir == "./output/csv"
        assert config.create_dirs is True
    
    def test_custom_values(self):
        """Test custom output configuration values."""
        config = OutputConfig(
            json_dir="/custom/json",
            csv_dir="/custom/csv",
            create_dirs=False
        )
        
        assert config.json_dir == "/custom/json"
        assert config.csv_dir == "/custom/csv"
        assert config.create_dirs is False

