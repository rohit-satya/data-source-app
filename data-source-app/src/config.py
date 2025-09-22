"""Configuration management for PostgreSQL metadata app."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    dsn: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None

    def get_connection_string(self) -> str:
        """Get the PostgreSQL connection string."""
        if self.dsn:
            return self.dsn
        
        # Build DSN from individual parameters
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("Missing required database connection parameters")
        
        port = self.port or 5432
        return f"postgresql://{self.user}:{self.password}@{self.host}:{port}/{self.database}"


@dataclass
class MetricsConfig:
    """Quality metrics configuration."""
    enabled: bool = True
    sample_limit: int = 10000
    top_k_values: int = 10
    include_null_counts: bool = True
    include_distinct_counts: bool = True


@dataclass
class OutputConfig:
    """Output configuration."""
    json_dir: str = "./output/json"
    csv_dir: str = "./output/csv"
    create_dirs: bool = True


@dataclass
class BusinessContextConfig:
    """Business context configuration."""
    extract_comments: bool = True
    parse_tags: bool = True
    metadata_table: Optional[str] = None
    metadata_yaml: Optional[str] = None


@dataclass
class LineageConfig:
    """Lineage configuration."""
    enabled: bool = True
    extract_foreign_keys: bool = True
    parse_view_dependencies: bool = True


@dataclass
class EncryptionConfig:
    """Encryption configuration."""
    master_key: Optional[str] = None


@dataclass
class AppConfig:
    """Main application configuration."""
    database: DatabaseConfig
    schemas: List[str]
    metrics: MetricsConfig
    output: OutputConfig
    business_context: BusinessContextConfig
    lineage: LineageConfig
    encryption: EncryptionConfig

    @classmethod
    def from_file(cls, config_path: str) -> "AppConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Load database config
        db_config = config_data.get('database', {})
        database = DatabaseConfig(
            dsn=db_config.get('dsn'),
            host=db_config.get('host'),
            port=db_config.get('port'),
            database=db_config.get('database'),
            user=db_config.get('user'),
            password=db_config.get('password')
        )
        
        # Load other configs
        schemas = config_data.get('schemas', [])
        
        metrics_data = config_data.get('metrics', {})
        metrics = MetricsConfig(
            enabled=metrics_data.get('enabled', True),
            sample_limit=metrics_data.get('sample_limit', 10000),
            top_k_values=metrics_data.get('top_k_values', 10),
            include_null_counts=metrics_data.get('include_null_counts', True),
            include_distinct_counts=metrics_data.get('include_distinct_counts', True)
        )
        
        output_data = config_data.get('output', {})
        output = OutputConfig(
            json_dir=output_data.get('json_dir', './output/json'),
            csv_dir=output_data.get('csv_dir', './output/csv'),
            create_dirs=output_data.get('create_dirs', True)
        )
        
        business_data = config_data.get('business_context', {})
        business_context = BusinessContextConfig(
            extract_comments=business_data.get('extract_comments', True),
            parse_tags=business_data.get('parse_tags', True),
            metadata_table=business_data.get('metadata_table'),
            metadata_yaml=business_data.get('metadata_yaml')
        )
        
        lineage_data = config_data.get('lineage', {})
        lineage = LineageConfig(
            enabled=lineage_data.get('enabled', True),
            extract_foreign_keys=lineage_data.get('extract_foreign_keys', True),
            parse_view_dependencies=lineage_data.get('parse_view_dependencies', True)
        )
        
        encryption_data = config_data.get('encryption', {})
        encryption = EncryptionConfig(
            master_key=encryption_data.get('master_key')
        )
        
        return cls(
            database=database,
            schemas=schemas,
            metrics=metrics,
            output=output,
            business_context=business_context,
            lineage=lineage,
            encryption=encryption
        )

    def get_encryption_key(self) -> Optional[str]:
        """Get the encryption key from config."""
        return self.encryption.master_key

    def load_environment_variables(self) -> None:
        """Load configuration from environment variables if not set in config file."""
        # Load database config from environment
        if not self.database.dsn and not self.database.host:
            self.database.host = os.getenv('POSTGRES_HOST')
            self.database.port = int(os.getenv('POSTGRES_PORT', '5432'))
            self.database.database = os.getenv('POSTGRES_DB')
            self.database.user = os.getenv('POSTGRES_USER')
            self.database.password = os.getenv('POSTGRES_PASSWORD')
            
            # Check for DSN format
            dsn = os.getenv('POSTGRES_DSN')
            if dsn:
                self.database.dsn = dsn
