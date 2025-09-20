"""Main CLI application for PostgreSQL metadata extraction."""

import os
import time
import logging
from pathlib import Path
from typing import Optional, List

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import AppConfig
from .db.connection import DatabaseConnection
from .extractor.metadata_extractor import MetadataExtractor
from .extractor.quality_metrics import QualityMetricsExtractor
from .exporters.json_exporter import JSONExporter
from .exporters.csv_exporter import CSVExporter
from .exporters.postgres_exporter import PostgreSQLExporter
from .credentials.manager import CredentialsManager, DatabaseCredentials
from .utils import setup_logging, ensure_output_dirs
from .utils.encryption import get_encryption_instance

# Initialize Typer app
app = typer.Typer(
    name="postgres-metadata",
    help="PostgreSQL metadata extraction and quality metrics application",
    add_completion=False
)

# Initialize Rich console
console = Console()

# Global variables for app state
db_connection: Optional[DatabaseConnection] = None
config: Optional[AppConfig] = None


def get_database_connection(connection_id: str = "test") -> Optional[DatabaseConnection]:
    """Get database connection using stored credentials.
    
    Args:
        connection_id: Connection identifier to use
        
    Returns:
        DatabaseConnection object or None if failed
    """
    global config
    
    if config is None:
        return None
    
    try:
        # First, connect to dsa_production to get credentials
        initial_connection = DatabaseConnection(config.database.get_connection_string())
        credentials_manager = CredentialsManager(initial_connection)
        
        # Get credentials
        credentials = credentials_manager.get_credentials(connection_id)
        if not credentials:
            console.print(f"❌ No credentials found for connection_id: {connection_id}", style="red")
            return None
        
        # Build connection string from credentials
        connection_string = f"postgresql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
        
        # Create new connection
        return DatabaseConnection(connection_string)
        
    except Exception as e:
        console.print(f"❌ Failed to get database connection: {e}", style="red")
        return None


@app.command()
def scan(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to scan"),
    output_format: str = typer.Option("postgres", "--format", "-f", help="Output format (json, csv, postgres, all)"),
    connection_id: str = typer.Option("test", "--connection-id", help="Connection ID to use for database connection"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path")
):
    """Extract metadata from PostgreSQL database."""
    global db_connection, config
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level, log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Ensure output directories exist
        ensure_output_dirs(config)
        
        # Get database connection using stored credentials
        console.print(f"🔌 Getting database connection for ID: {connection_id}...", style="blue")
        db_connection = get_database_connection(connection_id)
        
        if not db_connection:
            console.print("❌ Failed to get database connection", style="red")
            raise typer.Exit(1)
        
        # Test connection
        console.print("🔌 Testing database connection...", style="blue")
        if not db_connection.test_connection():
            console.print("❌ Failed to connect to database", style="red")
            raise typer.Exit(1)
        
        server_version = db_connection.get_server_version()
        console.print(f"✅ Connected to PostgreSQL: {server_version}", style="green")
        
        # Determine target schemas
        target_schemas = [schema] if schema else config.schemas
        if not target_schemas:
            target_schemas = db_connection.get_available_schemas()
        
        console.print(f"📊 Extracting metadata for schemas: {', '.join(target_schemas)}", style="blue")
        
        # Extract metadata
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Extracting metadata...", total=None)
            
            extractor = MetadataExtractor(db_connection, config)
            schemas = extractor.extract_all_metadata(target_schemas)
            
            progress.update(task, description="✅ Metadata extraction completed")
        
        # Export results
        console.print("📁 Exporting results...", style="blue")
        
        if output_format in ["postgres", "all"]:
            postgres_exporter = PostgreSQLExporter(config, db_connection)
            run_id = postgres_exporter.export_metadata(schemas)
            console.print(f"🗄️  PostgreSQL export: run_id {run_id}", style="green")
        
        if output_format in ["json", "all"]:
            json_exporter = JSONExporter(config)
            json_file = json_exporter.export_metadata(schemas)
            console.print(f"📄 JSON export: {json_file}", style="green")
        
        if output_format in ["csv", "all"]:
            csv_exporter = CSVExporter(config)
            csv_files = csv_exporter.export_metadata(schemas)
            console.print(f"📊 CSV exports: {len(csv_files)} files", style="green")
            for file in csv_files:
                console.print(f"  - {file}", style="dim")
        
        # Display summary
        _display_metadata_summary(schemas)
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error during metadata extraction: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def scan_all(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    output_format: str = typer.Option("postgres", "--format", "-f", help="Output format (json, csv, postgres, all)"),
    connection_id: str = typer.Option("test", "--connection-id", help="Connection ID to use for database connection"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path")
):
    """Extract metadata from all available schemas."""
    scan(config_file, None, output_format, connection_id, verbose, log_file)


@app.command()
def export(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    format: str = typer.Option("json", "--format", "-f", help="Export format (json, csv)"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to export"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Export previously extracted metadata (placeholder for future implementation)."""
    console.print("ℹ️  Export command - this would export previously extracted metadata", style="yellow")
    console.print("ℹ️  For now, use 'scan' or 'scan-all' to extract and export metadata", style="yellow")


@app.command()
def populate_sample(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Load sample data and schema into the database."""
    global db_connection, config
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Initialize database connection
        db_connection = DatabaseConnection(config.database.get_connection_string())
        
        # Test connection
        console.print("🔌 Testing database connection...", style="blue")
        if not db_connection.test_connection():
            console.print("❌ Failed to connect to database", style="red")
            raise typer.Exit(1)
        
        console.print("✅ Connected to database", style="green")
        
        # Load sample schema
        sample_schema_file = Path("sample_data/sample_schema.sql")
        if not sample_schema_file.exists():
            console.print("❌ Sample schema file not found: sample_data/sample_schema.sql", style="red")
            raise typer.Exit(1)
        
        console.print("📄 Loading sample schema...", style="blue")
        
        with db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                with open(sample_schema_file, 'r') as f:
                    sql_content = f.read()
                
                # Execute SQL statements
                cur.execute(sql_content)
                conn.commit()
        
        console.print("✅ Sample schema loaded successfully", style="green")
        console.print("ℹ️  You can now run 'scan' to extract metadata from the sample data", style="blue")
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error loading sample data: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def quality_metrics(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to analyze"),
    output_format: str = typer.Option("postgres", "--format", "-f", help="Output format (json, csv, postgres, all)"),
    connection_id: str = typer.Option("test", "--connection-id", help="Connection ID to use for database connection"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path")
):
    """Extract quality metrics from PostgreSQL database."""
    global db_connection, config
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level, log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Ensure output directories exist
        ensure_output_dirs(config)
        
        # Get database connection using stored credentials
        console.print(f"🔌 Getting database connection for ID: {connection_id}...", style="blue")
        db_connection = get_database_connection(connection_id)
        
        if not db_connection:
            console.print("❌ Failed to get database connection", style="red")
            raise typer.Exit(1)
        
        # Test connection
        console.print("🔌 Testing database connection...", style="blue")
        if not db_connection.test_connection():
            console.print("❌ Failed to connect to database", style="red")
            raise typer.Exit(1)
        
        console.print("✅ Connected to database", style="green")
        
        # Determine target schemas
        target_schemas = [schema] if schema else config.schemas
        if not target_schemas:
            target_schemas = db_connection.get_available_schemas()
        
        console.print(f"📊 Extracting quality metrics for schemas: {', '.join(target_schemas)}", style="blue")
        
        # Extract quality metrics
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Extracting quality metrics...", total=None)
            
            metrics_extractor = QualityMetricsExtractor(db_connection, config)
            metrics = metrics_extractor.extract_all_metrics(target_schemas)
            
            progress.update(task, description="✅ Quality metrics extraction completed")
        
        # Export results
        console.print("📁 Exporting results...", style="blue")
        
        if output_format in ["postgres", "all"]:
            postgres_exporter = PostgreSQLExporter(config, db_connection)
            run_id = postgres_exporter.export_quality_metrics(metrics)
            console.print(f"🗄️  PostgreSQL export: run_id {run_id}", style="green")
        
        if output_format in ["json", "all"]:
            json_exporter = JSONExporter(config)
            json_file = json_exporter.export_quality_metrics(metrics)
            console.print(f"📄 JSON export: {json_file}", style="green")
        
        if output_format in ["csv", "all"]:
            csv_exporter = CSVExporter(config)
            csv_files = csv_exporter.export_quality_metrics(metrics)
            console.print(f"📊 CSV exports: {len(csv_files)} files", style="green")
            for file in csv_files:
                console.print(f"  - {file}", style="dim")
        
        # Display summary
        _display_quality_summary(metrics)
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error during quality metrics extraction: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


def _display_metadata_summary(schemas):
    """Display metadata extraction summary."""
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
    
    table = Table(title="Metadata Extraction Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    
    table.add_row("Schemas", str(len(schemas)))
    table.add_row("Tables", str(total_tables))
    table.add_row("Columns", str(total_columns))
    table.add_row("Constraints", str(total_constraints))
    table.add_row("Indexes", str(total_indexes))
    
    console.print(table)


def _display_quality_summary(metrics):
    """Display quality metrics summary."""
    total_tables = sum(len(table_list) for table_list in metrics.values())
    total_columns = sum(
        len(table.column_metrics) for table_list in metrics.values() 
        for table in table_list
    )
    
    # Calculate quality score
    high_null_columns = sum(
        sum(1 for col in table.column_metrics if col.null_percentage > 50)
        for table_list in metrics.values() for table in table_list
    )
    low_distinct_columns = sum(
        sum(1 for col in table.column_metrics 
            if col.distinct_percentage < 10 and col.total_count > 100)
        for table_list in metrics.values() for table in table_list
    )
    
    if total_columns > 0:
        quality_score = 100 - (high_null_columns / total_columns * 30) - (low_distinct_columns / total_columns * 20)
        quality_score = max(0, min(100, round(quality_score, 1)))
    else:
        quality_score = 100.0
    
    table = Table(title="Quality Metrics Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    
    table.add_row("Tables Analyzed", str(total_tables))
    table.add_row("Columns Analyzed", str(total_columns))
    table.add_row("High Null Columns (>50%)", str(high_null_columns))
    table.add_row("Low Distinct Columns (<10%)", str(low_distinct_columns))
    table.add_row("Overall Quality Score", f"{quality_score}/100")
    
    console.print(table)


@app.command()
def status(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Show status of latest metadata and quality metrics extractions."""
    global db_connection, config
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Initialize database connection
        db_connection = DatabaseConnection(config.database.get_connection_string())
        
        # Test connection
        console.print("🔌 Testing database connection...", style="blue")
        if not db_connection.test_connection():
            console.print("❌ Failed to connect to database", style="red")
            raise typer.Exit(1)
        
        console.print("✅ Connected to database", style="green")
        
        # Get latest metadata run
        postgres_exporter = PostgreSQLExporter(config, db_connection)
        latest_metadata = postgres_exporter.get_latest_metadata_run()
        latest_quality = postgres_exporter.get_latest_quality_metrics_run()
        
        # Display metadata status
        if latest_metadata:
            table = Table(title="Latest Metadata Extraction")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="magenta")
            
            table.add_row("Run ID", str(latest_metadata['run_id']))
            table.add_row("Timestamp", str(latest_metadata['extraction_timestamp']))
            table.add_row("Target Schemas", ", ".join(latest_metadata['target_schemas']))
            table.add_row("Total Schemas", str(latest_metadata['total_schemas']))
            table.add_row("Total Tables", str(latest_metadata['total_tables']))
            table.add_row("Total Columns", str(latest_metadata['total_columns']))
            table.add_row("Total Constraints", str(latest_metadata['total_constraints']))
            table.add_row("Total Indexes", str(latest_metadata['total_indexes']))
            table.add_row("Duration (s)", str(latest_metadata['extraction_duration_seconds']))
            table.add_row("Status", latest_metadata['status'])
            
            console.print(table)
        else:
            console.print("ℹ️  No metadata extractions found", style="yellow")
        
        # Display quality metrics status
        if latest_quality:
            table = Table(title="Latest Quality Metrics Extraction")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="magenta")
            
            table.add_row("Run ID", str(latest_quality['metrics_run_id']))
            table.add_row("Timestamp", str(latest_quality['extraction_timestamp']))
            table.add_row("Target Schemas", ", ".join(latest_quality['target_schemas']))
            table.add_row("Total Tables", str(latest_quality['total_tables']))
            table.add_row("Total Columns", str(latest_quality['total_columns']))
            table.add_row("High Null Columns", str(latest_quality['high_null_columns']))
            table.add_row("Low Distinct Columns", str(latest_quality['low_distinct_columns']))
            table.add_row("Quality Score", str(latest_quality['overall_quality_score']))
            table.add_row("Duration (s)", str(latest_quality['extraction_duration_seconds']))
            table.add_row("Status", latest_quality['status'])
            
            console.print(table)
        else:
            console.print("ℹ️  No quality metrics extractions found", style="yellow")
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def cleanup(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    days: int = typer.Option(30, "--days", "-d", help="Number of days to keep metadata"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Clean up old metadata from the production database."""
    global db_connection, config
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Initialize database connection
        db_connection = DatabaseConnection(config.database.get_connection_string())
        
        # Test connection
        console.print("🔌 Testing database connection...", style="blue")
        if not db_connection.test_connection():
            console.print("❌ Failed to connect to database", style="red")
            raise typer.Exit(1)
        
        console.print("✅ Connected to database", style="green")
        
        # Clean up old metadata
        postgres_exporter = PostgreSQLExporter(config, db_connection)
        deleted_count = postgres_exporter.cleanup_old_metadata(days)
        
        console.print(f"🧹 Cleaned up {deleted_count} old metadata records (older than {days} days)", style="green")
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def credentials_add(
    connection_id: str = typer.Option("test", "--connection-id", help="Connection identifier"),
    host: str = typer.Option("localhost", "--host", help="Database host"),
    port: int = typer.Option(5432, "--port", help="Database port"),
    database: str = typer.Option("postgres", "--database", help="Database name"),
    username: str = typer.Option(..., "--username", help="Database username"),
    password: str = typer.Option(..., "--password", help="Database password"),
    ssl_mode: str = typer.Option("prefer", "--ssl-mode", help="SSL mode"),
    description: Optional[str] = typer.Option(None, "--description", help="Connection description"),
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file")
):
    """Add or update database credentials."""
    global config
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Connect to dsa_production to manage credentials
        db_connection = DatabaseConnection(config.database.get_connection_string())
        credentials_manager = CredentialsManager(db_connection)
        
        # Create credentials object
        credentials = DatabaseCredentials(
            credential_id=0,  # Will be set by database
            connection_id=connection_id,
            source_type="postgresql",
            host=host,
            port=port,
            database_name=database,
            username=username,
            password=password,
            ssl_mode=ssl_mode,
            is_active=True,
            description=description
        )
        
        # Test connection first
        console.print("🔌 Testing connection...", style="blue")
        if not credentials_manager.test_connection(credentials):
            console.print("❌ Connection test failed", style="red")
            raise typer.Exit(1)
        
        # Save credentials
        if credentials_manager.save_credentials(credentials):
            console.print(f"✅ Credentials saved for connection_id: {connection_id}", style="green")
        else:
            console.print("❌ Failed to save credentials", style="red")
            raise typer.Exit(1)
    
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def credentials_list(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file")
):
    """List all stored credentials."""
    global config
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Connect to dsa_production to manage credentials
        db_connection = DatabaseConnection(config.database.get_connection_string())
        credentials_manager = CredentialsManager(db_connection)
        
        # List credentials
        credentials_list = credentials_manager.list_credentials()
        
        if not credentials_list:
            console.print("ℹ️  No credentials found", style="yellow")
            return
        
        # Display credentials in a table
        from rich.table import Table
        
        table = Table(title="Stored Credentials")
        table.add_column("Connection ID", style="cyan")
        table.add_column("Host", style="magenta")
        table.add_column("Port", style="green")
        table.add_column("Database", style="blue")
        table.add_column("Username", style="yellow")
        table.add_column("Active", style="red")
        table.add_column("Description", style="white")
        
        for cred in credentials_list:
            table.add_row(
                cred['connection_id'],
                cred['host'],
                str(cred['port']),
                cred['database_name'],
                cred['username'],
                "✅" if cred['is_active'] else "❌",
                cred['description'] or ""
            )
        
        console.print(table)
    
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def credentials_delete(
    connection_id: str = typer.Option(..., "--connection-id", help="Connection identifier to delete"),
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file")
):
    """Delete credentials for a connection ID."""
    global config
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        # Connect to dsa_production to manage credentials
        db_connection = DatabaseConnection(config.database.get_connection_string())
        credentials_manager = CredentialsManager(db_connection)
        
        # Confirm deletion
        if not typer.confirm(f"Are you sure you want to delete credentials for '{connection_id}'?"):
            console.print("❌ Operation cancelled", style="yellow")
            return
        
        # Delete credentials
        if credentials_manager.delete_credentials(connection_id):
            console.print(f"✅ Credentials deleted for connection_id: {connection_id}", style="green")
        else:
            console.print("❌ Failed to delete credentials", style="red")
            raise typer.Exit(1)
    
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def key_info():
    """Show information about the stored encryption key."""
    try:
        encryption = get_encryption_instance()
        key_info = encryption.get_stored_key_info()
        
        if key_info:
            console.print("🔑 Encryption Key Information", style="bold blue")
            console.print(f"Key File: {key_info['key_file']}", style="green")
            console.print(f"Created At: {key_info['created_at']}", style="green")
            console.print(f"Version: {key_info['version']}", style="green")
            console.print("Status: ✅ Key is stored and available", style="green")
        else:
            console.print("ℹ️  No encryption key is currently stored", style="yellow")
            console.print("A new key will be generated automatically when needed", style="yellow")
    
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def key_generate():
    """Generate and store a new encryption key."""
    try:
        encryption = get_encryption_instance()
        
        if encryption.store_current_key():
            console.print("✅ New encryption key generated and stored", style="green")
            key_info = encryption.get_stored_key_info()
            if key_info:
                console.print(f"Key File: {key_info['key_file']}", style="green")
        else:
            console.print("❌ Failed to store encryption key", style="red")
            raise typer.Exit(1)
    
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def key_delete():
    """Delete the stored encryption key."""
    try:
        encryption = get_encryption_instance()
        
        if not encryption.key_storage.key_exists():
            console.print("ℹ️  No encryption key is currently stored", style="yellow")
            return
        
        if typer.confirm("Are you sure you want to delete the stored encryption key?"):
            if encryption.delete_stored_key():
                console.print("✅ Encryption key deleted", style="green")
                console.print("⚠️  Note: Existing encrypted passwords may become inaccessible", style="yellow")
            else:
                console.print("❌ Failed to delete encryption key", style="red")
                raise typer.Exit(1)
        else:
            console.print("❌ Operation cancelled", style="yellow")
    
    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


def main():
    """Main entry point for the application."""
    app()


if __name__ == "__main__":
    main()