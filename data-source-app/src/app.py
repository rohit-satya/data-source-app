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
from .connector import BaseConnector, SourceConnection, ConnectorFactory
from .exporters.metadata_exporter import MetadataExporter
from .services.database_service import DatabaseService
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
sync_id: Optional[str] = None


def generate_sync_id() -> str:
    """Generate a unique sync ID for this extraction run.
    
    Returns:
        Unique sync ID as string
    """
    import uuid
    return str(uuid.uuid4())


def get_source_connection(connection_id: str = "test") -> Optional[SourceConnection]:
    """Get source connection using stored credentials.
    
    Args:
        connection_id: Connection identifier to use
        
    Returns:
        SourceConnection object or None if failed
    """
    global config
    
    if config is None:
        return None
    
    try:
        # Use DatabaseService to get credentials and create source connection
        database_service = DatabaseService(config)
        encryption_key = config.get_encryption_key()
        source_connection = database_service.create_source_connection(connection_id, encryption_key)
        
        if not source_connection:
            console.print(f"❌ No credentials found for connection_id: {connection_id}", style="red")
            return None
        
        return source_connection
        
    except Exception as e:
        console.print(f"❌ Failed to get source connection: {e}", style="red")
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
    global db_connection, config, sync_id
    
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

        # 1. Generate sync ID for this extraction run
        sync_id = generate_sync_id()
        console.print(f"🆔 Sync ID: {sync_id}", style="blue")

        # 2. Identify the connector source_type
        console.print(f"🔌 Getting source connection for ID: {connection_id}...", style="blue")
        source_connection = get_source_connection(connection_id)

        if not source_connection:
            console.print("❌ Failed to get source connection", style="red")
            raise typer.Exit(1)

        console.print(f"📋 Source type identified: {source_connection.source_type}", style="blue")

        # 3. Instantiate the connector
        connector = ConnectorFactory.create_connector(source_connection, config, sync_id)
        if not connector:
            console.print(f"❌ Unsupported source type: {source_connection.source_type}", style="red")
            raise typer.Exit(1)
        
        # Test connection
        console.print("🔌 Testing source connection...", style="blue")
        if not connector.test_connection():
            console.print("❌ Failed to connect to source", style="red")
            raise typer.Exit(1)
        
        # Get connection info
        conn_info = connector.get_connection_info()
        console.print(f"✅ Connected to {conn_info.get('source_type', 'unknown')}: {conn_info.get('server_version', 'unknown')}", style="green")
        
        # Determine target schemas
        target_schemas = [schema] if schema else []
        if not target_schemas:
            target_schemas = connector.get_available_schemas()
        
        console.print(f"📊 Extracting metadata for schemas: {', '.join(target_schemas)}", style="blue")
        
        # 3. Connector should return the metadata
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Extracting metadata...", total=None)
            
            schemas = connector.extract_metadata(target_schemas)
            
            progress.update(task, description="✅ Metadata extraction completed")
        
        # 4. Create and instantiate a separate extractor class which takes care of dumping the metadata
        console.print("📁 Exporting results...", style="blue")
        
        # Create database service for PostgreSQL operations
        database_service = DatabaseService(config)
        
        # Create metadata exporter (agnostic of connector)
        metadata_exporter = MetadataExporter(config, database_service)
        
        # Export metadata using the exporter
        export_results = metadata_exporter.export_metadata(schemas, output_format, sync_id)
        
        # Display export results
        for format_name, result in export_results.items():
            if result['success']:
                console.print(f"✅ {result['message']}", style="green")
                if format_name == 'json' and ',' in result['message']:
                    # Handle multiple files from JSON export
                    files = result['message'].split(',')
                    for file in files:
                        console.print(f"  - {file.strip()}", style="dim")
                elif format_name == 'csv' and 'files' in result:
                    for file in result['files']:
                        console.print(f"  - {file}", style="dim")
            else:
                console.print(f"❌ {result['message']}", style="red")
        
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
def quality_metrics(
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to analyze"),
    output_format: str = typer.Option("postgres", "--format", "-f", help="Output format (json, csv, postgres, all)"),
    connection_id: str = typer.Option("test", "--connection-id", help="Connection ID to use for database connection"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path")
):
    """Extract quality metrics from PostgreSQL database."""
    global db_connection, config, sync_id
    
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

        # 1. Generate sync ID for this extraction run
        sync_id = generate_sync_id()
        console.print(f"🆔 Sync ID: {sync_id}", style="blue")

        # 2. Identify the connector source_type
        console.print(f"🔌 Getting source connection for ID: {connection_id}...", style="blue")
        source_connection = get_source_connection(connection_id)

        if not source_connection:
            console.print("❌ Failed to get source connection", style="red")
            raise typer.Exit(1)

        console.print(f"📋 Source type identified: {source_connection.source_type}", style="blue")

        # 3. Instantiate the connector
        connector = ConnectorFactory.create_connector(source_connection, config, sync_id)
        if not connector:
            console.print(f"❌ Unsupported source type: {source_connection.source_type}", style="red")
            raise typer.Exit(1)
        
        # Test connection
        console.print("🔌 Testing source connection...", style="blue")
        if not connector.test_connection():
            console.print("❌ Failed to connect to source", style="red")
            raise typer.Exit(1)
        
        # Get connection info
        conn_info = connector.get_connection_info()
        console.print(f"✅ Connected to {conn_info.get('source_type', 'unknown')}", style="green")
        
        # Determine target schemas
        target_schemas = [schema] if schema else []
        if not target_schemas:
            target_schemas = connector.get_available_schemas()
        
        console.print(f"📊 Extracting quality metrics for schemas: {', '.join(target_schemas)}", style="blue")
        
        # 3. Connector should return the quality metrics
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Extracting quality metrics...", total=None)
            
            metrics = connector.extract_quality_metrics(target_schemas)
            
            progress.update(task, description="✅ Quality metrics extraction completed")
        
        # 4. Create and instantiate a separate extractor class which takes care of dumping the metrics
        console.print("📁 Exporting results...", style="blue")
        
        # Create database service for PostgreSQL operations
        database_service = DatabaseService(config)
        
        # Create metadata exporter (agnostic of connector)
        metadata_exporter = MetadataExporter(config, database_service)
        
        # Export quality metrics using the exporter
        export_results = metadata_exporter.export_quality_metrics(
            metrics, output_format, sync_id, 
            connection_name=connection_id,
            connector_name=source_connection.source_type,
            tenant_id="default"
        )
        
        # Display export results
        for format_name, result in export_results.items():
            if result['success']:
                console.print(f"✅ {result['message']}", style="green")
                if format_name == 'json' and ',' in result['message']:
                    # Handle multiple files from JSON export
                    files = result['message'].split(',')
                    for file in files:
                        console.print(f"  - {file.strip()}", style="dim")
                elif format_name == 'csv' and 'files' in result:
                    for file in result['files']:
                        console.print(f"  - {file}", style="dim")
            else:
                console.print(f"❌ {result['message']}", style="red")
        
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
    # Normalized models don't have constraints and indexes in the same way
    total_constraints = 0  # Not available in normalized structure
    total_indexes = 0      # Not available in normalized structure
    
    table = Table(title="Metadata Extraction Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    
    table.add_row("Schemas", str(len(schemas)))
    table.add_row("Tables", str(total_tables))
    table.add_row("Columns", str(total_columns))
    
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
        
        # Use DatabaseService to manage credentials
        database_service = DatabaseService(config)
        encryption_key = config.get_encryption_key()
        credentials_manager = database_service.get_credentials_manager(encryption_key)
        
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
        
        # Use DatabaseService to manage credentials
        database_service = DatabaseService(config)
        encryption_key = config.get_encryption_key()
        credentials_manager = database_service.get_credentials_manager(encryption_key)
        
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
        
        # Use DatabaseService to manage credentials
        database_service = DatabaseService(config)
        encryption_key = config.get_encryption_key()
        credentials_manager = database_service.get_credentials_manager(encryption_key)
        
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
def incremental_diff(
    connection_id: str = typer.Option(..., "--connection-id", help="Connection ID to compare"),
    format: str = typer.Option("postgres", "--format", "-f", help="Output format (postgres, json)"),
    config_file: str = typer.Option("config.yml", "--config", "-c", help="Path to configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path")
):
    """Run incremental diff between the last two sync runs for a connection."""
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level, log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = AppConfig.from_file(config_file)
        config.load_environment_variables()
        
        console.print(f"🔍 Starting incremental diff for connection: {connection_id}", style="blue")
        console.print(f"📊 Output format: {format}", style="blue")
        
        # Import the incremental diff service
        from .services.incremental_diff_service import IncrementalDiffService
        
        # Create the service
        diff_service = IncrementalDiffService(config)
        
        # Run the incremental diff
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running incremental diff...", total=None)
            
            result = diff_service.run_incremental_diff(connection_id, format)
            
            if result["success"]:
                progress.update(task, description="✅ Incremental diff completed")
                
                # Display results
                console.print(f"\n🎉 Incremental diff completed successfully!", style="bold green")
                console.print(f"🆔 Diff Sync ID: {result['diff_sync_id']}", style="green")
                console.print(f"🔌 Connection: {result['connection_id']}", style="green")
                console.print(f"📊 Sync Run 1 (older): {result['sync_run_1_id']}", style="blue")
                console.print(f"📊 Sync Run 2 (newer): {result['sync_run_2_id']}", style="blue")
                
                # Display change summary
                table = Table(title="Change Summary")
                table.add_column("Asset Type", style="cyan")
                table.add_column("Changes", style="magenta")
                
                table.add_row("Schemas", str(result['schemas_changed']))
                table.add_row("Tables", str(result['tables_changed']))
                table.add_row("Columns", str(result['columns_changed']))
                table.add_row("Total", str(result['total_changes']))
                
                console.print(table)
                
                if result['total_changes'] > 0:
                    console.print(f"\n💡 View detailed changes in dsa_production.incremental_diff_* tables", style="blue")
                    console.print(f"💡 Query diff summary: SELECT * FROM dsa_production.diff_summary WHERE diff_sync_id = '{result['diff_sync_id']}'", style="blue")
                else:
                    console.print(f"\n✨ No changes detected between the two sync runs!", style="green")
                
            else:
                progress.update(task, description="❌ Incremental diff failed")
                console.print(f"❌ Incremental diff failed: {result['error']}", style="red")
                raise typer.Exit(1)
        
    except FileNotFoundError as e:
        console.print(f"❌ Configuration file not found: {e}", style="red")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error during incremental diff: {e}")
        console.print(f"❌ Error: {e}", style="red")
        raise typer.Exit(1)


def main():
    """Main entry point for the application."""
    app()


if __name__ == "__main__":
    main()