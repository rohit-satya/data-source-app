# Metadata Frontend

A simple frontend application to display database metadata for a given connection_id. This frontend queries the normalized metadata tables directly without touching the existing extraction components.

## Features

- **Command Line Interface**: Simple CLI tool to display metadata in the terminal
- **Web Interface**: Flask-based web application with a modern UI
- **API Endpoints**: RESTful API for programmatic access
- **Connection Discovery**: Automatically discovers available connections
- **Real-time Data**: Shows the latest metadata from the most recent sync run

## Installation

1. Install the required dependencies:
```bash
cd frontend
pip install -r requirements.txt
```

## Usage

### Command Line Interface

Display metadata for a specific connection:

```bash
python app.py <connection_id>
```

Example:
```bash
python app.py test-connection
```

### Web Interface

Start the web server:

```bash
python web_app.py
```

Then open your browser and go to:
- http://localhost:5000 - Main page with available connections
- http://localhost:5000/metadata/<connection_id> - Specific connection metadata

### API Endpoints

- `GET /api/connections` - List all available connections
- `GET /api/metadata/<connection_id>` - Get metadata for a specific connection

Example API usage:
```bash
curl http://localhost:5000/api/connections
curl http://localhost:5000/api/metadata/test-connection
```

## How It Works

1. **Connection Discovery**: Queries the `sync_runs` table to find available connections
2. **Latest Sync Identification**: For each connection, finds the most recent completed sync run
3. **Metadata Retrieval**: Fetches normalized metadata from:
   - `normalized_schemas` - Schema information
   - `normalized_tables` - Table information  
   - `normalized_columns` - Column information
4. **Data Display**: Presents the data in a user-friendly format

## Data Structure

The frontend displays:

- **Schema Information**: Schema names, status, custom attributes
- **Table Information**: Table names, types, custom attributes
- **Column Information**: Column names, data types, nullability, comments
- **Sync Information**: Sync ID, timestamp, connector type
- **Statistics**: Counts of schemas, tables, and columns

## Configuration

The frontend uses the same configuration file as the main application (`config.yml`) to connect to the database.

## Requirements

- Python 3.7+
- PostgreSQL database with normalized metadata tables
- Flask (for web interface)
- psycopg (for database connectivity)

## Architecture

The frontend is designed to be completely independent of the existing extraction components:

- **No Dependencies**: Doesn't import or use any extractor, connector, or exporter modules
- **Direct Database Access**: Queries the normalized tables directly
- **Configuration Sharing**: Uses the same config system for database connection
- **Standalone**: Can be run independently of the main application

## Troubleshooting

1. **No connections found**: Make sure metadata has been extracted first
2. **Database connection errors**: Check your `config.yml` file
3. **Missing data**: Ensure the sync run completed successfully
4. **Web interface not loading**: Check that Flask is installed and port 5000 is available
