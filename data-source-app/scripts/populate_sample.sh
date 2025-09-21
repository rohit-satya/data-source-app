#!/bin/bash

# PostgreSQL Metadata App - Sample Data Population Script
# This script loads sample data into a PostgreSQL database for testing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CONFIG_FILE="config.yml"
DB_HOST="localhost" 
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="password"
SAMPLE_SCHEMA_FILE="sample_data/sample_schema.sql"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -c, --config FILE     Configuration file (default: config.yml)"
    echo "  -h, --host HOST        Database host (default: localhost)"
    echo "  -p, --port PORT        Database port (default: 5432)"
    echo "  -d, --database NAME    Database name (default: postgres)"
    echo "  -u, --user USER        Database user (default: postgres)"
    echo "  -w, --password PASS    Database password (default: password)"
    echo "  -s, --schema FILE      Sample schema file (default: sample_data/sample_schema.sql)"
    echo "  --reset                Reset existing data before loading"
    echo "  --views-only           Only create views, skip sample data"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use default settings"
    echo "  $0 --host localhost --port 5432      # Specify host and port"
    echo "  $0 --config my_config.yml            # Use custom config file"
    echo "  $0 --reset                           # Reset and reload all data"
    echo "  $0 --views-only                      # Only create views for existing data"
}

# Function to parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -h|--host)
                DB_HOST="$2"
                shift 2
                ;;
            -p|--port)
                DB_PORT="$2"
                shift 2
                ;;
            -d|--database)
                DB_NAME="$2"
                shift 2
                ;;
            -u|--user)
                DB_USER="$2"
                shift 2
                ;;
            -w|--password)
                DB_PASSWORD="$2"
                shift 2
                ;;
            -s|--schema)
                SAMPLE_SCHEMA_FILE="$2"
                shift 2
                ;;
            --reset)
                RESET_DATA=true
                shift
                ;;
            --views-only)
                VIEWS_ONLY=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Function to load configuration from YAML file
load_config() {
    if [[ -f "$CONFIG_FILE" ]]; then
        print_status "Loading configuration from $CONFIG_FILE"
        
        # Use Python to parse YAML and extract database connection info
        DB_CONFIG=$(python3 -c "
import yaml
import sys
try:
    with open('$CONFIG_FILE', 'r') as f:
        config = yaml.safe_load(f)
    db_config = config.get('database', {})
    
    # Handle DSN format
    if 'dsn' in db_config:
        dsn = db_config['dsn']
        # Parse DSN: postgresql://user:pass@host:port/database
        if dsn.startswith('postgresql://'):
            dsn = dsn[13:]  # Remove postgresql://
            if '@' in dsn:
                user_pass, host_db = dsn.split('@', 1)
                if ':' in user_pass:
                    user, password = user_pass.split(':', 1)
                else:
                    user = user_pass
                    password = ''
                if ':' in host_db:
                    host_port, database = host_db.split('/', 1)
                    if ':' in host_port:
                        host, port = host_port.split(':', 1)
                    else:
                        host = host_port
                        port = '5432'
                else:
                    host = host_db
                    port = '5432'
                    database = 'postgres'
            else:
                host = dsn
                port = '5432'
                database = 'postgres'
                user = 'postgres'
                password = ''
        else:
            host = db_config.get('host', 'localhost')
            port = str(db_config.get('port', 5432))
            database = db_config.get('database', 'postgres')
            user = db_config.get('user', 'postgres')
            password = db_config.get('password', '')
    else:
        host = db_config.get('host', 'localhost')
        port = str(db_config.get('port', 5432))
        database = db_config.get('database', 'postgres')
        user = db_config.get('user', 'postgres')
        password = db_config.get('password', '')
    
    print(f'{host}|{port}|{database}|{user}|{password}')
except Exception as e:
    print('Error loading config:', e, file=sys.stderr)
    sys.exit(1)
" 2>/dev/null)
        
        if [[ $? -eq 0 && -n "$DB_CONFIG" ]]; then
            IFS='|' read -r DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD <<< "$DB_CONFIG"
            print_success "Configuration loaded successfully"
        else
            print_warning "Failed to load configuration, using defaults"
        fi
    else
        print_warning "Configuration file $CONFIG_FILE not found, using defaults"
    fi
}

# Function to test database connection
test_connection() {
    print_status "Testing database connection..."
    
    if PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
        print_success "Database connection successful"
        return 0
    else
        print_error "Database connection failed"
        return 1
    fi
}

# Function to reset existing data
reset_data() {
    print_status "Resetting existing data..."
    
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
-- Drop existing schemas and recreate
DROP SCHEMA IF EXISTS dsa_ecommerce CASCADE;
DROP SCHEMA IF EXISTS dsa_production CASCADE;

-- Drop the function if it exists
DROP FUNCTION IF EXISTS get_credentials(VARCHAR(50));
DROP FUNCTION IF EXISTS update_updated_at_column();

SELECT 'Data reset completed' as status;
EOF

    print_success "Data reset completed"
}

# Function to load sample schema
load_schema() {
    print_status "Loading sample schema from $SAMPLE_SCHEMA_FILE..."
    
    if [[ ! -f "$SAMPLE_SCHEMA_FILE" ]]; then
        print_error "Sample schema file $SAMPLE_SCHEMA_FILE not found"
        exit 1
    fi
    
    if PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SAMPLE_SCHEMA_FILE"; then
        print_success "Sample schema loaded successfully"
    else
        print_error "Failed to load sample schema"
        exit 1
    fi
}

# Function to create views only
create_views_only() {
    print_status "Creating views for existing data..."
    
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
-- Set search path
SET search_path TO dsa_production, public;

-- Create views for latest metadata and quality metrics
-- (Views are already included in the sample_schema.sql file)
SELECT 'Views created successfully' as status;
EOF

    print_success "Views created successfully"
}

# Function to verify data
verify_data() {
    print_status "Verifying loaded data..."
    
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
-- Set search path
SET search_path TO dsa_ecommerce, dsa_production, public;

-- Check sample data
SELECT 'Sample Data Verification' as section;
SELECT 'Customers' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 'Categories', COUNT(*) FROM categories
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items
UNION ALL
SELECT 'Reviews', COUNT(*) FROM reviews;

-- Check normalized schema structure
SELECT 'Normalized Schema Structure' as section;
SELECT 'sync_runs' as table_name, COUNT(*) as count FROM sync_runs
UNION ALL
SELECT 'normalized_schemas', COUNT(*) FROM normalized_schemas
UNION ALL
SELECT 'normalized_tables', COUNT(*) FROM normalized_tables
UNION ALL
SELECT 'normalized_columns', COUNT(*) FROM normalized_columns
UNION ALL
SELECT 'quality_metrics_runs', COUNT(*) FROM quality_metrics_runs
UNION ALL
SELECT 'table_quality_metrics', COUNT(*) FROM table_quality_metrics
UNION ALL
SELECT 'column_quality_metrics', COUNT(*) FROM column_quality_metrics
UNION ALL
SELECT 'column_top_values', COUNT(*) FROM column_top_values
UNION ALL
SELECT 'credentials', COUNT(*) FROM credentials;

-- Check views
SELECT 'Views Verification' as section;
SELECT 'latest_schema_metadata' as view_name, COUNT(*) as count FROM latest_schema_metadata
UNION ALL
SELECT 'latest_table_metadata', COUNT(*) FROM latest_table_metadata
UNION ALL
SELECT 'latest_column_metadata', COUNT(*) FROM latest_column_metadata
UNION ALL
SELECT 'latest_quality_metrics_summary', COUNT(*) FROM latest_quality_metrics_summary
UNION ALL
SELECT 'latest_table_quality_metrics', COUNT(*) FROM latest_table_quality_metrics
UNION ALL
SELECT 'latest_column_quality_metrics', COUNT(*) FROM latest_column_quality_metrics
UNION ALL
SELECT 'latest_column_top_values', COUNT(*) FROM latest_column_top_values
UNION ALL
SELECT 'active_credentials', COUNT(*) FROM active_credentials
UNION ALL
SELECT 'sync_run_history', COUNT(*) FROM sync_run_history;
EOF

    print_success "Data verification completed"
}

# Function to show next steps
show_next_steps() {
    echo ""
    print_success "Sample data population completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Run metadata extraction:"
    echo "   python -m src.app scan --schema dsa_ecommerce --format postgres"
    echo ""
    echo "2. Run quality metrics extraction:"
    echo "   python -m src.app quality-metrics --schema dsa_ecommerce --format postgres"
    echo ""
    echo "3. Query the latest metadata views:"
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_schema_metadata;\""
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_table_metadata;\""
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_column_metadata;\""
    echo ""
    echo "4. Query the latest quality metrics views:"
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_quality_metrics_summary;\""
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_table_quality_metrics;\""
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM latest_column_quality_metrics;\""
    echo ""
    echo "5. Check sync run history:"
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM sync_run_history;\""
    echo ""
    echo "6. Check active credentials:"
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SELECT * FROM active_credentials;\""
}

# Main execution
main() {
    echo "PostgreSQL Metadata App - Sample Data Population Script"
    echo "======================================================"
    echo ""
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Load configuration
    load_config
    
    # Display configuration
    echo "Configuration:"
    echo "  Host: $DB_HOST"
    echo "  Port: $DB_PORT"
    echo "  Database: $DB_NAME"
    echo "  User: $DB_USER"
    echo "  Schema File: $SAMPLE_SCHEMA_FILE"
    echo "  Reset Data: ${RESET_DATA:-false}"
    echo "  Views Only: ${VIEWS_ONLY:-false}"
    echo ""
    
    # Test database connection
    if ! test_connection; then
        exit 1
    fi
    
    # Reset data if requested
    if [[ "$RESET_DATA" == "true" ]]; then
        reset_data
    fi
    
    # Load schema or create views only
    if [[ "$VIEWS_ONLY" == "true" ]]; then
        create_views_only
    else
        load_schema
    fi
    
    # Verify data
    verify_data
    
    # Show next steps
    show_next_steps
}

# Run main function
main "$@"