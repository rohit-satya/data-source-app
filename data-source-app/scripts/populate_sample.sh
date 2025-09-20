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
    echo "  -f, --file FILE        Sample schema file (default: sample_data/sample_schema.sql)"
    echo "  --help                 Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  POSTGRES_HOST          Database host"
    echo "  POSTGRES_PORT          Database port"
    echo "  POSTGRES_DB            Database name"
    echo "  POSTGRES_USER          Database user"
    echo "  POSTGRES_PASSWORD      Database password"
    echo "  POSTGRES_DSN           Full database connection string"
}

# Parse command line arguments
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
        -f|--file)
            SAMPLE_SCHEMA_FILE="$2"
            shift 2
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

# Load environment variables if they exist
if [[ -n "$POSTGRES_HOST" ]]; then DB_HOST="$POSTGRES_HOST"; fi
if [[ -n "$POSTGRES_PORT" ]]; then DB_PORT="$POSTGRES_PORT"; fi
if [[ -n "$POSTGRES_DB" ]]; then DB_NAME="$POSTGRES_DB"; fi
if [[ -n "$POSTGRES_USER" ]]; then DB_USER="$POSTGRES_USER"; fi
if [[ -n "$POSTGRES_PASSWORD" ]]; then DB_PASSWORD="$POSTGRES_PASSWORD"; fi

# Check if PostgreSQL client is installed
if ! command -v psql &> /dev/null; then
    print_error "PostgreSQL client (psql) is not installed or not in PATH"
    exit 1
fi

# Check if sample schema file exists
if [[ ! -f "$SAMPLE_SCHEMA_FILE" ]]; then
    print_error "Sample schema file not found: $SAMPLE_SCHEMA_FILE"
    exit 1
fi

# Build connection string
if [[ -n "$POSTGRES_DSN" ]]; then
    CONNECTION_STRING="$POSTGRES_DSN"
else
    CONNECTION_STRING="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"
fi

print_status "PostgreSQL Metadata App - Sample Data Population"
echo "=================================================="
print_status "Database: $DB_HOST:$DB_PORT/$DB_NAME"
print_status "User: $DB_USER"
print_status "Schema file: $SAMPLE_SCHEMA_FILE"
echo ""

# Test database connection
print_status "Testing database connection..."
if psql "$CONNECTION_STRING" -c "SELECT 1;" > /dev/null 2>&1; then
    print_success "Database connection successful"
else
    print_error "Failed to connect to database"
    print_error "Please check your connection parameters and ensure PostgreSQL is running"
    exit 1
fi

# Get PostgreSQL version
PG_VERSION=$(psql "$CONNECTION_STRING" -t -c "SELECT version();" | head -1)
print_status "PostgreSQL version: $PG_VERSION"

# Load sample schema
print_status "Loading sample schema..."
if psql "$CONNECTION_STRING" -f "$SAMPLE_SCHEMA_FILE"; then
    print_success "Sample schema loaded successfully"
else
    print_error "Failed to load sample schema"
    exit 1
fi

# Verify data was loaded
print_status "Verifying sample data..."

# Check schemas
SCHEMA_COUNT=$(psql "$CONNECTION_STRING" -t -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'ecommerce';")
if [[ "$SCHEMA_COUNT" -gt 0 ]]; then
    print_success "Ecommerce schema created"
else
    print_warning "Ecommerce schema not found"
fi

# Check tables
TABLE_COUNT=$(psql "$CONNECTION_STRING" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'ecommerce';")
if [[ "$TABLE_COUNT" -gt 0 ]]; then
    print_success "Created $TABLE_COUNT tables in ecommerce schema"
else
    print_warning "No tables found in ecommerce schema"
fi

# Check sample data
CUSTOMER_COUNT=$(psql "$CONNECTION_STRING" -t -c "SELECT COUNT(*) FROM ecommerce.customers;")
PRODUCT_COUNT=$(psql "$CONNECTION_STRING" -t -c "SELECT COUNT(*) FROM ecommerce.products;")
ORDER_COUNT=$(psql "$CONNECTION_STRING" -t -c "SELECT COUNT(*) FROM ecommerce.orders;")

print_status "Sample data counts:"
echo "  - Customers: $CUSTOMER_COUNT"
echo "  - Products: $PRODUCT_COUNT"
echo "  - Orders: $ORDER_COUNT"

# Show next steps
echo ""
print_success "Sample data population completed!"
echo ""
print_status "Next steps:"
echo "1. Update your config.yml with the correct database connection details"
echo "2. Run the metadata extraction:"
echo "   python -m src.app scan --config config.yml --schema ecommerce"
echo "3. Or run quality metrics:"
echo "   python -m src.app quality-metrics --config config.yml --schema ecommerce"
echo ""
print_status "Sample data includes:"
echo "  - E-commerce schema with customers, products, orders, reviews"
echo "  - Various data types (VARCHAR, INTEGER, DECIMAL, JSONB, etc.)"
echo "  - Foreign key relationships and constraints"
echo "  - Indexes for performance"
echo "  - Sample data with comments and tags"
echo "  - A product summary view"

