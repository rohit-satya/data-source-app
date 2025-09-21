-- Create normalized schema for metadata storage
-- This script creates tables that match the normalized entity structure

-- Connect to the database
\c postgres;

-- Create dsa_production schema
CREATE SCHEMA IF NOT EXISTS dsa_production;

-- Set search path
SET search_path TO dsa_production, public;

-- =============================================
-- NORMALIZED METADATA STORAGE SCHEMA
-- =============================================

-- Sync runs table (replaces metadata_extraction_runs)
CREATE TABLE sync_runs (
    sync_id UUID PRIMARY KEY,
    sync_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    connector_name VARCHAR(50) NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('running', 'completed', 'failed')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment on sync runs table
COMMENT ON TABLE sync_runs IS 'Tracks sync runs with normalized structure';
COMMENT ON COLUMN sync_runs.sync_id IS 'Unique sync identifier (UUID)';
COMMENT ON COLUMN sync_runs.connector_name IS 'Type of connector used (postgres, mysql, etc.)';

-- Normalized schemas table
CREATE TABLE normalized_schemas (
    id SERIAL PRIMARY KEY,
    sync_id UUID NOT NULL REFERENCES sync_runs(sync_id) ON DELETE CASCADE,
    type_name VARCHAR(20) NOT NULL DEFAULT 'Schema',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    name VARCHAR(63) NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    last_sync_run UUID NOT NULL,
    last_sync_run_at BIGINT NOT NULL,
    connector_name VARCHAR(50) NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    custom_attributes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sync_id, name)
);

-- Comment on normalized schemas table
COMMENT ON TABLE normalized_schemas IS 'Normalized schema metadata following the new structure';
COMMENT ON COLUMN normalized_schemas.attributes IS 'JSONB containing qualified names and other metadata';
COMMENT ON COLUMN normalized_schemas.custom_attributes IS 'JSONB containing custom metadata';

-- Normalized tables table
CREATE TABLE normalized_tables (
    id SERIAL PRIMARY KEY,
    sync_id UUID NOT NULL REFERENCES sync_runs(sync_id) ON DELETE CASCADE,
    type_name VARCHAR(20) NOT NULL DEFAULT 'Table',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    name VARCHAR(63) NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    last_sync_run UUID NOT NULL,
    last_sync_run_at BIGINT NOT NULL,
    connector_name VARCHAR(50) NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    custom_attributes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sync_id, name)
);

-- Comment on normalized tables table
COMMENT ON TABLE normalized_tables IS 'Normalized table metadata following the new structure';
COMMENT ON COLUMN normalized_tables.attributes IS 'JSONB containing qualified names and table metadata';
COMMENT ON COLUMN normalized_tables.custom_attributes IS 'JSONB containing table-specific metadata';

-- Normalized columns table
CREATE TABLE normalized_columns (
    id SERIAL PRIMARY KEY,
    sync_id UUID NOT NULL REFERENCES sync_runs(sync_id) ON DELETE CASCADE,
    type_name VARCHAR(20) NOT NULL DEFAULT 'Column',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    name VARCHAR(63) NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL DEFAULT 'default',
    last_sync_run UUID NOT NULL,
    last_sync_run_at BIGINT NOT NULL,
    connector_name VARCHAR(50) NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    custom_attributes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sync_id, name)
);

-- Comment on normalized columns table
COMMENT ON TABLE normalized_columns IS 'Normalized column metadata following the new structure';
COMMENT ON COLUMN normalized_columns.attributes IS 'JSONB containing qualified names and column metadata';
COMMENT ON COLUMN normalized_columns.custom_attributes IS 'JSONB containing column-specific metadata';

-- Quality metrics runs table (updated for normalized structure)
CREATE TABLE quality_metrics_runs (
    run_id SERIAL PRIMARY KEY,
    sync_id UUID NOT NULL REFERENCES sync_runs(sync_id) ON DELETE CASCADE,
    extraction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_schemas TEXT[] NOT NULL,
    total_tables INTEGER NOT NULL,
    total_columns INTEGER NOT NULL,
    extraction_duration_seconds DECIMAL(10,3),
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('running', 'completed', 'failed')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table quality metrics (unchanged)
CREATE TABLE table_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name)
);

-- Column quality metrics (unchanged)
CREATE TABLE column_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    total_count INTEGER NOT NULL,
    non_null_count INTEGER NOT NULL,
    null_count INTEGER NOT NULL,
    null_percentage DECIMAL(5,2) NOT NULL,
    distinct_count INTEGER NOT NULL,
    distinct_percentage DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name, column_name)
);

-- Column top values (unchanged)
CREATE TABLE column_top_values (
    value_id SERIAL PRIMARY KEY,
    metric_id INTEGER NOT NULL REFERENCES column_quality_metrics(metric_id) ON DELETE CASCADE,
    value_text TEXT NOT NULL,
    frequency INTEGER NOT NULL,
    percentage DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_normalized_schemas_sync_id ON normalized_schemas(sync_id);
CREATE INDEX idx_normalized_schemas_name ON normalized_schemas(name);
CREATE INDEX idx_normalized_tables_sync_id ON normalized_tables(sync_id);
CREATE INDEX idx_normalized_tables_name ON normalized_tables(name);
CREATE INDEX idx_normalized_columns_sync_id ON normalized_columns(sync_id);
CREATE INDEX idx_normalized_columns_name ON normalized_columns(name);

-- Create indexes on JSONB attributes for better querying
CREATE INDEX idx_normalized_schemas_attributes ON normalized_schemas USING GIN (attributes);
CREATE INDEX idx_normalized_tables_attributes ON normalized_tables USING GIN (attributes);
CREATE INDEX idx_normalized_columns_attributes ON normalized_columns USING GIN (attributes);

-- Credentials table (for storing connection credentials)
CREATE TABLE credentials (
    credential_id SERIAL PRIMARY KEY,
    connection_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    password TEXT NOT NULL,
    ssl_mode VARCHAR(20) DEFAULT 'prefer',
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(connection_id, source_type)
);

-- Comment on credentials table
COMMENT ON TABLE credentials IS 'Stores database connection credentials';
COMMENT ON COLUMN credentials.connection_id IS 'Unique identifier for the connection';
COMMENT ON COLUMN credentials.source_type IS 'Type of database (postgresql, mysql, etc.)';

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for credentials table
CREATE TRIGGER update_credentials_updated_at 
    BEFORE UPDATE ON credentials 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Display confirmation
SELECT 'Normalized schema created successfully' as status;
