-- Create incremental diff tables for tracking changes between sync runs

-- Table to track diff sync runs
CREATE TABLE IF NOT EXISTS dsa_production.diff_sync_runs (
    diff_sync_id UUID PRIMARY KEY,
    connection_id TEXT NOT NULL,
    sync_run_1_id UUID NOT NULL,
    sync_run_2_id UUID NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL DEFAULT 'running',
    total_schemas_changed INTEGER DEFAULT 0,
    total_tables_changed INTEGER DEFAULT 0,
    total_columns_changed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (sync_run_1_id) REFERENCES dsa_production.sync_runs(sync_id),
    FOREIGN KEY (sync_run_2_id) REFERENCES dsa_production.sync_runs(sync_id)
);

-- Table to track schema differences
CREATE TABLE IF NOT EXISTS dsa_production.incremental_diff_schema (
    diff_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    diff_sync_id UUID NOT NULL,
    schema_name TEXT NOT NULL,
    change_type TEXT NOT NULL, -- 'added', 'removed', 'modified'
    sync_run_1_data JSONB,
    sync_run_2_data JSONB,
    differences JSONB, -- Detailed differences
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (diff_sync_id) REFERENCES dsa_production.diff_sync_runs(diff_sync_id)
);

-- Table to track table differences
CREATE TABLE IF NOT EXISTS dsa_production.incremental_diff_table (
    diff_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    diff_sync_id UUID NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    change_type TEXT NOT NULL, -- 'added', 'removed', 'modified'
    sync_run_1_data JSONB,
    sync_run_2_data JSONB,
    differences JSONB, -- Detailed differences
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (diff_sync_id) REFERENCES dsa_production.diff_sync_runs(diff_sync_id)
);

-- Table to track column differences
CREATE TABLE IF NOT EXISTS dsa_production.incremental_diff_column (
    diff_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    diff_sync_id UUID NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    change_type TEXT NOT NULL, -- 'added', 'removed', 'modified'
    sync_run_1_data JSONB,
    sync_run_2_data JSONB,
    differences JSONB, -- Detailed differences
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (diff_sync_id) REFERENCES dsa_production.diff_sync_runs(diff_sync_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_diff_sync_runs_connection_id ON dsa_production.diff_sync_runs(connection_id);
CREATE INDEX IF NOT EXISTS idx_diff_sync_runs_started_at ON dsa_production.diff_sync_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_diff_schema_diff_sync_id ON dsa_production.incremental_diff_schema(diff_sync_id);
CREATE INDEX IF NOT EXISTS idx_diff_table_diff_sync_id ON dsa_production.incremental_diff_table(diff_sync_id);
CREATE INDEX IF NOT EXISTS idx_diff_column_diff_sync_id ON dsa_production.incremental_diff_column(diff_sync_id);
CREATE INDEX IF NOT EXISTS idx_diff_column_schema_table ON dsa_production.incremental_diff_column(schema_name, table_name);

-- Create a view for easy querying of diff results
CREATE OR REPLACE VIEW dsa_production.diff_summary AS
SELECT 
    dsr.diff_sync_id,
    dsr.connection_id,
    dsr.started_at,
    dsr.completed_at,
    dsr.status,
    dsr.total_schemas_changed,
    dsr.total_tables_changed,
    dsr.total_columns_changed,
    dsr.error_message,
    sr1.sync_timestamp as sync_run_1_timestamp,
    sr2.sync_timestamp as sync_run_2_timestamp
FROM dsa_production.diff_sync_runs dsr
LEFT JOIN dsa_production.sync_runs sr1 ON dsr.sync_run_1_id = sr1.sync_id
LEFT JOIN dsa_production.sync_runs sr2 ON dsr.sync_run_2_id = sr2.sync_id;
