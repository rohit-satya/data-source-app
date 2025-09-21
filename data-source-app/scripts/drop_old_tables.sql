-- Drop existing metadata tables in dsa_production schema
-- This script removes the old table structure to prepare for normalized schema

-- Connect to the database
\c postgres;

-- Drop existing tables in dsa_production schema
DROP SCHEMA IF EXISTS dsa_production CASCADE;

-- Recreate the schema
CREATE SCHEMA dsa_production;

-- Set search path
SET search_path TO dsa_production, public;

-- Display confirmation
SELECT 'Old dsa_production schema and tables dropped successfully' as status;
