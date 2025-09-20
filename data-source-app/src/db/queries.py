"""SQL queries for metadata extraction from PostgreSQL."""

from typing import List


class MetadataQueries:
    """Collection of SQL queries for extracting metadata from PostgreSQL."""
    
    def get_schemas(self) -> str:
        """Get all schemas in the database."""
        return """
            SELECT 
                schema_name,
                schema_owner
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """
    
    def get_tables(self, schema_name: str) -> str:
        """Get all tables in a specific schema."""
        return """
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
    
    def get_table_comments(self, schema_name: str, table_name: str) -> str:
        """Get table comment."""
        return """
            SELECT 
                obj_description(c.oid) as comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
    
    def get_columns(self, schema_name: str, table_name: str) -> str:
        """Get all columns for a specific table."""
        return """
            SELECT 
                column_name,
                ordinal_position,
                column_default,
                is_nullable,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
    
    def get_column_comments(self, schema_name: str, table_name: str) -> str:
        """Get column comments."""
        return """
            SELECT 
                a.attname as column_name,
                col_description(a.attrelid, a.attnum) as comment
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s 
            AND c.relname = %s
            AND a.attnum > 0
            AND NOT a.attisdropped
        """
    
    def get_primary_keys(self, schema_name: str, table_name: str) -> str:
        """Get primary key columns for a table."""
        return """
            SELECT 
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """
    
    def get_foreign_keys(self, schema_name: str, table_name: str) -> str:
        """Get foreign key constraints for a table."""
        return """
            SELECT 
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
        """
    
    def get_unique_constraints(self, schema_name: str, table_name: str) -> str:
        """Get unique constraints for a table."""
        return """
            SELECT 
                tc.constraint_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
            AND tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """
    
    def get_indexes(self, schema_name: str, table_name: str) -> str:
        """Get indexes for a table."""
        return """
            SELECT 
                i.relname as index_name,
                pg_get_indexdef(i.oid) as definition,
                a.attname as column_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_class i
            JOIN pg_index ix ON i.oid = ix.indexrelid
            JOIN pg_class t ON ix.indrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_attribute a ON t.oid = a.attrelid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = %s
            AND t.relname = %s
            AND i.relkind = 'i'
            ORDER BY i.relname, a.attnum
        """
    
    def get_available_schemas(self) -> str:
        """Get list of available schemas for scanning."""
        return """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> str:
        """Get row count for a table (for quality metrics)."""
        return """
            SELECT COUNT(*) as row_count
            FROM {}.{}
        """.format(schema_name, table_name)
    
    def get_column_sample_data(self, schema_name: str, table_name: str, column_name: str, limit: int = 10000) -> str:
        """Get sample data for a column (for quality metrics)."""
        return """
            SELECT {} as value
            FROM {}.{}
            WHERE {} IS NOT NULL
            LIMIT {}
        """.format(column_name, schema_name, table_name, column_name, limit)
    
    def get_column_distinct_count(self, schema_name: str, table_name: str, column_name: str) -> str:
        """Get distinct count for a column."""
        return """
            SELECT COUNT(DISTINCT {}) as distinct_count
            FROM {}.{}
        """.format(column_name, schema_name, table_name)
    
    def get_column_null_count(self, schema_name: str, table_name: str, column_name: str) -> str:
        """Get null count for a column."""
        return """
            SELECT COUNT(*) as null_count
            FROM {}.{}
            WHERE {} IS NULL
        """.format(column_name, schema_name, table_name, column_name)
    
    def get_column_top_values(self, schema_name: str, table_name: str, column_name: str, limit: int = 10) -> str:
        """Get top values for a column."""
        return """
            SELECT 
                {} as value,
                COUNT(*) as frequency
            FROM {}.{}
            WHERE {} IS NOT NULL
            GROUP BY {}
            ORDER BY frequency DESC
            LIMIT {}
        """.format(column_name, schema_name, table_name, column_name, column_name, limit)
    
    def get_column_stats(self, schema_name: str, table_name: str, column_name: str, 
                        sample_limit: int = 10000) -> str:
        """Get column statistics for quality metrics."""
        return """
            SELECT 
                COUNT(*) as total_count,
                COUNT({}) as non_null_count,
                COUNT(*) - COUNT({}) as null_count,
                COUNT(DISTINCT {}) as distinct_count
            FROM {}.{}
        """.format(column_name, column_name, column_name, schema_name, table_name)
    
    def get_top_values(self, schema_name: str, table_name: str, column_name: str, 
                      limit: int = 10) -> str:
        """Get top values for a column (alias for get_column_top_values)."""
        return self.get_column_top_values(schema_name, table_name, column_name, limit)
