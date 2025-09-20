-- Sample PostgreSQL schema for testing the metadata extraction app
-- This creates a sample e-commerce database with various data types and relationships

-- Create schema
CREATE SCHEMA IF NOT EXISTS dsa_ecommerce;

-- Set search path
SET search_path TO dsa_ecommerce, public;

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    credit_score INTEGER CHECK (credit_score >= 300 AND credit_score <= 850),
    notes TEXT
);

-- Add comments to customers table
COMMENT ON TABLE customers IS 'Customer information table [tags: core,user]';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier [tags: primary,id]';
COMMENT ON COLUMN customers.email IS 'Customer email address [tags: contact,unique]';
COMMENT ON COLUMN customers.credit_score IS 'FICO credit score [tags: financial,score]';

-- Create categories table
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_category_id INTEGER REFERENCES categories(category_id),
    is_active BOOLEAN DEFAULT TRUE,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to categories table
COMMENT ON TABLE categories IS 'Product categories with hierarchical structure [tags: catalog,hierarchy]';
COMMENT ON COLUMN categories.parent_category_id IS 'Parent category for hierarchical structure [tags: hierarchy,self-reference]';

-- Create products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    sku VARCHAR(50) UNIQUE NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(category_id),
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    cost DECIMAL(10,2) CHECK (cost >= 0),
    weight_kg DECIMAL(8,3),
    dimensions_cm JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    reorder_level INTEGER DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to products table
COMMENT ON TABLE products IS 'Product catalog with inventory tracking [tags: catalog,inventory]';
COMMENT ON COLUMN products.sku IS 'Stock Keeping Unit identifier [tags: inventory,unique]';
COMMENT ON COLUMN products.dimensions_cm IS 'Product dimensions in JSON format [tags: physical,json]';

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_number VARCHAR(20) UNIQUE NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    subtotal DECIMAL(10,2) NOT NULL CHECK (subtotal >= 0),
    tax_amount DECIMAL(10,2) DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount DECIMAL(10,2) DEFAULT 0 CHECK (shipping_amount >= 0),
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    shipping_address JSONB NOT NULL,
    billing_address JSONB,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to orders table
COMMENT ON TABLE orders IS 'Customer orders with financial tracking [tags: sales,financial]';
COMMENT ON COLUMN orders.shipping_address IS 'Shipping address in JSON format [tags: address,json]';

-- Create order_items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to order_items table
COMMENT ON TABLE order_items IS 'Individual items within orders [tags: sales,line-items]';

-- Create reviews table
CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(200),
    comment TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0 CHECK (helpful_votes >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, customer_id)
);

-- Add comments to reviews table
COMMENT ON TABLE reviews IS 'Product reviews and ratings [tags: feedback,ratings]';
COMMENT ON COLUMN reviews.rating IS 'Star rating from 1 to 5 [tags: rating,score]';

-- Create indexes for better performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_created_at ON customers(created_at);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_is_active ON products(is_active);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_reviews_product_id ON reviews(product_id);
CREATE INDEX idx_reviews_customer_id ON reviews(customer_id);
CREATE INDEX idx_reviews_rating ON reviews(rating);

-- Create a view for product summary
CREATE VIEW product_summary AS
SELECT 
    p.product_id,
    p.name,
    p.sku,
    c.name as category_name,
    p.price,
    p.stock_quantity,
    p.is_active,
    COALESCE(AVG(r.rating), 0) as avg_rating,
    COUNT(r.review_id) as review_count
FROM products p
LEFT JOIN categories c ON p.category_id = c.category_id
LEFT JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name, p.sku, c.name, p.price, p.stock_quantity, p.is_active;

-- Add comment to view
COMMENT ON VIEW product_summary IS 'Product summary with ratings and review counts [tags: view,summary]';

-- Insert sample data
INSERT INTO categories (name, description, is_active, sort_order) VALUES
('Electronics', 'Electronic devices and accessories [tags: electronics,tech]', TRUE, 1),
('Clothing', 'Apparel and fashion items [tags: fashion,apparel]', TRUE, 2),
('Books', 'Books and educational materials [tags: education,media]', TRUE, 3),
('Home & Garden', 'Home improvement and garden supplies [tags: home,garden]', TRUE, 4);

INSERT INTO categories (name, description, parent_category_id, is_active, sort_order) VALUES
('Smartphones', 'Mobile phones and accessories', 1, TRUE, 1),
('Laptops', 'Portable computers', 1, TRUE, 2),
('Men''s Clothing', 'Clothing for men', 2, TRUE, 1),
('Women''s Clothing', 'Clothing for women', 2, TRUE, 2);

INSERT INTO customers (first_name, last_name, email, phone, date_of_birth, credit_score, notes) VALUES
('John', 'Doe', 'john.doe@example.com', '+1-555-0101', '1985-03-15', 750, 'VIP customer [tags: vip,premium]'),
('Jane', 'Smith', 'jane.smith@example.com', '+1-555-0102', '1990-07-22', 680, 'Regular customer'),
('Bob', 'Johnson', 'bob.johnson@example.com', '+1-555-0103', '1978-11-08', 720, 'Bulk buyer [tags: bulk,corporate]'),
('Alice', 'Brown', 'alice.brown@example.com', '+1-555-0104', '1992-05-30', 650, NULL),
('Charlie', 'Wilson', 'charlie.wilson@example.com', '+1-555-0105', '1988-12-12', 780, 'High-value customer [tags: high-value,premium]');

INSERT INTO products (name, description, sku, category_id, price, cost, weight_kg, dimensions_cm, stock_quantity, reorder_level) VALUES
('iPhone 15 Pro', 'Latest Apple smartphone with advanced features [tags: smartphone,apple,premium]', 'IPH15PRO-128', 5, 999.99, 650.00, 0.187, '{"length": 14.67, "width": 7.15, "height": 0.83}', 50, 10),
('MacBook Air M2', 'Lightweight laptop with M2 chip [tags: laptop,apple,portable]', 'MBA-M2-256', 6, 1199.99, 800.00, 1.24, '{"length": 30.41, "width": 21.5, "height": 1.13}', 25, 5),
('Cotton T-Shirt', 'Comfortable cotton t-shirt [tags: clothing,cotton,basic]', 'TSHIRT-COTTON-M', 7, 19.99, 8.50, 0.2, '{"length": 71, "width": 51, "height": 1}', 100, 20),
('Programming Book', 'Learn Python programming [tags: book,programming,education]', 'BOOK-PYTHON-001', 3, 49.99, 25.00, 0.8, '{"length": 23.5, "width": 19.1, "height": 3.2}', 75, 15),
('Garden Hose', '50ft garden hose with spray nozzle [tags: garden,tools,outdoor]', 'GARDEN-HOSE-50', 4, 29.99, 15.00, 2.5, '{"length": 1524, "width": 2.5, "height": 2.5}', 30, 10);

INSERT INTO orders (customer_id, order_number, status, subtotal, tax_amount, shipping_amount, total_amount, shipping_address, billing_address) VALUES
(1, 'ORD-2024-001', 'delivered', 1019.98, 81.60, 15.00, 1116.58, '{"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}', '{"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}'),
(2, 'ORD-2024-002', 'shipped', 69.98, 5.60, 10.00, 85.58, '{"street": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "zip": "90210"}', '{"street": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "zip": "90210"}'),
(3, 'ORD-2024-003', 'pending', 49.99, 4.00, 5.00, 58.99, '{"street": "789 Pine St", "city": "Chicago", "state": "IL", "zip": "60601"}', '{"street": "789 Pine St", "city": "Chicago", "state": "IL", "zip": "60601"}');

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 999.99, 999.99),
(1, 2, 1, 19.99, 19.99),
(2, 3, 1, 19.99, 19.99),
(2, 4, 1, 49.99, 49.99),
(3, 5, 1, 49.99, 49.99);

INSERT INTO reviews (product_id, customer_id, rating, title, comment, is_verified, helpful_votes) VALUES
(1, 1, 5, 'Excellent phone!', 'Great camera and battery life [tags: positive,detailed]', TRUE, 3),
(1, 2, 4, 'Good but expensive', 'Nice features but pricey [tags: mixed,price]', TRUE, 1),
(2, 1, 5, 'Perfect laptop', 'Fast and lightweight [tags: positive,performance]', TRUE, 2),
(3, 2, 3, 'Average quality', 'Decent t-shirt for the price [tags: neutral,quality]', FALSE, 0),
(4, 3, 5, 'Great book', 'Very helpful for learning Python [tags: positive,educational]', TRUE, 5);

-- Create some additional constraints
ALTER TABLE order_items ADD CONSTRAINT chk_total_price CHECK (total_price = quantity * unit_price);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_reviews_updated_at BEFORE UPDATE ON reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================
-- METADATA STORAGE SCHEMA FOR PRODUCTION
-- =============================================

-- Create dsa_production schema for metadata storage
CREATE SCHEMA IF NOT EXISTS dsa_production;

-- Set search path to include dsa_production schema
SET search_path TO dsa_production, dsa_ecommerce, public;

-- Metadata extraction runs table
CREATE TABLE metadata_extraction_runs (
    run_id SERIAL PRIMARY KEY,
    extraction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_schemas TEXT[] NOT NULL,
    total_schemas INTEGER NOT NULL,
    total_tables INTEGER NOT NULL,
    total_columns INTEGER NOT NULL,
    total_constraints INTEGER NOT NULL,
    total_indexes INTEGER NOT NULL,
    extraction_duration_seconds DECIMAL(10,3),
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('running', 'completed', 'failed')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment on metadata extraction runs table
COMMENT ON TABLE metadata_extraction_runs IS 'Tracks metadata extraction runs and their statistics';
COMMENT ON COLUMN metadata_extraction_runs.target_schemas IS 'Array of schema names that were scanned';
COMMENT ON COLUMN metadata_extraction_runs.extraction_duration_seconds IS 'Time taken for extraction in seconds';

-- Schemas metadata table
CREATE TABLE schemas_metadata (
    schema_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES metadata_extraction_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    owner VARCHAR(63) NOT NULL,
    table_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name)
);

-- Comment on schemas metadata table
COMMENT ON TABLE schemas_metadata IS 'Schema-level metadata extracted from PostgreSQL';
COMMENT ON COLUMN schemas_metadata.run_id IS 'Reference to the extraction run';

-- Tables metadata table
CREATE TABLE tables_metadata (
    table_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES metadata_extraction_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    table_type VARCHAR(20) NOT NULL,
    comment TEXT,
    tags TEXT[],
    column_count INTEGER NOT NULL DEFAULT 0,
    constraint_count INTEGER NOT NULL DEFAULT 0,
    index_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name)
);

-- Comment on tables metadata table
COMMENT ON TABLE tables_metadata IS 'Table-level metadata extracted from PostgreSQL';
COMMENT ON COLUMN tables_metadata.tags IS 'Array of tags parsed from comments or YAML';

-- Columns metadata table
CREATE TABLE columns_metadata (
    column_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES metadata_extraction_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    position INTEGER NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    is_nullable BOOLEAN NOT NULL,
    default_value TEXT,
    max_length INTEGER,
    precision_value INTEGER,
    scale_value INTEGER,
    comment TEXT,
    tags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name, column_name)
);

-- Comment on columns metadata table
COMMENT ON TABLE columns_metadata IS 'Column-level metadata extracted from PostgreSQL';
COMMENT ON COLUMN columns_metadata.precision_value IS 'Numeric precision for numeric types';
COMMENT ON COLUMN columns_metadata.scale_value IS 'Numeric scale for numeric types';

-- Constraints metadata table
CREATE TABLE constraints_metadata (
    constraint_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES metadata_extraction_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    constraint_name VARCHAR(63) NOT NULL,
    constraint_type VARCHAR(20) NOT NULL,
    columns TEXT[] NOT NULL,
    referenced_schema VARCHAR(63),
    referenced_table VARCHAR(63),
    referenced_columns TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name, constraint_name)
);

-- Comment on constraints metadata table
COMMENT ON TABLE constraints_metadata IS 'Constraint metadata extracted from PostgreSQL';
COMMENT ON COLUMN constraints_metadata.columns IS 'Array of column names involved in the constraint';

-- Indexes metadata table
CREATE TABLE indexes_metadata (
    index_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES metadata_extraction_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    index_name VARCHAR(63) NOT NULL,
    definition TEXT NOT NULL,
    columns TEXT[] NOT NULL,
    is_unique BOOLEAN NOT NULL,
    is_primary BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, schema_name, table_name, index_name)
);

-- Comment on indexes metadata table
COMMENT ON TABLE indexes_metadata IS 'Index metadata extracted from PostgreSQL';

-- Quality metrics runs table
CREATE TABLE quality_metrics_runs (
    metrics_run_id SERIAL PRIMARY KEY,
    extraction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_schemas TEXT[] NOT NULL,
    total_tables INTEGER NOT NULL,
    total_columns INTEGER NOT NULL,
    high_null_columns INTEGER NOT NULL DEFAULT 0,
    low_distinct_columns INTEGER NOT NULL DEFAULT 0,
    overall_quality_score DECIMAL(5,2),
    extraction_duration_seconds DECIMAL(10,3),
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('running', 'completed', 'failed')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment on quality metrics runs table
COMMENT ON TABLE quality_metrics_runs IS 'Tracks quality metrics extraction runs and their statistics';

-- Table quality metrics table
CREATE TABLE table_quality_metrics (
    table_metrics_id SERIAL PRIMARY KEY,
    metrics_run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(metrics_run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    row_count BIGINT NOT NULL,
    column_count INTEGER NOT NULL,
    high_null_columns INTEGER NOT NULL DEFAULT 0,
    low_distinct_columns INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metrics_run_id, schema_name, table_name)
);

-- Comment on table quality metrics table
COMMENT ON TABLE table_quality_metrics IS 'Table-level quality metrics';

-- Column quality metrics table
CREATE TABLE column_quality_metrics (
    column_metrics_id SERIAL PRIMARY KEY,
    metrics_run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(metrics_run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    total_count BIGINT NOT NULL,
    non_null_count BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    null_percentage DECIMAL(5,2) NOT NULL,
    distinct_count BIGINT NOT NULL,
    distinct_percentage DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metrics_run_id, schema_name, table_name, column_name)
);

-- Comment on column quality metrics table
COMMENT ON TABLE column_quality_metrics IS 'Column-level quality metrics';

-- Top values table for storing most frequent values
CREATE TABLE column_top_values (
    top_value_id SERIAL PRIMARY KEY,
    metrics_run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(metrics_run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    value_text TEXT NOT NULL,
    frequency BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment on column top values table
COMMENT ON TABLE column_top_values IS 'Most frequent values for each column';

-- Create indexes for better performance
CREATE INDEX idx_schemas_metadata_run_id ON schemas_metadata(run_id);
CREATE INDEX idx_schemas_metadata_schema_name ON schemas_metadata(schema_name);

CREATE INDEX idx_tables_metadata_run_id ON tables_metadata(run_id);
CREATE INDEX idx_tables_metadata_schema_name ON tables_metadata(schema_name);
CREATE INDEX idx_tables_metadata_table_name ON tables_metadata(table_name);

CREATE INDEX idx_columns_metadata_run_id ON columns_metadata(run_id);
CREATE INDEX idx_columns_metadata_schema_table ON columns_metadata(schema_name, table_name);
CREATE INDEX idx_columns_metadata_data_type ON columns_metadata(data_type);

CREATE INDEX idx_constraints_metadata_run_id ON constraints_metadata(run_id);
CREATE INDEX idx_constraints_metadata_schema_table ON constraints_metadata(schema_name, table_name);
CREATE INDEX idx_constraints_metadata_type ON constraints_metadata(constraint_type);

CREATE INDEX idx_indexes_metadata_run_id ON indexes_metadata(run_id);
CREATE INDEX idx_indexes_metadata_schema_table ON indexes_metadata(schema_name, table_name);
CREATE INDEX idx_indexes_metadata_is_unique ON indexes_metadata(is_unique);

CREATE INDEX idx_table_quality_metrics_run_id ON table_quality_metrics(metrics_run_id);
CREATE INDEX idx_table_quality_metrics_schema_table ON table_quality_metrics(schema_name, table_name);

CREATE INDEX idx_column_quality_metrics_run_id ON column_quality_metrics(metrics_run_id);
CREATE INDEX idx_column_quality_metrics_schema_table ON column_quality_metrics(schema_name, table_name);
CREATE INDEX idx_column_quality_metrics_null_percentage ON column_quality_metrics(null_percentage);

CREATE INDEX idx_column_top_values_run_id ON column_top_values(metrics_run_id);
CREATE INDEX idx_column_top_values_schema_table ON column_top_values(schema_name, table_name);

-- Create views for easy querying
CREATE VIEW latest_metadata_extraction AS
SELECT 
    mer.*,
    sm.schema_name,
    sm.owner,
    sm.table_count
FROM metadata_extraction_runs mer
LEFT JOIN schemas_metadata sm ON mer.run_id = sm.run_id
WHERE mer.run_id = (
    SELECT MAX(run_id) 
    FROM metadata_extraction_runs 
    WHERE status = 'completed'
);

CREATE VIEW latest_quality_metrics AS
SELECT 
    qmr.metrics_run_id,
    qmr.extraction_timestamp,
    qmr.target_schemas,
    qmr.total_tables,
    qmr.total_columns,
    qmr.overall_quality_score,
    qmr.extraction_duration_seconds,
    qmr.status,
    qmr.error_message,
    qmr.created_at,
    tqm.schema_name,
    tqm.table_name,
    tqm.row_count,
    tqm.column_count,
    tqm.high_null_columns,
    tqm.low_distinct_columns
FROM quality_metrics_runs qmr
LEFT JOIN table_quality_metrics tqm ON qmr.metrics_run_id = tqm.metrics_run_id
WHERE qmr.metrics_run_id = (
    SELECT MAX(metrics_run_id) 
    FROM quality_metrics_runs 
    WHERE status = 'completed'
);

-- Create a function to clean up old metadata (optional)
CREATE OR REPLACE FUNCTION cleanup_old_metadata(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
    cutoff_date TIMESTAMP;
BEGIN
    cutoff_date := CURRENT_TIMESTAMP - INTERVAL '1 day' * days_to_keep;
    
    -- Delete old metadata extraction runs and cascade to related tables
    DELETE FROM metadata_extraction_runs 
    WHERE extraction_timestamp < cutoff_date;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Delete old quality metrics runs and cascade to related tables
    DELETE FROM quality_metrics_runs 
    WHERE extraction_timestamp < cutoff_date;
    
    deleted_count := deleted_count + ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Comment on cleanup function
COMMENT ON FUNCTION cleanup_old_metadata(INTEGER) IS 'Cleans up metadata older than specified days (default 30)';

-- =============================================
-- CREDENTIALS MANAGEMENT
-- =============================================

-- Credentials table for storing database connection information
CREATE TABLE credentials (
    credential_id SERIAL PRIMARY KEY,
    connection_id VARCHAR(50) NOT NULL DEFAULT 'test',
    source_type VARCHAR(20) NOT NULL DEFAULT 'postgresql',
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL DEFAULT 5432,
    database_name VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    password_encrypted TEXT NOT NULL,
    ssl_mode VARCHAR(20) DEFAULT 'prefer',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    description TEXT,
    UNIQUE(connection_id, source_type)
);

-- Comment on credentials table
COMMENT ON TABLE credentials IS 'Stores encrypted database connection credentials';
COMMENT ON COLUMN credentials.connection_id IS 'Unique identifier for the connection';
COMMENT ON COLUMN credentials.source_type IS 'Type of database (postgresql, mysql, etc.)';
COMMENT ON COLUMN credentials.password_encrypted IS 'Encrypted password using application key';
COMMENT ON COLUMN credentials.ssl_mode IS 'SSL connection mode (disable, prefer, require)';

-- Create index for active credentials lookup
CREATE INDEX idx_credentials_active ON credentials(connection_id, is_active) WHERE is_active = TRUE;

-- Create trigger to update updated_at timestamp
CREATE TRIGGER update_credentials_updated_at BEFORE UPDATE ON credentials
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default test credentials (password will be encrypted by application)
INSERT INTO credentials (connection_id, source_type, host, port, database_name, username, password_encrypted, description) 
VALUES ('test', 'postgresql', 'localhost', 5432, 'postgres', 'rohit.satyainti', 'PLACEHOLDER_ENCRYPTED_PASSWORD', 'Default test connection');

-- Create a function to get credentials by connection_id
CREATE OR REPLACE FUNCTION get_credentials(conn_id VARCHAR(50))
RETURNS TABLE (
    credential_id INTEGER,
    connection_id VARCHAR(50),
    source_type VARCHAR(20),
    host VARCHAR(255),
    port INTEGER,
    database_name VARCHAR(100),
    username VARCHAR(100),
    password_encrypted TEXT,
    ssl_mode VARCHAR(20),
    is_active BOOLEAN,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.credential_id,
        c.connection_id,
        c.source_type,
        c.host,
        c.port,
        c.database_name,
        c.username,
        c.password_encrypted,
        c.ssl_mode,
        c.is_active,
        c.description
    FROM dsa_production.credentials c
    WHERE c.connection_id = conn_id 
    AND c.is_active = TRUE
    ORDER BY c.created_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Comment on get_credentials function
COMMENT ON FUNCTION get_credentials(VARCHAR(50)) IS 'Retrieves active credentials for a given connection_id';

