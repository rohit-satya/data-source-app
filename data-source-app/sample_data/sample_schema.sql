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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    category_id INTEGER REFERENCES categories(category_id),
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    cost DECIMAL(10,2) CHECK (cost >= 0),
    weight DECIMAL(8,2),
    dimensions JSONB, -- Store as {"length": 10, "width": 5, "height": 3}
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to products table
COMMENT ON TABLE products IS 'Product catalog with pricing and inventory [tags: catalog,product]';
COMMENT ON COLUMN products.sku IS 'Stock Keeping Unit identifier [tags: inventory,unique]';
COMMENT ON COLUMN products.price IS 'Selling price in USD [tags: pricing,currency]';
COMMENT ON COLUMN products.dimensions IS 'Product dimensions in JSON format [tags: physical,measurements]';

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    total_amount DECIMAL(10,2) NOT NULL,
    shipping_address JSONB NOT NULL, -- Store as {"street": "...", "city": "...", "state": "...", "zip": "..."}
    billing_address JSONB,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to orders table
COMMENT ON TABLE orders IS 'Customer orders with status tracking [tags: transaction,order]';
COMMENT ON COLUMN orders.status IS 'Current order status [tags: workflow,state]';
COMMENT ON COLUMN orders.shipping_address IS 'Delivery address in JSON format [tags: location,address]';

-- Create order_items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    total_price DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to order_items table
COMMENT ON TABLE order_items IS 'Individual items within orders [tags: transaction,line-item]';
COMMENT ON COLUMN order_items.total_price IS 'Calculated total for this line item [tags: calculated,financial]';

-- Create reviews table
CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(200),
    comment TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    helpful_votes INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments to reviews table
COMMENT ON TABLE reviews IS 'Product reviews and ratings [tags: feedback,rating]';
COMMENT ON COLUMN reviews.rating IS 'Star rating from 1 to 5 [tags: quality,score]';
COMMENT ON COLUMN reviews.is_verified IS 'Whether this is a verified purchase review [tags: trust,verification]';

-- Create indexes for better performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_created_at ON customers(created_at);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_reviews_product_id ON reviews(product_id);
CREATE INDEX idx_reviews_customer_id ON reviews(customer_id);
CREATE INDEX idx_reviews_rating ON reviews(rating);

-- Create a function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_reviews_updated_at BEFORE UPDATE ON reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample data
INSERT INTO categories (name, description, sort_order) VALUES
('Electronics', 'Electronic devices and accessories', 1),
('Clothing', 'Apparel and fashion items', 2),
('Books', 'Books and educational materials', 3),
('Home & Garden', 'Home improvement and garden supplies', 4),
('Sports', 'Sports equipment and outdoor gear', 5);

-- Insert subcategories
INSERT INTO categories (name, description, parent_category_id, sort_order) VALUES
('Smartphones', 'Mobile phones and accessories', 1, 1),
('Laptops', 'Portable computers', 1, 2),
('Headphones', 'Audio devices', 1, 3),
('Men''s Clothing', 'Men''s apparel', 2, 1),
('Women''s Clothing', 'Women''s apparel', 2, 2),
('Fiction', 'Fictional books', 3, 1),
('Non-Fiction', 'Educational and reference books', 3, 2);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone, date_of_birth, credit_score, notes) VALUES
('John', 'Doe', 'john.doe@email.com', '+1-555-0101', '1985-03-15', 750, 'VIP customer'),
('Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102', '1990-07-22', 680, 'Regular customer'),
('Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103', '1978-11-08', 720, 'Bulk buyer'),
('Alice', 'Brown', 'alice.brown@email.com', '+1-555-0104', '1992-04-12', 800, 'New customer'),
('Charlie', 'Wilson', 'charlie.wilson@email.com', '+1-555-0105', '1988-09-30', 650, 'Price sensitive');

-- Insert sample products
INSERT INTO products (name, description, sku, category_id, price, cost, weight, dimensions) VALUES
('iPhone 15 Pro', 'Latest Apple smartphone with advanced camera system', 'IPH15PRO-256', 6, 999.99, 750.00, 0.187, '{"length": 6.1, "width": 2.78, "height": 0.32}'),
('MacBook Air M2', 'Lightweight laptop with M2 chip', 'MBA-M2-512', 7, 1199.99, 900.00, 2.7, '{"length": 11.97, "width": 8.46, "height": 0.44}'),
('Sony WH-1000XM5', 'Premium noise-canceling headphones', 'SONY-WH1000XM5', 8, 399.99, 250.00, 0.25, '{"length": 7.28, "width": 3.15, "height": 9.84}'),
('Nike Air Max 270', 'Comfortable running shoes', 'NIKE-AM270-10', 9, 150.00, 80.00, 0.8, '{"length": 12, "width": 8, "height": 4}'),
('The Great Gatsby', 'Classic American novel', 'BOOK-GATSBY', 10, 12.99, 5.00, 0.5, '{"length": 8, "width": 5.25, "height": 0.5}'),
('Python Programming Guide', 'Comprehensive Python programming book', 'BOOK-PYTHON', 11, 49.99, 25.00, 1.2, '{"length": 9.25, "width": 7.5, "height": 1.5}');

-- Insert sample orders
INSERT INTO orders (customer_id, status, total_amount, shipping_address, billing_address, notes) VALUES
(1, 'delivered', 999.99, '{"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}', '{"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}', 'Express delivery'),
(2, 'shipped', 1199.99, '{"street": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "zip": "90210"}', '{"street": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "zip": "90210"}', 'Gift wrapping requested'),
(3, 'processing', 549.98, '{"street": "789 Pine Rd", "city": "Chicago", "state": "IL", "zip": "60601"}', '{"street": "789 Pine Rd", "city": "Chicago", "state": "IL", "zip": "60601"}', 'Bulk order discount applied'),
(4, 'pending', 162.98, '{"street": "321 Elm St", "city": "Houston", "state": "TX", "zip": "77001"}', '{"street": "321 Elm St", "city": "Houston", "state": "TX", "zip": "77001"}', 'Customer requested hold'),
(5, 'delivered', 399.99, '{"street": "654 Maple Dr", "city": "Phoenix", "state": "AZ", "zip": "85001"}', '{"street": "654 Maple Dr", "city": "Phoenix", "state": "AZ", "zip": "85001"}', 'No special instructions');

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 999.99),
(2, 2, 1, 1199.99),
(3, 3, 1, 399.99),
(3, 4, 1, 150.00),
(4, 5, 1, 12.99),
(4, 6, 3, 49.99),
(5, 3, 1, 399.99);

-- Insert sample reviews
INSERT INTO reviews (product_id, customer_id, rating, title, comment, is_verified, helpful_votes) VALUES
(1, 1, 5, 'Amazing phone!', 'The camera quality is outstanding and the battery life is great.', TRUE, 12),
(2, 2, 4, 'Great laptop', 'Very fast and lightweight. Perfect for work and travel.', TRUE, 8),
(3, 5, 5, 'Best headphones ever', 'The noise cancellation is incredible. Worth every penny.', TRUE, 15),
(4, 3, 4, 'Comfortable shoes', 'Great for running and daily wear. Good value for money.', TRUE, 6),
(5, 4, 5, 'Classic literature', 'A timeless masterpiece. Every home should have this book.', FALSE, 3),
(6, 4, 4, 'Comprehensive guide', 'Very detailed and well-written. Great for beginners and advanced users.', FALSE, 7);

-- =============================================
-- NORMALIZED METADATA STORAGE SCHEMA
-- =============================================

-- Create dsa_production schema for metadata storage
CREATE SCHEMA IF NOT EXISTS dsa_production;

-- Set search path to include dsa_production schema
SET search_path TO dsa_production, dsa_ecommerce, public;

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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    UNIQUE(sync_id)
);

-- Table quality metrics (unchanged)
CREATE TABLE table_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES quality_metrics_runs(run_id) ON DELETE CASCADE,
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

-- Credentials table (for storing connection credentials)
CREATE TABLE credentials (
    credential_id SERIAL PRIMARY KEY,
    connection_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    password TEXT,
    password_encrypted TEXT,
    ssl_mode VARCHAR(20) DEFAULT 'prefer',
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(connection_id, source_type)
);

-- Comment on credentials table
COMMENT ON TABLE credentials IS 'Stores database connection credentials';
COMMENT ON COLUMN credentials.connection_id IS 'Unique identifier for the connection';
COMMENT ON COLUMN credentials.source_type IS 'Type of database (postgresql, mysql, etc.)';

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

-- Create trigger for credentials table
CREATE TRIGGER update_credentials_updated_at 
    BEFORE UPDATE ON credentials 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default test credentials (password will be encrypted by application)
INSERT INTO credentials (connection_id, source_type, host, port, database_name, username, password_encrypted, description) 
VALUES ('test', 'postgresql', 'localhost', 5432, 'postgres', 'rohit.satyainti', 'PLACEHOLDER_ENCRYPTED_PASSWORD', 'Default test connection');

-- =============================================
-- VIEWS FOR LATEST METADATA AND QUALITY METRICS
-- =============================================

-- View for latest schema metadata
CREATE OR REPLACE VIEW latest_schema_metadata AS
SELECT 
    ns.type_name,
    ns.status,
    ns.name,
    ns.connection_name,
    ns.tenant_id,
    ns.last_sync_run,
    ns.last_sync_run_at,
    ns.connector_name,
    ns.attributes,
    ns.custom_attributes,
    sr.sync_timestamp,
    sr.status as sync_status
FROM normalized_schemas ns
JOIN sync_runs sr ON ns.sync_id = sr.sync_id
WHERE sr.sync_timestamp = (
    SELECT MAX(sync_timestamp) 
    FROM sync_runs 
    WHERE status = 'completed'
);

-- View for latest table metadata
CREATE OR REPLACE VIEW latest_table_metadata AS
SELECT 
    nt.type_name,
    nt.status,
    nt.name,
    nt.connection_name,
    nt.tenant_id,
    nt.last_sync_run,
    nt.last_sync_run_at,
    nt.connector_name,
    nt.attributes,
    nt.custom_attributes,
    sr.sync_timestamp,
    sr.status as sync_status,
    -- Extract schema name from attributes
    nt.attributes->>'schemaName' as schema_name
FROM normalized_tables nt
JOIN sync_runs sr ON nt.sync_id = sr.sync_id
WHERE sr.sync_timestamp = (
    SELECT MAX(sync_timestamp) 
    FROM sync_runs 
    WHERE status = 'completed'
);

-- View for latest column metadata
CREATE OR REPLACE VIEW latest_column_metadata AS
SELECT 
    nc.type_name,
    nc.status,
    nc.name,
    nc.connection_name,
    nc.tenant_id,
    nc.last_sync_run,
    nc.last_sync_run_at,
    nc.connector_name,
    nc.attributes,
    nc.custom_attributes,
    sr.sync_timestamp,
    sr.status as sync_status,
    -- Extract schema and table names from attributes
    nc.attributes->>'schemaName' as schema_name,
    nc.attributes->>'tableName' as table_name
FROM normalized_columns nc
JOIN sync_runs sr ON nc.sync_id = sr.sync_id
WHERE sr.sync_timestamp = (
    SELECT MAX(sync_timestamp) 
    FROM sync_runs 
    WHERE status = 'completed'
);

-- View for latest quality metrics summary
CREATE OR REPLACE VIEW latest_quality_metrics_summary AS
SELECT 
    qmr.sync_id,
    qmr.extraction_timestamp,
    qmr.target_schemas,
    qmr.total_tables,
    qmr.total_columns,
    qmr.extraction_duration_seconds,
    qmr.status as metrics_status,
    sr.connector_name,
    sr.connection_name,
    sr.tenant_id
FROM quality_metrics_runs qmr
JOIN sync_runs sr ON qmr.sync_id = sr.sync_id
WHERE qmr.extraction_timestamp = (
    SELECT MAX(extraction_timestamp) 
    FROM quality_metrics_runs 
    WHERE status = 'completed'
);

-- View for latest table quality metrics
CREATE OR REPLACE VIEW latest_table_quality_metrics AS
SELECT 
    tqm.schema_name,
    tqm.table_name,
    tqm.row_count,
    tqm.created_at,
    qmr.sync_id,
    qmr.extraction_timestamp,
    sr.connector_name,
    sr.connection_name
FROM table_quality_metrics tqm
JOIN quality_metrics_runs qmr ON tqm.run_id = qmr.run_id
JOIN sync_runs sr ON qmr.sync_id = sr.sync_id
WHERE qmr.extraction_timestamp = (
    SELECT MAX(extraction_timestamp) 
    FROM quality_metrics_runs 
    WHERE status = 'completed'
);

-- View for latest column quality metrics
CREATE OR REPLACE VIEW latest_column_quality_metrics AS
SELECT 
    cqm.schema_name,
    cqm.table_name,
    cqm.column_name,
    cqm.total_count,
    cqm.non_null_count,
    cqm.null_count,
    cqm.null_percentage,
    cqm.distinct_count,
    cqm.distinct_percentage,
    cqm.created_at,
    qmr.sync_id,
    qmr.extraction_timestamp,
    sr.connector_name,
    sr.connection_name
FROM column_quality_metrics cqm
JOIN quality_metrics_runs qmr ON cqm.run_id = qmr.run_id
JOIN sync_runs sr ON qmr.sync_id = sr.sync_id
WHERE qmr.extraction_timestamp = (
    SELECT MAX(extraction_timestamp) 
    FROM quality_metrics_runs 
    WHERE status = 'completed'
);

-- View for latest column top values
CREATE OR REPLACE VIEW latest_column_top_values AS
SELECT 
    ctv.value_text,
    ctv.frequency,
    ctv.percentage,
    ctv.created_at,
    cqm.schema_name,
    cqm.table_name,
    cqm.column_name,
    qmr.sync_id,
    qmr.extraction_timestamp,
    sr.connector_name,
    sr.connection_name
FROM column_top_values ctv
JOIN column_quality_metrics cqm ON ctv.metric_id = cqm.metric_id
JOIN quality_metrics_runs qmr ON cqm.run_id = qmr.run_id
JOIN sync_runs sr ON qmr.sync_id = sr.sync_id
WHERE qmr.extraction_timestamp = (
    SELECT MAX(extraction_timestamp) 
    FROM quality_metrics_runs 
    WHERE status = 'completed'
);

-- View for active credentials
CREATE OR REPLACE VIEW active_credentials AS
SELECT 
    credential_id,
    connection_id,
    source_type,
    host,
    port,
    database_name,
    username,
    ssl_mode,
    description,
    created_at,
    updated_at
FROM credentials
WHERE is_active = TRUE
ORDER BY created_at DESC;

-- View for sync run history
CREATE OR REPLACE VIEW sync_run_history AS
SELECT 
    sync_id,
    sync_timestamp,
    connector_name,
    connection_name,
    tenant_id,
    status,
    error_message,
    created_at,
    -- Count related records
    (SELECT COUNT(*) FROM normalized_schemas WHERE sync_id = sr.sync_id) as schema_count,
    (SELECT COUNT(*) FROM normalized_tables WHERE sync_id = sr.sync_id) as table_count,
    (SELECT COUNT(*) FROM normalized_columns WHERE sync_id = sr.sync_id) as column_count
FROM sync_runs sr
ORDER BY sync_timestamp DESC;

-- Comments on views
COMMENT ON VIEW latest_schema_metadata IS 'Latest schema metadata from the most recent successful sync';
COMMENT ON VIEW latest_table_metadata IS 'Latest table metadata from the most recent successful sync';
COMMENT ON VIEW latest_column_metadata IS 'Latest column metadata from the most recent successful sync';
COMMENT ON VIEW latest_quality_metrics_summary IS 'Summary of the latest quality metrics extraction';
COMMENT ON VIEW latest_table_quality_metrics IS 'Latest table quality metrics from the most recent successful extraction';
COMMENT ON VIEW latest_column_quality_metrics IS 'Latest column quality metrics from the most recent successful extraction';
COMMENT ON VIEW latest_column_top_values IS 'Latest column top values from the most recent successful extraction';
COMMENT ON VIEW active_credentials IS 'Currently active database connection credentials';
COMMENT ON VIEW sync_run_history IS 'History of all sync runs with record counts';

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

-- Display completion message
SELECT 'Sample schema and normalized metadata structure created successfully!' as status;