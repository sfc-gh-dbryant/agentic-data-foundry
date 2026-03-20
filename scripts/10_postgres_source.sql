-- =============================================================================
-- SNOWFLAKE POSTGRES TRANSACTIONAL LAYER
-- Source of truth for OLTP workloads, CDC replication to Bronze
-- =============================================================================

-- ============================================================================
-- PART 1: CREATE SNOWFLAKE POSTGRES INSTANCE
-- ============================================================================

-- Create the Postgres instance (run via snow CLI or Snowsight)
-- snow postgres instance create techmart-oltp --database TECHMART_POSTGRES

-- Or via SQL (Preview feature)
-- CREATE POSTGRES INSTANCE techmart_oltp
--     WAREHOUSE = SNOWADHOC
--     AUTO_SUSPEND = 300;

-- ============================================================================
-- PART 2: APPLICATION SCHEMA (Run in Postgres)
-- These tables represent the transactional system
-- ============================================================================

-- Connect to Postgres instance and create schema
-- psql -h <instance-host> -U admin -d postgres

/*
-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    segment VARCHAR(50) CHECK (segment IN ('enterprise', 'mid-market', 'smb')),
    annual_revenue DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    list_price DECIMAL(12,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE DEFAULT CURRENT_DATE,
    total_amount DECIMAL(12,2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12,2),
    line_total DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- Customer insights table (for AI feedback loop)
CREATE TABLE customer_insights (
    insight_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    insight_type VARCHAR(50),
    recommendation TEXT,
    confidence_score DECIMAL(5,4),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actioned_at TIMESTAMP,
    actioned_by VARCHAR(100)
);

-- Create indexes for performance
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_insights_customer ON customer_insights(customer_id);

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = 'logical';
*/

-- ============================================================================
-- PART 3: SAMPLE TRANSACTIONAL DATA (Run in Postgres)
-- ============================================================================

/*
-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, company_name, segment, annual_revenue) VALUES
('John', 'Smith', 'john.smith@acme.com', 'Acme Corporation', 'enterprise', 2500000.00),
('Sarah', 'Johnson', 'sarah.j@techstart.io', 'TechStart Inc', 'mid-market', 450000.00),
('Mike', 'Williams', 'mike.w@globalcorp.com', 'Global Corp', 'enterprise', 5800000.00),
('Emily', 'Brown', 'emily@smallbiz.net', 'Small Biz LLC', 'smb', 85000.00),
('David', 'Lee', 'david.lee@innovate.co', 'Innovate Co', 'mid-market', 320000.00);

-- Insert sample products
INSERT INTO products (sku, name, category, list_price) VALUES
('ENT-SUITE-001', 'Enterprise Suite', 'Software', 15000.00),
('ANL-PLAT-001', 'Analytics Platform', 'Software', 8500.00),
('INT-CONN-001', 'Integration Connectors', 'Integration', 3500.00),
('SUP-PREM-001', 'Premium Support', 'Services', 5000.00);

-- Insert sample orders
INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
(1, '2024-06-15', 45000.00, 'completed'),
(2, '2024-06-16', 8500.00, 'completed'),
(1, '2024-06-20', 20000.00, 'completed'),
(3, '2024-06-22', 75000.00, 'shipped'),
(4, '2024-06-25', 3500.00, 'pending');

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 3, 15000.00),
(2, 2, 1, 8500.00),
(3, 3, 4, 3500.00),
(3, 4, 1, 5000.00),
(4, 1, 5, 15000.00),
(5, 3, 1, 3500.00);
*/

-- ============================================================================
-- PART 4: OPENFLOW CDC CONFIGURATION
-- Configure CDC replication from Postgres to Bronze
-- ============================================================================

-- This would be configured in Openflow UI or via API
-- Example Openflow connector configuration (conceptual):

/*
{
  "connector_type": "postgresql_cdc",
  "source": {
    "host": "<snowflake-postgres-host>",
    "port": 5432,
    "database": "postgres",
    "schema": "public",
    "tables": ["customers", "orders", "products", "order_items"],
    "slot_name": "techmart_cdc_slot"
  },
  "destination": {
    "database": "AGENTIC_PIPELINE",
    "schema": "BRONZE",
    "table_prefix": "RAW_",
    "format": "VARIANT"
  },
  "settings": {
    "snapshot_mode": "initial",
    "include_schema_changes": true,
    "heartbeat_interval": 60000
  }
}
*/

-- ============================================================================
-- PART 5: BRONZE TABLES FOR CDC DATA
-- Landing zone for Openflow CDC output
-- ============================================================================

USE DATABASE AGENTIC_PIPELINE;

-- Enhanced Bronze tables with CDC metadata
CREATE OR REPLACE TABLE BRONZE.RAW_PG_CUSTOMERS (
    _cdc_operation VARCHAR,           -- 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    _cdc_timestamp TIMESTAMP_LTZ,     -- When change occurred in source
    _cdc_source VARCHAR,              -- Source table identifier
    _cdc_lsn VARCHAR,                 -- Log sequence number for ordering
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT               -- Full row as JSON
);

CREATE OR REPLACE TABLE BRONZE.RAW_PG_ORDERS (
    _cdc_operation VARCHAR,
    _cdc_timestamp TIMESTAMP_LTZ,
    _cdc_source VARCHAR,
    _cdc_lsn VARCHAR,
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE.RAW_PG_PRODUCTS (
    _cdc_operation VARCHAR,
    _cdc_timestamp TIMESTAMP_LTZ,
    _cdc_source VARCHAR,
    _cdc_lsn VARCHAR,
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE.RAW_PG_ORDER_ITEMS (
    _cdc_operation VARCHAR,
    _cdc_timestamp TIMESTAMP_LTZ,
    _cdc_source VARCHAR,
    _cdc_lsn VARCHAR,
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

-- ============================================================================
-- PART 6: SIMULATE CDC DATA (For demo without actual Postgres)
-- ============================================================================

-- Simulate CDC inserts from Postgres
INSERT INTO BRONZE.RAW_PG_CUSTOMERS (_cdc_operation, _cdc_timestamp, _cdc_source, _cdc_lsn, raw_payload)
SELECT 'c', CURRENT_TIMESTAMP(), 'postgres.public.customers', '0/1234' || ROW_NUMBER() OVER(), PARSE_JSON(column1)
FROM VALUES
('{"customer_id": 1, "first_name": "John", "last_name": "Smith", "email": "john.smith@acme.com", "company_name": "Acme Corporation", "segment": "enterprise", "annual_revenue": 2500000.00, "created_at": "2024-01-15T10:30:00Z", "is_active": true}'),
('{"customer_id": 2, "first_name": "Sarah", "last_name": "Johnson", "email": "sarah.j@techstart.io", "company_name": "TechStart Inc", "segment": "mid-market", "annual_revenue": 450000.00, "created_at": "2024-02-20T14:45:00Z", "is_active": true}'),
('{"customer_id": 3, "first_name": "Mike", "last_name": "Williams", "email": "mike.w@globalcorp.com", "company_name": "Global Corp", "segment": "enterprise", "annual_revenue": 5800000.00, "created_at": "2024-03-10T09:15:00Z", "is_active": true}'),
('{"customer_id": 4, "first_name": "Emily", "last_name": "Brown", "email": "emily@smallbiz.net", "company_name": "Small Biz LLC", "segment": "smb", "annual_revenue": 85000.00, "created_at": "2024-03-12T11:00:00Z", "is_active": true}'),
('{"customer_id": 5, "first_name": "David", "last_name": "Lee", "email": "david.lee@innovate.co", "company_name": "Innovate Co", "segment": "mid-market", "annual_revenue": 320000.00, "created_at": "2024-04-05T16:20:00Z", "is_active": true}')
AS t;

INSERT INTO BRONZE.RAW_PG_PRODUCTS (_cdc_operation, _cdc_timestamp, _cdc_source, _cdc_lsn, raw_payload)
SELECT 'c', CURRENT_TIMESTAMP(), 'postgres.public.products', '0/2234' || ROW_NUMBER() OVER(), PARSE_JSON(column1)
FROM VALUES
('{"product_id": 1, "sku": "ENT-SUITE-001", "name": "Enterprise Suite", "category": "Software", "list_price": 15000.00, "is_active": true}'),
('{"product_id": 2, "sku": "ANL-PLAT-001", "name": "Analytics Platform", "category": "Software", "list_price": 8500.00, "is_active": true}'),
('{"product_id": 3, "sku": "INT-CONN-001", "name": "Integration Connectors", "category": "Integration", "list_price": 3500.00, "is_active": true}'),
('{"product_id": 4, "sku": "SUP-PREM-001", "name": "Premium Support", "category": "Services", "list_price": 5000.00, "is_active": true}')
AS t;

INSERT INTO BRONZE.RAW_PG_ORDERS (_cdc_operation, _cdc_timestamp, _cdc_source, _cdc_lsn, raw_payload)
SELECT 'c', CURRENT_TIMESTAMP(), 'postgres.public.orders', '0/3234' || ROW_NUMBER() OVER(), PARSE_JSON(column1)
FROM VALUES
('{"order_id": 1, "customer_id": 1, "order_date": "2024-06-15", "total_amount": 45000.00, "status": "completed", "created_at": "2024-06-15T09:00:00Z"}'),
('{"order_id": 2, "customer_id": 2, "order_date": "2024-06-16", "total_amount": 8500.00, "status": "completed", "created_at": "2024-06-16T11:30:00Z"}'),
('{"order_id": 3, "customer_id": 1, "order_date": "2024-06-20", "total_amount": 20000.00, "status": "completed", "created_at": "2024-06-20T14:15:00Z"}'),
('{"order_id": 4, "customer_id": 3, "order_date": "2024-06-22", "total_amount": 75000.00, "status": "shipped", "created_at": "2024-06-22T10:45:00Z"}'),
('{"order_id": 5, "customer_id": 4, "order_date": "2024-06-25", "total_amount": 3500.00, "status": "pending", "created_at": "2024-06-25T16:00:00Z"}')
AS t;

INSERT INTO BRONZE.RAW_PG_ORDER_ITEMS (_cdc_operation, _cdc_timestamp, _cdc_source, _cdc_lsn, raw_payload)
SELECT 'c', CURRENT_TIMESTAMP(), 'postgres.public.order_items', '0/4234' || ROW_NUMBER() OVER(), PARSE_JSON(column1)
FROM VALUES
('{"item_id": 1, "order_id": 1, "product_id": 1, "quantity": 3, "unit_price": 15000.00}'),
('{"item_id": 2, "order_id": 2, "product_id": 2, "quantity": 1, "unit_price": 8500.00}'),
('{"item_id": 3, "order_id": 3, "product_id": 3, "quantity": 4, "unit_price": 3500.00}'),
('{"item_id": 4, "order_id": 3, "product_id": 4, "quantity": 1, "unit_price": 5000.00}'),
('{"item_id": 5, "order_id": 4, "product_id": 1, "quantity": 5, "unit_price": 15000.00}'),
('{"item_id": 6, "order_id": 5, "product_id": 3, "quantity": 1, "unit_price": 3500.00}')
AS t;

-- Verify CDC data
SELECT 'RAW_PG_CUSTOMERS' as table_name, COUNT(*) as row_count FROM BRONZE.RAW_PG_CUSTOMERS
UNION ALL SELECT 'RAW_PG_ORDERS', COUNT(*) FROM BRONZE.RAW_PG_ORDERS
UNION ALL SELECT 'RAW_PG_PRODUCTS', COUNT(*) FROM BRONZE.RAW_PG_PRODUCTS
UNION ALL SELECT 'RAW_PG_ORDER_ITEMS', COUNT(*) FROM BRONZE.RAW_PG_ORDER_ITEMS;
