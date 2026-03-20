-- ============================================================================
-- 02_LANDING: Create Tables in Landing PostgreSQL
-- ============================================================================
-- Purpose: Create matching table structures in LANDING for replication
-- Run via: psql -h <LANDING_HOST> -U snowflake_admin -d postgres
-- ============================================================================

-- Create the landing database
CREATE DATABASE IF NOT EXISTS dbaontap;
\c dbaontap

-- ============================================================================
-- Mirror table structures from SOURCE (without data)
-- ============================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    company_name VARCHAR(200),
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    sku VARCHAR(50) UNIQUE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(12,2),
    shipping_address TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    subject VARCHAR(500),
    description TEXT,
    priority VARCHAR(20) DEFAULT 'medium',
    status VARCHAR(50) DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

-- ============================================================================
-- Create subscription to SOURCE (run after tables exist)
-- ============================================================================
-- Replace <SOURCE_HOST> with actual hostname from SHOW POSTGRES INSTANCES

-- CREATE SUBSCRIPTION dbaontap_sub
--     CONNECTION 'host=<SOURCE_HOST> port=5432 dbname=dbaontap user=snowflake_admin password=<PASSWORD>'
--     PUBLICATION dbaontap_pub
--     WITH (copy_data = true);

-- Verify subscription
-- SELECT * FROM pg_subscription;
-- SELECT * FROM pg_stat_subscription;
