-- ============================================================================
-- 01_SOURCE: Seed Data for PostgreSQL Source
-- ============================================================================
-- Purpose: Create tables and seed initial data in SOURCE PostgreSQL
-- Run via: psql -h <SOURCE_HOST> -U snowflake_admin -d postgres
-- ============================================================================

-- Create the application database
CREATE DATABASE IF NOT EXISTS dbaontap;
\c dbaontap

-- ============================================================================
-- CUSTOMERS TABLE
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

-- ============================================================================
-- PRODUCTS TABLE
-- ============================================================================
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

-- ============================================================================
-- ORDERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(12,2),
    shipping_address TEXT,
    notes TEXT
);

-- ============================================================================
-- ORDER_ITEMS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0
);

-- ============================================================================
-- SUPPORT_TICKETS TABLE
-- ============================================================================
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
-- SEED DATA: CUSTOMERS
-- ============================================================================
INSERT INTO customers (first_name, last_name, email, phone, company_name, industry) VALUES
('John', 'Smith', 'john.smith@acme.com', '555-0101', 'Acme Corp', 'Technology'),
('Sarah', 'Johnson', 'sarah.j@globaltech.io', '555-0102', 'GlobalTech', 'Software'),
('Mike', 'Williams', 'mike.w@enterprise.com', '555-0103', 'Enterprise Inc', 'Finance'),
('Emily', 'Brown', 'emily.b@startup.io', '555-0104', 'Startup Labs', 'Healthcare'),
('David', 'Lee', 'david.lee@bigcorp.com', '555-0105', 'BigCorp', 'Manufacturing'),
('Lisa', 'Chen', 'lisa.chen@innovate.co', '555-0106', 'Innovate Co', 'Retail'),
('James', 'Taylor', 'james.t@solutions.net', '555-0107', 'Solutions Ltd', 'Consulting'),
('Anna', 'Wilson', 'anna.w@techstart.io', '555-0108', 'TechStart', 'Technology'),
('Robert', 'Martinez', 'robert.m@dataflow.com', '555-0109', 'DataFlow', 'Analytics'),
('Jennifer', 'Garcia', 'jen.g@cloudnine.io', '555-0110', 'CloudNine', 'Cloud Services')
ON CONFLICT (email) DO NOTHING;

-- ============================================================================
-- SEED DATA: PRODUCTS
-- ============================================================================
INSERT INTO products (product_name, category, price, cost, sku, active) VALUES
('Enterprise License', 'Software', 50000.00, 5000.00, 'ENT-001', TRUE),
('Professional License', 'Software', 15000.00, 1500.00, 'PRO-001', TRUE),
('Starter License', 'Software', 5000.00, 500.00, 'STR-001', TRUE),
('Premium Support', 'Services', 25000.00, 10000.00, 'SUP-PRM', TRUE),
('Standard Support', 'Services', 10000.00, 4000.00, 'SUP-STD', TRUE),
('Training Package', 'Services', 7500.00, 3000.00, 'TRN-001', TRUE)
ON CONFLICT (sku) DO NOTHING;

-- ============================================================================
-- SEED DATA: ORDERS
-- ============================================================================
INSERT INTO orders (customer_id, order_date, status, total_amount, shipping_address) VALUES
(1, NOW() - INTERVAL '30 days', 'completed', 75000.00, '123 Main St, City, ST 12345'),
(1, NOW() - INTERVAL '15 days', 'completed', 15000.00, '123 Main St, City, ST 12345'),
(2, NOW() - INTERVAL '25 days', 'completed', 50000.00, '456 Oak Ave, Town, ST 23456'),
(3, NOW() - INTERVAL '20 days', 'completed', 100000.00, '789 Pine Rd, Village, ST 34567'),
(3, NOW() - INTERVAL '5 days', 'shipped', 25000.00, '789 Pine Rd, Village, ST 34567'),
(4, NOW() - INTERVAL '10 days', 'processing', 22500.00, '321 Elm St, Metro, ST 45678'),
(5, NOW() - INTERVAL '8 days', 'completed', 12000.00, '654 Maple Dr, Urban, ST 56789'),
(6, NOW() - INTERVAL '3 days', 'pending', 62000.00, '987 Cedar Ln, Suburb, ST 67890'),
(7, NOW() - INTERVAL '2 days', 'processing', 5000.00, '147 Birch Way, County, ST 78901'),
(8, NOW() - INTERVAL '1 day', 'pending', 33500.00, '258 Spruce Ct, District, ST 89012'),
(1, NOW(), 'pending', 999.99, '123 Main St, City, ST 12345');

-- ============================================================================
-- SEED DATA: ORDER_ITEMS
-- ============================================================================
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent) VALUES
(1, 1, 1, 50000.00, 0),
(1, 4, 1, 25000.00, 0),
(2, 2, 1, 15000.00, 0),
(3, 1, 1, 50000.00, 0),
(4, 1, 2, 50000.00, 0),
(5, 4, 1, 25000.00, 0),
(6, 2, 1, 15000.00, 0),
(6, 6, 1, 7500.00, 0),
(7, 3, 2, 5000.00, 0),
(7, 5, 1, 2000.00, 80),
(8, 1, 1, 50000.00, 0),
(8, 5, 1, 10000.00, 0),
(9, 3, 1, 5000.00, 0),
(10, 2, 1, 15000.00, 0),
(10, 4, 1, 25000.00, 0);

-- ============================================================================
-- SEED DATA: SUPPORT_TICKETS
-- ============================================================================
INSERT INTO support_tickets (customer_id, subject, description, priority, status) VALUES
(1, 'License activation issue', 'Unable to activate new license key', 'high', 'resolved'),
(2, 'Performance questions', 'Questions about scaling the platform', 'medium', 'open'),
(3, 'Billing inquiry', 'Need invoice copy for Q3', 'low', 'resolved'),
(4, 'Feature request', 'Would like to see API improvements', 'medium', 'open'),
(5, 'Training scheduling', 'Need to reschedule training session', 'low', 'open');

-- ============================================================================
-- Enable logical replication (run as superuser)
-- ============================================================================
-- ALTER SYSTEM SET wal_level = 'logical';
-- SELECT pg_reload_conf();

-- Create publication for all tables
CREATE PUBLICATION dbaontap_pub FOR ALL TABLES;

-- Verify publication
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;
