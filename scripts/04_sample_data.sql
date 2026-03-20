-- =============================================================================
-- SAMPLE DATA & DEMONSTRATION
-- Populate Bronze layer with realistic CDC data for agent processing
-- =============================================================================

USE DATABASE AGENTIC_PIPELINE;

-- Insert sample CDC data into Bronze (simulating Openflow PostgreSQL CDC)
INSERT INTO BRONZE.RAW_CUSTOMERS (raw_payload) 
SELECT PARSE_JSON(column1) FROM VALUES
('{"id": "C001", "first_name": "John", "last_name": "Smith", "email": "john.smith@example.com", "created_at": "2024-01-15T10:30:00Z", "segment": "enterprise", "annual_revenue": 1500000}'),
('{"id": "C002", "first_name": "Sarah", "last_name": "Johnson", "email": "sarah.j@company.net", "created_at": "2024-02-20T14:45:00Z", "segment": "mid-market", "annual_revenue": 450000}'),
('{"id": "C003", "first_name": "Mike", "last_name": "Williams", "email": "mwilliams@enterprise.org", "created_at": "2024-03-10T09:15:00Z", "segment": "enterprise", "annual_revenue": 2800000}'),
('{"id": "C004", "first_name": null, "last_name": "Davis", "email": "incomplete@test.com", "created_at": "2024-03-12T11:00:00Z", "segment": "smb", "annual_revenue": null}')
AS t;

INSERT INTO BRONZE.RAW_ORDERS (raw_payload)
SELECT PARSE_JSON(column1) FROM VALUES
('{"order_id": "ORD-10001", "customer_id": "C001", "order_date": "2024-06-15", "total_amount": 15000.00, "status": "completed", "items": [{"sku": "PRD-A1", "qty": 10, "price": 1500}]}'),
('{"order_id": "ORD-10002", "customer_id": "C002", "order_date": "2024-06-16", "total_amount": 8500.50, "status": "pending", "items": [{"sku": "PRD-B2", "qty": 5, "price": 1700}]}'),
('{"order_id": "ORD-10003", "customer_id": "C001", "order_date": "2024-06-17", "total_amount": 22000.00, "status": "completed", "items": [{"sku": "PRD-C3", "qty": 20, "price": 1100}]}'),
('{"order_id": "ORD-10004", "customer_id": "C003", "order_date": "2024-06-18", "total_amount": 45000.00, "status": "shipped", "items": [{"sku": "PRD-A1", "qty": 30, "price": 1500}]}')
AS t;

INSERT INTO BRONZE.RAW_PRODUCTS (raw_payload)
SELECT PARSE_JSON(column1) FROM VALUES
('{"product_id": "PRD-A1", "name": "Enterprise Suite", "category": "Software", "price": 1500.00, "active": true, "created_date": "2023-01-01"}'),
('{"product_id": "PRD-B2", "name": "Analytics Platform", "category": "Software", "price": 1700.00, "active": true, "created_date": "2023-03-15"}'),
('{"product_id": "PRD-C3", "name": "Data Connector Pack", "category": "Integration", "price": 1100.00, "active": true, "created_date": "2023-06-20"}')
AS t;

-- Verify data
SELECT 'BRONZE.RAW_CUSTOMERS' as table_name, COUNT(*) as row_count FROM BRONZE.RAW_CUSTOMERS
UNION ALL
SELECT 'BRONZE.RAW_ORDERS', COUNT(*) FROM BRONZE.RAW_ORDERS
UNION ALL
SELECT 'BRONZE.RAW_PRODUCTS', COUNT(*) FROM BRONZE.RAW_PRODUCTS;
