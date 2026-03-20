-- =============================================================================
-- AGENTIC WORKFLOW: End-to-End Demonstration
-- Shows how the agent discovers, analyzes, and transforms Bronze → Silver → Gold
-- =============================================================================

-- STEP 1: DISCOVERY PHASE
-- Agent discovers all Bronze tables and their structures
-- ============================================================================

-- List Bronze tables for discovery
SHOW TABLES IN SCHEMA AGENTIC_PIPELINE.BRONZE;

-- Agent uses DISCOVER_SCHEMA tool
SELECT * FROM TABLE(AGENTIC_PIPELINE.AGENTS.DISCOVER_SCHEMA('AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS'));

-- Agent uses CORTEX to infer optimal schema
CALL AGENTIC_PIPELINE.AGENTS.CORTEX_INFER_SCHEMA('AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS');


-- STEP 2: ANALYSIS PHASE
-- Agent analyzes data quality issues
-- ============================================================================

-- Identify nulls and type issues
SELECT 
    'CUSTOMERS' as entity,
    COUNT(*) as total_rows,
    COUNT_IF(raw_payload:first_name IS NULL) as null_first_names,
    COUNT_IF(raw_payload:annual_revenue IS NULL) as null_revenue,
    COUNT(DISTINCT raw_payload:segment::VARCHAR) as segment_cardinality
FROM AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS;


-- STEP 3: TRANSFORMATION PHASE
-- Agent generates and executes Dynamic Table transformations
-- ============================================================================

-- SILVER: Customers (flattened, typed, cleansed)
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.SILVER.CUSTOMERS
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    _metadata_load_ts as ingested_at,
    _metadata_source as source_system,
    raw_payload:id::VARCHAR as customer_id,
    COALESCE(raw_payload:first_name::VARCHAR, 'Unknown') as first_name,
    raw_payload:last_name::VARCHAR as last_name,
    raw_payload:email::VARCHAR as email,
    TRY_TO_TIMESTAMP(raw_payload:created_at::VARCHAR) as created_at,
    UPPER(raw_payload:segment::VARCHAR) as segment,
    COALESCE(raw_payload:annual_revenue::NUMBER, 0) as annual_revenue,
    CASE 
        WHEN raw_payload:annual_revenue::NUMBER >= 1000000 THEN 'ENTERPRISE'
        WHEN raw_payload:annual_revenue::NUMBER >= 100000 THEN 'MID-MARKET'
        ELSE 'SMB'
    END as revenue_tier
FROM AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS
WHERE raw_payload:id IS NOT NULL;

-- SILVER: Orders (flattened with nested array handling)
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.SILVER.ORDERS
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    _metadata_load_ts as ingested_at,
    raw_payload:order_id::VARCHAR as order_id,
    raw_payload:customer_id::VARCHAR as customer_id,
    TRY_TO_DATE(raw_payload:order_date::VARCHAR) as order_date,
    raw_payload:total_amount::NUMBER(12,2) as total_amount,
    UPPER(raw_payload:status::VARCHAR) as status,
    ARRAY_SIZE(raw_payload:items) as line_item_count
FROM AGENTIC_PIPELINE.BRONZE.RAW_ORDERS
WHERE raw_payload:order_id IS NOT NULL;

-- SILVER: Order Line Items (exploded from nested array)
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.SILVER.ORDER_ITEMS
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    raw_payload:order_id::VARCHAR as order_id,
    item.value:sku::VARCHAR as product_sku,
    item.value:qty::NUMBER as quantity,
    item.value:price::NUMBER(12,2) as unit_price,
    item.value:qty::NUMBER * item.value:price::NUMBER(12,2) as line_total
FROM AGENTIC_PIPELINE.BRONZE.RAW_ORDERS,
LATERAL FLATTEN(input => raw_payload:items) item;

-- SILVER: Products
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.SILVER.PRODUCTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    _metadata_load_ts as ingested_at,
    raw_payload:product_id::VARCHAR as product_id,
    raw_payload:name::VARCHAR as product_name,
    raw_payload:category::VARCHAR as category,
    raw_payload:price::NUMBER(12,2) as list_price,
    raw_payload:active::BOOLEAN as is_active
FROM AGENTIC_PIPELINE.BRONZE.RAW_PRODUCTS;


-- STEP 4: GOLD LAYER
-- Business-ready aggregations and dimensional models
-- ============================================================================

-- GOLD: Customer 360 View
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.GOLD.CUSTOMER_360
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name as full_name,
    c.email,
    c.segment,
    c.revenue_tier,
    c.annual_revenue,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    AVG(o.total_amount) as avg_order_value
FROM AGENTIC_PIPELINE.SILVER.CUSTOMERS c
LEFT JOIN AGENTIC_PIPELINE.SILVER.ORDERS o ON c.customer_id = o.customer_id
GROUP BY 1,2,3,4,5,6;

-- GOLD: Sales Summary
CREATE OR REPLACE DYNAMIC TABLE AGENTIC_PIPELINE.GOLD.SALES_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    o.order_date,
    c.segment,
    c.revenue_tier,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM AGENTIC_PIPELINE.SILVER.ORDERS o
JOIN AGENTIC_PIPELINE.SILVER.CUSTOMERS c ON o.customer_id = c.customer_id
GROUP BY 1,2,3;


-- STEP 5: LOG TRANSFORMATION METADATA
-- Record what the agent did for lineage and audit
-- ============================================================================

INSERT INTO AGENTIC_PIPELINE.METADATA.TRANSFORMATION_LOG 
    (source_table, target_table, transformation_sql, agent_reasoning, status)
VALUES
    ('BRONZE.RAW_CUSTOMERS', 'SILVER.CUSTOMERS', 
     'Dynamic Table with COALESCE for nulls, revenue_tier derivation',
     'Agent detected null first_names and annual_revenue. Applied defensive COALESCE. Added business logic for revenue_tier classification.',
     'SUCCESS'),
    ('BRONZE.RAW_ORDERS', 'SILVER.ORDERS', 
     'Dynamic Table with nested array extraction for line items',
     'Agent identified nested items array. Created separate ORDER_ITEMS table using LATERAL FLATTEN for normalization.',
     'SUCCESS'),
    ('SILVER.*', 'GOLD.CUSTOMER_360', 
     'Dynamic Table joining customers and orders for 360 view',
     'Agent generated business-ready customer analytics combining all Silver tables.',
     'SUCCESS');

-- View transformation lineage
SELECT * FROM AGENTIC_PIPELINE.METADATA.TRANSFORMATION_LOG ORDER BY created_at;
