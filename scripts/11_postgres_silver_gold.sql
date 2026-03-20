-- =============================================================================
-- AGENTIC SILVER: CDC-AWARE TRANSFORMATIONS
-- Dynamic Tables that handle CDC operations (insert, update, delete)
-- =============================================================================

USE DATABASE AGENTIC_PIPELINE;

-- ============================================================================
-- SILVER LAYER: CDC-AWARE DYNAMIC TABLES
-- These handle the full CDC lifecycle from Postgres
-- ============================================================================

-- SILVER: Customers (with CDC deduplication)
CREATE OR REPLACE DYNAMIC TABLE SILVER.PG_CUSTOMERS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWADHOC
AS
WITH ranked_changes AS (
    SELECT 
        raw_payload:customer_id::INTEGER as customer_id,
        raw_payload:first_name::VARCHAR as first_name,
        raw_payload:last_name::VARCHAR as last_name,
        raw_payload:email::VARCHAR as email,
        raw_payload:company_name::VARCHAR as company_name,
        UPPER(raw_payload:segment::VARCHAR) as segment,
        raw_payload:annual_revenue::NUMBER(15,2) as annual_revenue,
        CASE 
            WHEN raw_payload:annual_revenue >= 1000000 THEN 'ENTERPRISE'
            WHEN raw_payload:annual_revenue >= 100000 THEN 'MID-MARKET'
            ELSE 'SMB'
        END as revenue_tier,
        TRY_TO_TIMESTAMP(raw_payload:created_at::VARCHAR) as created_at,
        COALESCE(raw_payload:is_active::BOOLEAN, TRUE) as is_active,
        _cdc_operation,
        _cdc_timestamp,
        ROW_NUMBER() OVER (PARTITION BY raw_payload:customer_id ORDER BY _cdc_timestamp DESC, _cdc_lsn DESC) as rn
    FROM BRONZE.RAW_PG_CUSTOMERS
)
SELECT 
    customer_id,
    COALESCE(first_name, 'Unknown') as first_name,
    last_name,
    CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) as full_name,
    LOWER(email) as email,
    company_name,
    segment,
    annual_revenue,
    revenue_tier,
    created_at,
    is_active,
    _cdc_timestamp as last_updated
FROM ranked_changes
WHERE rn = 1 AND _cdc_operation != 'd';

-- SILVER: Products (with CDC deduplication)
CREATE OR REPLACE DYNAMIC TABLE SILVER.PG_PRODUCTS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWADHOC
AS
WITH ranked_changes AS (
    SELECT 
        raw_payload:product_id::INTEGER as product_id,
        raw_payload:sku::VARCHAR as sku,
        raw_payload:name::VARCHAR as product_name,
        raw_payload:category::VARCHAR as category,
        raw_payload:list_price::NUMBER(12,2) as list_price,
        COALESCE(raw_payload:is_active::BOOLEAN, TRUE) as is_active,
        _cdc_operation,
        _cdc_timestamp,
        ROW_NUMBER() OVER (PARTITION BY raw_payload:product_id ORDER BY _cdc_timestamp DESC, _cdc_lsn DESC) as rn
    FROM BRONZE.RAW_PG_PRODUCTS
)
SELECT 
    product_id,
    sku,
    product_name,
    category,
    list_price,
    is_active,
    _cdc_timestamp as last_updated
FROM ranked_changes
WHERE rn = 1 AND _cdc_operation != 'd';

-- SILVER: Orders (with CDC deduplication)
CREATE OR REPLACE DYNAMIC TABLE SILVER.PG_ORDERS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWADHOC
AS
WITH ranked_changes AS (
    SELECT 
        raw_payload:order_id::INTEGER as order_id,
        raw_payload:customer_id::INTEGER as customer_id,
        TRY_TO_DATE(raw_payload:order_date::VARCHAR) as order_date,
        raw_payload:total_amount::NUMBER(12,2) as total_amount,
        UPPER(raw_payload:status::VARCHAR) as status,
        TRY_TO_TIMESTAMP(raw_payload:created_at::VARCHAR) as created_at,
        _cdc_operation,
        _cdc_timestamp,
        ROW_NUMBER() OVER (PARTITION BY raw_payload:order_id ORDER BY _cdc_timestamp DESC, _cdc_lsn DESC) as rn
    FROM BRONZE.RAW_PG_ORDERS
)
SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    _cdc_timestamp as last_updated
FROM ranked_changes
WHERE rn = 1 AND _cdc_operation != 'd';

-- SILVER: Order Items (with CDC deduplication)
CREATE OR REPLACE DYNAMIC TABLE SILVER.PG_ORDER_ITEMS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWADHOC
AS
WITH ranked_changes AS (
    SELECT 
        raw_payload:item_id::INTEGER as item_id,
        raw_payload:order_id::INTEGER as order_id,
        raw_payload:product_id::INTEGER as product_id,
        raw_payload:quantity::INTEGER as quantity,
        raw_payload:unit_price::NUMBER(12,2) as unit_price,
        raw_payload:quantity::INTEGER * raw_payload:unit_price::NUMBER(12,2) as line_total,
        _cdc_operation,
        _cdc_timestamp,
        ROW_NUMBER() OVER (PARTITION BY raw_payload:item_id ORDER BY _cdc_timestamp DESC, _cdc_lsn DESC) as rn
    FROM BRONZE.RAW_PG_ORDER_ITEMS
)
SELECT 
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    _cdc_timestamp as last_updated
FROM ranked_changes
WHERE rn = 1 AND _cdc_operation != 'd';

-- ============================================================================
-- GOLD LAYER: AI-READY ANALYTICS
-- Pre-computed features and aggregations for AI consumption
-- ============================================================================

-- GOLD: Customer 360 (AI-Ready Features)
CREATE OR REPLACE DYNAMIC TABLE GOLD.PG_CUSTOMER_360
    TARGET_LAG = '10 minutes'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.company_name,
    c.segment,
    c.revenue_tier,
    c.annual_revenue,
    c.is_active,
    c.created_at as customer_since,
    
    -- Order Metrics
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    AVG(o.total_amount) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as days_since_last_order,
    
    -- RFM Features (for ML)
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as recency_days,
    COUNT(DISTINCT o.order_id) as frequency_orders,
    SUM(o.total_amount) as monetary_total,
    
    -- Derived Insights
    CASE 
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 90 THEN 'AT_RISK'
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 60 THEN 'COOLING'
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 30 THEN 'ACTIVE'
        ELSE 'HOT'
    END as engagement_status,
    
    c.last_updated

FROM SILVER.PG_CUSTOMERS c
LEFT JOIN SILVER.PG_ORDERS o ON c.customer_id = o.customer_id
WHERE c.is_active = TRUE
GROUP BY 
    c.customer_id, c.full_name, c.email, c.company_name, 
    c.segment, c.revenue_tier, c.annual_revenue, c.is_active,
    c.created_at, c.last_updated;

-- GOLD: Product Performance (AI-Ready)
CREATE OR REPLACE DYNAMIC TABLE GOLD.PG_PRODUCT_PERFORMANCE
    TARGET_LAG = '10 minutes'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.list_price,
    p.is_active,
    
    -- Sales Metrics
    COUNT(DISTINCT oi.order_id) as orders_containing,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.line_total) as total_revenue,
    AVG(oi.quantity) as avg_units_per_order,
    
    -- Customer Insights
    COUNT(DISTINCT o.customer_id) as unique_customers,
    
    p.last_updated

FROM SILVER.PG_PRODUCTS p
LEFT JOIN SILVER.PG_ORDER_ITEMS oi ON p.product_id = oi.product_id
LEFT JOIN SILVER.PG_ORDERS o ON oi.order_id = o.order_id
GROUP BY 
    p.product_id, p.sku, p.product_name, p.category, 
    p.list_price, p.is_active, p.last_updated;

-- GOLD: Sales Features for ML
CREATE OR REPLACE DYNAMIC TABLE GOLD.PG_SALES_FEATURES
    TARGET_LAG = '10 minutes'
    WAREHOUSE = SNOWADHOC
AS
SELECT
    c.customer_id,
    c.segment,
    c.revenue_tier,
    c.annual_revenue,
    
    -- Time-based features
    DATEDIFF('day', c.customer_since, CURRENT_DATE()) as customer_tenure_days,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as recency_days,
    
    -- Frequency features
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT o.order_id) / NULLIF(DATEDIFF('month', MIN(o.order_date), CURRENT_DATE()), 0) as orders_per_month,
    
    -- Monetary features
    SUM(o.total_amount) as total_spend,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.total_amount) as max_order_value,
    STDDEV(o.total_amount) as order_value_stddev,
    
    -- Product diversity
    COUNT(DISTINCT oi.product_id) as unique_products_purchased,
    
    -- Engagement score (composite)
    (
        (1.0 / NULLIF(DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()), 1)) * 100 +
        COUNT(DISTINCT o.order_id) * 10 +
        LOG(10, NULLIF(SUM(o.total_amount), 0) + 1) * 5
    ) as engagement_score,
    
    -- Churn indicator (for supervised learning)
    CASE WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 90 THEN 1 ELSE 0 END as churn_flag

FROM SILVER.PG_CUSTOMERS c
LEFT JOIN SILVER.PG_ORDERS o ON c.customer_id = o.customer_id
LEFT JOIN SILVER.PG_ORDER_ITEMS oi ON o.order_id = oi.order_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.segment, c.revenue_tier, c.annual_revenue, c.customer_since;

-- ============================================================================
-- VERIFY TRANSFORMATIONS
-- ============================================================================

SELECT 'SILVER.PG_CUSTOMERS' as table_name, COUNT(*) as rows FROM SILVER.PG_CUSTOMERS
UNION ALL SELECT 'SILVER.PG_PRODUCTS', COUNT(*) FROM SILVER.PG_PRODUCTS
UNION ALL SELECT 'SILVER.PG_ORDERS', COUNT(*) FROM SILVER.PG_ORDERS
UNION ALL SELECT 'SILVER.PG_ORDER_ITEMS', COUNT(*) FROM SILVER.PG_ORDER_ITEMS
UNION ALL SELECT 'GOLD.PG_CUSTOMER_360', COUNT(*) FROM GOLD.PG_CUSTOMER_360
UNION ALL SELECT 'GOLD.PG_PRODUCT_PERFORMANCE', COUNT(*) FROM GOLD.PG_PRODUCT_PERFORMANCE
UNION ALL SELECT 'GOLD.PG_SALES_FEATURES', COUNT(*) FROM GOLD.PG_SALES_FEATURES;
