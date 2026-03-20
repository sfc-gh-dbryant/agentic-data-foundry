-- ============================================================================
-- 06_SILVER: CDC-Aware Dynamic Tables
-- ============================================================================
-- Purpose: Create Silver layer with CDC deduplication and soft-delete handling
-- Pattern: ROW_NUMBER() partitioned by PK, ordered by _SNOWFLAKE_UPDATED_AT DESC
-- Run as: ACCOUNTADMIN
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- SILVER.CUSTOMERS - CDC-aware with deduplication
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.CUSTOMERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:customer_id::INTEGER as customer_id,
        payload:first_name::VARCHAR as first_name,
        payload:last_name::VARCHAR as last_name,
        payload:email::VARCHAR as email,
        payload:phone::VARCHAR as phone,
        payload:company_name::VARCHAR as company_name,
        payload:industry::VARCHAR as industry,
        TRY_TO_TIMESTAMP(payload:created_at::VARCHAR) as created_at,
        TRY_TO_TIMESTAMP(payload:updated_at::VARCHAR) as updated_at,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:customer_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT
)
SELECT 
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone,
    company_name,
    industry,
    created_at,
    updated_at,
    cdc_timestamp,
    -- Derived fields
    CASE 
        WHEN company_name ILIKE '%enterprise%' OR company_name ILIKE '%corp%' THEN 'ENTERPRISE'
        WHEN company_name ILIKE '%startup%' OR company_name ILIKE '%labs%' THEN 'STARTUP'
        ELSE 'MID-MARKET'
    END as segment
FROM ranked_changes
WHERE rn = 1 
  AND (is_deleted = FALSE OR is_deleted IS NULL);

-- ============================================================================
-- SILVER.ORDERS - CDC-aware with deduplication
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.ORDERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:order_id::INTEGER as order_id,
        payload:customer_id::INTEGER as customer_id,
        TRY_TO_TIMESTAMP(payload:order_date::VARCHAR) as order_date,
        payload:status::VARCHAR as status,
        payload:total_amount::NUMBER(12,2) as total_amount,
        payload:shipping_address::VARCHAR as shipping_address,
        payload:notes::VARCHAR as notes,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:order_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT
)
SELECT 
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    shipping_address,
    notes,
    cdc_timestamp,
    -- Derived fields
    DATE_TRUNC('month', order_date) as order_month,
    DATEDIFF('day', order_date, CURRENT_DATE()) as days_since_order
FROM ranked_changes
WHERE rn = 1 
  AND (is_deleted = FALSE OR is_deleted IS NULL);

-- ============================================================================
-- SILVER.PRODUCTS - CDC-aware with deduplication
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.PRODUCTS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:product_id::INTEGER as product_id,
        payload:product_name::VARCHAR as product_name,
        payload:category::VARCHAR as category,
        payload:price::NUMBER(10,2) as price,
        payload:cost::NUMBER(10,2) as cost,
        payload:sku::VARCHAR as sku,
        payload:active::BOOLEAN as active,
        TRY_TO_TIMESTAMP(payload:created_at::VARCHAR) as created_at,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:product_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT
)
SELECT 
    product_id,
    product_name,
    category,
    price,
    cost,
    price - COALESCE(cost, 0) as margin,
    CASE WHEN cost > 0 THEN (price - cost) / cost * 100 ELSE 0 END as margin_percent,
    sku,
    active,
    created_at,
    cdc_timestamp
FROM ranked_changes
WHERE rn = 1 
  AND (is_deleted = FALSE OR is_deleted IS NULL);

-- ============================================================================
-- SILVER.ORDER_ITEMS - CDC-aware with deduplication
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.ORDER_ITEMS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:item_id::INTEGER as item_id,
        payload:order_id::INTEGER as order_id,
        payload:product_id::INTEGER as product_id,
        payload:quantity::INTEGER as quantity,
        payload:unit_price::NUMBER(10,2) as unit_price,
        payload:discount_percent::NUMBER(5,2) as discount_percent,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:item_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM DBAONTAP_ANALYTICS.BRONZE.ORDER_ITEMS_VARIANT
)
SELECT 
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_percent,
    quantity * unit_price as line_total,
    quantity * unit_price * (1 - COALESCE(discount_percent, 0) / 100) as line_total_after_discount,
    cdc_timestamp
FROM ranked_changes
WHERE rn = 1 
  AND (is_deleted = FALSE OR is_deleted IS NULL);

-- ============================================================================
-- SILVER.SUPPORT_TICKETS - CDC-aware with deduplication
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.SUPPORT_TICKETS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:ticket_id::INTEGER as ticket_id,
        payload:customer_id::INTEGER as customer_id,
        payload:subject::VARCHAR as subject,
        payload:description::VARCHAR as description,
        payload:priority::VARCHAR as priority,
        payload:status::VARCHAR as status,
        TRY_TO_TIMESTAMP(payload:created_at::VARCHAR) as created_at,
        TRY_TO_TIMESTAMP(payload:resolved_at::VARCHAR) as resolved_at,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:ticket_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM DBAONTAP_ANALYTICS.BRONZE.SUPPORT_TICKETS_VARIANT
)
SELECT 
    ticket_id,
    customer_id,
    subject,
    description,
    priority,
    status,
    created_at,
    resolved_at,
    cdc_timestamp,
    -- Derived fields
    CASE WHEN resolved_at IS NOT NULL 
         THEN DATEDIFF('hour', created_at, resolved_at) 
         ELSE NULL 
    END as resolution_hours
FROM ranked_changes
WHERE rn = 1 
  AND (is_deleted = FALSE OR is_deleted IS NULL);

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT 'CUSTOMERS' as tbl, COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS
-- UNION ALL SELECT 'ORDERS', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.ORDERS
-- UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.PRODUCTS
-- UNION ALL SELECT 'ORDER_ITEMS', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.ORDER_ITEMS
-- UNION ALL SELECT 'SUPPORT_TICKETS', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.SUPPORT_TICKETS;
