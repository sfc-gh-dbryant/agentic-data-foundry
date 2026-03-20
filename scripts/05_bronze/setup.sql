-- ============================================================================
-- 05_BRONZE: VARIANT Dynamic Tables
-- ============================================================================
-- Purpose: Create Bronze layer with VARIANT payloads for schema-on-read
-- Run as: ACCOUNTADMIN
-- Prerequisite: Openflow tables exist in "public" schema
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- BRONZE.CUSTOMERS_VARIANT
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,
    'customers' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM DBAONTAP_ANALYTICS."public"."customers";

-- ============================================================================
-- BRONZE.ORDERS_VARIANT
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,
    'orders' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM DBAONTAP_ANALYTICS."public"."orders";

-- ============================================================================
-- BRONZE.PRODUCTS_VARIANT
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,
    'products' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM DBAONTAP_ANALYTICS."public"."products";

-- ============================================================================
-- BRONZE.ORDER_ITEMS_VARIANT
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.ORDER_ITEMS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,
    'order_items' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM DBAONTAP_ANALYTICS."public"."order_items";

-- ============================================================================
-- BRONZE.SUPPORT_TICKETS_VARIANT
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.SUPPORT_TICKETS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,
    'support_tickets' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM DBAONTAP_ANALYTICS."public"."support_tickets";

-- ============================================================================
-- BRONZE.ALL_DATA_VARIANT (Unified View)
-- ============================================================================
CREATE OR REPLACE VIEW DBAONTAP_ANALYTICS.BRONZE.ALL_DATA_VARIANT AS
SELECT payload, source_table, ingested_at FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT
UNION ALL
SELECT payload, source_table, ingested_at FROM DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT
UNION ALL
SELECT payload, source_table, ingested_at FROM DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT
UNION ALL
SELECT payload, source_table, ingested_at FROM DBAONTAP_ANALYTICS.BRONZE.ORDER_ITEMS_VARIANT
UNION ALL
SELECT payload, source_table, ingested_at FROM DBAONTAP_ANALYTICS.BRONZE.SUPPORT_TICKETS_VARIANT;

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT source_table, COUNT(*) as cnt 
-- FROM DBAONTAP_ANALYTICS.BRONZE.ALL_DATA_VARIANT 
-- GROUP BY source_table;
