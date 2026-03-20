-- ============================================================================
-- FULL RESET FOR TESTING - Agentic Data Foundry Demo
-- ============================================================================
-- Run with: snow sql -c CoCo-Green -f scripts/99_reset_for_testing.sql
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- Step 1: Suspend Tasks (prevent interference during reset)
-- ============================================================================
ALTER TASK IF EXISTS AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK SUSPEND;
ALTER TASK IF EXISTS AGENTS.STREAM_CONSUMER_TASK SUSPEND;

SELECT 'Tasks suspended' as status;

-- ============================================================================
-- Step 2: Drop Semantic Views in GOLD
-- ============================================================================
DROP SEMANTIC VIEW IF EXISTS GOLD.CUSTOMER_360_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.CUSTOMER_METRICS_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.ML_CUSTOMER_FEATURES_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.ORDER_SUMMARY_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.PRODUCT_PERFORMANCE_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.ORDER_ITEMS_ANALYSIS_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.TICKET_METRICS_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.TICKET_RESOLUTION_METRICS_HYBRID_SV;
DROP SEMANTIC VIEW IF EXISTS GOLD.ORDER_ITEM_METRICS_HYBRID_SV;

SELECT 'Gold semantic views dropped' as status;

-- ============================================================================
-- Step 3: Drop Gold Dynamic Tables
-- ============================================================================
DROP DYNAMIC TABLE IF EXISTS GOLD.CUSTOMER_360;
DROP DYNAMIC TABLE IF EXISTS GOLD.CUSTOMER_METRICS;
DROP DYNAMIC TABLE IF EXISTS GOLD.ML_CUSTOMER_FEATURES;
DROP DYNAMIC TABLE IF EXISTS GOLD.ORDER_SUMMARY;
DROP DYNAMIC TABLE IF EXISTS GOLD.PRODUCT_PERFORMANCE;
DROP DYNAMIC TABLE IF EXISTS GOLD.ORDER_ITEMS_ANALYSIS;
DROP DYNAMIC TABLE IF EXISTS GOLD.TICKET_METRICS;
DROP DYNAMIC TABLE IF EXISTS GOLD.TICKET_RESOLUTION_METRICS;
DROP DYNAMIC TABLE IF EXISTS GOLD.ORDER_ITEM_METRICS;

SELECT 'Gold dynamic tables dropped' as status;

-- ============================================================================
-- Step 4: Drop Silver Dynamic Tables
-- ============================================================================
DROP DYNAMIC TABLE IF EXISTS SILVER.CUSTOMERS;
DROP DYNAMIC TABLE IF EXISTS SILVER.ORDERS;
DROP DYNAMIC TABLE IF EXISTS SILVER.PRODUCTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.ORDER_ITEMS;
DROP DYNAMIC TABLE IF EXISTS SILVER.SUPPORT_TICKETS;

SELECT 'Silver dynamic tables dropped' as status;

-- ============================================================================
-- Step 5: Bronze DTs are permanent infrastructure — NOT dropped on reset.
-- They refresh automatically when landing tables are cleared (Step 7).
-- ============================================================================

SELECT 'Bronze dynamic tables preserved (will refresh to empty after landing clear)' as status;

-- ============================================================================
-- Step 6: Drop Streams in AGENTS schema
-- ============================================================================
DROP STREAM IF EXISTS AGENTS.CUSTOMERS_LANDING_STREAM;
DROP STREAM IF EXISTS AGENTS.CUSTOMERS_STREAM;
DROP STREAM IF EXISTS AGENTS.ORDERS_LANDING_STREAM;
DROP STREAM IF EXISTS AGENTS.ORDERS_STREAM;
DROP STREAM IF EXISTS AGENTS.ORDER_ITEMS_LANDING_STREAM;
DROP STREAM IF EXISTS AGENTS.ORDER_ITEMS_STREAM;
DROP STREAM IF EXISTS AGENTS.PRODUCTS_LANDING_STREAM;
DROP STREAM IF EXISTS AGENTS.PRODUCTS_STREAM;
DROP STREAM IF EXISTS AGENTS.SUPPORT_TICKETS_LANDING_STREAM;
DROP STREAM IF EXISTS AGENTS.SUPPORT_TICKETS_STREAM;

SELECT 'AGENTS streams dropped' as status;

-- ============================================================================
-- Step 7: Drop Streams in METADATA schema
-- ============================================================================
DROP STREAM IF EXISTS METADATA.CUSTOMERS_STREAM;
DROP STREAM IF EXISTS METADATA.ORDERS_STREAM;
DROP STREAM IF EXISTS METADATA.ORDER_ITEMS_STREAM;
DROP STREAM IF EXISTS METADATA.PRODUCTS_STREAM;
DROP STREAM IF EXISTS METADATA.SUPPORT_TICKETS_STREAM;

SELECT 'METADATA streams dropped' as status;

-- ============================================================================
-- Step 8: Clear Landing Tables (Snowflake "public" schema - Openflow CDC)
-- ============================================================================
DELETE FROM "public"."order_items";
DELETE FROM "public"."orders";
DELETE FROM "public"."support_tickets";
DELETE FROM "public"."products";
DELETE FROM "public"."customers";

SELECT 'Landing tables cleared' as status;

-- ============================================================================
-- Step 9: Clear Knowledge Graph
-- ============================================================================
TRUNCATE TABLE IF EXISTS KNOWLEDGE_GRAPH.KG_NODE;
TRUNCATE TABLE IF EXISTS KNOWLEDGE_GRAPH.KG_EDGE;

SELECT 'Knowledge graph cleared' as status;

-- ============================================================================
-- Step 10: Clear ALL Metadata Tables
-- ============================================================================
TRUNCATE TABLE IF EXISTS METADATA.WORKFLOW_EXECUTIONS;
TRUNCATE TABLE IF EXISTS METADATA.PLANNER_DECISIONS;
TRUNCATE TABLE IF EXISTS METADATA.VALIDATION_RESULTS;
TRUNCATE TABLE IF EXISTS METADATA.WORKFLOW_LEARNINGS;
TRUNCATE TABLE IF EXISTS METADATA.TRANSFORMATION_LOG;
TRUNCATE TABLE IF EXISTS METADATA.WORKFLOW_LOG;
TRUNCATE TABLE IF EXISTS METADATA.WORKFLOW_STATE;
TRUNCATE TABLE IF EXISTS METADATA.AGENT_REFLECTIONS;
TRUNCATE TABLE IF EXISTS METADATA.ONBOARDED_TABLES;
TRUNCATE TABLE IF EXISTS METADATA.BRONZE_SCHEMA_REGISTRY;

SELECT 'Metadata tables cleared' as status;

-- ============================================================================
-- Step 11: Recreate METADATA append-only streams on landing tables
-- ============================================================================
CREATE OR REPLACE STREAM METADATA.CUSTOMERS_STREAM ON TABLE "public"."customers" APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM METADATA.ORDERS_STREAM ON TABLE "public"."orders" APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM METADATA.ORDER_ITEMS_STREAM ON TABLE "public"."order_items" APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM METADATA.PRODUCTS_STREAM ON TABLE "public"."products" APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM METADATA.SUPPORT_TICKETS_STREAM ON TABLE "public"."support_tickets" APPEND_ONLY = TRUE;

SELECT 'METADATA streams recreated' as status;

-- ============================================================================
-- Step 12: Recreate AGENTS default streams on landing tables
-- ============================================================================
CREATE OR REPLACE STREAM AGENTS.CUSTOMERS_STREAM ON TABLE "public"."customers";
CREATE OR REPLACE STREAM AGENTS.ORDERS_STREAM ON TABLE "public"."orders";
CREATE OR REPLACE STREAM AGENTS.ORDER_ITEMS_STREAM ON TABLE "public"."order_items";
CREATE OR REPLACE STREAM AGENTS.PRODUCTS_STREAM ON TABLE "public"."products";
CREATE OR REPLACE STREAM AGENTS.SUPPORT_TICKETS_STREAM ON TABLE "public"."support_tickets";

SELECT 'AGENTS streams recreated' as status;

-- ============================================================================
-- Step 13: Resume Tasks
-- ============================================================================
ALTER TASK IF EXISTS AGENTS.STREAM_CONSUMER_TASK RESUME;
ALTER TASK IF EXISTS AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK RESUME;

SELECT 'Tasks resumed' as status;

-- ============================================================================
-- Step 14: Clear Postgres SOURCE & LANDING (via psql)
-- ============================================================================
-- NOTE: Run these MANUALLY before or after this script:
--   psql "service=dbaontap_source"  -c "TRUNCATE order_items, orders, support_tickets, products, customers CASCADE;"
--   psql "service=dbaontap_landing" -c "TRUNCATE order_items, orders, support_tickets, products, customers CASCADE;"
-- ============================================================================

-- ============================================================================
-- Verification Summary
-- ============================================================================
SELECT 'Landing' as layer, (SELECT COUNT(*) FROM "public"."customers") as row_count
UNION ALL SELECT 'Bronze DTs', (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_TYPE = 'DYNAMIC TABLE')
UNION ALL SELECT 'Silver DTs', (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_TYPE = 'DYNAMIC TABLE')
UNION ALL SELECT 'Gold DTs', (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'DYNAMIC TABLE')
UNION ALL SELECT 'Metadata Execs', (SELECT COUNT(*) FROM METADATA.WORKFLOW_EXECUTIONS)
UNION ALL SELECT 'Learnings', (SELECT COUNT(*) FROM METADATA.WORKFLOW_LEARNINGS)
UNION ALL SELECT 'Onboarded', (SELECT COUNT(*) FROM METADATA.ONBOARDED_TABLES)
UNION ALL SELECT 'KG Nodes', (SELECT COUNT(*) FROM KNOWLEDGE_GRAPH.KG_NODE);

SELECT 'FULL RESET COMPLETE - All layers cleared!' as final_status;
