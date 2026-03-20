-- ============================================================================
-- 04_OPENFLOW: Openflow CDC Configuration
-- ============================================================================
-- Purpose: Configure Openflow to replicate from LANDING PG to Snowflake
-- Run as: ACCOUNTADMIN
-- Prerequisite: Openflow connector deployed in SPCS
-- ============================================================================

-- ============================================================================
-- Target Database Setup
-- ============================================================================
CREATE DATABASE IF NOT EXISTS DBAONTAP_ANALYTICS;
CREATE WAREHOUSE IF NOT EXISTS DBRYANT_COCO_WH_S 
    WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

USE DATABASE DBAONTAP_ANALYTICS;

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS "public";    -- Landing tables from Openflow
CREATE SCHEMA IF NOT EXISTS BRONZE;      -- VARIANT Dynamic Tables
CREATE SCHEMA IF NOT EXISTS SILVER;      -- CDC-aware cleaned data
CREATE SCHEMA IF NOT EXISTS GOLD;        -- Aggregated business metrics
CREATE SCHEMA IF NOT EXISTS AGENTS;      -- Agentic procedures
CREATE SCHEMA IF NOT EXISTS METADATA;    -- Workflow tracking

-- ============================================================================
-- Openflow CDC Connector Configuration
-- ============================================================================
-- Note: Openflow is configured via Snowsight UI or Openflow NiFi interface
-- 
-- Configuration steps:
-- 1. Deploy Openflow connector from Snowflake Marketplace or SPCS
-- 2. Configure PostgreSQL CDC source:
--    - Host: <LANDING_PG_HOST>
--    - Port: 5432
--    - Database: dbaontap
--    - User: snowflake_admin
--    - Tables: customers, orders, products, order_items, support_tickets
--    - Slot name: openflow_slot
--    - Plugin: pgoutput
-- 
-- 3. Configure Snowflake destination:
--    - Database: DBAONTAP_ANALYTICS
--    - Schema: public
--    - Warehouse: DBRYANT_COCO_WH_S
--
-- 4. Enable CDC columns:
--    - _SNOWFLAKE_DELETED (soft delete flag)
--    - _SNOWFLAKE_UPDATED_AT (CDC timestamp)
-- ============================================================================

-- Verify Openflow landed tables (run after connector is active)
-- SHOW TABLES IN SCHEMA DBAONTAP_ANALYTICS."public";

-- Check row counts
-- SELECT 
--     'customers' as table_name, COUNT(*) as row_count FROM DBAONTAP_ANALYTICS."public".customers
-- UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS."public".orders
-- UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS."public".products
-- UNION ALL SELECT 'order_items', COUNT(*) FROM DBAONTAP_ANALYTICS."public".order_items
-- UNION ALL SELECT 'support_tickets', COUNT(*) FROM DBAONTAP_ANALYTICS."public".support_tickets;
