-- ============================================================================
-- 02_LANDING: PostgreSQL Landing Instance Setup
-- ============================================================================
-- Purpose: Create the LANDING PostgreSQL instance (CDC staging)
-- Run as: ACCOUNTADMIN
-- ============================================================================

-- Create the PostgreSQL LANDING instance
CREATE DATABASE IF NOT EXISTS DBAONTAP_LANDING;

-- Create PostgreSQL instance for LANDING (CDC staging)
CREATE POSTGRES INSTANCE IF NOT EXISTS DBAONTAP_LANDING.PUBLIC.LANDING_PG
    INSTANCE_NAME = 'landing_pg'
    ADMIN_USER = 'snowflake_admin'
    AUTO_RESUME = TRUE
    COMMENT = 'Landing PostgreSQL instance for CDC staging';

-- Get connection details (run after instance is ready)
-- SHOW POSTGRES INSTANCES LIKE 'LANDING_PG' IN DATABASE DBAONTAP_LANDING;

-- ============================================================================
-- IMPORTANT: After instance creation, note the hostname from SHOW command
-- Store credentials in ~/.pgpass:
-- <hostname>:5432:*:snowflake_admin:<password>
-- ============================================================================
