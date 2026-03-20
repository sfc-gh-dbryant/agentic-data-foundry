-- ============================================================================
-- 01_SOURCE: PostgreSQL Source Instance Setup
-- ============================================================================
-- Purpose: Create the SOURCE PostgreSQL instance (simulates application DB)
-- Run as: ACCOUNTADMIN
-- ============================================================================

-- Create the PostgreSQL SOURCE instance
CREATE DATABASE IF NOT EXISTS DBAONTAP_SOURCE;

-- Create PostgreSQL instance for SOURCE (application database)
-- Note: This creates a Snowflake Managed PostgreSQL instance
CREATE POSTGRES INSTANCE IF NOT EXISTS DBAONTAP_SOURCE.PUBLIC.SOURCE_PG
    INSTANCE_NAME = 'source_pg'
    ADMIN_USER = 'snowflake_admin'
    AUTO_RESUME = TRUE
    COMMENT = 'Source PostgreSQL instance for agentic silver layer demo';

-- Get connection details (run after instance is ready)
-- SHOW POSTGRES INSTANCES LIKE 'SOURCE_PG' IN DATABASE DBAONTAP_SOURCE;

-- ============================================================================
-- IMPORTANT: After instance creation, note the hostname from SHOW command
-- Store credentials in ~/.pgpass:
-- <hostname>:5432:*:snowflake_admin:<password>
-- ============================================================================
