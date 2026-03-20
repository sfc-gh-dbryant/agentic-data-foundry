-- ============================================================================
-- External Access Integration for PostgreSQL from Streamlit in Snowflake
-- ============================================================================
-- Enables the SiS app to connect directly to PostgreSQL SOURCE and LANDING
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- Step 1: Create Network Rule for PostgreSQL endpoints
-- ============================================================================
-- These are the Snowflake Managed PostgreSQL hosts

CREATE OR REPLACE NETWORK RULE DBAONTAP_ANALYTICS.METADATA.POSTGRES_NETWORK_RULE
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = (
        'source-pg-host.example.snowflake.app:5432',   -- SOURCE
        'landing-pg-host.example.snowflake.app:5432'    -- LANDING
    );

-- ============================================================================
-- Step 2: Create Secret for PostgreSQL credentials
-- ============================================================================
-- Store credentials securely (update with your actual credentials)

CREATE OR REPLACE SECRET DBAONTAP_ANALYTICS.METADATA.POSTGRES_SOURCE_CREDS
    TYPE = PASSWORD
    USERNAME = 'dbryant'
    PASSWORD = '<YOUR_SOURCE_PASSWORD>';

CREATE OR REPLACE SECRET DBAONTAP_ANALYTICS.METADATA.POSTGRES_LANDING_CREDS
    TYPE = PASSWORD
    USERNAME = 'dbryant'
    PASSWORD = '<YOUR_LANDING_PASSWORD>';

-- ============================================================================
-- Step 3: Create External Access Integration
-- ============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBAONTAP_ANALYTICS_POSTGRES_EAI
    ALLOWED_NETWORK_RULES = (DBAONTAP_ANALYTICS.METADATA.POSTGRES_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (
        DBAONTAP_ANALYTICS.METADATA.POSTGRES_SOURCE_CREDS,
        DBAONTAP_ANALYTICS.METADATA.POSTGRES_LANDING_CREDS
    )
    ENABLED = TRUE
    COMMENT = 'EAI for Streamlit app to connect to PostgreSQL SOURCE and LANDING databases';

-- ============================================================================
-- Step 4: Create Streamlit app with EAI
-- ============================================================================

-- First, ensure the stage exists
CREATE STAGE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the Streamlit app WITH external access
CREATE OR REPLACE STREAMLIT DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER
    ROOT_LOCATION = '@DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = DBRYANT_COCO_WH_S
    EXTERNAL_ACCESS_INTEGRATIONS = (DBAONTAP_ANALYTICS_POSTGRES_EAI)
    SECRETS = (
        'pg_source_creds' = DBAONTAP_ANALYTICS.METADATA.POSTGRES_SOURCE_CREDS,
        'pg_landing_creds' = DBAONTAP_ANALYTICS.METADATA.POSTGRES_LANDING_CREDS
    )
    COMMENT = 'Agentic Silver Layer Demo Manager with PostgreSQL access';

-- Grant access
GRANT USAGE ON STREAMLIT DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- Verification
-- ============================================================================
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'DBAONTAP_ANALYTICS_POSTGRES_EAI';
SHOW STREAMLITS IN SCHEMA DBAONTAP_ANALYTICS.METADATA;
