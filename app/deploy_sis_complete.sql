-- ============================================================================
-- Deploy Streamlit in Snowflake: Agentic Silver Layer Demo Manager
-- WITH External Access Integration for PostgreSQL connectivity
-- ============================================================================
-- Run via: snow sql --connection CoCo-Green -f deploy_sis_complete.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- STEP 1: Network Rule for PostgreSQL hosts
-- ============================================================================
-- Allow outbound connections to Snowflake Managed PostgreSQL instances

CREATE OR REPLACE NETWORK RULE DBAONTAP_ANALYTICS.METADATA.PG_EGRESS_RULE
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = (
        'source-pg-host.example.snowflake.app:5432',
        'landing-pg-host.example.snowflake.app:5432'
    )
    COMMENT = 'Egress to PostgreSQL SOURCE and LANDING instances';

-- ============================================================================
-- STEP 2: Secrets for PostgreSQL credentials
-- ============================================================================
-- Note: Update passwords with actual values before running

CREATE OR REPLACE SECRET DBAONTAP_ANALYTICS.METADATA.PG_SOURCE_SECRET
    TYPE = PASSWORD
    USERNAME = 'dbryant'
    PASSWORD = 'REPLACE_WITH_SOURCE_PASSWORD'
    COMMENT = 'Credentials for PostgreSQL SOURCE (dbaontap_source)';

CREATE OR REPLACE SECRET DBAONTAP_ANALYTICS.METADATA.PG_LANDING_SECRET
    TYPE = PASSWORD
    USERNAME = 'dbryant'
    PASSWORD = 'REPLACE_WITH_LANDING_PASSWORD'
    COMMENT = 'Credentials for PostgreSQL LANDING (dbaontap_landing)';

-- ============================================================================
-- STEP 3: External Access Integration
-- ============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBAONTAP_PG_EAI
    ALLOWED_NETWORK_RULES = (DBAONTAP_ANALYTICS.METADATA.PG_EGRESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (
        DBAONTAP_ANALYTICS.METADATA.PG_SOURCE_SECRET,
        DBAONTAP_ANALYTICS.METADATA.PG_LANDING_SECRET
    )
    ENABLED = TRUE
    COMMENT = 'External Access Integration for Streamlit to connect to PostgreSQL instances';

-- ============================================================================
-- STEP 4: Create Stage for Streamlit app
-- ============================================================================

CREATE STAGE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.SIS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit in Snowflake application files';

-- ============================================================================
-- STEP 5: Create environment.yml for psycopg2
-- ============================================================================
-- psycopg2-binary is available in Snowflake's Anaconda channel

-- Note: We'll upload this via snow CLI

-- ============================================================================
-- STEP 6: Create Streamlit App (after uploading files)
-- ============================================================================
-- This will be run after files are uploaded to the stage

-- CREATE OR REPLACE STREAMLIT DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER
--     ROOT_LOCATION = '@DBAONTAP_ANALYTICS.METADATA.SIS_STAGE'
--     MAIN_FILE = 'streamlit_app.py'
--     QUERY_WAREHOUSE = DBRYANT_COCO_WH_S
--     EXTERNAL_ACCESS_INTEGRATIONS = (DBAONTAP_PG_EAI)
--     SECRETS = (
--         'pg_source_creds' = DBAONTAP_ANALYTICS.METADATA.PG_SOURCE_SECRET,
--         'pg_landing_creds' = DBAONTAP_ANALYTICS.METADATA.PG_LANDING_SECRET
--     )
--     COMMENT = 'Agentic Silver Layer Demo Manager - Data gen, pipeline status, semantic views';

-- ============================================================================
-- Verification
-- ============================================================================
SHOW NETWORK RULES IN SCHEMA DBAONTAP_ANALYTICS.METADATA;
SHOW SECRETS IN SCHEMA DBAONTAP_ANALYTICS.METADATA;
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'DBAONTAP_PG_EAI';
