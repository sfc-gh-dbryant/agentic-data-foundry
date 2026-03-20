-- ============================================================================
-- Deploy Streamlit in Snowflake App: Agentic Silver Layer Demo Manager
-- ============================================================================
-- Purpose: Deploy the demo manager app to Snowflake
-- Run as: ACCOUNTADMIN or role with CREATE STREAMLIT privilege
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- Create a stage for the Streamlit app
CREATE STAGE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Upload the app file (run via snow CLI)
-- PUT file:///path/to/streamlit_app.py @DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER
    ROOT_LOCATION = '@DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = DBRYANT_COCO_WH_S
    COMMENT = 'Agentic Silver Layer Demo Manager - Generate data, view pipeline status, manage semantic views';

-- Grant access to the app
GRANT USAGE ON STREAMLIT DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER TO ROLE ACCOUNTADMIN;

-- Show the app URL
SHOW STREAMLITS LIKE 'DEMO_MANAGER' IN SCHEMA DBAONTAP_ANALYTICS.METADATA;
