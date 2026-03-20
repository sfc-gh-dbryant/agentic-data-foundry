-- =============================================================================
-- AGENTIC SILVER LAYER TRANSFORMATION FRAMEWORK
-- Bronze Activation → Agentic Silver → Gold (Dynamic Tables)
-- =============================================================================

-- 1. CREATE DATABASES AND SCHEMAS
CREATE DATABASE IF NOT EXISTS AGENTIC_PIPELINE;
USE DATABASE AGENTIC_PIPELINE;

CREATE SCHEMA IF NOT EXISTS BRONZE;      -- Raw ingested data (Openflow/Iceberg)
CREATE SCHEMA IF NOT EXISTS SILVER;      -- Agentic transformations
CREATE SCHEMA IF NOT EXISTS GOLD;        -- Business-ready Dynamic Tables
CREATE SCHEMA IF NOT EXISTS AGENTS;      -- Cortex Agents for transformation
CREATE SCHEMA IF NOT EXISTS METADATA;    -- Schema discovery & lineage

-- 2. BRONZE LAYER: Raw data landing (simulating Openflow CDC output)
CREATE OR REPLACE TABLE BRONZE.RAW_CUSTOMERS (
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _metadata_source VARCHAR DEFAULT 'openflow_postgres_cdc',
    raw_payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE.RAW_ORDERS (
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _metadata_source VARCHAR DEFAULT 'openflow_postgres_cdc',
    raw_payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE.RAW_PRODUCTS (
    _metadata_load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _metadata_source VARCHAR DEFAULT 'openflow_salesforce_api',
    raw_payload VARIANT
);

-- 3. METADATA TABLES: For agent discovery
CREATE OR REPLACE TABLE METADATA.BRONZE_SCHEMA_REGISTRY (
    table_name VARCHAR,
    detected_columns VARIANT,
    sample_values VARIANT,
    data_quality_issues VARIANT,
    recommended_transformations VARIANT,
    last_analyzed TIMESTAMP_LTZ,
    agent_analysis_id VARCHAR
);

CREATE OR REPLACE TABLE METADATA.TRANSFORMATION_LOG (
    transformation_id VARCHAR DEFAULT UUID_STRING(),
    source_table VARCHAR,
    target_table VARCHAR,
    transformation_sql TEXT,
    agent_reasoning TEXT,
    status VARCHAR,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    executed_at TIMESTAMP_LTZ,
    error_message TEXT
);

-- 4. Grant permissions
GRANT USAGE ON DATABASE AGENTIC_PIPELINE TO ROLE PUBLIC;
GRANT USAGE ON ALL SCHEMAS IN DATABASE AGENTIC_PIPELINE TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA METADATA TO ROLE PUBLIC;
