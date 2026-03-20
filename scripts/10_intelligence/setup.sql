-- ============================================================================
-- 10_INTELLIGENCE: Snowflake Intelligence Setup
-- ============================================================================
-- Purpose: Configure Snowflake Intelligence for natural language queries
-- Prerequisite: Semantic views created in GOLD schema
-- Run as: ACCOUNTADMIN
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- GRANT PERMISSIONS FOR SNOWFLAKE INTELLIGENCE
-- ============================================================================

-- Create a role for Intelligence users (optional)
CREATE ROLE IF NOT EXISTS DBAONTAP_ANALYST;

-- Grant access to semantic views
GRANT USAGE ON DATABASE DBAONTAP_ANALYTICS TO ROLE DBAONTAP_ANALYST;
GRANT USAGE ON SCHEMA DBAONTAP_ANALYTICS.GOLD TO ROLE DBAONTAP_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA DBAONTAP_ANALYTICS.GOLD TO ROLE DBAONTAP_ANALYST;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA DBAONTAP_ANALYTICS.GOLD TO ROLE DBAONTAP_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD TO ROLE DBAONTAP_ANALYST;

-- Grant access to semantic views specifically
GRANT SELECT ON ALL SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD TO ROLE DBAONTAP_ANALYST;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE DBRYANT_COCO_WH_S TO ROLE DBAONTAP_ANALYST;

-- ============================================================================
-- SNOWFLAKE INTELLIGENCE CONFIGURATION
-- ============================================================================
-- Snowflake Intelligence is configured via Snowsight UI:
-- 
-- 1. Navigate to: AI & ML > Snowflake Intelligence
-- 2. Click "New Data Source"
-- 3. Select semantic views:
--    - DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360_SV
--    - DBAONTAP_ANALYTICS.GOLD.PRODUCT_PERFORMANCE_SV
--    - DBAONTAP_ANALYTICS.GOLD.CUSTOMER_METRICS_SV
--    - DBAONTAP_ANALYTICS.GOLD.ML_CUSTOMER_FEATURES_SV
-- 4. Configure access permissions
-- 5. Test with sample questions:
--    - "Who are our top 5 customers by lifetime value?"
--    - "What is the average order value by customer segment?"
--    - "Show me product performance by category"
--    - "Which customers are at risk of churning?"
-- ============================================================================

-- ============================================================================
-- VERIFY SEMANTIC VIEWS ARE READY
-- ============================================================================
-- Check all semantic views
SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;

-- Test query against semantic view
SELECT full_name, lifetime_value, segment, engagement_status
FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360
ORDER BY lifetime_value DESC NULLS LAST
LIMIT 10;

-- ============================================================================
-- SAMPLE CORTEX ANALYST QUERIES
-- ============================================================================
-- These queries demonstrate what Snowflake Intelligence can answer:

-- Customer Analysis
-- SELECT * FROM TABLE(
--     SNOWFLAKE.CORTEX.ANALYST(
--         SEMANTIC_VIEW => 'DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360_SV',
--         QUERY => 'Who are our top customers by revenue?'
--     )
-- );

-- Product Analysis
-- SELECT * FROM TABLE(
--     SNOWFLAKE.CORTEX.ANALYST(
--         SEMANTIC_VIEW => 'DBAONTAP_ANALYTICS.GOLD.PRODUCT_PERFORMANCE_SV',
--         QUERY => 'What are our best selling products?'
--     )
-- );

-- ============================================================================
-- END-TO-END VERIFICATION
-- ============================================================================
-- Run this to verify the complete pipeline is working
SELECT 
    'SOURCE→LANDING' as stage, 
    'PostgreSQL Replication' as technology,
    (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS."public".customers) as row_count
UNION ALL
SELECT 'BRONZE', 'VARIANT Dynamic Tables', 
    (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT)
UNION ALL
SELECT 'SILVER', 'CDC-Aware Dynamic Tables', 
    (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS)
UNION ALL
SELECT 'GOLD', 'Aggregation Dynamic Tables', 
    (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360)
UNION ALL
SELECT 'SEMANTIC VIEWS', 'LLM-Generated', 
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS WHERE TABLE_SCHEMA = 'GOLD');
