-- ============================================================================
-- 09_SEMANTIC_VIEWS: Agentic Semantic View Pipeline
-- ============================================================================
-- Purpose: Auto-discover Gold tables and generate semantic views using LLM
-- LLM: claude-3-5-sonnet
-- Run as: ACCOUNTADMIN
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- PIPELINE: RUN_SEMANTIC_VIEW_PIPELINE
-- Purpose: Automatically discover all Gold tables and generate semantic views
-- Features:
--   - Iterates through all tables in GOLD schema
--   - Uses LLM to generate semantic view DDL
--   - Handles errors gracefully (logs failures for manual review)
--   - Returns summary with success/fail counts
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    tables_cursor CURSOR FOR 
        SELECT TABLE_NAME 
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE';
    table_name_var VARCHAR;
    sv_name VARCHAR;
    col_list VARCHAR;
    prompt VARCHAR;
    llm_response VARCHAR;
    ddl_sql VARCHAR;
    pk_column VARCHAR;
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    results ARRAY DEFAULT ARRAY_CONSTRUCT();
    err_msg VARCHAR;
BEGIN
    FOR record IN tables_cursor DO
        table_name_var := record.TABLE_NAME;
        sv_name := table_name_var || '_SV';
        
        -- Get column list
        SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        INTO :col_list
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = :table_name_var;
        
        -- Detect primary key column
        SELECT COLUMN_NAME INTO :pk_column
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = :table_name_var 
        AND (COLUMN_NAME LIKE '%_ID' OR ORDINAL_POSITION = 1)
        ORDER BY CASE WHEN COLUMN_NAME LIKE '%_ID' THEN 0 ELSE 1 END, ORDINAL_POSITION
        LIMIT 1;
        
        -- Build LLM prompt
        prompt := 'Generate Snowflake SEMANTIC VIEW DDL. Table: DBAONTAP_ANALYTICS.GOLD.' || :table_name_var || 
                  ' View: DBAONTAP_ANALYTICS.GOLD.' || :sv_name || 
                  ' PK: ' || :pk_column || 
                  ' Columns: ' || :col_list || 
                  ' FORMAT: CREATE OR REPLACE SEMANTIC VIEW DBAONTAP_ANALYTICS.GOLD.' || :sv_name || 
                  ' TABLES (DBAONTAP_ANALYTICS.GOLD.' || :table_name_var || ' PRIMARY KEY (' || :pk_column || '))' ||
                  ' FACTS (' || :table_name_var || '.col AS alias)' ||
                  ' DIMENSIONS (' || :table_name_var || '.col AS alias)' ||
                  ' METRICS (' || :table_name_var || '.name AS AGG(col));' ||
                  ' RULES: Each column once. COUNT(DISTINCT x) not COUNT_DISTINCT. FACTS=numeric IDs DIMENSIONS=text/dates METRICS=aggregations. Return ONLY valid SQL.';
        
        -- Call Cortex LLM
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', :prompt) INTO :llm_response;
        ddl_sql := TRIM(REGEXP_REPLACE(:llm_response, '^```sql|```$|^```', ''));
        
        -- Try to execute the generated DDL
        BEGIN
            EXECUTE IMMEDIATE :ddl_sql;
            
            -- Log success
            INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
                (source_table, target_table, transformation_sql, agent_reasoning, status, executed_at)
            VALUES ('GOLD.' || :table_name_var, :sv_name, :ddl_sql, 'Pipeline auto-generated', 'SUCCESS', CURRENT_TIMESTAMP());
            
            success_count := success_count + 1;
            results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT('table', :table_name_var, 'view', :sv_name, 'status', 'SUCCESS'));
            
        EXCEPTION WHEN OTHER THEN
            -- Log failure for manual review
            err_msg := SQLCODE || ': ' || SQLERRM;
            INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
                (source_table, target_table, transformation_sql, agent_reasoning, status, executed_at)
            VALUES ('GOLD.' || :table_name_var, :sv_name, :ddl_sql, 'MANUAL_REQUIRED: ' || :err_msg, 'FAILED', CURRENT_TIMESTAMP());
            
            fail_count := fail_count + 1;
            results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT('table', :table_name_var, 'view', :sv_name, 'status', 'FAILED', 'error', :err_msg, 'ddl', :ddl_sql));
        END;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT('success_count', :success_count, 'fail_count', :fail_count, 'details', :results);
END;
$$;

-- ============================================================================
-- RUN THE PIPELINE
-- ============================================================================
-- Execute the agentic pipeline to generate all semantic views
-- CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE();

-- ============================================================================
-- REVIEW RESULTS
-- ============================================================================
-- Check generated semantic views
-- SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;

-- Check transformation log
-- SELECT source_table, target_table, status, agent_reasoning, executed_at 
-- FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
-- WHERE agent_reasoning LIKE 'Pipeline%' OR agent_reasoning LIKE 'MANUAL%'
-- ORDER BY executed_at DESC;

-- Get failed items for manual creation
-- SELECT target_table, transformation_sql, agent_reasoning 
-- FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
-- WHERE status = 'FAILED';
