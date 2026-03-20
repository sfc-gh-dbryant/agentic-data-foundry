-- =============================================================================
-- AGENTIC DISCOVERY: UDFs for Bronze Schema Analysis
-- These tools are used by the Discovery Agent to understand raw data
-- =============================================================================

USE DATABASE AGENTIC_PIPELINE;
USE SCHEMA AGENTS;

-- 1. DISCOVER BRONZE SCHEMAS: Extracts structure from VARIANT columns
CREATE OR REPLACE FUNCTION AGENTS.DISCOVER_SCHEMA(table_fqn VARCHAR)
RETURNS TABLE (
    column_path VARCHAR,
    inferred_type VARCHAR,
    sample_value VARCHAR,
    null_percentage FLOAT,
    distinct_count NUMBER
)
LANGUAGE SQL
AS
$$
    SELECT 
        f.path::VARCHAR as column_path,
        TYPEOF(f.value)::VARCHAR as inferred_type,
        f.value::VARCHAR as sample_value,
        0.0 as null_percentage,
        1 as distinct_count
    FROM TABLE(FLATTEN(input => (
        SELECT TOP 1 raw_payload 
        FROM IDENTIFIER(table_fqn)
        WHERE raw_payload IS NOT NULL
    ), recursive => true)) f
    WHERE NOT IS_OBJECT(f.value) AND NOT IS_ARRAY(f.value)
$$;

-- 2. ANALYZE DATA QUALITY: Identifies issues in Bronze data
CREATE OR REPLACE PROCEDURE AGENTS.ANALYZE_DATA_QUALITY(
    source_table VARCHAR,
    analysis_depth NUMBER DEFAULT 1000
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    sample_query VARCHAR;
    schema_info VARIANT;
BEGIN
    -- Get sample and analyze
    LET query := 'SELECT COUNT(*) as total_rows, ' ||
                 'COUNT_IF(raw_payload IS NULL) as null_payloads, ' ||
                 'MIN(_metadata_load_ts) as earliest_load, ' ||
                 'MAX(_metadata_load_ts) as latest_load ' ||
                 'FROM ' || :source_table;
    
    EXECUTE IMMEDIATE :query;
    
    RETURN OBJECT_CONSTRUCT(
        'table', :source_table,
        'analysis_timestamp', CURRENT_TIMESTAMP(),
        'status', 'analyzed'
    );
END;
$$;

-- 3. GENERATE TRANSFORMATION SQL: AI-powered transformation generation
CREATE OR REPLACE PROCEDURE AGENTS.GENERATE_TRANSFORMATION(
    source_table VARCHAR,
    target_schema VARCHAR DEFAULT 'SILVER',
    transformation_type VARCHAR DEFAULT 'flatten_and_type'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    sql_output VARCHAR;
    target_table VARCHAR;
    source_schema_json VARCHAR;
BEGIN
    -- Extract table name from FQN
    target_table := SPLIT_PART(:source_table, '.', -1);
    target_table := REPLACE(target_table, 'RAW_', '');
    
    -- Generate flattening transformation
    sql_output := 'CREATE OR REPLACE DYNAMIC TABLE ' || :target_schema || '.' || :target_table || '\n' ||
                  'TARGET_LAG = ''1 hour''\n' ||
                  'WAREHOUSE = SNOWADHOC\n' ||
                  'AS\n' ||
                  'SELECT\n' ||
                  '    _metadata_load_ts,\n' ||
                  '    _metadata_source,\n' ||
                  '    raw_payload:id::VARCHAR as id,\n' ||
                  '    raw_payload:* \n' ||
                  'FROM ' || :source_table || ';';
    
    RETURN :sql_output;
END;
$$;

-- 4. CORTEX-POWERED SCHEMA INFERENCE
CREATE OR REPLACE PROCEDURE AGENTS.CORTEX_INFER_SCHEMA(
    source_table VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    sample_data VARCHAR;
    llm_response VARCHAR;
    prompt VARCHAR;
BEGIN
    -- Get sample JSON
    EXECUTE IMMEDIATE 'SELECT TOP 1 raw_payload::VARCHAR FROM ' || :source_table INTO :sample_data;
    
    -- Build prompt for Cortex
    prompt := 'Analyze this JSON structure and return a SQL CREATE TABLE statement with appropriate Snowflake data types. JSON: ' || :sample_data;
    
    -- Call Cortex LLM
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :prompt) INTO :llm_response;
    
    RETURN OBJECT_CONSTRUCT(
        'source_table', :source_table,
        'sample_data', PARSE_JSON(:sample_data),
        'recommended_schema', :llm_response,
        'generated_at', CURRENT_TIMESTAMP()
    );
END;
$$;
