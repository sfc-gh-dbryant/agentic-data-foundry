-- ============================================================================
-- 08_AGENTS: Agentic Procedures with Cortex LLM
-- ============================================================================
-- Purpose: Create AI-powered procedures for schema discovery and transformation
-- LLMs Used: claude-3-5-sonnet, llama3.1-8b
-- Run as: ACCOUNTADMIN
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- METADATA TABLES
-- ============================================================================
CREATE TABLE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG (
    transformation_id VARCHAR DEFAULT UUID_STRING(),
    source_table VARCHAR,
    target_table VARCHAR,
    transformation_sql VARCHAR,
    agent_reasoning VARCHAR,
    status VARCHAR DEFAULT 'PENDING',
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    executed_at TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LOG (
    workflow_id VARCHAR,
    workflow_type VARCHAR,
    source_table VARCHAR,
    status VARCHAR,
    details VARIANT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.WORKFLOW_STATE (
    workflow_id VARCHAR PRIMARY KEY,
    workflow_name VARCHAR,
    trigger_source VARCHAR,
    trigger_type VARCHAR,
    status VARCHAR DEFAULT 'PENDING',
    current_step VARCHAR,
    steps_completed ARRAY,
    context VARIANT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.AGENT_REFLECTIONS (
    reflection_id VARCHAR DEFAULT UUID_STRING(),
    workflow_id VARCHAR,
    reflection_type VARCHAR,
    source_context VARCHAR,
    observation VARCHAR,
    recommendation VARCHAR,
    confidence_score NUMBER(3,2),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- AGENT: ANALYZE_GOLD_SCHEMA
-- Purpose: Introspect Gold table columns for semantic view generation
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.ANALYZE_GOLD_SCHEMA(gold_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    table_name_var VARCHAR;
    result VARIANT;
BEGIN
    table_name_var := UPPER(SPLIT_PART(:gold_table, '.', -1));
    
    SELECT OBJECT_CONSTRUCT(
        'table', :gold_table,
        'columns', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'name', COLUMN_NAME,
                'data_type', DATA_TYPE,
                'is_nullable', IS_NULLABLE
            )
        ),
        'analyzed_at', CURRENT_TIMESTAMP()
    ) INTO :result
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = :table_name_var;
    
    RETURN :result;
END;
$$;

-- ============================================================================
-- AGENT: CORTEX_INFER_SCHEMA
-- Purpose: Use LLM to infer schema from JSON sample
-- LLM: llama3.1-8b (fast, cost-effective for schema inference)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.CORTEX_INFER_SCHEMA(source_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    sample_data VARCHAR;
    llm_response VARCHAR;
    prompt VARCHAR;
BEGIN
    LET rs RESULTSET := (EXECUTE IMMEDIATE 'SELECT TOP 1 payload::VARCHAR FROM ' || :source_table);
    LET cur CURSOR FOR rs;
    FOR row_var IN cur DO
        sample_data := row_var[0];
    END FOR;
    
    prompt := 'Analyze this JSON and return a Snowflake CREATE TABLE statement with appropriate types. Be concise. JSON: ' || :sample_data;
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :prompt) INTO :llm_response FROM DUAL;
    
    RETURN OBJECT_CONSTRUCT(
        'source_table', :source_table,
        'sample_data', TRY_PARSE_JSON(:sample_data),
        'recommended_schema', :llm_response,
        'generated_at', CURRENT_TIMESTAMP()
    );
END;
$$;

-- ============================================================================
-- AGENT: INFER_SEMANTIC_CONTEXT
-- Purpose: Use LLM to generate business descriptions and synonyms for columns
-- LLM: claude-3-5-sonnet (high quality for semantic understanding)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.INFER_SEMANTIC_CONTEXT(gold_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    schema_info VARIANT;
    prompt VARCHAR;
    llm_response VARCHAR;
BEGIN
    CALL DBAONTAP_ANALYTICS.AGENTS.ANALYZE_GOLD_SCHEMA(:gold_table) INTO :schema_info;
    
    prompt := 'You are a data analyst. Given these columns from a ' || :gold_table || ' table: ' ||
              :schema_info::VARCHAR ||
              '. For each column, provide: 1) A business-friendly description, 2) Synonyms users might use, 3) Sample values if categorical. Return as JSON array.';
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', :prompt) INTO :llm_response;
    
    RETURN OBJECT_CONSTRUCT(
        'table', :gold_table,
        'schema', :schema_info,
        'semantic_context', TRY_PARSE_JSON(:llm_response),
        'generated_at', CURRENT_TIMESTAMP()
    );
END;
$$;

-- ============================================================================
-- AGENT: GENERATE_SEMANTIC_VIEW
-- Purpose: Use LLM to generate semantic view DDL and execute it
-- LLM: claude-3-5-sonnet (high quality for DDL generation)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.GENERATE_SEMANTIC_VIEW(
    gold_table VARCHAR,
    semantic_view_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    schema_info VARIANT;
    prompt VARCHAR;
    llm_response VARCHAR;
    ddl_sql VARCHAR;
    table_alias VARCHAR;
BEGIN
    CALL DBAONTAP_ANALYTICS.AGENTS.ANALYZE_GOLD_SCHEMA(:gold_table) INTO :schema_info;
    table_alias := UPPER(SPLIT_PART(:gold_table, '.', -1));
    
    prompt := 'Generate a Snowflake SEMANTIC VIEW DDL for ' || :gold_table || '.

Columns: ' || :schema_info:columns::VARCHAR || '

RULES:
1. Each column in ONLY ONE section
2. Format:

CREATE OR REPLACE SEMANTIC VIEW DBAONTAP_ANALYTICS.GOLD.' || :semantic_view_name || '
TABLES (' || :gold_table || ' PRIMARY KEY (CUSTOMER_ID))
FACTS (' || :table_alias || '.COLUMN AS alias)
DIMENSIONS (' || :table_alias || '.COLUMN AS alias)
METRICS (' || :table_alias || '.metric AS AGG_FUNC(col));

FACTS=numeric IDs, DIMENSIONS=text/dates, METRICS=aggregations.
Return ONLY SQL.';

    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', :prompt) INTO :llm_response;
    
    ddl_sql := REGEXP_REPLACE(:llm_response, '^```sql\n|\n```$', '');
    ddl_sql := REGEXP_REPLACE(:ddl_sql, '^```\n|\n```$', '');
    ddl_sql := TRIM(:ddl_sql);
    
    EXECUTE IMMEDIATE :ddl_sql;
    
    INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
        (source_table, target_table, transformation_sql, agent_reasoning, status, executed_at)
    VALUES (:gold_table, :semantic_view_name, :ddl_sql, 'LLM-generated semantic view', 'SUCCESS', CURRENT_TIMESTAMP());
    
    RETURN 'SUCCESS: Agent created ' || :semantic_view_name;
END;
$$;

-- ============================================================================
-- AGENT: ANALYZE_DATA_QUALITY
-- Purpose: Analyze data quality metrics for a source table
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.ANALYZE_DATA_QUALITY(
    source_table VARCHAR, 
    analysis_depth NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    stats_row VARIANT;
BEGIN
    LET rs RESULTSET := (EXECUTE IMMEDIATE 
        'SELECT OBJECT_CONSTRUCT(''total_rows'', COUNT(*), ''null_payloads'', COUNT_IF(payload IS NULL)) FROM ' || :source_table);
    LET cur CURSOR FOR rs;
    FOR row_var IN cur DO
        stats_row := row_var[0];
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'table', :source_table,
        'stats', :stats_row,
        'analysis_timestamp', CURRENT_TIMESTAMP(),
        'status', 'analyzed'
    );
END;
$$;

-- ============================================================================
-- AGENT: REFLECT_ON_WORKFLOW
-- Purpose: Analyze completed workflows for learnings
-- LLM: llama3.1-8b (cost-effective for reflection tasks)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.REFLECT_ON_WORKFLOW(workflow_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    workflow_json VARCHAR;
    llm_reflection VARCHAR;
    prompt VARCHAR;
BEGIN
    SELECT TO_JSON(OBJECT_CONSTRUCT(*))::VARCHAR INTO :workflow_json
    FROM METADATA.WORKFLOW_STATE
    WHERE workflow_id = :workflow_id;
    
    prompt := 'Analyze this data transformation workflow and provide learnings. What patterns can be reused? What could be optimized? ' || :workflow_json;
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :prompt) INTO :llm_reflection FROM DUAL;
    
    INSERT INTO METADATA.AGENT_REFLECTIONS (workflow_id, reflection_type, source_context, observation, recommendation, confidence_score)
    VALUES (:workflow_id, 'workflow_analysis', :workflow_json, :llm_reflection, 'Apply learnings to future workflows', 0.8);
    
    RETURN OBJECT_CONSTRUCT(
        'workflow_id', :workflow_id,
        'reflection', :llm_reflection,
        'stored', TRUE
    );
END;
$$;

-- ============================================================================
-- AGENT: DISCOVER_BRONZE_SCHEMA (Python UDF)
-- Purpose: Introspect VARIANT payloads to discover schema
-- ============================================================================
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.DISCOVER_BRONZE_SCHEMA(table_name VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'discover'
AS
$$
import json

def discover(session, table_name):
    # Get sample row
    sample = session.sql(f'SELECT payload FROM {table_name} LIMIT 1').collect()
    if not sample:
        return {'error': 'No data found'}
    
    payload = json.loads(str(sample[0]['PAYLOAD']))
    
    # Analyze schema
    schema = {}
    for key, value in payload.items():
        schema[key] = {
            'inferred_type': type(value).__name__,
            'sample_value': str(value)[:50] if value else None
        }
    
    # Get count
    count = session.sql(f'SELECT COUNT(*) as cnt FROM {table_name}').collect()[0]['CNT']
    
    return {
        'table_name': table_name,
        'row_count': count,
        'discovered_columns': schema,
        'column_count': len(schema)
    }
$$;

-- ============================================================================
-- Verification
-- ============================================================================
-- SHOW PROCEDURES IN SCHEMA DBAONTAP_ANALYTICS.AGENTS;
