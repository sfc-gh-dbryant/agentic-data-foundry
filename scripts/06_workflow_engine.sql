-- =============================================================================
-- AGENTIC WORKFLOW ENGINE
-- Stream-based triggers, Task orchestration, Reflection loops
-- =============================================================================

USE DATABASE AGENTIC_PIPELINE;

-- ============================================================================
-- PART 1: WORKFLOW TRIGGERS (Streams on Bronze)
-- ============================================================================

-- Stream to detect new data in Bronze tables
CREATE OR REPLACE STREAM METADATA.BRONZE_CUSTOMERS_STREAM 
    ON TABLE BRONZE.RAW_CUSTOMERS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM METADATA.BRONZE_ORDERS_STREAM 
    ON TABLE BRONZE.RAW_ORDERS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM METADATA.BRONZE_PRODUCTS_STREAM 
    ON TABLE BRONZE.RAW_PRODUCTS
    APPEND_ONLY = TRUE;

-- Workflow state table
CREATE OR REPLACE TABLE METADATA.WORKFLOW_STATE (
    workflow_id VARCHAR DEFAULT UUID_STRING(),
    workflow_name VARCHAR,
    trigger_source VARCHAR,
    trigger_type VARCHAR,  -- 'data_arrival', 'schema_change', 'quality_alert', 'manual'
    status VARCHAR DEFAULT 'PENDING',  -- PENDING, RUNNING, COMPLETED, FAILED
    current_step VARCHAR,
    steps_completed ARRAY,
    agent_reasoning TEXT,
    started_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at TIMESTAMP_LTZ,
    error_message TEXT
);

-- ============================================================================
-- PART 2: WORKFLOW ORCHESTRATION PROCEDURES
-- ============================================================================

-- Master workflow procedure - orchestrates the full Bronze→Silver transformation
CREATE OR REPLACE PROCEDURE AGENTS.RUN_TRANSFORMATION_WORKFLOW(
    source_table VARCHAR,
    transformation_type VARCHAR DEFAULT 'flatten_and_type'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    workflow_id VARCHAR;
    step_result VARIANT;
    schema_analysis VARIANT;
    quality_analysis VARIANT;
    generated_sql VARCHAR;
    final_result VARIANT;
BEGIN
    -- Step 0: Initialize workflow
    workflow_id := UUID_STRING();
    INSERT INTO METADATA.WORKFLOW_STATE (workflow_id, workflow_name, trigger_source, trigger_type, status, current_step)
    VALUES (:workflow_id, 'Bronze to Silver Transformation', :source_table, 'manual', 'RUNNING', 'DISCOVER');
    
    -- Step 1: DISCOVER - Analyze Bronze schema
    UPDATE METADATA.WORKFLOW_STATE SET current_step = 'DISCOVER' WHERE workflow_id = :workflow_id;
    
    CALL AGENTS.CORTEX_INFER_SCHEMA(:source_table);
    LET schema_result RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    
    -- Step 2: ANALYZE - Check data quality
    UPDATE METADATA.WORKFLOW_STATE 
    SET current_step = 'ANALYZE', 
        steps_completed = ARRAY_APPEND(COALESCE(steps_completed, ARRAY_CONSTRUCT()), 'DISCOVER')
    WHERE workflow_id = :workflow_id;
    
    CALL AGENTS.ANALYZE_DATA_QUALITY(:source_table, 1000);
    
    -- Step 3: TRANSFORM - Generate and execute Dynamic Table
    UPDATE METADATA.WORKFLOW_STATE 
    SET current_step = 'TRANSFORM',
        steps_completed = ARRAY_APPEND(steps_completed, 'ANALYZE')
    WHERE workflow_id = :workflow_id;
    
    CALL AGENTS.GENERATE_TRANSFORMATION(:source_table, 'SILVER', :transformation_type);
    SELECT * INTO :generated_sql FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    -- Log the transformation
    INSERT INTO METADATA.TRANSFORMATION_LOG (source_table, target_table, transformation_sql, agent_reasoning, status)
    VALUES (:source_table, 'SILVER.' || REPLACE(SPLIT_PART(:source_table, '.', -1), 'RAW_', ''), 
            :generated_sql, 'Automated workflow execution', 'PENDING');
    
    -- Step 4: VALIDATE - (placeholder for validation logic)
    UPDATE METADATA.WORKFLOW_STATE 
    SET current_step = 'VALIDATE',
        steps_completed = ARRAY_APPEND(steps_completed, 'TRANSFORM')
    WHERE workflow_id = :workflow_id;
    
    -- Step 5: Complete workflow
    UPDATE METADATA.WORKFLOW_STATE 
    SET status = 'COMPLETED',
        current_step = 'DONE',
        steps_completed = ARRAY_APPEND(steps_completed, 'VALIDATE'),
        completed_at = CURRENT_TIMESTAMP()
    WHERE workflow_id = :workflow_id;
    
    RETURN OBJECT_CONSTRUCT(
        'workflow_id', :workflow_id,
        'source_table', :source_table,
        'transformation_type', :transformation_type,
        'status', 'COMPLETED',
        'generated_sql', :generated_sql
    );
    
EXCEPTION
    WHEN OTHER THEN
        UPDATE METADATA.WORKFLOW_STATE 
        SET status = 'FAILED', error_message = SQLERRM
        WHERE workflow_id = :workflow_id;
        RETURN OBJECT_CONSTRUCT('error', SQLERRM, 'workflow_id', :workflow_id);
END;
$$;

-- ============================================================================
-- PART 3: TASK-BASED AUTOMATION (Event-Driven)
-- ============================================================================

-- Task to check for new Bronze data and trigger workflows
CREATE OR REPLACE TASK METADATA.BRONZE_WATCHER_TASK
    WAREHOUSE = SNOWADHOC
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('METADATA.BRONZE_CUSTOMERS_STREAM')
       OR SYSTEM$STREAM_HAS_DATA('METADATA.BRONZE_ORDERS_STREAM')
       OR SYSTEM$STREAM_HAS_DATA('METADATA.BRONZE_PRODUCTS_STREAM')
AS
BEGIN
    -- Log trigger event
    INSERT INTO METADATA.WORKFLOW_STATE (workflow_name, trigger_type, status, current_step, agent_reasoning)
    VALUES ('Auto-Triggered Transformation Check', 'data_arrival', 'TRIGGERED', 'EVALUATION',
            'New data detected in Bronze streams. Evaluating transformation requirements.');
    
    -- Consume streams (in production, would call transformation workflows)
    -- For now, just acknowledge the new data
    INSERT INTO METADATA.TRANSFORMATION_LOG (source_table, target_table, transformation_sql, agent_reasoning, status)
    SELECT 'BRONZE.RAW_CUSTOMERS', 'SILVER.CUSTOMERS', 'Auto-triggered', 
           'Stream detected ' || COUNT(*) || ' new records', 'TRIGGERED'
    FROM METADATA.BRONZE_CUSTOMERS_STREAM;
END;

-- ============================================================================
-- PART 4: REFLECTION & FEEDBACK LOOP
-- ============================================================================

-- Table to capture agent reflections and learnings
CREATE OR REPLACE TABLE METADATA.AGENT_REFLECTIONS (
    reflection_id VARCHAR DEFAULT UUID_STRING(),
    workflow_id VARCHAR,
    reflection_type VARCHAR,  -- 'success_pattern', 'failure_pattern', 'optimization_opportunity'
    source_context VARCHAR,
    observation TEXT,
    recommendation TEXT,
    confidence_score FLOAT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Procedure for agent self-reflection after workflow completion
CREATE OR REPLACE PROCEDURE AGENTS.REFLECT_ON_WORKFLOW(workflow_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    workflow_record VARIANT;
    reflection_prompt VARCHAR;
    llm_reflection VARCHAR;
BEGIN
    -- Get workflow details
    SELECT OBJECT_CONSTRUCT(*) INTO :workflow_record
    FROM METADATA.WORKFLOW_STATE
    WHERE workflow_id = :workflow_id;
    
    -- Generate reflection using Cortex
    reflection_prompt := 'Analyze this data transformation workflow and provide learnings: ' || :workflow_record::VARCHAR ||
                        '. What patterns can be reused? What could be optimized? Format as JSON with keys: patterns, optimizations, confidence.';
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :reflection_prompt) INTO :llm_reflection;
    
    -- Store reflection
    INSERT INTO METADATA.AGENT_REFLECTIONS (workflow_id, reflection_type, source_context, observation, recommendation, confidence_score)
    VALUES (:workflow_id, 'workflow_analysis', :workflow_record::VARCHAR, :llm_reflection, 
            'Review and apply learnings to future workflows', 0.8);
    
    RETURN PARSE_JSON(:llm_reflection);
END;
$$;

-- ============================================================================
-- PART 5: SEMANTIC LAYER INTEGRATION
-- ============================================================================

-- Create semantic views for each layer (for Cortex Analyst)

-- Bronze Semantic View
CREATE OR REPLACE VIEW METADATA.BRONZE_CATALOG AS
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as fqn,
    table_name,
    row_count,
    bytes,
    created as created_at,
    last_altered,
    comment
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'BRONZE'
  AND table_type = 'BASE TABLE';

-- Silver Semantic View  
CREATE OR REPLACE VIEW METADATA.SILVER_CATALOG AS
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as fqn,
    table_name,
    CASE WHEN table_type = 'DYNAMIC TABLE' THEN 'Dynamic Table' ELSE table_type END as object_type,
    row_count,
    bytes,
    created as created_at,
    last_altered
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SILVER';

-- Gold Semantic View
CREATE OR REPLACE VIEW METADATA.GOLD_CATALOG AS
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as fqn,
    table_name,
    'Business Analytics' as layer,
    row_count,
    bytes,
    created as created_at
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'GOLD';

-- Lineage View
CREATE OR REPLACE VIEW METADATA.TRANSFORMATION_LINEAGE AS
SELECT 
    transformation_id,
    source_table,
    target_table,
    transformation_sql,
    agent_reasoning,
    status,
    created_at,
    executed_at
FROM METADATA.TRANSFORMATION_LOG
ORDER BY created_at DESC;

-- ============================================================================
-- PART 6: ENABLE TASKS (Run this manually to activate automation)
-- ============================================================================

-- ALTER TASK METADATA.BRONZE_WATCHER_TASK RESUME;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Run a manual transformation workflow:
-- CALL AGENTS.RUN_TRANSFORMATION_WORKFLOW('AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS', 'flatten_and_type');

-- Check workflow status:
-- SELECT * FROM METADATA.WORKFLOW_STATE ORDER BY started_at DESC;

-- View transformation lineage:
-- SELECT * FROM METADATA.TRANSFORMATION_LINEAGE;

-- Trigger reflection on completed workflow:
-- CALL AGENTS.REFLECT_ON_WORKFLOW('<workflow_id>');
