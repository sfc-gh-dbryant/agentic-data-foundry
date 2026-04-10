-- =============================================================================
-- AGENTIC WORKFLOW ENGINE v2.0
-- Full implementation of: TRIGGER → PLANNER → EXECUTOR → VALIDATOR → REFLECTOR
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;

-- ============================================================================
-- PART 0: WORKFLOW METADATA TABLES
-- ============================================================================

-- Workflow execution state tracking
CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_EXECUTIONS (
    execution_id VARCHAR DEFAULT UUID_STRING(),
    workflow_name VARCHAR NOT NULL,
    trigger_source VARCHAR,
    trigger_type VARCHAR,  -- 'data_arrival', 'schema_change', 'quality_alert', 'manual', 'scheduled'
    status VARCHAR DEFAULT 'PENDING',  -- PENDING, PLANNING, EXECUTING, VALIDATING, REFLECTING, COMPLETED, FAILED
    current_phase VARCHAR,
    
    -- Phase outputs
    planner_output VARIANT,
    executor_output VARIANT,
    validator_output VARIANT,
    reflector_output VARIANT,
    
    -- Timing
    started_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    planning_completed_at TIMESTAMP_LTZ,
    execution_completed_at TIMESTAMP_LTZ,
    validation_completed_at TIMESTAMP_LTZ,
    reflection_completed_at TIMESTAMP_LTZ,
    completed_at TIMESTAMP_LTZ,
    
    -- Error handling
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    last_error TEXT,
    
    PRIMARY KEY (execution_id)
);

-- Planner decisions log
CREATE TABLE IF NOT EXISTS METADATA.PLANNER_DECISIONS (
    decision_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    source_table VARCHAR,
    target_schema VARCHAR,
    transformation_strategy VARCHAR,  -- 'flatten_and_type', 'deduplicate', 'scd_type2', 'aggregate', 'normalize'
    detected_patterns VARIANT,        -- JSON of detected data patterns
    recommended_actions ARRAY,        -- Array of recommended transformations
    priority INTEGER,                 -- Execution priority (1=highest)
    llm_reasoning TEXT,              -- Full LLM explanation
    confidence_score FLOAT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Validation results
CREATE TABLE IF NOT EXISTS METADATA.VALIDATION_RESULTS (
    validation_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    source_table VARCHAR,
    target_table VARCHAR,
    validation_type VARCHAR,  -- 'row_count', 'schema_match', 'data_quality', 'referential_integrity'
    expected_value VARIANT,
    actual_value VARIANT,
    passed BOOLEAN,
    variance_pct FLOAT,
    details TEXT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Reflector learnings (persisted knowledge)
CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_LEARNINGS (
    learning_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    learning_type VARCHAR,  -- 'success_pattern', 'failure_pattern', 'optimization', 'schema_pattern'
    source_context VARCHAR,
    pattern_signature VARCHAR,  -- Hash/signature for pattern matching
    observation TEXT,
    recommendation TEXT,
    times_observed INTEGER DEFAULT 1,
    last_observed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    confidence_score FLOAT,
    is_active BOOLEAN DEFAULT TRUE
);

-- Workflow log for debugging/monitoring
CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_LOG (
    log_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    phase VARCHAR,
    status VARCHAR,
    message TEXT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- PART 1: TRIGGER PHASE
-- Detects events that should initiate transformation workflows
-- NOW INCLUDES: Auto-discovery and onboarding of new landing tables
-- ============================================================================

-- Track onboarded tables
CREATE TABLE IF NOT EXISTS METADATA.ONBOARDED_TABLES (
    table_name VARCHAR PRIMARY KEY,
    landing_table VARCHAR,
    stream_name VARCHAR,
    bronze_table VARCHAR,
    onboarded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    onboarded_by VARCHAR DEFAULT 'AGENTIC_WORKFLOW'
);

-- Helper procedure: Onboard a single new landing table
CREATE OR REPLACE PROCEDURE AGENTS.AUTO_ONBOARD_TABLE(landing_table_name VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    table_base_name VARCHAR;
    stream_name VARCHAR;
    bronze_dt_name VARCHAR;
    create_stream_sql VARCHAR;
    create_bronze_sql VARCHAR;
BEGIN
    -- Extract base name (e.g., 'customers' from 'public.customers')
    table_base_name := UPPER(SPLIT_PART(landing_table_name, '.', -1));
    stream_name := table_base_name || '_LANDING_STREAM';
    bronze_dt_name := table_base_name || '_VARIANT';
    
    -- Create stream on landing table
    create_stream_sql := 'CREATE STREAM IF NOT EXISTS AGENTS.' || stream_name || 
                         ' ON TABLE ' || landing_table_name || 
                         ' SHOW_INITIAL_ROWS = TRUE';
    EXECUTE IMMEDIATE :create_stream_sql;
    
    -- Create Bronze Dynamic Table with VARIANT payload
    create_bronze_sql := '
        CREATE OR REPLACE DYNAMIC TABLE BRONZE.' || bronze_dt_name || '
        TARGET_LAG = ''1 minute''
        WAREHOUSE = DBRYANT_COCO_WH_S
        AS
        SELECT 
            OBJECT_CONSTRUCT(*) AS PAYLOAD,
            ''' || table_base_name || ''' AS SOURCE_TABLE,
            CURRENT_TIMESTAMP() AS INGESTED_AT
        FROM ' || landing_table_name;
    EXECUTE IMMEDIATE :create_bronze_sql;
    
    -- Track in metadata
    INSERT INTO METADATA.ONBOARDED_TABLES (table_name, landing_table, stream_name, bronze_table)
    SELECT :table_base_name, :landing_table_name, :stream_name, 'BRONZE.' || :bronze_dt_name
    WHERE NOT EXISTS (SELECT 1 FROM METADATA.ONBOARDED_TABLES WHERE table_name = :table_base_name);
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'ONBOARDED',
        'landing_table', landing_table_name,
        'stream', stream_name,
        'bronze_table', 'BRONZE.' || bronze_dt_name
    );
END;
$$;

-- Helper procedure: Discover and onboard all new landing tables
CREATE OR REPLACE PROCEDURE AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    new_tables ARRAY;
    onboarded_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    current_table VARCHAR;
    onboard_result VARIANT;
    i INTEGER;
BEGIN
    -- Find landing tables (public.*) that don't have corresponding Bronze _VARIANT tables
    -- EXCLUDE OpenFlow journal tables (contain 'JOURNAL_')
    SELECT ARRAY_AGG(landing.TABLE_CATALOG || '.' || landing.TABLE_SCHEMA || '.' || landing.TABLE_NAME)
    INTO :new_tables
    FROM INFORMATION_SCHEMA.TABLES landing
    WHERE landing.TABLE_SCHEMA = 'public'
      AND landing.TABLE_TYPE = 'BASE TABLE'
      AND landing.TABLE_NAME NOT LIKE '%JOURNAL%'
      AND NOT EXISTS (
          SELECT 1 FROM INFORMATION_SCHEMA.TABLES bronze
          WHERE bronze.TABLE_SCHEMA = 'BRONZE'
            AND bronze.TABLE_NAME = UPPER(landing.TABLE_NAME) || '_VARIANT'
      );
    
    -- If no new tables, return early
    IF (new_tables IS NULL OR ARRAY_SIZE(new_tables) = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'NO_NEW_TABLES',
            'message', 'All landing tables already have Bronze counterparts'
        );
    END IF;
    
    -- Onboard each new table
    FOR i IN 0 TO ARRAY_SIZE(new_tables) - 1 DO
        current_table := new_tables[i]::VARCHAR;
        
        BEGIN
            onboard_result := (CALL AGENTS.AUTO_ONBOARD_TABLE(:current_table));
            onboarded_results := ARRAY_APPEND(onboarded_results, onboard_result);
        EXCEPTION WHEN OTHER THEN
            onboarded_results := ARRAY_APPEND(onboarded_results, OBJECT_CONSTRUCT(
                'status', 'FAILED',
                'table', current_table,
                'error', SQLERRM
            ));
        END;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'DISCOVERY_COMPLETE',
        'new_tables_found', ARRAY_SIZE(new_tables),
        'onboarded', onboarded_results
    );
END;
$$;

-- Main TRIGGER procedure with auto-discovery
CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_TRIGGER(
    trigger_type VARCHAR,           -- 'manual', 'stream', 'scheduled', 'quality_alert'
    source_tables ARRAY DEFAULT NULL -- Optional: specific tables to process
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    execution_id VARCHAR;
    tables_to_process ARRAY;
    trigger_details VARIANT;
    trigger_source_str VARCHAR;
    discovery_result VARIANT;
    new_tables_onboarded INTEGER DEFAULT 0;
BEGIN
    execution_id := UUID_STRING();
    
    -- =========================================================================
    -- PHASE 0: AUTO-DISCOVERY - Check for new landing tables and onboard them
    -- =========================================================================
    discovery_result := (CALL AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES());
    IF (discovery_result:status::VARCHAR = 'DISCOVERY_COMPLETE') THEN
        new_tables_onboarded := discovery_result:new_tables_found::INTEGER;
    END IF;
    
    -- =========================================================================
    -- PHASE 1: Determine which tables need processing based on trigger type
    -- =========================================================================
    CASE trigger_type
        WHEN 'manual' THEN
            -- Use provided tables or discover all Bronze _VARIANT tables
            IF (source_tables IS NULL OR ARRAY_SIZE(source_tables) = 0) THEN
                SELECT ARRAY_AGG(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME)
                INTO :tables_to_process
                FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'BRONZE' 
                  AND TABLE_NAME LIKE '%_VARIANT'
                  AND TABLE_TYPE = 'BASE TABLE';
            ELSE
                tables_to_process := source_tables;
            END IF;
            
        WHEN 'stream' THEN
            -- Check known streams for data (static list - auto-discovery handles new tables)
            tables_to_process := ARRAY_CONSTRUCT();
            
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.CUSTOMERS_LANDING_STREAM')) THEN
                tables_to_process := ARRAY_APPEND(tables_to_process, 'DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.ORDERS_LANDING_STREAM')) THEN
                tables_to_process := ARRAY_APPEND(tables_to_process, 'DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.ORDER_ITEMS_LANDING_STREAM')) THEN
                tables_to_process := ARRAY_APPEND(tables_to_process, 'DBAONTAP_ANALYTICS.BRONZE.ORDER_ITEMS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.PRODUCTS_LANDING_STREAM')) THEN
                tables_to_process := ARRAY_APPEND(tables_to_process, 'DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.SUPPORT_TICKETS_LANDING_STREAM')) THEN
                tables_to_process := ARRAY_APPEND(tables_to_process, 'DBAONTAP_ANALYTICS.BRONZE.SUPPORT_TICKETS_VARIANT');
            END IF;
            
        WHEN 'scheduled' THEN
            -- Process all Bronze _VARIANT tables on schedule
            SELECT ARRAY_AGG(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME)
            INTO :tables_to_process
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'BRONZE' 
              AND TABLE_NAME LIKE '%_VARIANT'
              AND TABLE_TYPE = 'BASE TABLE';
            
        ELSE
            tables_to_process := COALESCE(source_tables, ARRAY_CONSTRUCT());
    END CASE;
    
    -- =========================================================================
    -- PHASE 2: Create workflow execution record
    -- =========================================================================
    trigger_details := OBJECT_CONSTRUCT(
        'trigger_type', trigger_type,
        'tables_count', ARRAY_SIZE(tables_to_process),
        'tables', tables_to_process,
        'new_tables_onboarded', new_tables_onboarded,
        'triggered_at', CURRENT_TIMESTAMP()::VARCHAR
    );
    
    trigger_source_str := ARRAY_TO_STRING(tables_to_process, ', ');
    
    INSERT INTO METADATA.WORKFLOW_EXECUTIONS (
        execution_id, workflow_name, trigger_source, trigger_type, status, current_phase, planner_output
    )
    SELECT 
        :execution_id, 
        'Bronze to Silver Transformation', 
        :trigger_source_str,
        :trigger_type,
        'TRIGGERED',
        'TRIGGER',
        :trigger_details;
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', 'TRIGGERED',
        'tables_to_process', tables_to_process,
        'new_tables_onboarded', new_tables_onboarded,
        'discovery_result', discovery_result,
        'next_phase', 'PLANNER'
    );
END;
$$;

-- ============================================================================
-- PART 2: PLANNER PHASE
-- Uses LLM to analyze tables and decide transformation strategy
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_PLANNER(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    tables_to_process ARRAY;
    current_table VARCHAR;
    schema_info VARIANT;
    quality_info VARIANT;
    existing_learnings VARCHAR;
    planner_prompt VARCHAR;
    llm_response VARCHAR;
    parsed_plan VARIANT;
    all_decisions ARRAY DEFAULT ARRAY_CONSTRUCT();
    i INTEGER;
BEGIN
    -- Update status
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'PLANNING', current_phase = 'PLANNER'
    WHERE execution_id = :execution_id;
    
    -- Get tables from trigger phase
    SELECT planner_output:tables INTO :tables_to_process
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;
    
    -- Get relevant learnings from past workflows
    SELECT LISTAGG(observation || ' -> ' || recommendation, '; ') 
    INTO :existing_learnings
    FROM METADATA.WORKFLOW_LEARNINGS
    WHERE is_active = TRUE AND confidence_score > 0.7
    LIMIT 5;
    
    -- Process each table
    FOR i IN 0 TO ARRAY_SIZE(tables_to_process) - 1 DO
        current_table := tables_to_process[i]::VARCHAR;
        
        -- Get schema info
        schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:current_table));
        
        -- Get quality info (sample)
        quality_info := (CALL AGENTS.ANALYZE_DATA_QUALITY(:current_table, 500));
        
        -- Build planner prompt
        planner_prompt := '
You are the PLANNER agent in an agentic data transformation workflow.

TASK: Analyze this Bronze table and decide the transformation strategy for Silver layer.

SOURCE TABLE: ' || current_table || '

SCHEMA ANALYSIS:
' || schema_info::VARCHAR || '

DATA QUALITY ANALYSIS:
' || quality_info::VARCHAR || '

PAST LEARNINGS (apply if relevant):
' || COALESCE(existing_learnings, 'No prior learnings') || '

AVAILABLE STRATEGIES:
1. flatten_and_type - Extract VARIANT fields, apply proper types, handle nulls
2. deduplicate - Remove duplicates based on key columns
3. scd_type2 - Slowly changing dimension with history tracking
4. aggregate - Pre-aggregate for performance
5. normalize - Split nested arrays into separate tables

OUTPUT FORMAT (JSON only, no explanation):
{
  "source_table": "...",
  "target_table": "SILVER.<name>",
  "strategy": "<strategy_name>",
  "detected_patterns": {
    "has_nested_arrays": true/false,
    "has_null_issues": true/false,
    "needs_type_casting": true/false,
    "has_duplicates": true/false
  },
  "transformations": [
    {"column": "...", "action": "...", "reason": "..."}
  ],
  "priority": 1-5,
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation"
}';

        -- Call LLM for planning decision
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', :planner_prompt) INTO :llm_response;
        
        -- Parse response (extract JSON - handle nested objects)
        BEGIN
            LET json_start INTEGER := POSITION('{' IN llm_response);
            LET json_end INTEGER := 0;
            LET brace_count INTEGER := 0;
            LET i INTEGER := json_start;
            LET response_len INTEGER := LENGTH(llm_response);
            
            WHILE (i <= response_len AND (json_end = 0 OR brace_count > 0)) DO
                IF (SUBSTR(llm_response, i, 1) = '{') THEN
                    brace_count := brace_count + 1;
                ELSEIF (SUBSTR(llm_response, i, 1) = '}') THEN
                    brace_count := brace_count - 1;
                    IF (brace_count = 0) THEN
                        json_end := i;
                    END IF;
                END IF;
                i := i + 1;
            END WHILE;
            
            IF (json_start > 0 AND json_end > json_start) THEN
                parsed_plan := PARSE_JSON(SUBSTR(llm_response, json_start, json_end - json_start + 1));
            ELSE
                parsed_plan := NULL;
            END IF;
        EXCEPTION WHEN OTHER THEN
            parsed_plan := OBJECT_CONSTRUCT(
                'source_table', current_table,
                'target_table', 'SILVER.' || REPLACE(SPLIT_PART(current_table, '.', -1), 'RAW_', ''),
                'strategy', 'flatten_and_type',
                'confidence', 0.5,
                'reasoning', 'Default strategy due to parse error: ' || SUBSTR(llm_response, 1, 500)
            );
        END;
        
        -- Log planner decision
        INSERT INTO METADATA.PLANNER_DECISIONS (
            execution_id, source_table, target_schema, transformation_strategy,
            detected_patterns, recommended_actions, priority, llm_reasoning, confidence_score
        )
        SELECT
            :execution_id,
            :current_table,
            'SILVER',
            :parsed_plan:strategy::VARCHAR,
            :parsed_plan:detected_patterns,
            :parsed_plan:transformations,
            COALESCE(:parsed_plan:priority::INTEGER, 3),
            :parsed_plan:reasoning::VARCHAR,
            COALESCE(:parsed_plan:confidence::FLOAT, 0.5);
        
        all_decisions := ARRAY_APPEND(all_decisions, parsed_plan);
    END FOR;
    
    -- Update execution with planner output
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET planner_output = OBJECT_CONSTRUCT(
            'decisions', :all_decisions,
            'tables_planned', ARRAY_SIZE(:tables_to_process),
            'completed_at', CURRENT_TIMESTAMP()::VARCHAR
        ),
        planning_completed_at = CURRENT_TIMESTAMP(),
        current_phase = 'PLANNER_COMPLETE'
    WHERE execution_id = :execution_id;
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', 'PLANNED',
        'decisions_count', ARRAY_SIZE(all_decisions),
        'decisions', all_decisions,
        'next_phase', 'EXECUTOR'
    );
END;
$$;

-- ============================================================================
-- PART 3: EXECUTOR PHASE
-- Executes planned transformations with retry logic
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_EXECUTOR(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    decisions_cursor CURSOR FOR
        SELECT decision_id, source_table, transformation_strategy, recommended_actions, llm_reasoning
        FROM METADATA.PLANNER_DECISIONS
        WHERE execution_id = ?
        ORDER BY priority ASC;
    
    current_decision VARIANT;
    generated_sql VARCHAR;
    execution_prompt VARCHAR;
    llm_response VARCHAR;
    retry_count INTEGER;
    max_retries INTEGER DEFAULT 3;
    execution_succeeded BOOLEAN;
    last_error VARCHAR;
    execution_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    cur_source_table VARCHAR;
    cur_strategy VARCHAR;
    cur_actions VARCHAR;
    cur_reasoning VARCHAR;
    cur_schema_info VARIANT;
    variant_column VARCHAR;
    discovered_fields VARCHAR;
BEGIN
    -- Update status
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'EXECUTING', current_phase = 'EXECUTOR'
    WHERE execution_id = :execution_id;
    
    -- Process each planned decision
    OPEN decisions_cursor USING (execution_id);
    FOR record IN decisions_cursor DO
        cur_source_table := record.source_table;
        cur_strategy := record.transformation_strategy;
        cur_actions := record.recommended_actions::VARCHAR;
        cur_reasoning := record.llm_reasoning;
        
        -- Get schema info for this table to know the VARIANT column name
        cur_schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:cur_source_table));
        variant_column := COALESCE(cur_schema_info:variant_column::VARCHAR, 'PAYLOAD');
        
        -- Build discovered fields list
        SELECT LISTAGG(key || ' (' || value:inferred_type::VARCHAR || ')', ', ')
        INTO :discovered_fields
        FROM TABLE(FLATTEN(input => :cur_schema_info:discovered_columns));
        
        retry_count := 0;
        execution_succeeded := FALSE;
        last_error := NULL;
        
        -- Retry loop with self-correction
        WHILE (retry_count < max_retries AND NOT execution_succeeded) DO
            
            -- Build execution prompt
            IF (retry_count = 0) THEN
                execution_prompt := '
Generate Snowflake Dynamic Table DDL for this transformation.

SOURCE TABLE: ' || cur_source_table || '
STRATEGY: ' || cur_strategy || '
ACTIONS: ' || cur_actions || '
REASONING: ' || cur_reasoning || '

CRITICAL SCHEMA INFO:
- The source table has a VARIANT/OBJECT column named: ' || variant_column || '
- All data fields are INSIDE this column and must be accessed as: ' || variant_column || ':field_name
- Discovered fields: ' || COALESCE(discovered_fields, 'unknown') || '

REQUIREMENTS:
1. Create a DYNAMIC TABLE in the SILVER schema (use database DBAONTAP_ANALYTICS)
2. Use TARGET_LAG = ''1 hour''
3. Use WAREHOUSE = DBRYANT_COCO_WH_S
4. Access all fields using ' || variant_column || ':field_name syntax (e.g., ' || variant_column || ':customer_id)
5. Cast fields to proper types using ::TYPE or CAST()
6. Handle NULL values with COALESCE where appropriate
7. Add meaningful column aliases

EXAMPLE SQL PATTERN:
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.TABLE_NAME
TARGET_LAG = ''1 hour''
WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT
    ' || variant_column || ':id::INTEGER AS id,
    ' || variant_column || ':name::VARCHAR AS name,
    COALESCE(' || variant_column || ':status::VARCHAR, ''UNKNOWN'') AS status
FROM ' || cur_source_table || ';

OUTPUT: Only the CREATE OR REPLACE DYNAMIC TABLE statement, nothing else.';
            ELSE
                execution_prompt := '
The previous DDL failed with error: ' || last_error || '

Fix the DDL and try again.

SOURCE TABLE: ' || cur_source_table || '
STRATEGY: ' || cur_strategy || '

CRITICAL: The VARIANT column is named "' || variant_column || '". 
Access fields as: ' || variant_column || ':field_name (NOT as direct columns!)
Discovered fields: ' || COALESCE(discovered_fields, 'unknown') || '

FAILED SQL:
' || generated_sql || '

CORRECT PATTERN:
SELECT ' || variant_column || ':field_name::TYPE AS alias FROM table;

OUTPUT: Only the corrected CREATE OR REPLACE DYNAMIC TABLE statement.';
            END IF;
            
            -- Generate SQL via LLM
            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', :execution_prompt) INTO :llm_response;
            
            -- Clean up response
            generated_sql := TRIM(REGEXP_REPLACE(llm_response, '```sql|```', ''));
            LET create_pos INTEGER := POSITION('CREATE' IN UPPER(generated_sql));
            IF (create_pos > 1) THEN
                generated_sql := SUBSTR(generated_sql, create_pos);
            END IF;
            
            -- Attempt execution
            BEGIN
                EXECUTE IMMEDIATE :generated_sql;
                execution_succeeded := TRUE;
                success_count := success_count + 1;
                
                -- Log success
                INSERT INTO METADATA.TRANSFORMATION_LOG (
                    source_table, target_table, transformation_sql, agent_reasoning, status, executed_at
                )
                VALUES (
                    cur_source_table,
                    'SILVER.' || REPLACE(SPLIT_PART(cur_source_table, '.', -1), 'RAW_', ''),
                    :generated_sql,
                    CASE WHEN :retry_count > 0 
                         THEN 'Self-corrected after ' || retry_count || ' retries'
                         ELSE 'Executed on first attempt'
                    END,
                    'SUCCESS',
                    CURRENT_TIMESTAMP()
                );
                
                execution_results := ARRAY_APPEND(execution_results, OBJECT_CONSTRUCT(
                    'source_table', cur_source_table,
                    'status', 'SUCCESS',
                    'retries', retry_count,
                    'sql', generated_sql
                ));
                
            EXCEPTION WHEN OTHER THEN
                last_error := SQLERRM;
                retry_count := retry_count + 1;
            END;
        END WHILE;
        
        -- Log failure if all retries exhausted
        IF (NOT execution_succeeded) THEN
            fail_count := fail_count + 1;
            
            INSERT INTO METADATA.TRANSFORMATION_LOG (
                source_table, target_table, transformation_sql, agent_reasoning, status, executed_at
            )
            SELECT
                :cur_source_table,
                'SILVER.' || REPLACE(SPLIT_PART(:cur_source_table, '.', -1), 'RAW_', ''),
                :generated_sql,
                'FAILED after ' || :max_retries || ' attempts: ' || :last_error,
                'FAILED',
                CURRENT_TIMESTAMP();
            
            execution_results := ARRAY_APPEND(execution_results, OBJECT_CONSTRUCT(
                'source_table', cur_source_table,
                'status', 'FAILED',
                'retries', retry_count,
                'error', last_error,
                'sql', generated_sql
            ));
        END IF;
    END FOR;
    
    -- Update execution record
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET executor_output = OBJECT_CONSTRUCT(
            'success_count', :success_count,
            'fail_count', :fail_count,
            'results', :execution_results,
            'completed_at', CURRENT_TIMESTAMP()::VARCHAR
        ),
        execution_completed_at = CURRENT_TIMESTAMP(),
        current_phase = 'EXECUTOR_COMPLETE',
        retry_count = (SELECT SUM(r.value:retries::INTEGER) FROM TABLE(FLATTEN(input => :execution_results)) r)
    WHERE execution_id = :execution_id;
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', CASE WHEN fail_count = 0 THEN 'ALL_SUCCEEDED' ELSE 'PARTIAL_FAILURE' END,
        'success_count', success_count,
        'fail_count', fail_count,
        'results', execution_results,
        'next_phase', 'VALIDATOR'
    );
END;
$$;

-- ============================================================================
-- PART 4: VALIDATOR PHASE
-- Validates transformation results
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_VALIDATOR(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    executor_results ARRAY;
    current_result VARIANT;
    source_table VARCHAR;
    target_table VARCHAR;
    source_count INTEGER;
    target_count INTEGER;
    count_variance FLOAT;
    all_validations ARRAY DEFAULT ARRAY_CONSTRUCT();
    passed_count INTEGER DEFAULT 0;
    failed_count INTEGER DEFAULT 0;
    i INTEGER;
BEGIN
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'VALIDATING', current_phase = 'VALIDATOR'
    WHERE execution_id = :execution_id;
    
    SELECT executor_output:results INTO :executor_results
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;
    
    FOR i IN 0 TO ARRAY_SIZE(executor_results) - 1 DO
        current_result := executor_results[i];
        
        IF (current_result:status::VARCHAR = 'SUCCESS') THEN
            source_table := current_result:source_table::VARCHAR;
            target_table := 'DBAONTAP_ANALYTICS.SILVER.' || REPLACE(SPLIT_PART(source_table, '.', -1), 'RAW_', '');
            
            BEGIN
                SELECT COUNT(*) INTO :source_count FROM IDENTIFIER(:source_table);
                SELECT COUNT(*) INTO :target_count FROM IDENTIFIER(:target_table);
                
                count_variance := ABS(source_count - target_count) * 100.0 / NULLIF(source_count, 0);
                
                INSERT INTO METADATA.VALIDATION_RESULTS (
                    execution_id, source_table, target_table, validation_type,
                    expected_value, actual_value, passed, variance_pct, details
                )
                VALUES (
                    :execution_id, :source_table, :target_table, 'row_count',
                    :source_count::VARIANT, :target_count::VARIANT,
                    :count_variance < 5,
                    :count_variance,
                    'Source: ' || source_count || ', Target: ' || target_count
                );
                
                IF (count_variance < 5) THEN
                    passed_count := passed_count + 1;
                ELSE
                    failed_count := failed_count + 1;
                END IF;
                
                all_validations := ARRAY_APPEND(all_validations, OBJECT_CONSTRUCT(
                    'source_table', source_table,
                    'target_table', target_table,
                    'validation_type', 'row_count',
                    'passed', count_variance < 5,
                    'source_count', source_count,
                    'target_count', target_count,
                    'variance_pct', count_variance
                ));
                
            EXCEPTION WHEN OTHER THEN
                failed_count := failed_count + 1;
                all_validations := ARRAY_APPEND(all_validations, OBJECT_CONSTRUCT(
                    'source_table', source_table,
                    'target_table', target_table,
                    'validation_type', 'row_count',
                    'passed', FALSE,
                    'error', SQLERRM
                ));
            END;
        END IF;
    END FOR;
    
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET validator_output = OBJECT_CONSTRUCT(
            'passed_count', :passed_count,
            'failed_count', :failed_count,
            'validations', :all_validations,
            'overall_passed', :failed_count = 0,
            'completed_at', CURRENT_TIMESTAMP()::VARCHAR
        ),
        validation_completed_at = CURRENT_TIMESTAMP(),
        current_phase = 'VALIDATOR_COMPLETE'
    WHERE execution_id = :execution_id;
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', CASE WHEN failed_count = 0 THEN 'ALL_PASSED' ELSE 'VALIDATION_FAILURES' END,
        'passed_count', passed_count,
        'failed_count', failed_count,
        'validations', all_validations,
        'next_phase', 'REFLECTOR'
    );
END;
$$;

-- ============================================================================
-- PART 5: REFLECTOR PHASE
-- Analyzes workflow results and extracts learnings
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_REFLECTOR(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    workflow_data VARIANT;
    reflection_prompt VARCHAR;
    llm_reflection VARCHAR;
    parsed_learnings VARIANT;
    learnings_array ARRAY;
    i INTEGER;
BEGIN
    -- Update status
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'REFLECTING', current_phase = 'REFLECTOR'
    WHERE execution_id = :execution_id;
    
    -- Gather all workflow data for reflection
    SELECT OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'trigger', planner_output,
        'planner_decisions', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM METADATA.PLANNER_DECISIONS WHERE execution_id = :execution_id),
        'executor_results', executor_output,
        'validation_results', validator_output
    ) INTO :workflow_data
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;
    
    -- Build reflection prompt
    reflection_prompt := '
You are the REFLECTOR agent. Analyze this completed workflow and extract learnings.

WORKFLOW SUMMARY:
' || workflow_data::VARCHAR || '

TASKS:
1. Identify successful patterns that can be reused
2. Identify failures and their root causes
3. Suggest optimizations for future runs
4. Note any schema patterns discovered

OUTPUT FORMAT (JSON array):
[
  {
    "learning_type": "success_pattern|failure_pattern|optimization|schema_pattern",
    "pattern_signature": "unique identifier for this pattern",
    "observation": "what was observed",
    "recommendation": "what to do differently",
    "confidence": 0.0-1.0
  }
]

Output only the JSON array, no explanation.';

    -- Generate reflections via LLM
    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', :reflection_prompt) INTO :llm_reflection;
    
    -- Parse learnings
    BEGIN
        parsed_learnings := PARSE_JSON(REGEXP_SUBSTR(llm_reflection, '\\[.*\\]'));
        learnings_array := parsed_learnings;
    EXCEPTION WHEN OTHER THEN
        learnings_array := ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
            'learning_type', 'reflection_error',
            'observation', 'Could not parse reflection output',
            'recommendation', 'Review raw output: ' || llm_reflection,
            'confidence', 0.3
        ));
    END;
    
    -- Store learnings (upsert based on pattern_signature)
    FOR i IN 0 TO ARRAY_SIZE(learnings_array) - 1 DO
        LET learning VARIANT := learnings_array[i];
        
        MERGE INTO METADATA.WORKFLOW_LEARNINGS t
        USING (SELECT 
            :learning:pattern_signature::VARCHAR as pattern_signature,
            :learning:learning_type::VARCHAR as learning_type,
            :execution_id as execution_id,
            :learning:observation::VARCHAR as observation,
            :learning:recommendation::VARCHAR as recommendation,
            COALESCE(:learning:confidence::FLOAT, 0.5) as confidence_score
        ) s
        ON t.pattern_signature = s.pattern_signature
        WHEN MATCHED THEN UPDATE SET
            times_observed = t.times_observed + 1,
            last_observed_at = CURRENT_TIMESTAMP(),
            confidence_score = (t.confidence_score + s.confidence_score) / 2
        WHEN NOT MATCHED THEN INSERT (
            execution_id, learning_type, pattern_signature, observation, recommendation, confidence_score
        ) VALUES (
            s.execution_id, s.learning_type, s.pattern_signature, s.observation, s.recommendation, s.confidence_score
        );
    END FOR;
    
    -- Update execution record
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET reflector_output = OBJECT_CONSTRUCT(
            'learnings_count', ARRAY_SIZE(:learnings_array),
            'learnings', :learnings_array,
            'completed_at', CURRENT_TIMESTAMP()::VARCHAR
        ),
        reflection_completed_at = CURRENT_TIMESTAMP(),
        status = 'COMPLETED',
        current_phase = 'COMPLETE',
        completed_at = CURRENT_TIMESTAMP()
    WHERE execution_id = :execution_id;
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', 'COMPLETED',
        'learnings_count', ARRAY_SIZE(learnings_array),
        'learnings', learnings_array,
        'workflow_complete', TRUE
    );
END;
$$;

-- ============================================================================
-- PART 6: MASTER ORCHESTRATOR
-- Runs the full workflow: TRIGGER → PLANNER → EXECUTOR → VALIDATOR → REFLECTOR
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.RUN_AGENTIC_WORKFLOW(
    trigger_type VARCHAR DEFAULT 'manual',
    source_tables ARRAY DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    trigger_result VARIANT;
    planner_result VARIANT;
    executor_result VARIANT;
    validator_result VARIANT;
    reflector_result VARIANT;
    execution_id VARCHAR;
    workflow_failed BOOLEAN DEFAULT FALSE;
    failure_phase VARCHAR;
    failure_error VARCHAR;
BEGIN
    -- Phase 1: TRIGGER
    trigger_result := (CALL AGENTS.WORKFLOW_TRIGGER(:trigger_type, :source_tables));
    execution_id := trigger_result:execution_id::VARCHAR;
    
    IF (ARRAY_SIZE(trigger_result:tables_to_process) = 0) THEN
        UPDATE METADATA.WORKFLOW_EXECUTIONS 
        SET status = 'COMPLETED', current_phase = 'NO_WORK', completed_at = CURRENT_TIMESTAMP()
        WHERE execution_id = :execution_id;
        
        RETURN OBJECT_CONSTRUCT(
            'execution_id', execution_id,
            'status', 'NO_TABLES_TO_PROCESS',
            'message', 'No Bronze tables found to transform'
        );
    END IF;
    
    -- Phase 2: PLANNER
    BEGIN
        planner_result := (CALL AGENTS.WORKFLOW_PLANNER(:execution_id));
    EXCEPTION WHEN OTHER THEN
        workflow_failed := TRUE;
        failure_phase := 'PLANNER';
        failure_error := SQLERRM;
    END;
    
    -- Phase 3: EXECUTOR
    IF (NOT workflow_failed) THEN
        BEGIN
            executor_result := (CALL AGENTS.WORKFLOW_EXECUTOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'EXECUTOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    -- Phase 4: VALIDATOR
    IF (NOT workflow_failed) THEN
        BEGIN
            validator_result := (CALL AGENTS.WORKFLOW_VALIDATOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'VALIDATOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    -- Phase 5: REFLECTOR (always runs to capture learnings, even on partial failure)
    BEGIN
        reflector_result := (CALL AGENTS.WORKFLOW_REFLECTOR(:execution_id));
    EXCEPTION WHEN OTHER THEN
        -- Reflection failure is non-fatal
        reflector_result := OBJECT_CONSTRUCT('error', SQLERRM);
    END;
    
    -- Handle workflow failure
    IF (workflow_failed) THEN
        UPDATE METADATA.WORKFLOW_EXECUTIONS 
        SET status = 'FAILED', 
            last_error = :failure_phase || ': ' || :failure_error,
            completed_at = CURRENT_TIMESTAMP()
        WHERE execution_id = :execution_id;
        
        RETURN OBJECT_CONSTRUCT(
            'execution_id', execution_id,
            'status', 'FAILED',
            'failed_phase', failure_phase,
            'error', failure_error,
            'trigger_result', trigger_result,
            'planner_result', planner_result,
            'executor_result', executor_result,
            'validator_result', validator_result,
            'reflector_result', reflector_result
        );
    END IF;
    
    -- Return success summary
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', 'COMPLETED',
        'summary', OBJECT_CONSTRUCT(
            'tables_triggered', trigger_result:tables_to_process,
            'tables_planned', planner_result:decisions_count,
            'executions_succeeded', executor_result:success_count,
            'executions_failed', executor_result:fail_count,
            'validations_passed', validator_result:passed_count,
            'validations_failed', validator_result:failed_count,
            'learnings_captured', reflector_result:learnings_count
        ),
        'phases', OBJECT_CONSTRUCT(
            'trigger', trigger_result,
            'planner', planner_result,
            'executor', executor_result,
            'validator', validator_result,
            'reflector', reflector_result
        )
    );
END;
$$;

-- ============================================================================
-- PART 7: MONITORING VIEWS
-- ============================================================================

CREATE OR REPLACE VIEW METADATA.WORKFLOW_DASHBOARD AS
SELECT 
    execution_id,
    workflow_name,
    trigger_type,
    status,
    current_phase,
    planner_output:tables_count::INTEGER as tables_triggered,
    executor_output:success_count::INTEGER as executions_succeeded,
    executor_output:fail_count::INTEGER as executions_failed,
    validator_output:passed_count::INTEGER as validations_passed,
    validator_output:failed_count::INTEGER as validations_failed,
    reflector_output:learnings_count::INTEGER as learnings_captured,
    retry_count,
    TIMESTAMPDIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP())) as duration_seconds,
    started_at,
    completed_at,
    last_error
FROM METADATA.WORKFLOW_EXECUTIONS
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW METADATA.ACTIVE_LEARNINGS AS
SELECT 
    learning_type,
    pattern_signature,
    observation,
    recommendation,
    times_observed,
    confidence_score,
    last_observed_at
FROM METADATA.WORKFLOW_LEARNINGS
WHERE is_active = TRUE
ORDER BY confidence_score DESC, times_observed DESC;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Run full agentic workflow manually:
CALL AGENTS.RUN_AGENTIC_WORKFLOW('manual');

-- Run for specific tables:
CALL AGENTS.RUN_AGENTIC_WORKFLOW('manual', ARRAY_CONSTRUCT('AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS'));

-- Monitor workflow status:
SELECT * FROM METADATA.WORKFLOW_DASHBOARD;

-- View learnings:
SELECT * FROM METADATA.ACTIVE_LEARNINGS;

-- Check validation results:
SELECT * FROM METADATA.VALIDATION_RESULTS ORDER BY created_at DESC;

-- View planner decisions:
SELECT * FROM METADATA.PLANNER_DECISIONS ORDER BY created_at DESC;
*/

-- ============================================================================
-- PART 8: AUTOMATED TASK
-- Runs on schedule, TRIGGER procedure handles stream checking internally
-- ============================================================================

-- Note: Task WHEN clause only allows SYSTEM$STREAM_HAS_DATA directly
-- The TRIGGER procedure handles dynamic stream checking and auto-discovery

-- Suspend existing task first
ALTER TASK IF EXISTS AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK SUSPEND;

-- Create task that runs every minute
-- The TRIGGER procedure will check streams and auto-discover new tables
CREATE OR REPLACE TASK AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK
    WAREHOUSE = DBRYANT_COCO_WH_S
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Auto-triggers agentic workflow - checks streams internally and auto-discovers new tables'
AS
    CALL DBAONTAP_ANALYTICS.AGENTS.RUN_AGENTIC_WORKFLOW('stream');

-- Resume the task
ALTER TASK AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK RESUME;
