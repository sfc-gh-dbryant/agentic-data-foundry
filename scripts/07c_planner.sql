-- =============================================================================
-- 07c_planner.sql
-- PLANNER phase: LLM-driven transformation strategy for each Bronze table.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'PLANNING', current_phase = 'PLANNER'
    WHERE execution_id = :execution_id;
    
    SELECT planner_output:tables INTO :tables_to_process
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;
    
    SELECT LISTAGG(observation || ' -> ' || recommendation, '; ') 
    INTO :existing_learnings
    FROM METADATA.WORKFLOW_LEARNINGS
    WHERE is_active = TRUE AND confidence_score > 0.7
    LIMIT 5;
    
    FOR i IN 0 TO ARRAY_SIZE(tables_to_process) - 1 DO
        current_table := tables_to_process[i]::VARCHAR;
        
        schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:current_table));
        quality_info := (CALL AGENTS.ANALYZE_DATA_QUALITY(:current_table, 500));
        
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

        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', :planner_prompt) INTO :llm_response;
        
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
