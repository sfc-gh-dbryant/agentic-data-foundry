USE DATABASE DBAONTAP_ANALYTICS;

-- VALIDATOR PROCEDURE
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
    rs RESULTSET;
    c1 CURSOR FOR rs;
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
                rs := (EXECUTE IMMEDIATE 'SELECT COUNT(*) as cnt FROM ' || source_table);
                OPEN c1;
                FETCH c1 INTO source_count;
                CLOSE c1;
                
                rs := (EXECUTE IMMEDIATE 'SELECT COUNT(*) as cnt FROM ' || target_table);
                OPEN c1;
                FETCH c1 INTO target_count;
                CLOSE c1;
                
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

-- REFLECTOR PROCEDURE
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
    learning VARIANT;
BEGIN
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'REFLECTING', current_phase = 'REFLECTOR'
    WHERE execution_id = :execution_id;
    
    SELECT OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'trigger', planner_output,
        'executor_results', executor_output,
        'validation_results', validator_output
    ) INTO :workflow_data
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;
    
    reflection_prompt := 'You are the REFLECTOR agent. Analyze this completed workflow and extract learnings.

WORKFLOW SUMMARY:
' || workflow_data::VARCHAR || '

TASKS:
1. Identify successful patterns that can be reused
2. Identify failures and their root causes
3. Suggest optimizations for future runs

OUTPUT FORMAT (JSON array only, no explanation):
[{"learning_type": "success_pattern", "pattern_signature": "unique_id", "observation": "what was observed", "recommendation": "what to do", "confidence": 0.8}]';

    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', :reflection_prompt) INTO :llm_reflection;
    
    BEGIN
        parsed_learnings := PARSE_JSON(REGEXP_SUBSTR(llm_reflection, '\\[.*\\]'));
        learnings_array := parsed_learnings;
    EXCEPTION WHEN OTHER THEN
        learnings_array := ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
            'learning_type', 'reflection_error',
            'observation', 'Could not parse reflection output',
            'recommendation', 'Review raw output',
            'confidence', 0.3
        ));
    END;
    
    FOR i IN 0 TO ARRAY_SIZE(learnings_array) - 1 DO
        learning := learnings_array[i];
        
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

-- MASTER ORCHESTRATOR
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
    CALL AGENTS.WORKFLOW_TRIGGER(:trigger_type, :source_tables);
    SELECT PARSE_JSON(RESULT) INTO :trigger_result FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
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
    
    BEGIN
        CALL AGENTS.WORKFLOW_PLANNER(:execution_id);
        SELECT PARSE_JSON(RESULT) INTO :planner_result FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    EXCEPTION WHEN OTHER THEN
        workflow_failed := TRUE;
        failure_phase := 'PLANNER';
        failure_error := SQLERRM;
    END;
    
    IF (NOT workflow_failed) THEN
        BEGIN
            CALL AGENTS.WORKFLOW_EXECUTOR(:execution_id);
            SELECT PARSE_JSON(RESULT) INTO :executor_result FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'EXECUTOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    IF (NOT workflow_failed) THEN
        BEGIN
            CALL AGENTS.WORKFLOW_VALIDATOR(:execution_id);
            SELECT PARSE_JSON(RESULT) INTO :validator_result FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'VALIDATOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    BEGIN
        CALL AGENTS.WORKFLOW_REFLECTOR(:execution_id);
        SELECT PARSE_JSON(RESULT) INTO :reflector_result FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    EXCEPTION WHEN OTHER THEN
        reflector_result := OBJECT_CONSTRUCT('error', SQLERRM);
    END;
    
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
            'error', failure_error
        );
    END IF;
    
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
        )
    );
END;
$$;

-- MONITORING VIEWS
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
