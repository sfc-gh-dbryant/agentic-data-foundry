-- =============================================================================
-- 07f_reflector.sql
-- REFLECTOR phase: LLM-driven post-mortem with MERGE-based learning accumulation.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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
    
    FOR i IN 0 TO ARRAY_SIZE(COALESCE(learnings_array, ARRAY_CONSTRUCT())) - 1 DO
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
