CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.WORKFLOW_REFLECTOR(EXECUTION_ID VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
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
    UPDATE DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
    SET status = 'REFLECTING', current_phase = 'REFLECTOR'
    WHERE execution_id = :execution_id;

    SELECT OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'trigger', planner_output,
        'executor_results', executor_output,
        'validation_results', validator_output
    ) INTO :workflow_data
    FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;

    reflection_prompt := 'You are the REFLECTOR agent. Analyze this completed workflow and extract learnings.

WORKFLOW SUMMARY:
' || SUBSTR(workflow_data::VARCHAR, 1, 8000) || '

TASKS:
1. Identify successful patterns that can be reused
2. Identify failures and their root causes
3. Suggest optimizations for future runs

OUTPUT FORMAT (JSON array only, no explanation):
[{"learning_type": "success_pattern", "pattern_signature": "unique_id", "observation": "what was observed", "recommendation": "what to do", "confidence": 0.8}]';

    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'claude-3-7-sonnet',
        ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('role', 'user', 'content', :reflection_prompt)
        ),
        OBJECT_CONSTRUCT('temperature', 0.3, 'max_tokens', 2000)
    ):choices[0]:messages INTO :llm_reflection;

    BEGIN
        parsed_learnings := PARSE_JSON(REGEXP_SUBSTR(llm_reflection, '\\[.*\\]'));
        IF (parsed_learnings IS NOT NULL AND ARRAY_SIZE(parsed_learnings) > 0) THEN
            learnings_array := parsed_learnings;
        ELSE
            learnings_array := ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
                'learning_type', 'reflection_fallback',
                'pattern_signature', 'fallback_' || :execution_id,
                'observation', COALESCE(SUBSTR(llm_reflection, 1, 500), 'No LLM output'),
                'recommendation', 'Review raw reflection output',
                'confidence', 0.4
            ));
        END IF;
    EXCEPTION WHEN OTHER THEN
        learnings_array := ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
            'learning_type', 'reflection_error',
            'pattern_signature', 'error_' || :execution_id,
            'observation', 'Could not parse reflection output',
            'recommendation', 'Review raw output',
            'confidence', 0.3
        ));
    END;

    FOR i IN 0 TO ARRAY_SIZE(COALESCE(learnings_array, ARRAY_CONSTRUCT())) - 1 DO
        learning := learnings_array[i];

        MERGE INTO DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LEARNINGS t
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

    UPDATE DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
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
