-- =============================================================================
-- 07g_orchestrator.sql
-- Master orchestrator: TRIGGER -> PLANNER -> EXECUTOR -> VALIDATOR -> REFLECTOR
-- Uses direct CALL assignment pattern: var := (CALL proc(...));
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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
    
    BEGIN
        planner_result := (CALL AGENTS.WORKFLOW_PLANNER(:execution_id));
    EXCEPTION WHEN OTHER THEN
        workflow_failed := TRUE;
        failure_phase := 'PLANNER';
        failure_error := SQLERRM;
    END;
    
    IF (NOT workflow_failed) THEN
        BEGIN
            executor_result := (CALL AGENTS.WORKFLOW_EXECUTOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'EXECUTOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    IF (NOT workflow_failed) THEN
        BEGIN
            validator_result := (CALL AGENTS.WORKFLOW_VALIDATOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := 'VALIDATOR';
            failure_error := SQLERRM;
        END;
    END IF;
    
    BEGIN
        reflector_result := (CALL AGENTS.WORKFLOW_REFLECTOR(:execution_id));
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
            'error', failure_error,
            'trigger_result', trigger_result,
            'planner_result', planner_result,
            'executor_result', executor_result,
            'validator_result', validator_result,
            'reflector_result', reflector_result
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
