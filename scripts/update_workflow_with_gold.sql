-- Update RUN_AGENTIC_WORKFLOW to include Phase 6: Gold Propagation
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.RUN_AGENTIC_WORKFLOW(
    "TRIGGER_TYPE" VARCHAR DEFAULT 'manual',
    "SOURCE_TABLES" ARRAY DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
DECLARE
    trigger_result VARIANT;
    planner_result VARIANT;
    executor_result VARIANT;
    validator_result VARIANT;
    reflector_result VARIANT;
    gold_propagation_result VARIANT;
    execution_id VARCHAR;
    workflow_failed BOOLEAN DEFAULT FALSE;
    failure_phase VARCHAR;
    failure_error VARCHAR;
    new_tables_onboarded INTEGER;
BEGIN
    -- Phase 1: TRIGGER (always runs - handles auto-discovery)
    trigger_result := (CALL AGENTS.WORKFLOW_TRIGGER(:trigger_type, :source_tables));
    execution_id := trigger_result:execution_id::VARCHAR;
    new_tables_onboarded := COALESCE(trigger_result:new_tables_onboarded::INTEGER, 0);

    -- For ''stream'' trigger: Only run full workflow if new tables were onboarded
    IF (trigger_type = ''stream'' AND new_tables_onboarded = 0) THEN
        UPDATE METADATA.WORKFLOW_EXECUTIONS
        SET status = ''SKIPPED'',
            current_phase = ''DT_AUTO_REFRESH'',
            completed_at = CURRENT_TIMESTAMP()
        WHERE execution_id = :execution_id;

        RETURN OBJECT_CONSTRUCT(
            ''execution_id'', execution_id,
            ''status'', ''SKIPPED'',
            ''reason'', ''No new tables - Dynamic Tables handle normal data refresh automatically'',
            ''discovery_result'', trigger_result:discovery_result
        );
    END IF;

    -- Check if we have tables to process (for manual/scheduled triggers)
    IF (trigger_result:tables_to_process IS NULL OR ARRAY_SIZE(trigger_result:tables_to_process) = 0) THEN
        UPDATE METADATA.WORKFLOW_EXECUTIONS
        SET status = ''COMPLETED'', current_phase = ''NO_WORK'',
            completed_at = CURRENT_TIMESTAMP()
        WHERE execution_id = :execution_id;

        RETURN OBJECT_CONSTRUCT(
            ''execution_id'', execution_id,
            ''status'', ''NO_TABLES_TO_PROCESS'',
            ''discovery_result'', trigger_result:discovery_result
        );
    END IF;

    -- Phase 2: PLANNER
    BEGIN
        planner_result := (CALL AGENTS.WORKFLOW_PLANNER(:execution_id));
    EXCEPTION WHEN OTHER THEN
        workflow_failed := TRUE;
        failure_phase := ''PLANNER'';
        failure_error := SQLERRM;
    END;

    -- Phase 3: EXECUTOR
    IF (NOT workflow_failed) THEN
        BEGIN
            executor_result := (CALL AGENTS.WORKFLOW_EXECUTOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := ''EXECUTOR'';
            failure_error := SQLERRM;
        END;
    END IF;

    -- Phase 4: VALIDATOR
    IF (NOT workflow_failed) THEN
        BEGIN
            validator_result := (CALL AGENTS.WORKFLOW_VALIDATOR(:execution_id));
        EXCEPTION WHEN OTHER THEN
            workflow_failed := TRUE;
            failure_phase := ''VALIDATOR'';
            failure_error := SQLERRM;
        END;
    END IF;

    -- Phase 5: REFLECTOR (always runs to capture learnings)
    BEGIN
        reflector_result := (CALL AGENTS.WORKFLOW_REFLECTOR(:execution_id));
    EXCEPTION WHEN OTHER THEN
        reflector_result := OBJECT_CONSTRUCT(''error'', SQLERRM);
    END;

    -- Phase 6: GOLD PROPAGATION (auto-propagate new columns to Gold DTs + refresh SVs)
    IF (NOT workflow_failed) THEN
        BEGIN
            UPDATE METADATA.WORKFLOW_EXECUTIONS
            SET current_phase = ''GOLD_PROPAGATION''
            WHERE execution_id = :execution_id;

            gold_propagation_result := (CALL AGENTS.PROPAGATE_TO_GOLD(NULL, FALSE, TRUE, TRUE, TRUE));
        EXCEPTION WHEN OTHER THEN
            gold_propagation_result := OBJECT_CONSTRUCT(''status'', ''FAILED'', ''error'', SQLERRM);
        END;
    END IF;

    IF (workflow_failed) THEN
        UPDATE METADATA.WORKFLOW_EXECUTIONS
        SET status = ''FAILED'',
            last_error = :failure_phase || '': '' || :failure_error,
            completed_at = CURRENT_TIMESTAMP()
        WHERE execution_id = :execution_id;

        RETURN OBJECT_CONSTRUCT(
            ''execution_id'', execution_id,
            ''status'', ''FAILED'',
            ''failed_phase'', failure_phase,
            ''error'', failure_error
        );
    END IF;

    UPDATE METADATA.WORKFLOW_EXECUTIONS
    SET current_phase = ''COMPLETED'',
        status = ''COMPLETED'',
        completed_at = CURRENT_TIMESTAMP()
    WHERE execution_id = :execution_id;

    RETURN OBJECT_CONSTRUCT(
        ''execution_id'', execution_id,
        ''status'', ''COMPLETED'',
        ''new_tables_onboarded'', new_tables_onboarded,
        ''tables_processed'', ARRAY_SIZE(trigger_result:tables_to_process),
        ''gold_propagation'', gold_propagation_result,
        ''summary'', OBJECT_CONSTRUCT(
            ''tables_planned'', COALESCE(planner_result:tables_planned::INTEGER, 0),
            ''executions_succeeded'', COALESCE(executor_result:success_count::INTEGER, 0),
            ''executions_failed'', COALESCE(executor_result:fail_count::INTEGER, 0),
            ''validations_passed'', COALESCE(validator_result:passed::INTEGER, 0),
            ''learnings_captured'', COALESCE(reflector_result:learnings_count::INTEGER, 0),
            ''gold_drift_columns'', COALESCE(gold_propagation_result:summary:total_drift_columns::INTEGER, 0),
            ''gold_tables_updated'', COALESCE(gold_propagation_result:summary:affected_gold_tables::INTEGER, 0)
        )
    );
END;
';
