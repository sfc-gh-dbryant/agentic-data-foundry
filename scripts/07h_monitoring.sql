-- =============================================================================
-- 07h_monitoring.sql
-- Monitoring views and scheduled task for the agentic workflow engine.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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

ALTER TASK IF EXISTS AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK SUSPEND;

CREATE OR REPLACE TASK AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK
    WAREHOUSE = DBRYANT_COCO_WH_S
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Auto-triggers agentic workflow - checks streams internally and auto-discovers new tables'
AS
    CALL DBAONTAP_ANALYTICS.AGENTS.RUN_AGENTIC_WORKFLOW('stream');

ALTER TASK AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK RESUME;
