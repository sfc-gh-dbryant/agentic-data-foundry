-- =============================================================================
-- 07a_metadata_tables.sql
-- Workflow metadata schema: execution tracking, planner decisions, validation,
-- learnings, and logging tables.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_EXECUTIONS (
    execution_id VARCHAR DEFAULT UUID_STRING(),
    workflow_name VARCHAR NOT NULL,
    trigger_source VARCHAR,
    trigger_type VARCHAR,
    status VARCHAR DEFAULT 'PENDING',
    current_phase VARCHAR,
    planner_output VARIANT,
    executor_output VARIANT,
    validator_output VARIANT,
    reflector_output VARIANT,
    started_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    planning_completed_at TIMESTAMP_LTZ,
    execution_completed_at TIMESTAMP_LTZ,
    validation_completed_at TIMESTAMP_LTZ,
    reflection_completed_at TIMESTAMP_LTZ,
    completed_at TIMESTAMP_LTZ,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    last_error TEXT,
    PRIMARY KEY (execution_id)
);

CREATE TABLE IF NOT EXISTS METADATA.PLANNER_DECISIONS (
    decision_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    source_table VARCHAR,
    target_schema VARCHAR,
    transformation_strategy VARCHAR,
    detected_patterns VARIANT,
    recommended_actions ARRAY,
    priority INTEGER,
    llm_reasoning TEXT,
    confidence_score FLOAT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS METADATA.VALIDATION_RESULTS (
    validation_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    source_table VARCHAR,
    target_table VARCHAR,
    validation_type VARCHAR,
    expected_value VARIANT,
    actual_value VARIANT,
    passed BOOLEAN,
    variance_pct FLOAT,
    details TEXT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_LEARNINGS (
    learning_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    learning_type VARCHAR,
    source_context VARCHAR,
    pattern_signature VARCHAR,
    observation TEXT,
    recommendation TEXT,
    times_observed INTEGER DEFAULT 1,
    last_observed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    confidence_score FLOAT,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS METADATA.WORKFLOW_LOG (
    log_id VARCHAR DEFAULT UUID_STRING(),
    execution_id VARCHAR,
    phase VARCHAR,
    status VARCHAR,
    message TEXT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS METADATA.ONBOARDED_TABLES (
    table_name VARCHAR PRIMARY KEY,
    landing_table VARCHAR,
    stream_name VARCHAR,
    bronze_table VARCHAR,
    onboarded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    onboarded_by VARCHAR DEFAULT 'AGENTIC_WORKFLOW'
);
