-- =============================================================================
-- 07e_validator.sql
-- VALIDATOR phase: Row count parity checks between Bronze and Silver.
-- Uses EXECUTE IMMEDIATE + cursor for dynamic table name resolution.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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
    rs RESULTSET DEFAULT (SELECT 1);
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
            target_table := 'DBAONTAP_ANALYTICS.SILVER.' || REPLACE(SPLIT_PART(source_table, '.', -1), '_VARIANT', '');
            
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
