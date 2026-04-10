-- =============================================================================
-- 07d_executor.sql
-- EXECUTOR phase: LLM-generated DDL with 3-retry self-correcting loop.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

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
    UPDATE METADATA.WORKFLOW_EXECUTIONS 
    SET status = 'EXECUTING', current_phase = 'EXECUTOR'
    WHERE execution_id = :execution_id;
    
    OPEN decisions_cursor USING (execution_id);
    FOR record IN decisions_cursor DO
        cur_source_table := record.source_table;
        cur_strategy := record.transformation_strategy;
        cur_actions := record.recommended_actions::VARCHAR;
        cur_reasoning := record.llm_reasoning;
        
        cur_schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:cur_source_table));
        variant_column := COALESCE(cur_schema_info:variant_column::VARCHAR, 'PAYLOAD');
        
        SELECT LISTAGG(key || ' (' || value:inferred_type::VARCHAR || ')', ', ')
        INTO :discovered_fields
        FROM TABLE(FLATTEN(input => :cur_schema_info:discovered_columns));
        
        retry_count := 0;
        execution_succeeded := FALSE;
        last_error := NULL;
        
        WHILE (retry_count < max_retries AND NOT execution_succeeded) DO
            
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
            
            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', :execution_prompt) INTO :llm_response;
            
            generated_sql := TRIM(REGEXP_REPLACE(llm_response, '```sql|```', ''));
            LET create_pos INTEGER := POSITION('CREATE' IN UPPER(generated_sql));
            IF (create_pos > 1) THEN
                generated_sql := SUBSTR(generated_sql, create_pos);
            END IF;
            
            BEGIN
                EXECUTE IMMEDIATE :generated_sql;
                execution_succeeded := TRUE;
                success_count := success_count + 1;
                
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
