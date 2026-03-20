CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.WORKFLOW_TRIGGER(
    trigger_type VARCHAR,
    source_tables ARRAY DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    execution_id VARCHAR;
    all_bronze_tables ARRAY;
    filtered_result VARIANT;
    tables_to_process ARRAY;
    tables_skipped ARRAY;
    trigger_details VARIANT;
    trigger_source_str VARCHAR;
    discovery_result VARIANT;
    new_tables_onboarded INTEGER DEFAULT 0;
BEGIN
    execution_id := UUID_STRING();
    
    -- DECISION POINT 1: AUTO-DISCOVERY
    discovery_result := (CALL DBAONTAP_ANALYTICS.AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES());
    IF (discovery_result:status::VARCHAR = 'DISCOVERY_COMPLETE') THEN
        new_tables_onboarded := discovery_result:new_tables_found::INTEGER;
    END IF;
    
    -- Determine candidate Bronze tables based on trigger type
    CASE trigger_type
        WHEN 'manual' THEN
            IF (source_tables IS NULL OR ARRAY_SIZE(source_tables) = 0) THEN
                SELECT ARRAY_AGG(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME)
                INTO :all_bronze_tables
                FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'BRONZE' 
                  AND TABLE_NAME LIKE '%_VARIANT'
                  AND TABLE_TYPE = 'BASE TABLE';
            ELSE
                all_bronze_tables := source_tables;
            END IF;
            
        WHEN 'stream' THEN
            all_bronze_tables := ARRAY_CONSTRUCT();
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.CUSTOMERS_LANDING_STREAM')) THEN
                all_bronze_tables := ARRAY_APPEND(all_bronze_tables, 'DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.ORDERS_LANDING_STREAM')) THEN
                all_bronze_tables := ARRAY_APPEND(all_bronze_tables, 'DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.ORDER_ITEMS_LANDING_STREAM')) THEN
                all_bronze_tables := ARRAY_APPEND(all_bronze_tables, 'DBAONTAP_ANALYTICS.BRONZE.ORDER_ITEMS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.PRODUCTS_LANDING_STREAM')) THEN
                all_bronze_tables := ARRAY_APPEND(all_bronze_tables, 'DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT');
            END IF;
            IF (SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.SUPPORT_TICKETS_LANDING_STREAM')) THEN
                all_bronze_tables := ARRAY_APPEND(all_bronze_tables, 'DBAONTAP_ANALYTICS.BRONZE.SUPPORT_TICKETS_VARIANT');
            END IF;
            
        WHEN 'scheduled' THEN
            SELECT ARRAY_AGG(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME)
            INTO :all_bronze_tables
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'BRONZE' 
              AND TABLE_NAME LIKE '%_VARIANT'
              AND TABLE_TYPE = 'BASE TABLE';
            
        ELSE
            all_bronze_tables := COALESCE(source_tables, ARRAY_CONSTRUCT());
    END CASE;
    
    -- DECISION POINT 2: Filter tables - only process those needing workflow
    IF (ARRAY_SIZE(COALESCE(all_bronze_tables, ARRAY_CONSTRUCT())) > 0) THEN
        filtered_result := (CALL DBAONTAP_ANALYTICS.AGENTS.FILTER_TABLES_FOR_AGENTIC_WORKFLOW(:all_bronze_tables));
        
        SELECT ARRAY_AGG(t.value:table::VARCHAR)
        INTO :tables_to_process
        FROM TABLE(FLATTEN(input => :filtered_result:tables_needing_workflow)) t;
        
        tables_skipped := filtered_result:tables_to_skip;
    ELSE
        tables_to_process := ARRAY_CONSTRUCT();
        tables_skipped := ARRAY_CONSTRUCT();
        filtered_result := OBJECT_CONSTRUCT('workflow_count', 0, 'skip_count', 0);
    END IF;
    
    -- Create workflow execution record
    trigger_details := OBJECT_CONSTRUCT(
        'trigger_type', trigger_type,
        'candidate_tables_count', ARRAY_SIZE(COALESCE(all_bronze_tables, ARRAY_CONSTRUCT())),
        'tables_to_process_count', ARRAY_SIZE(COALESCE(tables_to_process, ARRAY_CONSTRUCT())),
        'tables_skipped_count', ARRAY_SIZE(COALESCE(tables_skipped, ARRAY_CONSTRUCT())),
        'tables', COALESCE(tables_to_process, ARRAY_CONSTRUCT()),
        'tables_skipped', tables_skipped,
        'new_tables_onboarded', new_tables_onboarded,
        'decision_point_2_applied', TRUE,
        'triggered_at', CURRENT_TIMESTAMP()::VARCHAR
    );
    
    trigger_source_str := ARRAY_TO_STRING(COALESCE(tables_to_process, ARRAY_CONSTRUCT()), ', ');
    
    INSERT INTO DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS (
        execution_id, workflow_name, trigger_source, trigger_type, status, current_phase, planner_output
    )
    SELECT 
        :execution_id, 
        'Bronze to Silver Transformation', 
        :trigger_source_str,
        :trigger_type,
        'TRIGGERED',
        'TRIGGER',
        :trigger_details;
    
    -- Log to workflow log (using existing schema) - use SELECT INTO to avoid VALUES clause issues
    INSERT INTO DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LOG (workflow_id, workflow_type, source_table, status, details)
    SELECT 
        :execution_id, 
        'TRIGGER_DP2', 
        ARRAY_TO_STRING(COALESCE(:all_bronze_tables, ARRAY_CONSTRUCT()), ', '), 
        'DECISION_POINT_2_APPLIED', 
        OBJECT_CONSTRUCT(
            'message', 'Decision Point 2 applied',
            'skipped_count', ARRAY_SIZE(COALESCE(:tables_skipped, ARRAY_CONSTRUCT())),
            'process_count', ARRAY_SIZE(COALESCE(:tables_to_process, ARRAY_CONSTRUCT()))
        );
    
    RETURN OBJECT_CONSTRUCT(
        'execution_id', execution_id,
        'status', 'TRIGGERED',
        'decision_point_1', OBJECT_CONSTRUCT(
            'new_tables_onboarded', new_tables_onboarded,
            'discovery_result', discovery_result
        ),
        'decision_point_2', OBJECT_CONSTRUCT(
            'candidate_tables', ARRAY_SIZE(COALESCE(all_bronze_tables, ARRAY_CONSTRUCT())),
            'tables_needing_workflow', ARRAY_SIZE(COALESCE(tables_to_process, ARRAY_CONSTRUCT())),
            'tables_skipped', ARRAY_SIZE(COALESCE(tables_skipped, ARRAY_CONSTRUCT())),
            'skipped_details', tables_skipped
        ),
        'tables_to_process', COALESCE(tables_to_process, ARRAY_CONSTRUCT()),
        'next_phase', CASE 
            WHEN ARRAY_SIZE(COALESCE(tables_to_process, ARRAY_CONSTRUCT())) > 0 THEN 'PLANNER'
            ELSE 'SKIP_WORKFLOW'
        END
    );
END;
$$;
