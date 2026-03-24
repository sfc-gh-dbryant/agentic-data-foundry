-- =============================================================================
-- 07b_trigger.sql
-- TRIGGER phase: Auto-discovery, onboarding, and workflow initiation.
-- Consolidates 08b_workflow_trigger.sql (Decision Point 2 filtering) with
-- monolith auto-discovery helpers.
-- =============================================================================
USE DATABASE DBAONTAP_ANALYTICS;

CREATE OR REPLACE PROCEDURE AGENTS.AUTO_ONBOARD_TABLE(landing_table_name VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    table_base_name VARCHAR;
    stream_name VARCHAR;
    bronze_dt_name VARCHAR;
    create_stream_sql VARCHAR;
    create_bronze_sql VARCHAR;
BEGIN
    table_base_name := UPPER(SPLIT_PART(landing_table_name, '.', -1));
    stream_name := table_base_name || '_LANDING_STREAM';
    bronze_dt_name := table_base_name || '_VARIANT';
    
    create_stream_sql := 'CREATE STREAM IF NOT EXISTS AGENTS.' || stream_name || 
                         ' ON TABLE ' || landing_table_name || 
                         ' SHOW_INITIAL_ROWS = TRUE';
    EXECUTE IMMEDIATE :create_stream_sql;
    
    create_bronze_sql := '
        CREATE OR REPLACE DYNAMIC TABLE BRONZE.' || bronze_dt_name || '
        TARGET_LAG = ''1 minute''
        WAREHOUSE = DBRYANT_COCO_WH_S
        AS
        SELECT 
            OBJECT_CONSTRUCT(*) AS PAYLOAD,
            ''' || table_base_name || ''' AS SOURCE_TABLE,
            CURRENT_TIMESTAMP() AS INGESTED_AT
        FROM ' || landing_table_name;
    EXECUTE IMMEDIATE :create_bronze_sql;
    
    INSERT INTO METADATA.ONBOARDED_TABLES (table_name, landing_table, stream_name, bronze_table)
    SELECT :table_base_name, :landing_table_name, :stream_name, 'BRONZE.' || :bronze_dt_name
    WHERE NOT EXISTS (SELECT 1 FROM METADATA.ONBOARDED_TABLES WHERE table_name = :table_base_name);
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'ONBOARDED',
        'landing_table', landing_table_name,
        'stream', stream_name,
        'bronze_table', 'BRONZE.' || bronze_dt_name
    );
END;
$$;

CREATE OR REPLACE PROCEDURE AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    new_tables ARRAY;
    onboarded_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    current_table VARCHAR;
    onboard_result VARIANT;
    i INTEGER;
BEGIN
    SELECT ARRAY_AGG(landing.TABLE_CATALOG || '.' || landing.TABLE_SCHEMA || '.' || landing.TABLE_NAME)
    INTO :new_tables
    FROM INFORMATION_SCHEMA.TABLES landing
    WHERE landing.TABLE_SCHEMA = 'public'
      AND landing.TABLE_TYPE = 'BASE TABLE'
      AND landing.TABLE_NAME NOT LIKE '%JOURNAL%'
      AND NOT EXISTS (
          SELECT 1 FROM INFORMATION_SCHEMA.TABLES bronze
          WHERE bronze.TABLE_SCHEMA = 'BRONZE'
            AND bronze.TABLE_NAME = UPPER(landing.TABLE_NAME) || '_VARIANT'
      );
    
    IF (new_tables IS NULL OR ARRAY_SIZE(new_tables) = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'NO_NEW_TABLES',
            'message', 'All landing tables already have Bronze counterparts'
        );
    END IF;
    
    FOR i IN 0 TO ARRAY_SIZE(new_tables) - 1 DO
        current_table := new_tables[i]::VARCHAR;
        
        BEGIN
            onboard_result := (CALL AGENTS.AUTO_ONBOARD_TABLE(:current_table));
            onboarded_results := ARRAY_APPEND(onboarded_results, onboard_result);
        EXCEPTION WHEN OTHER THEN
            onboarded_results := ARRAY_APPEND(onboarded_results, OBJECT_CONSTRUCT(
                'status', 'FAILED',
                'table', current_table,
                'error', SQLERRM
            ));
        END;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'DISCOVERY_COMPLETE',
        'new_tables_found', ARRAY_SIZE(new_tables),
        'onboarded', onboarded_results
    );
END;
$$;

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_TRIGGER(
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
    
    discovery_result := (CALL DBAONTAP_ANALYTICS.AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES());
    IF (discovery_result:status::VARCHAR = 'DISCOVERY_COMPLETE') THEN
        new_tables_onboarded := discovery_result:new_tables_found::INTEGER;
    END IF;
    
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
