-- =============================================================================
-- DECISION POINT 2: Silver Existence Check & Schema Change Detection
-- Implements the "dual-path processing model" from AGENTIC_TRANSFORMATION_OBJECTS.md
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE SCHEMA AGENTS;

-- ============================================================================
-- HELPER 1: Check if Silver DT exists for a given Bronze table
-- ============================================================================
CREATE OR REPLACE FUNCTION AGENTS.SILVER_DT_EXISTS(bronze_table_name VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT COUNT(*) > 0
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SILVER'
      AND TABLE_TYPE = 'BASE TABLE'
      AND TABLE_NAME = REPLACE(UPPER(SPLIT_PART(bronze_table_name, '.', -1)), '_VARIANT', '')
$$;

-- ============================================================================
-- HELPER 2: Detect schema changes between Bronze VARIANT and Silver columns
-- Returns TRUE if schema has changed (new columns added or columns removed)
-- ============================================================================
CREATE OR REPLACE PROCEDURE AGENTS.DETECT_SCHEMA_CHANGES(bronze_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$
DECLARE
    silver_table VARCHAR;
    silver_table_name VARCHAR;
    bronze_keys ARRAY;
    silver_cols ARRAY;
    new_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    dropped_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    silver_exists_count INTEGER DEFAULT 0;
    has_changes BOOLEAN DEFAULT FALSE;
BEGIN
    -- Derive Silver table name from Bronze
    silver_table_name := REPLACE(UPPER(SPLIT_PART(bronze_table, '.', -1)), '_VARIANT', '');
    silver_table := 'DBAONTAP_ANALYTICS.SILVER.' || silver_table_name;
    
    -- Check if Silver table exists first
    SELECT COUNT(*) INTO :silver_exists_count
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SILVER'
      AND TABLE_NAME = :silver_table_name;
    
    IF (silver_exists_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'bronze_table', bronze_table,
            'silver_table', silver_table,
            'silver_exists', FALSE,
            'schema_changed', TRUE,
            'reason', 'Silver table does not exist'
        );
    END IF;
    
    -- Get Bronze VARIANT keys (sample rows to discover schema)
    SELECT ARRAY_AGG(DISTINCT f.key)
    INTO :bronze_keys
    FROM (SELECT * FROM IDENTIFIER(:bronze_table) LIMIT 100) src,
         LATERAL FLATTEN(input => src.PAYLOAD) f;
    
    -- Get Silver column names (exclude metadata columns)
    SELECT ARRAY_AGG(COLUMN_NAME)
    INTO :silver_cols
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = :silver_table_name
      AND COLUMN_NAME NOT IN ('IS_DELETED', 'CDC_TIMESTAMP', 'RN', 'INGESTED_AT', 'SOURCE_TABLE');
    
    -- Find new columns in Bronze not in Silver (excluding CDC columns)
    SELECT ARRAY_AGG(bk.value)
    INTO :new_cols
    FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    WHERE UPPER(bk.value::VARCHAR) NOT IN (SELECT UPPER(sc.value::VARCHAR) FROM TABLE(FLATTEN(input => :silver_cols)) sc)
      AND bk.value::VARCHAR NOT LIKE '_SNOWFLAKE%';
    
    -- Find dropped columns in Silver not in Bronze
    SELECT ARRAY_AGG(sc.value)
    INTO :dropped_cols  
    FROM TABLE(FLATTEN(input => :silver_cols)) sc
    WHERE UPPER(sc.value::VARCHAR) NOT IN (SELECT UPPER(bk.value::VARCHAR) FROM TABLE(FLATTEN(input => :bronze_keys)) bk)
      AND sc.value::VARCHAR NOT IN ('IS_DELETED', 'CDC_TIMESTAMP', 'RN');
    
    has_changes := (ARRAY_SIZE(COALESCE(new_cols, ARRAY_CONSTRUCT())) > 0 OR 
                    ARRAY_SIZE(COALESCE(dropped_cols, ARRAY_CONSTRUCT())) > 0);
    
    RETURN OBJECT_CONSTRUCT(
        'bronze_table', bronze_table,
        'silver_table', silver_table,
        'silver_exists', TRUE,
        'schema_changed', has_changes,
        'bronze_keys_count', ARRAY_SIZE(COALESCE(bronze_keys, ARRAY_CONSTRUCT())),
        'silver_cols_count', ARRAY_SIZE(COALESCE(silver_cols, ARRAY_CONSTRUCT())),
        'new_columns', COALESCE(new_cols, ARRAY_CONSTRUCT()),
        'dropped_columns', COALESCE(dropped_cols, ARRAY_CONSTRUCT())
    );
END;
$;

-- ============================================================================
-- HELPER 3: Filter tables that need agentic processing
-- Only returns tables where: Silver doesn't exist OR schema has changed
-- ============================================================================
CREATE OR REPLACE PROCEDURE AGENTS.FILTER_TABLES_FOR_AGENTIC_WORKFLOW(tables_to_check ARRAY)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    tables_needing_workflow ARRAY DEFAULT ARRAY_CONSTRUCT();
    tables_to_skip ARRAY DEFAULT ARRAY_CONSTRUCT();
    current_table VARCHAR;
    schema_check VARIANT;
    i INTEGER;
BEGIN
    FOR i IN 0 TO ARRAY_SIZE(tables_to_check) - 1 DO
        current_table := tables_to_check[i]::VARCHAR;
        
        -- Check schema changes for this table
        schema_check := (CALL AGENTS.DETECT_SCHEMA_CHANGES(:current_table));
        
        IF (schema_check:silver_exists::BOOLEAN = FALSE OR schema_check:schema_changed::BOOLEAN = TRUE) THEN
            -- Needs agentic workflow
            tables_needing_workflow := ARRAY_APPEND(tables_needing_workflow, OBJECT_CONSTRUCT(
                'table', current_table,
                'reason', CASE 
                    WHEN schema_check:silver_exists::BOOLEAN = FALSE THEN 'NEW_TABLE'
                    ELSE 'SCHEMA_CHANGED'
                END,
                'details', schema_check
            ));
        ELSE
            -- Skip - let DT auto-refresh handle it
            tables_to_skip := ARRAY_APPEND(tables_to_skip, OBJECT_CONSTRUCT(
                'table', current_table,
                'reason', 'SILVER_EXISTS_NO_CHANGES',
                'action', 'DT_AUTO_REFRESH'
            ));
        END IF;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'tables_needing_workflow', tables_needing_workflow,
        'tables_to_skip', tables_to_skip,
        'workflow_count', ARRAY_SIZE(tables_needing_workflow),
        'skip_count', ARRAY_SIZE(tables_to_skip)
    );
END;
$$;

-- ============================================================================
-- UPDATED WORKFLOW_TRIGGER with Decision Point 2 Logic
-- Now filters tables before passing to PLANNER
-- ============================================================================
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
    
    -- =========================================================================
    -- DECISION POINT 1: AUTO-DISCOVERY - Check for new landing tables
    -- =========================================================================
    discovery_result := (CALL AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES());
    IF (discovery_result:status::VARCHAR = 'DISCOVERY_COMPLETE') THEN
        new_tables_onboarded := discovery_result:new_tables_found::INTEGER;
    END IF;
    
    -- =========================================================================
    -- Determine candidate Bronze tables based on trigger type
    -- =========================================================================
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
    
    -- =========================================================================
    -- DECISION POINT 2: Filter tables - only process those needing workflow
    -- Skip tables where Silver exists and schema hasn't changed
    -- =========================================================================
    IF (ARRAY_SIZE(COALESCE(all_bronze_tables, ARRAY_CONSTRUCT())) > 0) THEN
        filtered_result := (CALL AGENTS.FILTER_TABLES_FOR_AGENTIC_WORKFLOW(:all_bronze_tables));
        
        -- Extract just the table names for tables that need processing
        SELECT ARRAY_AGG(t.value:table::VARCHAR)
        INTO :tables_to_process
        FROM TABLE(FLATTEN(input => :filtered_result:tables_needing_workflow)) t;
        
        tables_skipped := filtered_result:tables_to_skip;
    ELSE
        tables_to_process := ARRAY_CONSTRUCT();
        tables_skipped := ARRAY_CONSTRUCT();
        filtered_result := OBJECT_CONSTRUCT('workflow_count', 0, 'skip_count', 0);
    END IF;
    
    -- =========================================================================
    -- Create workflow execution record
    -- =========================================================================
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
    
    INSERT INTO METADATA.WORKFLOW_EXECUTIONS (
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
    
    -- Log to workflow log
    INSERT INTO METADATA.WORKFLOW_LOG (execution_id, phase, status, message)
    VALUES (
        :execution_id,
        'TRIGGER',
        'DECISION_POINT_2_APPLIED',
        'Filtered ' || ARRAY_SIZE(COALESCE(all_bronze_tables, ARRAY_CONSTRUCT())) || ' candidates to ' || 
        ARRAY_SIZE(COALESCE(tables_to_process, ARRAY_CONSTRUCT())) || ' tables needing workflow. ' ||
        ARRAY_SIZE(COALESCE(tables_skipped, ARRAY_CONSTRUCT())) || ' skipped (Silver exists, no schema changes).'
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

-- ============================================================================
-- TEST QUERIES
-- ============================================================================
-- Test the helper function
-- SELECT AGENTS.SILVER_DT_EXISTS('DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT');

-- Test schema change detection
-- CALL AGENTS.DETECT_SCHEMA_CHANGES('DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT');

-- Test the filter
-- CALL AGENTS.FILTER_TABLES_FOR_AGENTIC_WORKFLOW(
--     ARRAY_CONSTRUCT(
--         'DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT',
--         'DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT'
--     )
-- );

-- Test updated trigger (should skip existing tables)
-- CALL AGENTS.WORKFLOW_TRIGGER('manual');
