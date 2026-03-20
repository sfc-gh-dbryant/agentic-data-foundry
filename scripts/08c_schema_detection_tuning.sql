-- =============================================================================
-- SCHEMA DETECTION TUNING
-- Configure known derived columns to ignore in schema change detection
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE SCHEMA METADATA;

-- =============================================================================
-- CONFIGURATION TABLE: Known derived/system columns to ignore
-- =============================================================================
CREATE TABLE IF NOT EXISTS SCHEMA_IGNORE_COLUMNS (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    column_pattern VARCHAR(255) NOT NULL,      -- Column name or pattern (supports LIKE)
    ignore_type VARCHAR(50) NOT NULL,          -- SILVER_DERIVED, SYSTEM_COLUMN, CDC_METADATA
    description VARCHAR(500),
    applies_to VARCHAR(50) DEFAULT 'ALL',      -- ALL or specific table name
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert known derived/system columns
MERGE INTO SCHEMA_IGNORE_COLUMNS tgt
USING (
    SELECT column_pattern, ignore_type, description FROM VALUES
    ('PHONE_STANDARDIZED', 'SILVER_DERIVED', 'Derived from phone column with standardization'),
    ('UPDATED_AT_SYSTEM', 'SYSTEM_COLUMN', 'System-generated update timestamp'),
    ('INSERTED_AT', 'SYSTEM_COLUMN', 'System-generated insert timestamp'),
    ('IS_DELETED', 'CDC_METADATA', 'CDC soft delete flag'),
    ('CDC_TIMESTAMP', 'CDC_METADATA', 'CDC operation timestamp'),
    ('_SNOWFLAKE%', 'SYSTEM_COLUMN', 'Snowflake internal metadata columns'),
    ('RN', 'SYSTEM_COLUMN', 'Row number for deduplication'),
    ('INGESTED_AT', 'SYSTEM_COLUMN', 'Ingestion timestamp'),
    ('SOURCE_TABLE', 'SYSTEM_COLUMN', 'Source table reference')
) AS src(column_pattern, ignore_type, description)
ON tgt.column_pattern = src.column_pattern
WHEN NOT MATCHED THEN INSERT (column_pattern, ignore_type, description)
VALUES (src.column_pattern, src.ignore_type, src.description);

-- =============================================================================
-- UPDATED DETECT_SCHEMA_CHANGES with ignore list
-- =============================================================================
USE SCHEMA AGENTS;

CREATE OR REPLACE PROCEDURE DETECT_SCHEMA_CHANGES(bronze_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    silver_table VARCHAR;
    silver_table_name VARCHAR;
    bronze_keys ARRAY;
    silver_cols ARRAY;
    ignore_patterns ARRAY;
    new_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    dropped_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    ignored_new ARRAY DEFAULT ARRAY_CONSTRUCT();
    ignored_dropped ARRAY DEFAULT ARRAY_CONSTRUCT();
    silver_exists_count INTEGER DEFAULT 0;
    has_changes BOOLEAN DEFAULT FALSE;
BEGIN
    -- Derive Silver table name from Bronze
    silver_table_name := REPLACE(UPPER(SPLIT_PART(bronze_table, '.', -1)), '_VARIANT', '');
    silver_table := 'DBAONTAP_ANALYTICS.SILVER.' || silver_table_name;
    
    -- Check if Silver table exists first
    SELECT COUNT(*) INTO :silver_exists_count
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
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
    
    -- Get ignore patterns from configuration table
    SELECT ARRAY_AGG(column_pattern)
    INTO :ignore_patterns
    FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS;
    
    -- Get Bronze VARIANT keys (sample rows to discover schema)
    SELECT ARRAY_AGG(DISTINCT f.key)
    INTO :bronze_keys
    FROM (SELECT * FROM IDENTIFIER(:bronze_table) LIMIT 100) src,
         LATERAL FLATTEN(input => src.PAYLOAD) f;
    
    -- Get Silver column names (exclude system columns via config)
    SELECT ARRAY_AGG(COLUMN_NAME)
    INTO :silver_cols
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = :silver_table_name
      AND NOT EXISTS (
          SELECT 1 FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS ic
          WHERE COLUMN_NAME LIKE ic.column_pattern
      );
    
    -- Find new columns in Bronze not in Silver (excluding ignored patterns)
    SELECT ARRAY_AGG(bk.value)
    INTO :new_cols
    FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    WHERE UPPER(bk.value::VARCHAR) NOT IN (
        SELECT UPPER(sc.value::VARCHAR) FROM TABLE(FLATTEN(input => :silver_cols)) sc
    )
    AND NOT EXISTS (
        SELECT 1 FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS ic
        WHERE UPPER(bk.value::VARCHAR) LIKE ic.column_pattern
    );
    
    -- Track which columns were ignored (for debugging)
    SELECT ARRAY_AGG(bk.value)
    INTO :ignored_new
    FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    WHERE EXISTS (
        SELECT 1 FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS ic
        WHERE UPPER(bk.value::VARCHAR) LIKE ic.column_pattern
    );
    
    -- Find dropped columns in Silver not in Bronze (excluding ignored patterns)
    SELECT ARRAY_AGG(sc.value)
    INTO :dropped_cols  
    FROM TABLE(FLATTEN(input => :silver_cols)) sc
    WHERE UPPER(sc.value::VARCHAR) NOT IN (
        SELECT UPPER(bk.value::VARCHAR) FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    )
    AND NOT EXISTS (
        SELECT 1 FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS ic
        WHERE UPPER(sc.value::VARCHAR) LIKE ic.column_pattern
    );
    
    -- Track which dropped columns were ignored
    SELECT ARRAY_AGG(col_name)
    INTO :ignored_dropped
    FROM (
        SELECT COLUMN_NAME AS col_name
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'SILVER' 
          AND TABLE_NAME = :silver_table_name
          AND EXISTS (
              SELECT 1 FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS ic
              WHERE COLUMN_NAME LIKE ic.column_pattern
          )
    );
    
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
        'dropped_columns', COALESCE(dropped_cols, ARRAY_CONSTRUCT()),
        'ignored_columns', OBJECT_CONSTRUCT(
            'new_ignored', COALESCE(ignored_new, ARRAY_CONSTRUCT()),
            'dropped_ignored', COALESCE(ignored_dropped, ARRAY_CONSTRUCT())
        )
    );
END;
$$;
