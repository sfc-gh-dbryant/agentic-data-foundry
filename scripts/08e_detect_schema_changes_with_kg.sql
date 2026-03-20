-- =============================================================================
-- ENHANCED DETECT_SCHEMA_CHANGES WITH KNOWLEDGE GRAPH INTEGRATION
-- Adds lineage impact analysis and similar table suggestions
-- =============================================================================

CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.DETECT_SCHEMA_CHANGES(bronze_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    silver_table VARCHAR;
    silver_table_name VARCHAR;
    bronze_keys ARRAY;
    silver_cols ARRAY;
    new_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    dropped_cols ARRAY DEFAULT ARRAY_CONSTRUCT();
    ignored_new ARRAY DEFAULT ARRAY_CONSTRUCT();
    ignored_dropped ARRAY DEFAULT ARRAY_CONSTRUCT();
    silver_exists_count INTEGER DEFAULT 0;
    has_changes BOOLEAN DEFAULT FALSE;
    -- KG integration
    downstream_impact ARRAY DEFAULT ARRAY_CONSTRUCT();
    similar_tables VARIANT;
    kg_node_id VARCHAR;
BEGIN
    silver_table_name := REPLACE(UPPER(SPLIT_PART(bronze_table, '.', -1)), '_VARIANT', '');
    silver_table := 'DBAONTAP_ANALYTICS.SILVER.' || silver_table_name;
    kg_node_id := 'TABLE:DBAONTAP_ANALYTICS.SILVER.' || silver_table_name;
    
    SELECT COUNT(*) INTO :silver_exists_count
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SILVER'
      AND TABLE_NAME = :silver_table_name;
    
    IF (silver_exists_count = 0) THEN
        -- For new tables, find similar tables from KG for pattern reuse
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'table_name', name,
            'schema_name', properties:schema::VARCHAR,
            'similarity', VECTOR_COSINE_SIMILARITY(embedding, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', :silver_table_name))
        )) INTO :similar_tables
        FROM (
            SELECT name, properties, embedding
            FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_NODE
            WHERE node_type = 'TABLE' 
              AND embedding IS NOT NULL
              AND properties:schema::VARCHAR = 'SILVER'
            ORDER BY VECTOR_COSINE_SIMILARITY(embedding, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', :silver_table_name)) DESC
            LIMIT 3
        );
        
        RETURN OBJECT_CONSTRUCT(
            'bronze_table', bronze_table,
            'silver_table', silver_table,
            'silver_exists', FALSE,
            'schema_changed', TRUE,
            'reason', 'Silver table does not exist',
            'kg_insight', OBJECT_CONSTRUCT(
                'similar_tables', COALESCE(similar_tables, ARRAY_CONSTRUCT()),
                'recommendation', 'Consider reusing transformation patterns from similar tables'
            )
        );
    END IF;
    
    -- Get downstream lineage impact from Knowledge Graph
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'table_name', table_name,
        'schema', schema_name,
        'hop_level', hop_level,
        'path', path
    )) INTO :downstream_impact
    FROM TABLE(DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GET_TABLE_LINEAGE(:silver_table_name, 'DOWNSTREAM'))
    WHERE hop_level > 0;
    
    SELECT ARRAY_AGG(DISTINCT f.key)
    INTO :bronze_keys
    FROM (SELECT * FROM IDENTIFIER(:bronze_table) LIMIT 100) src,
         LATERAL FLATTEN(input => src.PAYLOAD) f;
    
    SELECT ARRAY_AGG(COLUMN_NAME)
    INTO :silver_cols
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = :silver_table_name
      AND COLUMN_NAME NOT IN (
          SELECT column_pattern FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
      );
    
    SELECT ARRAY_AGG(bk.value)
    INTO :new_cols
    FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    WHERE UPPER(bk.value::VARCHAR) NOT IN (
        SELECT UPPER(sc.value::VARCHAR) FROM TABLE(FLATTEN(input => :silver_cols)) sc
    )
    AND UPPER(bk.value::VARCHAR) NOT IN (
        SELECT column_pattern FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
    )
    AND bk.value::VARCHAR NOT LIKE '_SNOWFLAKE%';
    
    SELECT ARRAY_AGG(bk.value)
    INTO :ignored_new
    FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    WHERE UPPER(bk.value::VARCHAR) IN (
        SELECT column_pattern FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
    )
    OR bk.value::VARCHAR LIKE '_SNOWFLAKE%';
    
    SELECT ARRAY_AGG(sc.value)
    INTO :dropped_cols  
    FROM TABLE(FLATTEN(input => :silver_cols)) sc
    WHERE UPPER(sc.value::VARCHAR) NOT IN (
        SELECT UPPER(bk.value::VARCHAR) FROM TABLE(FLATTEN(input => :bronze_keys)) bk
    )
    AND UPPER(sc.value::VARCHAR) NOT IN (
        SELECT column_pattern FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
    );
    
    SELECT ARRAY_AGG(COLUMN_NAME)
    INTO :ignored_dropped
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = :silver_table_name
      AND COLUMN_NAME IN (
          SELECT column_pattern FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
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
        ),
        'kg_insight', OBJECT_CONSTRUCT(
            'downstream_tables', COALESCE(downstream_impact, ARRAY_CONSTRUCT()),
            'downstream_count', ARRAY_SIZE(COALESCE(downstream_impact, ARRAY_CONSTRUCT())),
            'impact_warning', CASE 
                WHEN has_changes AND ARRAY_SIZE(COALESCE(downstream_impact, ARRAY_CONSTRUCT())) > 0 
                THEN 'Schema change will impact ' || ARRAY_SIZE(COALESCE(downstream_impact, ARRAY_CONSTRUCT())) || ' downstream Gold tables'
                ELSE NULL
            END
        )
    );
END;
$$;
