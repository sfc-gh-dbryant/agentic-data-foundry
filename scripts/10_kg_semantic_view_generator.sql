-- =============================================================================
-- KNOWLEDGE GRAPH SEMANTIC VIEW GENERATOR
-- Auto-generates Semantic Views from Knowledge Graph metadata
-- Uses KG_NODE for tables/columns and KG_EDGE for relationships
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE SCHEMA KNOWLEDGE_GRAPH;

-- =============================================================================
-- HELPER FUNCTION: Get column info for a table from KG
-- =============================================================================
CREATE OR REPLACE FUNCTION GET_TABLE_COLUMNS_FROM_KG(table_node_id VARCHAR)
RETURNS TABLE (
    column_name VARCHAR,
    data_type VARCHAR,
    description TEXT,
    is_primary_key BOOLEAN,
    is_foreign_key BOOLEAN,
    ordinal_position INTEGER
)
AS
$$
    SELECT 
        name AS column_name,
        properties:data_type::VARCHAR AS data_type,
        description,
        COALESCE(properties:is_primary_key::BOOLEAN, FALSE) AS is_primary_key,
        COALESCE(properties:is_foreign_key::BOOLEAN, FALSE) AS is_foreign_key,
        COALESCE(properties:ordinal_position::INTEGER, 0) AS ordinal_position
    FROM KG_NODE
    WHERE node_type = 'COLUMN'
      AND properties:table_id::VARCHAR = table_node_id
    ORDER BY ordinal_position
$$;

-- =============================================================================
-- HELPER FUNCTION: Get relationships for a table from KG
-- =============================================================================
CREATE OR REPLACE FUNCTION GET_TABLE_RELATIONSHIPS_FROM_KG(table_node_id VARCHAR)
RETURNS TABLE (
    related_table_name VARCHAR,
    related_table_schema VARCHAR,
    relationship_type VARCHAR,
    direction VARCHAR
)
AS
$$
    SELECT 
        CASE WHEN e.source_node_id = table_node_id THEN tgt.name ELSE src.name END AS related_table_name,
        CASE WHEN e.source_node_id = table_node_id THEN tgt.properties:schema::VARCHAR ELSE src.properties:schema::VARCHAR END AS related_table_schema,
        e.edge_type AS relationship_type,
        CASE WHEN e.source_node_id = table_node_id THEN 'OUTGOING' ELSE 'INCOMING' END AS direction
    FROM KG_EDGE e
    JOIN KG_NODE src ON e.source_node_id = src.node_id
    JOIN KG_NODE tgt ON e.target_node_id = tgt.node_id
    WHERE (e.source_node_id = table_node_id OR e.target_node_id = table_node_id)
      AND src.node_type = 'TABLE' AND tgt.node_type = 'TABLE'
      AND e.edge_type IN ('AGGREGATES_TO', 'DERIVED_FROM', 'REFERENCES')
$$;

-- =============================================================================
-- MAIN PROCEDURE: Generate Semantic View DDL from Knowledge Graph
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_SEMANTIC_VIEW_FROM_KG(
    table_name VARCHAR,
    schema_name VARCHAR DEFAULT 'GOLD'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    table_node_id VARCHAR;
    table_description TEXT;
    sv_name VARCHAR;
    ddl_statement VARCHAR;
    columns_yaml VARCHAR DEFAULT '';
    dimensions_yaml VARCHAR DEFAULT '';
    measures_yaml VARCHAR DEFAULT '';
    time_dimensions_yaml VARCHAR DEFAULT '';
    relationships_yaml VARCHAR DEFAULT '';
    col_count INTEGER DEFAULT 0;
    dim_count INTEGER DEFAULT 0;
    measure_count INTEGER DEFAULT 0;
BEGIN
    -- Build node ID
    table_node_id := 'TABLE:DBAONTAP_ANALYTICS.' || schema_name || '.' || table_name;
    
    -- Get table description from KG
    SELECT description INTO :table_description
    FROM KG_NODE
    WHERE node_id = :table_node_id;
    
    IF (table_description IS NULL) THEN
        table_description := 'Semantic view for ' || table_name;
    END IF;
    
    -- Semantic view name
    sv_name := table_name || '_KG_SV';
    
    -- Build columns section from KG
    FOR col IN (
        SELECT * FROM TABLE(GET_TABLE_COLUMNS_FROM_KG(:table_node_id))
    ) DO
        LET col_name VARCHAR := col.column_name;
        LET col_type VARCHAR := col.data_type;
        LET col_desc TEXT := COALESCE(col.description, 'Column: ' || col_name);
        LET is_pk BOOLEAN := col.is_primary_key;
        
        -- Skip internal Snowflake columns
        IF (col_name LIKE '_SNOWFLAKE%') THEN
            CONTINUE;
        END IF;
        
        -- Determine if dimension, measure, or time dimension
        IF (col_type IN ('DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ')) THEN
            -- Time dimension
            time_dimensions_yaml := time_dimensions_yaml || '
    - name: ' || LOWER(col_name) || '
      label: "' || INITCAP(REPLACE(col_name, '_', ' ')) || '"
      description: "' || REPLACE(col_desc, '"', '''') || '"
      expr: ' || col_name || '
      data_type: ' || col_type || '
      unique: ' || CASE WHEN is_pk THEN 'true' ELSE 'false' END;
            dim_count := dim_count + 1;
            
        ELSIF (col_type IN ('NUMBER', 'FLOAT', 'DECIMAL', 'DOUBLE', 'INTEGER', 'BIGINT') 
               AND LOWER(col_name) NOT LIKE '%_id'
               AND LOWER(col_name) NOT LIKE '%id') THEN
            -- Measure (numeric, not an ID)
            measures_yaml := measures_yaml || '
    - name: ' || LOWER(col_name) || '
      label: "' || INITCAP(REPLACE(col_name, '_', ' ')) || '"
      description: "' || REPLACE(col_desc, '"', '''') || '"
      expr: ' || col_name || '
      data_type: ' || col_type || '
      default_aggregation: ' || CASE 
          WHEN LOWER(col_name) LIKE '%count%' THEN 'sum'
          WHEN LOWER(col_name) LIKE '%total%' THEN 'sum'
          WHEN LOWER(col_name) LIKE '%amount%' THEN 'sum'
          WHEN LOWER(col_name) LIKE '%price%' THEN 'avg'
          WHEN LOWER(col_name) LIKE '%revenue%' THEN 'sum'
          WHEN LOWER(col_name) LIKE '%quantity%' THEN 'sum'
          ELSE 'sum'
      END;
            measure_count := measure_count + 1;
            
        ELSE
            -- Dimension (everything else)
            dimensions_yaml := dimensions_yaml || '
    - name: ' || LOWER(col_name) || '
      label: "' || INITCAP(REPLACE(col_name, '_', ' ')) || '"
      description: "' || REPLACE(col_desc, '"', '''') || '"
      expr: ' || col_name || '
      data_type: ' || CASE WHEN col_type IS NULL THEN 'TEXT' ELSE col_type END || '
      unique: ' || CASE WHEN is_pk THEN 'true' ELSE 'false' END;
            dim_count := dim_count + 1;
        END IF;
        
        col_count := col_count + 1;
    END FOR;
    
    -- Build complete DDL
    ddl_statement := 'CREATE OR REPLACE SEMANTIC VIEW DBAONTAP_ANALYTICS.GOLD.' || sv_name || '
COMMENT = ''Auto-generated from Knowledge Graph''
AS $$
@semantic_model(
  name: "' || sv_name || '"
  description: "' || REPLACE(table_description, '"', '''') || '"
  
  tables:
    - name: ' || table_name || '
      base_table:
        database: DBAONTAP_ANALYTICS
        schema: ' || schema_name || '
        table: ' || table_name || '
      
      dimensions:' || dimensions_yaml || '
      
      time_dimensions:' || CASE WHEN time_dimensions_yaml = '' THEN ' []' ELSE time_dimensions_yaml END || '
      
      measures:' || CASE WHEN measures_yaml = '' THEN ' []' ELSE measures_yaml END || '
)
$$';
    
    -- Return the result
    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'table_name', table_name,
        'semantic_view_name', sv_name,
        'ddl', ddl_statement,
        'statistics', OBJECT_CONSTRUCT(
            'total_columns', col_count,
            'dimensions', dim_count,
            'measures', measure_count
        )
    );
    
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'table_name', table_name,
            'error', SQLERRM
        );
END;
$$;

-- =============================================================================
-- BATCH PROCEDURE: Generate Semantic Views for all GOLD tables
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_ALL_SEMANTIC_VIEWS_FROM_KG()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    results ARRAY DEFAULT ARRAY_CONSTRUCT();
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
BEGIN
    -- Get all GOLD tables from KG
    FOR tbl IN (
        SELECT name, properties:schema::VARCHAR AS schema_name
        FROM KG_NODE
        WHERE node_type = 'TABLE'
          AND properties:schema::VARCHAR = 'GOLD'
    ) DO
        LET result VARIANT;
        LET ddl_to_execute VARCHAR;
        
        -- Generate DDL
        CALL GENERATE_SEMANTIC_VIEW_FROM_KG(tbl.name, tbl.schema_name) INTO :result;
        
        IF (result:status::VARCHAR = 'SUCCESS') THEN
            -- Execute the DDL
            ddl_to_execute := result:ddl::VARCHAR;
            BEGIN
                EXECUTE IMMEDIATE :ddl_to_execute;
                success_count := success_count + 1;
                results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
                    'table', tbl.name,
                    'semantic_view', result:semantic_view_name::VARCHAR,
                    'status', 'CREATED'
                ));
            EXCEPTION
                WHEN OTHER THEN
                    fail_count := fail_count + 1;
                    results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
                        'table', tbl.name,
                        'status', 'FAILED',
                        'error', SQLERRM,
                        'ddl', ddl_to_execute
                    ));
            END;
        ELSE
            fail_count := fail_count + 1;
            results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
                'table', tbl.name,
                'status', 'FAILED',
                'error', result:error::VARCHAR
            ));
        END IF;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'COMPLETED',
        'success_count', success_count,
        'fail_count', fail_count,
        'details', results
    );
END;
$$;

-- =============================================================================
-- ENRICHMENT: Add semantic view recommendations from KG relationships
-- =============================================================================
CREATE OR REPLACE FUNCTION SUGGEST_SEMANTIC_VIEW_JOINS(table_name VARCHAR)
RETURNS TABLE (
    source_table VARCHAR,
    target_table VARCHAR,
    suggested_join VARCHAR,
    confidence VARCHAR
)
AS
$$
    WITH table_node AS (
        SELECT node_id
        FROM KG_NODE
        WHERE node_type = 'TABLE' AND name = table_name
    ),
    column_refs AS (
        SELECT 
            src_col.name AS source_column,
            src_col.properties:table_id::VARCHAR AS source_table_id,
            tgt_col.name AS target_column,
            tgt_col.properties:table_id::VARCHAR AS target_table_id
        FROM KG_EDGE e
        JOIN KG_NODE src_col ON e.source_node_id = src_col.node_id
        JOIN KG_NODE tgt_col ON e.target_node_id = tgt_col.node_id
        WHERE e.edge_type = 'REFERENCES'
          AND src_col.node_type = 'COLUMN'
          AND (src_col.properties:table_id::VARCHAR LIKE '%.' || table_name 
               OR tgt_col.properties:table_id::VARCHAR LIKE '%.' || table_name)
    )
    SELECT 
        SPLIT_PART(source_table_id, '.', -1) AS source_table,
        SPLIT_PART(target_table_id, '.', -1) AS target_table,
        source_table || '.' || source_column || ' = ' || target_table || '.' || target_column AS suggested_join,
        'HIGH' AS confidence
    FROM column_refs
    
    UNION ALL
    
    -- Suggest joins based on naming convention (customer_id, order_id, etc.)
    SELECT DISTINCT
        t1.name AS source_table,
        t2.name AS target_table,
        t1.name || '.' || LOWER(t2.name) || '_id = ' || t2.name || '.' || LOWER(t2.name) || '_id' AS suggested_join,
        'MEDIUM' AS confidence
    FROM KG_NODE t1
    CROSS JOIN KG_NODE t2
    WHERE t1.node_type = 'TABLE' AND t2.node_type = 'TABLE'
      AND t1.name = table_name
      AND t1.name != t2.name
      AND t1.properties:schema::VARCHAR = 'GOLD'
      AND t2.properties:schema::VARCHAR = 'GOLD'
      AND EXISTS (
          SELECT 1 FROM KG_NODE c
          WHERE c.node_type = 'COLUMN'
            AND c.properties:table_id::VARCHAR LIKE '%.' || t1.name
            AND LOWER(c.name) = LOWER(t2.name) || '_id'
      )
$$;

-- =============================================================================
-- VIEW: Semantic View Recommendations Dashboard
-- =============================================================================
CREATE OR REPLACE VIEW V_SEMANTIC_VIEW_RECOMMENDATIONS AS
SELECT 
    n.name AS table_name,
    n.properties:schema::VARCHAR AS schema_name,
    n.description AS table_description,
    COALESCE(
        (SELECT COUNT(*) FROM KG_NODE c WHERE c.node_type = 'COLUMN' AND c.properties:table_id = n.node_id),
        0
    ) AS column_count,
    COALESCE(
        (SELECT COUNT(*) FROM KG_NODE c 
         WHERE c.node_type = 'COLUMN' 
           AND c.properties:table_id = n.node_id
           AND c.properties:data_type IN ('NUMBER', 'FLOAT', 'DECIMAL')
           AND c.name NOT LIKE '%_id'),
        0
    ) AS potential_measures,
    CASE 
        WHEN n.description IS NOT NULL THEN 'READY'
        ELSE 'NEEDS_ENRICHMENT'
    END AS kg_status,
    n.properties:row_count::INTEGER AS row_count
FROM KG_NODE n
WHERE n.node_type = 'TABLE'
  AND n.properties:schema::VARCHAR = 'GOLD'
ORDER BY n.name;

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================
GRANT USAGE ON SCHEMA KNOWLEDGE_GRAPH TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA KNOWLEDGE_GRAPH TO ROLE PUBLIC;
GRANT SELECT ON ALL VIEWS IN SCHEMA KNOWLEDGE_GRAPH TO ROLE PUBLIC;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA KNOWLEDGE_GRAPH TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE GENERATE_SEMANTIC_VIEW_FROM_KG(VARCHAR, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE GENERATE_ALL_SEMANTIC_VIEWS_FROM_KG() TO ROLE PUBLIC;
