-- =============================================================================
-- KNOWLEDGE GRAPH FOR TABLE CONTEXT
-- Stores metadata about tables, columns, relationships, and lineage
-- Uses Node-Edge pattern for graph representation in Snowflake
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS KNOWLEDGE_GRAPH;
USE SCHEMA KNOWLEDGE_GRAPH;

-- =============================================================================
-- CORE GRAPH TABLES
-- =============================================================================

-- KG_NODE: Universal entity storage
CREATE OR REPLACE TABLE KG_NODE (
    node_id VARCHAR(255) PRIMARY KEY,
    node_type VARCHAR(50) NOT NULL,        -- TABLE, COLUMN, SCHEMA, DATABASE, DOMAIN, BUSINESS_TERM
    name VARCHAR(500) NOT NULL,
    description TEXT,
    properties VARIANT,                     -- Flexible properties (data_type, row_count, etc.)
    embedding VECTOR(FLOAT, 768),           -- For semantic search
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- KG_EDGE: Relationships between nodes
CREATE OR REPLACE TABLE KG_EDGE (
    edge_id VARCHAR(255) PRIMARY KEY,
    source_node_id VARCHAR(255) NOT NULL REFERENCES KG_NODE(node_id),
    target_node_id VARCHAR(255) NOT NULL REFERENCES KG_NODE(node_id),
    edge_type VARCHAR(100) NOT NULL,        -- CONTAINS, REFERENCES, DERIVED_FROM, SIMILAR_TO, etc.
    properties VARIANT,                     -- Weight, confidence, metadata
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- CONVENIENCE VIEWS
-- =============================================================================

-- View: All Tables
CREATE OR REPLACE VIEW V_TABLES AS
SELECT 
    node_id,
    name AS table_name,
    properties:schema::VARCHAR AS schema_name,
    properties:database::VARCHAR AS database_name,
    properties:table_type::VARCHAR AS table_type,
    properties:row_count::INTEGER AS row_count,
    properties:medallion_layer::VARCHAR AS medallion_layer,
    description,
    properties
FROM KG_NODE
WHERE node_type = 'TABLE';

-- View: All Columns
CREATE OR REPLACE VIEW V_COLUMNS AS
SELECT 
    node_id,
    name AS column_name,
    properties:table_id::VARCHAR AS table_id,
    properties:data_type::VARCHAR AS data_type,
    properties:is_nullable::BOOLEAN AS is_nullable,
    properties:is_primary_key::BOOLEAN AS is_primary_key,
    properties:is_foreign_key::BOOLEAN AS is_foreign_key,
    description
FROM KG_NODE
WHERE node_type = 'COLUMN';

-- View: Table Lineage (DERIVED_FROM relationships)
CREATE OR REPLACE VIEW V_TABLE_LINEAGE AS
SELECT 
    e.edge_id,
    src.name AS source_table,
    src.properties:schema::VARCHAR AS source_schema,
    tgt.name AS target_table,
    tgt.properties:schema::VARCHAR AS target_schema,
    e.edge_type,
    e.properties:transformation_type::VARCHAR AS transformation_type
FROM KG_EDGE e
JOIN KG_NODE src ON e.source_node_id = src.node_id
JOIN KG_NODE tgt ON e.target_node_id = tgt.node_id
WHERE e.edge_type IN ('DERIVED_FROM', 'FEEDS_INTO', 'TRANSFORMS_TO');

-- View: Column Relationships (FK references)
CREATE OR REPLACE VIEW V_COLUMN_RELATIONSHIPS AS
SELECT 
    e.edge_id,
    src.name AS source_column,
    src.properties:table_id::VARCHAR AS source_table_id,
    tgt.name AS target_column,
    tgt.properties:table_id::VARCHAR AS target_table_id,
    e.edge_type,
    e.properties
FROM KG_EDGE e
JOIN KG_NODE src ON e.source_node_id = src.node_id
JOIN KG_NODE tgt ON e.target_node_id = tgt.node_id
WHERE src.node_type = 'COLUMN' AND tgt.node_type = 'COLUMN';

-- =============================================================================
-- POPULATE KNOWLEDGE GRAPH FROM EXISTING TABLES
-- =============================================================================

CREATE OR REPLACE PROCEDURE POPULATE_KG_FROM_INFORMATION_SCHEMA()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    tables_added INTEGER DEFAULT 0;
    columns_added INTEGER DEFAULT 0;
    edges_added INTEGER DEFAULT 0;
BEGIN
    -- Add Database node
    MERGE INTO KG_NODE tgt
    USING (SELECT 'DB:DBAONTAP_ANALYTICS' AS node_id, 'DATABASE' AS node_type, 
                  'DBAONTAP_ANALYTICS' AS name, 'Analytics database for agentic demo' AS description,
                  OBJECT_CONSTRUCT('created_at', CURRENT_TIMESTAMP()) AS properties) src
    ON tgt.node_id = src.node_id
    WHEN NOT MATCHED THEN INSERT (node_id, node_type, name, description, properties)
    VALUES (src.node_id, src.node_type, src.name, src.description, src.properties);
    
    -- Add Schema nodes
    MERGE INTO KG_NODE tgt
    USING (
        SELECT DISTINCT 
            'SCHEMA:DBAONTAP_ANALYTICS.' || TABLE_SCHEMA AS node_id,
            'SCHEMA' AS node_type,
            TABLE_SCHEMA AS name,
            CASE TABLE_SCHEMA
                WHEN 'BRONZE' THEN 'Raw data in VARIANT format'
                WHEN 'SILVER' THEN 'Cleaned and typed data'
                WHEN 'GOLD' THEN 'Aggregated business metrics'
                WHEN 'AGENTS' THEN 'Agentic workflow procedures'
                WHEN 'METADATA' THEN 'Workflow tracking tables'
                ELSE 'Schema: ' || TABLE_SCHEMA
            END AS description,
            OBJECT_CONSTRUCT(
                'database', 'DBAONTAP_ANALYTICS',
                'medallion_layer', CASE TABLE_SCHEMA 
                    WHEN 'BRONZE' THEN 'BRONZE'
                    WHEN 'SILVER' THEN 'SILVER' 
                    WHEN 'GOLD' THEN 'GOLD'
                    ELSE NULL 
                END
            ) AS properties
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD', 'AGENTS', 'METADATA')
    ) src
    ON tgt.node_id = src.node_id
    WHEN NOT MATCHED THEN INSERT (node_id, node_type, name, description, properties)
    VALUES (src.node_id, src.node_type, src.name, src.description, src.properties);
    
    -- Add Table nodes
    MERGE INTO KG_NODE tgt
    USING (
        SELECT 
            'TABLE:DBAONTAP_ANALYTICS.' || TABLE_SCHEMA || '.' || TABLE_NAME AS node_id,
            'TABLE' AS node_type,
            TABLE_NAME AS name,
            NULL AS description,
            OBJECT_CONSTRUCT(
                'database', 'DBAONTAP_ANALYTICS',
                'schema', TABLE_SCHEMA,
                'table_type', TABLE_TYPE,
                'medallion_layer', CASE TABLE_SCHEMA 
                    WHEN 'BRONZE' THEN 'BRONZE'
                    WHEN 'SILVER' THEN 'SILVER' 
                    WHEN 'GOLD' THEN 'GOLD'
                    ELSE NULL 
                END,
                'row_count', ROW_COUNT,
                'bytes', BYTES
            ) AS properties
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD')
    ) src
    ON tgt.node_id = src.node_id
    WHEN MATCHED THEN UPDATE SET properties = src.properties, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (node_id, node_type, name, description, properties)
    VALUES (src.node_id, src.node_type, src.name, src.description, src.properties);
    
    SELECT COUNT(*) INTO :tables_added FROM KG_NODE WHERE node_type = 'TABLE';
    
    -- Add Column nodes
    MERGE INTO KG_NODE tgt
    USING (
        SELECT 
            'COLUMN:DBAONTAP_ANALYTICS.' || TABLE_SCHEMA || '.' || TABLE_NAME || '.' || COLUMN_NAME AS node_id,
            'COLUMN' AS node_type,
            COLUMN_NAME AS name,
            COMMENT AS description,
            OBJECT_CONSTRUCT(
                'table_id', 'TABLE:DBAONTAP_ANALYTICS.' || TABLE_SCHEMA || '.' || TABLE_NAME,
                'data_type', DATA_TYPE,
                'is_nullable', IS_NULLABLE = 'YES',
                'ordinal_position', ORDINAL_POSITION
            ) AS properties
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD')
    ) src
    ON tgt.node_id = src.node_id
    WHEN MATCHED THEN UPDATE SET properties = src.properties, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (node_id, node_type, name, description, properties)
    VALUES (src.node_id, src.node_type, src.name, src.description, src.properties);
    
    SELECT COUNT(*) INTO :columns_added FROM KG_NODE WHERE node_type = 'COLUMN';
    
    -- Add SCHEMA -> TABLE edges (CONTAINS)
    MERGE INTO KG_EDGE tgt
    USING (
        SELECT 
            'EDGE:CONTAINS:' || n_schema.node_id || ':' || n_table.node_id AS edge_id,
            n_schema.node_id AS source_node_id,
            n_table.node_id AS target_node_id,
            'CONTAINS' AS edge_type,
            OBJECT_CONSTRUCT('relationship', 'schema_contains_table') AS properties
        FROM KG_NODE n_schema
        JOIN KG_NODE n_table ON n_table.properties:schema::VARCHAR = n_schema.name
        WHERE n_schema.node_type = 'SCHEMA' AND n_table.node_type = 'TABLE'
    ) src
    ON tgt.edge_id = src.edge_id
    WHEN NOT MATCHED THEN INSERT (edge_id, source_node_id, target_node_id, edge_type, properties)
    VALUES (src.edge_id, src.source_node_id, src.target_node_id, src.edge_type, src.properties);
    
    -- Add TABLE -> COLUMN edges (HAS_COLUMN)
    MERGE INTO KG_EDGE tgt
    USING (
        SELECT 
            'EDGE:HAS_COLUMN:' || n_table.node_id || ':' || n_col.node_id AS edge_id,
            n_table.node_id AS source_node_id,
            n_col.node_id AS target_node_id,
            'HAS_COLUMN' AS edge_type,
            OBJECT_CONSTRUCT('ordinal_position', n_col.properties:ordinal_position) AS properties
        FROM KG_NODE n_table
        JOIN KG_NODE n_col ON n_col.properties:table_id::VARCHAR = n_table.node_id
        WHERE n_table.node_type = 'TABLE' AND n_col.node_type = 'COLUMN'
    ) src
    ON tgt.edge_id = src.edge_id
    WHEN NOT MATCHED THEN INSERT (edge_id, source_node_id, target_node_id, edge_type, properties)
    VALUES (src.edge_id, src.source_node_id, src.target_node_id, src.edge_type, src.properties);
    
    -- Add BRONZE -> SILVER lineage edges (TRANSFORMS_TO)
    MERGE INTO KG_EDGE tgt
    USING (
        SELECT 
            'EDGE:TRANSFORMS_TO:' || bronze.node_id || ':' || silver.node_id AS edge_id,
            bronze.node_id AS source_node_id,
            silver.node_id AS target_node_id,
            'TRANSFORMS_TO' AS edge_type,
            OBJECT_CONSTRUCT(
                'transformation_type', 'AGENTIC_SILVER',
                'description', 'Bronze VARIANT transformed to typed Silver by agentic workflow'
            ) AS properties
        FROM KG_NODE bronze
        JOIN KG_NODE silver ON REPLACE(bronze.name, '_VARIANT', '') = silver.name
        WHERE bronze.node_type = 'TABLE' 
          AND bronze.properties:schema::VARCHAR = 'BRONZE'
          AND bronze.name LIKE '%_VARIANT'
          AND silver.node_type = 'TABLE'
          AND silver.properties:schema::VARCHAR = 'SILVER'
    ) src
    ON tgt.edge_id = src.edge_id
    WHEN NOT MATCHED THEN INSERT (edge_id, source_node_id, target_node_id, edge_type, properties)
    VALUES (src.edge_id, src.source_node_id, src.target_node_id, src.edge_type, src.properties);
    
    -- Add SILVER -> GOLD lineage edges based on naming conventions
    MERGE INTO KG_EDGE tgt
    USING (
        SELECT 
            'EDGE:AGGREGATES_TO:' || silver.node_id || ':' || gold.node_id AS edge_id,
            silver.node_id AS source_node_id,
            gold.node_id AS target_node_id,
            'AGGREGATES_TO' AS edge_type,
            OBJECT_CONSTRUCT(
                'transformation_type', 'AGGREGATION',
                'description', 'Silver table aggregated to Gold metrics'
            ) AS properties
        FROM KG_NODE silver
        CROSS JOIN KG_NODE gold
        WHERE silver.node_type = 'TABLE' 
          AND silver.properties:schema::VARCHAR = 'SILVER'
          AND gold.node_type = 'TABLE'
          AND gold.properties:schema::VARCHAR = 'GOLD'
          AND (
              (silver.name = 'CUSTOMERS' AND gold.name IN ('CUSTOMER_360', 'CUSTOMER_METRICS', 'ML_CUSTOMER_FEATURES'))
              OR (silver.name = 'ORDERS' AND gold.name IN ('ORDER_SUMMARY', 'CUSTOMER_360'))
              OR (silver.name = 'ORDER_ITEMS' AND gold.name IN ('ORDER_SUMMARY', 'PRODUCT_PERFORMANCE'))
              OR (silver.name = 'PRODUCTS' AND gold.name = 'PRODUCT_PERFORMANCE')
          )
    ) src
    ON tgt.edge_id = src.edge_id
    WHEN NOT MATCHED THEN INSERT (edge_id, source_node_id, target_node_id, edge_type, properties)
    VALUES (src.edge_id, src.source_node_id, src.target_node_id, src.edge_type, src.properties);
    
    SELECT COUNT(*) INTO :edges_added FROM KG_EDGE;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'tables_added', tables_added,
        'columns_added', columns_added,
        'edges_added', edges_added
    );
END;
$$;

-- =============================================================================
-- AI-POWERED ENRICHMENT
-- =============================================================================

-- Generate descriptions using Cortex LLM
CREATE OR REPLACE PROCEDURE ENRICH_TABLE_DESCRIPTIONS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    enriched_count INTEGER DEFAULT 0;
    cur CURSOR FOR 
        SELECT node_id, name, properties
        FROM KG_NODE 
        WHERE node_type = 'TABLE' AND (description IS NULL OR description = '');
BEGIN
    FOR rec IN cur DO
        LET table_name VARCHAR := rec.name;
        LET schema_name VARCHAR := rec.properties:schema::VARCHAR;
        LET columns_json VARCHAR;
        
        -- Get column list for this table
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT('name', name, 'type', properties:data_type::VARCHAR))::VARCHAR
        INTO :columns_json
        FROM KG_NODE 
        WHERE node_type = 'COLUMN' 
          AND properties:table_id::VARCHAR = rec.node_id;
        
        -- Generate description using Cortex
        LET prompt VARCHAR := 'Generate a brief 1-2 sentence description for a database table named "' || 
                              table_name || '" in the "' || schema_name || 
                              '" schema with these columns: ' || columns_json ||
                              '. Focus on the business purpose of this table.';
        
        LET description VARCHAR;
        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :prompt) INTO :description;
        
        UPDATE KG_NODE SET description = :description, updated_at = CURRENT_TIMESTAMP()
        WHERE node_id = rec.node_id;
        
        enriched_count := enriched_count + 1;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'tables_enriched', enriched_count);
END;
$$;

-- Generate embeddings for semantic search
CREATE OR REPLACE PROCEDURE GENERATE_NODE_EMBEDDINGS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE KG_NODE
    SET embedding = SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
        COALESCE(name, '') || ' ' || COALESCE(description, '') || ' ' || 
        COALESCE(properties::VARCHAR, '')
    ),
    updated_at = CURRENT_TIMESTAMP()
    WHERE embedding IS NULL OR updated_at < DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'nodes_updated', SQLROWCOUNT);
END;
$$;

-- =============================================================================
-- GRAPH TRAVERSAL FUNCTIONS
-- =============================================================================

-- Find related tables (1-hop neighbors)
CREATE OR REPLACE FUNCTION GET_RELATED_TABLES(table_node_id VARCHAR)
RETURNS TABLE (
    related_table VARCHAR,
    relationship VARCHAR,
    direction VARCHAR
)
AS
$$
    SELECT 
        CASE WHEN e.source_node_id = table_node_id THEN tgt.name ELSE src.name END AS related_table,
        e.edge_type AS relationship,
        CASE WHEN e.source_node_id = table_node_id THEN 'OUTGOING' ELSE 'INCOMING' END AS direction
    FROM KG_EDGE e
    JOIN KG_NODE src ON e.source_node_id = src.node_id
    JOIN KG_NODE tgt ON e.target_node_id = tgt.node_id
    WHERE (e.source_node_id = table_node_id OR e.target_node_id = table_node_id)
      AND src.node_type = 'TABLE' AND tgt.node_type = 'TABLE'
$$;

-- Get full lineage path (multi-hop using recursive CTE)
CREATE OR REPLACE FUNCTION GET_TABLE_LINEAGE(start_table VARCHAR, direction VARCHAR DEFAULT 'UPSTREAM')
RETURNS TABLE (
    table_name VARCHAR,
    schema_name VARCHAR,
    hop_level INTEGER,
    path VARCHAR
)
AS
$$
    WITH RECURSIVE lineage AS (
        -- Base case: starting table
        SELECT 
            n.name AS table_name,
            n.properties:schema::VARCHAR AS schema_name,
            0 AS hop_level,
            n.name AS path,
            n.node_id
        FROM KG_NODE n
        WHERE n.node_type = 'TABLE' AND n.name = start_table
        
        UNION ALL
        
        -- Recursive case: traverse edges
        SELECT 
            CASE WHEN direction = 'UPSTREAM' THEN src.name ELSE tgt.name END AS table_name,
            CASE WHEN direction = 'UPSTREAM' THEN src.properties:schema::VARCHAR ELSE tgt.properties:schema::VARCHAR END AS schema_name,
            l.hop_level + 1,
            l.path || ' -> ' || CASE WHEN direction = 'UPSTREAM' THEN src.name ELSE tgt.name END,
            CASE WHEN direction = 'UPSTREAM' THEN src.node_id ELSE tgt.node_id END
        FROM lineage l
        JOIN KG_EDGE e ON (
            (direction = 'UPSTREAM' AND e.target_node_id = l.node_id) OR
            (direction = 'DOWNSTREAM' AND e.source_node_id = l.node_id)
        )
        JOIN KG_NODE src ON e.source_node_id = src.node_id
        JOIN KG_NODE tgt ON e.target_node_id = tgt.node_id
        WHERE e.edge_type IN ('TRANSFORMS_TO', 'AGGREGATES_TO', 'DERIVED_FROM')
          AND l.hop_level < 5
          AND src.node_type = 'TABLE' AND tgt.node_type = 'TABLE'
    )
    SELECT DISTINCT table_name, schema_name, hop_level, path
    FROM lineage
    ORDER BY hop_level
$$;

-- Semantic search for tables
CREATE OR REPLACE FUNCTION SEARCH_TABLES(query_text VARCHAR, top_k INTEGER DEFAULT 5)
RETURNS TABLE (
    table_name VARCHAR,
    schema_name VARCHAR,
    description TEXT,
    similarity FLOAT
)
AS
$$
    SELECT 
        name AS table_name,
        properties:schema::VARCHAR AS schema_name,
        description,
        VECTOR_COSINE_SIMILARITY(embedding, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', query_text)) AS similarity
    FROM KG_NODE
    WHERE node_type = 'TABLE' AND embedding IS NOT NULL
    ORDER BY similarity DESC
    LIMIT top_k
$$;

-- =============================================================================
-- CORTEX SEARCH SERVICE FOR NATURAL LANGUAGE QUERIES
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE TABLE_CONTEXT_SEARCH
ON name, description
ATTRIBUTES schema_name, node_type, medallion_layer
WAREHOUSE = DBRYANT_COCO_WH_S
TARGET_LAG = '1 hour'
AS (
    SELECT 
        node_id,
        name,
        description,
        node_type,
        properties:schema::VARCHAR AS schema_name,
        properties:medallion_layer::VARCHAR AS medallion_layer
    FROM KG_NODE
    WHERE node_type IN ('TABLE', 'COLUMN', 'SCHEMA')
);

-- =============================================================================
-- RUN INITIAL POPULATION
-- =============================================================================
CALL POPULATE_KG_FROM_INFORMATION_SCHEMA();
