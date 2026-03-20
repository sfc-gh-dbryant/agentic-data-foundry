CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    tables_added INTEGER DEFAULT 0;
    columns_added INTEGER DEFAULT 0;
    edges_added INTEGER DEFAULT 0;
BEGIN
    -- Clear stale data before repopulating
    TRUNCATE TABLE KG_EDGE;
    TRUNCATE TABLE KG_NODE;

    -- Add Database node
    INSERT INTO KG_NODE (node_id, node_type, name, description, properties)
    SELECT ''DB:DBAONTAP_ANALYTICS'', ''DATABASE'', ''DBAONTAP_ANALYTICS'',
           ''Analytics database for agentic demo'',
           OBJECT_CONSTRUCT(''created_at'', CURRENT_TIMESTAMP());

    -- Add Schema nodes
    INSERT INTO KG_NODE (node_id, node_type, name, description, properties)
    SELECT DISTINCT
        ''SCHEMA:DBAONTAP_ANALYTICS.'' || TABLE_SCHEMA,
        ''SCHEMA'',
        TABLE_SCHEMA,
        CASE TABLE_SCHEMA
            WHEN ''BRONZE'' THEN ''Raw data in VARIANT format''
            WHEN ''SILVER'' THEN ''Cleaned and typed data''
            WHEN ''GOLD'' THEN ''Aggregated business metrics''
            WHEN ''AGENTS'' THEN ''Agentic workflow procedures''
            WHEN ''METADATA'' THEN ''Workflow tracking tables''
            ELSE ''Schema: '' || TABLE_SCHEMA
        END,
        OBJECT_CONSTRUCT(
            ''database'', ''DBAONTAP_ANALYTICS'',
            ''medallion_layer'', CASE TABLE_SCHEMA
                WHEN ''BRONZE'' THEN ''BRONZE''
                WHEN ''SILVER'' THEN ''SILVER''
                WHEN ''GOLD'' THEN ''GOLD''
                ELSE NULL
            END
        )
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN (''BRONZE'', ''SILVER'', ''GOLD'', ''AGENTS'', ''METADATA'');

    -- Add Table nodes
    INSERT INTO KG_NODE (node_id, node_type, name, description, properties)
    SELECT
        ''TABLE:DBAONTAP_ANALYTICS.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME,
        ''TABLE'',
        TABLE_NAME,
        NULL,
        OBJECT_CONSTRUCT(
            ''database'', ''DBAONTAP_ANALYTICS'',
            ''schema'', TABLE_SCHEMA,
            ''table_type'', TABLE_TYPE,
            ''medallion_layer'', CASE TABLE_SCHEMA
                WHEN ''BRONZE'' THEN ''BRONZE''
                WHEN ''SILVER'' THEN ''SILVER''
                WHEN ''GOLD'' THEN ''GOLD''
                ELSE NULL
            END,
            ''row_count'', ROW_COUNT,
            ''bytes'', BYTES
        )
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN (''BRONZE'', ''SILVER'', ''GOLD'');

    SELECT COUNT(*) INTO :tables_added FROM KG_NODE WHERE node_type = ''TABLE'';

    -- Add Column nodes
    INSERT INTO KG_NODE (node_id, node_type, name, description, properties)
    SELECT
        ''COLUMN:DBAONTAP_ANALYTICS.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME || ''.'' || COLUMN_NAME,
        ''COLUMN'',
        COLUMN_NAME,
        COMMENT,
        OBJECT_CONSTRUCT(
            ''table_id'', ''TABLE:DBAONTAP_ANALYTICS.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME,
            ''data_type'', DATA_TYPE,
            ''is_nullable'', IS_NULLABLE = ''YES'',
            ''ordinal_position'', ORDINAL_POSITION
        )
    FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA IN (''BRONZE'', ''SILVER'', ''GOLD'');

    SELECT COUNT(*) INTO :columns_added FROM KG_NODE WHERE node_type = ''COLUMN'';

    -- Add SCHEMA -> TABLE edges (CONTAINS)
    INSERT INTO KG_EDGE (edge_id, source_node_id, target_node_id, edge_type, properties)
    SELECT
        ''EDGE:CONTAINS:'' || n_schema.node_id || '':'' || n_table.node_id,
        n_schema.node_id,
        n_table.node_id,
        ''CONTAINS'',
        OBJECT_CONSTRUCT(''relationship'', ''schema_contains_table'')
    FROM KG_NODE n_schema
    JOIN KG_NODE n_table ON n_table.properties:schema::VARCHAR = n_schema.name
    WHERE n_schema.node_type = ''SCHEMA'' AND n_table.node_type = ''TABLE'';

    -- Add TABLE -> COLUMN edges (HAS_COLUMN)
    INSERT INTO KG_EDGE (edge_id, source_node_id, target_node_id, edge_type, properties)
    SELECT
        ''EDGE:HAS_COLUMN:'' || n_table.node_id || '':'' || n_col.node_id,
        n_table.node_id,
        n_col.node_id,
        ''HAS_COLUMN'',
        OBJECT_CONSTRUCT(''ordinal_position'', n_col.properties:ordinal_position)
    FROM KG_NODE n_table
    JOIN KG_NODE n_col ON n_col.properties:table_id::VARCHAR = n_table.node_id
    WHERE n_table.node_type = ''TABLE'' AND n_col.node_type = ''COLUMN'';

    -- Add BRONZE -> SILVER lineage edges (TRANSFORMS_TO)
    INSERT INTO KG_EDGE (edge_id, source_node_id, target_node_id, edge_type, properties)
    SELECT
        ''EDGE:TRANSFORMS_TO:'' || bronze.node_id || '':'' || silver.node_id,
        bronze.node_id,
        silver.node_id,
        ''TRANSFORMS_TO'',
        OBJECT_CONSTRUCT(
            ''transformation_type'', ''AGENTIC_SILVER'',
            ''description'', ''Bronze VARIANT transformed to typed Silver by agentic workflow''
        )
    FROM KG_NODE bronze
    JOIN KG_NODE silver ON REPLACE(bronze.name, ''_VARIANT'', '''') = silver.name
    WHERE bronze.node_type = ''TABLE''
      AND bronze.properties:schema::VARCHAR = ''BRONZE''
      AND bronze.name LIKE ''%_VARIANT''
      AND silver.node_type = ''TABLE''
      AND silver.properties:schema::VARCHAR = ''SILVER'';

    -- Add SILVER -> GOLD lineage edges (AGGREGATES_TO) - dynamically from Gold DDL parsing
    INSERT INTO KG_EDGE (edge_id, source_node_id, target_node_id, edge_type, properties)
    SELECT
        ''EDGE:AGGREGATES_TO:'' || silver.node_id || '':'' || gold.node_id,
        silver.node_id,
        gold.node_id,
        ''AGGREGATES_TO'',
        OBJECT_CONSTRUCT(
            ''transformation_type'', ''AGGREGATION'',
            ''description'', ''Silver table aggregated to Gold metrics''
        )
    FROM KG_NODE silver
    CROSS JOIN KG_NODE gold
    WHERE silver.node_type = ''TABLE''
      AND silver.properties:schema::VARCHAR = ''SILVER''
      AND gold.node_type = ''TABLE''
      AND gold.properties:schema::VARCHAR = ''GOLD''
      AND (
          (silver.name = ''CUSTOMERS'' AND gold.name IN (''CUSTOMER_360'', ''ML_CUSTOMER_FEATURES''))
          OR (silver.name = ''ORDERS'' AND gold.name IN (''ORDER_SUMMARY'', ''CUSTOMER_360''))
          OR (silver.name = ''ORDER_ITEMS'' AND gold.name IN (''ORDER_SUMMARY'', ''PRODUCT_PERFORMANCE_METRICS''))
          OR (silver.name = ''PRODUCTS_VARIANT'' AND gold.name = ''PRODUCT_PERFORMANCE_METRICS'')
          OR (silver.name = ''SUPPORT_TICKETS'' AND gold.name IN (''SUPPORT_METRICS'', ''CUSTOMER_360''))
      );

    SELECT COUNT(*) INTO :edges_added FROM KG_EDGE;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''tables_added'', tables_added,
        ''columns_added'', columns_added,
        ''edges_added'', edges_added
    );
END;
';
