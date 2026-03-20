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
    TRUNCATE TABLE KG_EDGE;
    TRUNCATE TABLE KG_NODE;

    INSERT INTO KG_NODE (node_id, node_type, name, description, properties)
    SELECT ''DB:DBAONTAP_ANALYTICS'', ''DATABASE'', ''DBAONTAP_ANALYTICS'',
           ''Analytics database for agentic demo'',
           OBJECT_CONSTRUCT(''created_at'', CURRENT_TIMESTAMP());

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

    -- Lineage edges driven by TABLE_LINEAGE_MAP (single source of truth)
    INSERT INTO KG_EDGE (edge_id, source_node_id, target_node_id, edge_type, properties)
    SELECT
        ''EDGE:'' || lm.EDGE_TYPE || '':TABLE:DBAONTAP_ANALYTICS.'' || lm.SOURCE_SCHEMA || ''.'' || lm.SOURCE_TABLE
            || '':TABLE:DBAONTAP_ANALYTICS.'' || lm.TARGET_SCHEMA || ''.'' || lm.TARGET_TABLE,
        ''TABLE:DBAONTAP_ANALYTICS.'' || lm.SOURCE_SCHEMA || ''.'' || lm.SOURCE_TABLE,
        ''TABLE:DBAONTAP_ANALYTICS.'' || lm.TARGET_SCHEMA || ''.'' || lm.TARGET_TABLE,
        lm.EDGE_TYPE,
        OBJECT_CONSTRUCT(
            ''transformation_type'', lm.EDGE_TYPE,
            ''description'', lm.DESCRIPTION,
            ''relationship_label'', lm.RELATIONSHIP_LABEL
        )
    FROM DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP lm
    WHERE EXISTS (
        SELECT 1 FROM KG_NODE src
        WHERE src.node_id = ''TABLE:DBAONTAP_ANALYTICS.'' || lm.SOURCE_SCHEMA || ''.'' || lm.SOURCE_TABLE
    )
    AND EXISTS (
        SELECT 1 FROM KG_NODE tgt
        WHERE tgt.node_id = ''TABLE:DBAONTAP_ANALYTICS.'' || lm.TARGET_SCHEMA || ''.'' || lm.TARGET_TABLE
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
