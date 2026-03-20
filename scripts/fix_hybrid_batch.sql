CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(P_USE_LLM BOOLEAN DEFAULT TRUE)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    results ARRAY DEFAULT ARRAY_CONSTRUCT();
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    table_name VARCHAR;
    result_obj VARIANT;
    ddl_stmt VARCHAR;
    c1 CURSOR FOR SELECT name FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'TABLE' AND properties:schema::VARCHAR = 'GOLD';
BEGIN
    FOR row IN c1 DO
        table_name := row.name;
        
        CALL KNOWLEDGE_GRAPH.GENERATE_HYBRID_SEMANTIC_VIEW(:table_name, 'GOLD', :P_USE_LLM) INTO :result_obj;
        
        IF (result_obj:status = 'SUCCESS') THEN
            ddl_stmt := result_obj:ddl::VARCHAR;
            BEGIN
                EXECUTE IMMEDIATE :ddl_stmt;
                success_count := success_count + 1;
                results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('table', :table_name, 'status', 'CREATED', 'semantic_view', result_obj:semantic_view_name));
            EXCEPTION
                WHEN OTHER THEN
                    fail_count := fail_count + 1;
                    results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('table', :table_name, 'status', 'FAILED', 'error', SQLERRM));
            END;
        ELSE
            fail_count := fail_count + 1;
            results := ARRAY_APPEND(results, OBJECT_CONSTRUCT('table', :table_name, 'status', 'FAILED', 'error', result_obj:error));
        END IF;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'COMPLETED',
        'llm_enrichment', :P_USE_LLM,
        'success_count', :success_count,
        'fail_count', :fail_count,
        'details', :results
    );
END;
