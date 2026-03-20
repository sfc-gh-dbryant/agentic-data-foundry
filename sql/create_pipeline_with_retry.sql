CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    tables_cursor CURSOR FOR
        SELECT TABLE_NAME
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE';
    table_name_var VARCHAR;
    sv_name VARCHAR;
    col_list VARCHAR;
    prompt VARCHAR;
    llm_response VARCHAR;
    ddl_sql VARCHAR;
    pk_column VARCHAR;
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    results ARRAY DEFAULT ARRAY_CONSTRUCT();
    err_msg VARCHAR;
    max_retries INTEGER DEFAULT 3;
    retry_count INTEGER;
    ddl_succeeded INTEGER;
    create_pos INTEGER;
    example_ddl VARCHAR;
BEGIN
    example_ddl := 'CREATE OR REPLACE SEMANTIC VIEW DB.SCHEMA.MY_VIEW TABLES (DB.SCHEMA.MY_TABLE PRIMARY KEY (ID)) FACTS (MY_TABLE.ORDER_ID AS order_id) DIMENSIONS (MY_TABLE.NAME AS name, MY_TABLE.DATE AS date) METRICS (MY_TABLE.AMOUNT AS SUM(amount), MY_TABLE.COUNT AS COUNT(DISTINCT count));';

    FOR record IN tables_cursor DO
        table_name_var := record.TABLE_NAME;
        sv_name := table_name_var || '_AGENTIC_SV';
        retry_count := 0;
        ddl_succeeded := 0;
        err_msg := NULL;

        SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        INTO :col_list
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = :table_name_var;

        SELECT COLUMN_NAME INTO :pk_column
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = :table_name_var
        AND (COLUMN_NAME LIKE '%_ID' OR ORDINAL_POSITION = 1)
        ORDER BY CASE WHEN COLUMN_NAME LIKE '%_ID' THEN 0 ELSE 1 END, ORDINAL_POSITION
        LIMIT 1;

        WHILE (:retry_count < :max_retries AND :ddl_succeeded = 0) DO
            IF (:retry_count = 0) THEN
                prompt := 'Generate Snowflake SEMANTIC VIEW DDL. EXAMPLE: ' || :example_ddl ||
                    ' NOW GENERATE FOR: Table=DBAONTAP_ANALYTICS.GOLD.' || :table_name_var || 
                    ' ViewName=DBAONTAP_ANALYTICS.GOLD.' || :sv_name || 
                    ' PrimaryKey=' || :pk_column || 
                    ' Columns=' || :col_list || 
                    ' CRITICAL RULES: 1) FACTS format: TABLE.COL AS alias (for numeric IDs). 2) DIMENSIONS format: TABLE.COL AS alias (for text/dates). 3) METRICS format: TABLE.COL AS AGG(alias) where AGG is SUM/AVG/COUNT. 4) Each column used exactly once. 5) Use COUNT(DISTINCT x) not COUNT_DISTINCT. 6) Output ONLY the CREATE statement, no explanation.';
            ELSE
                prompt := 'Fix this failed SEMANTIC VIEW DDL. Error: ' || :err_msg || 
                    ' CORRECT EXAMPLE: ' || :example_ddl ||
                    ' Table=DBAONTAP_ANALYTICS.GOLD.' || :table_name_var || 
                    ' ViewName=DBAONTAP_ANALYTICS.GOLD.' || :sv_name || 
                    ' PK=' || :pk_column || 
                    ' Columns=' || :col_list || 
                    ' Failed DDL: ' || :ddl_sql ||
                    ' CRITICAL: FACTS/DIMENSIONS must be TABLE.COL AS alias. METRICS must be TABLE.COL AS AGG(alias). Output ONLY corrected CREATE statement.';
            END IF;

            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', :prompt) INTO :llm_response;
            
            ddl_sql := TRIM(REGEXP_REPLACE(:llm_response, '```sql|```', ''));
            create_pos := POSITION('CREATE' IN UPPER(:ddl_sql));
            IF (:create_pos > 1) THEN
                ddl_sql := SUBSTR(:ddl_sql, :create_pos);
            END IF;

            BEGIN
                EXECUTE IMMEDIATE :ddl_sql;
                ddl_succeeded := 1;
                INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
                    (source_table, target_table, transformation_sql, agent_reasoning, status, executed_at)
                VALUES ('GOLD.' || :table_name_var, :sv_name, :ddl_sql, 
                    CASE WHEN :retry_count > 0 THEN 'Auto-corrected after ' || :retry_count || ' retries' ELSE 'Pipeline auto-generated' END, 
                    'SUCCESS', CURRENT_TIMESTAMP());
                success_count := success_count + 1;
                results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT('table', :table_name_var, 'view', :sv_name, 'status', 'SUCCESS', 'retries', :retry_count));
            EXCEPTION WHEN OTHER THEN
                err_msg := SQLCODE || ': ' || SQLERRM;
                retry_count := retry_count + 1;
            END;
        END WHILE;

        IF (:ddl_succeeded = 0) THEN
            INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
                (source_table, target_table, transformation_sql, agent_reasoning, status, executed_at)
            VALUES ('GOLD.' || :table_name_var, :sv_name, :ddl_sql, 
                'FAILED after ' || :max_retries || ' attempts: ' || :err_msg, 'FAILED', CURRENT_TIMESTAMP());
            fail_count := fail_count + 1;
            results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT('table', :table_name_var, 'view', :sv_name, 'status', 'FAILED', 'error', :err_msg, 'retries', :retry_count, 'ddl', :ddl_sql));
        END IF;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT('success_count', :success_count, 'fail_count', :fail_count, 'details', :results);
END;
$$;
