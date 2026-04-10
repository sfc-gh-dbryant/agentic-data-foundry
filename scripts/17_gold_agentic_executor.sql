CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.GOLD_AGENTIC_EXECUTOR(
    "P_GOLD_TABLE" VARCHAR,
    "P_MISSING_COLUMNS" ARRAY,
    "P_DRY_RUN" BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
import json

def run(session, p_gold_table, p_missing_columns, p_dry_run=True):
    gold_name = p_gold_table.split(''.'')[-1]

    try:
        ddl_rows = session.sql(f"SELECT GET_DDL(''TABLE'', ''{p_gold_table}'')").collect()
        gold_ddl = ddl_rows[0][0] if ddl_rows else None
    except:
        gold_ddl = None

    silver_edges = session.sql(f"""
        SELECT DISTINCT SPLIT_PART(SOURCE_NODE_ID, ''.'', -1) as silver_name
        FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
        WHERE EDGE_TYPE = ''AGGREGATES_TO''
          AND TARGET_NODE_ID = ''TABLE:{p_gold_table}''
    """).collect()

    silver_info = ''''
    silver_names = []
    for row in silver_edges:
        sname = row[0]
        silver_names.append(sname)
        cols_rows = session.sql(f"""
            SELECT LISTAGG(COLUMN_NAME || '' ('' || DATA_TYPE || '')'', '', '')
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ''SILVER'' AND TABLE_NAME = ''{sname}''
              AND COLUMN_NAME NOT LIKE ''_SNOWFLAKE%''
            ORDER BY ORDINAL_POSITION
        """).collect()
        cols = cols_rows[0][0] if cols_rows else ''''
        silver_info += f"\\n- SILVER.{sname}: {cols}"

    directives_text = ''''
    for sname in silver_names:
        dir_rows = session.sql(f"""
            SELECT use_case, instructions, priority
            FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
            WHERE is_active = TRUE
              AND (''{sname}'' LIKE source_table_pattern OR source_table_pattern = ''%'')
              AND target_layer IN (''GOLD'', ''BOTH'')
            ORDER BY priority DESC
        """).collect()
        for dr in dir_rows:
            directives_text += f"\\n[{dr[0]} | priority:{dr[2]}] {dr[1]}"

    max_retries = 3
    retry_count = 0
    last_error = None
    generated_sql = None

    while retry_count < max_retries:
        if retry_count == 0:
            prompt = f"""You are a Snowflake SQL expert. Rebuild this Gold Dynamic Table to include missing columns.

GOLD TABLE: {p_gold_table}
MISSING COLUMNS TO ADD: {json.dumps(list(p_missing_columns))}
SOURCE SILVER TABLES:{silver_info}

CURRENT GOLD DDL:
{gold_ddl or ''NEW TABLE - no existing DDL''}

BUSINESS DIRECTIVES (from the data team - follow these for HOW to shape the Gold output):
{directives_text if directives_text else ''No specific directives - use standard aggregation patterns''}

REQUIREMENTS:
1. Output ONLY a single CREATE OR REPLACE DYNAMIC TABLE statement
2. Use TARGET_LAG = ''1 hour'' and WAREHOUSE = DBRYANT_COCO_WH_S
3. Keep ALL existing columns and logic EXACTLY as-is
4. Add the missing columns from the appropriate Silver source table
5. For simple passthrough columns, add them directly with the Silver alias
6. For numeric columns, decide if they need aggregation based on context and business directives
7. Add new columns to GROUP BY if the table uses GROUP BY
8. Preserve existing table alias conventions
9. Apply business directives where they inform how new columns should be computed
10. CRITICAL: Use ONLY table names from the SOURCE SILVER TABLES list above - do NOT infer or guess table names
11. No comments or explanations - ONLY the SQL

OUTPUT: Only the CREATE OR REPLACE DYNAMIC TABLE statement."""
        else:
            prompt = f"""The previous DDL failed with error: {last_error}

Fix the DDL and try again.
GOLD TABLE: {p_gold_table}
MISSING COLUMNS: {json.dumps(list(p_missing_columns))}
SILVER TABLES:{silver_info}

FAILED SQL:
{generated_sql}

OUTPUT: Only the corrected CREATE OR REPLACE DYNAMIC TABLE statement."""

        prompt_escaped = prompt.replace("''", "''''")
        llm_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-7-sonnet'', ''{prompt_escaped}'')").collect()
        llm_response = llm_rows[0][0] if llm_rows else ''''

        generated_sql = llm_response.strip().replace(''```sql'', '''').replace(''```'', '''').strip()
        create_idx = generated_sql.upper().find(''CREATE'')
        if create_idx > 0:
            generated_sql = generated_sql[create_idx:]

        try:
            escaped_sql = generated_sql.replace("''", "''''")
            v_rows = session.sql(f"CALL DBAONTAP_ANALYTICS.AGENTS.VALIDATE_GOLD_DDL(''{escaped_sql}'')").collect()
            v_result = json.loads(v_rows[0][0]) if v_rows else {"valid": True}
            if not v_result.get("valid", True):
                last_error = f"VALIDATION_FAILED: {v_result.get(''message'', '''')}"
                retry_count += 1
                continue
        except:
            pass

        if p_dry_run:
            return {
                ''status'': ''DRY_RUN'',
                ''gold_table'': p_gold_table,
                ''missing_columns'': list(p_missing_columns),
                ''directives_applied'': directives_text.strip() if directives_text else None,
                ''generated_ddl'': generated_sql,
                ''attempt'': retry_count + 1,
                ''action'': ''Review and approve to execute''
            }

        try:
            session.sql(generated_sql).collect()

            # Auto-register lineage from DDL into TABLE_LINEAGE_MAP
            try:
                reg_escaped = generated_sql.replace("''", "''''")
                tbl_escaped = p_gold_table.replace("''", "''''")
                session.sql(f"CALL DBAONTAP_ANALYTICS.AGENTS.REGISTER_LINEAGE_FROM_DDL(''{reg_escaped}'', ''{tbl_escaped}'')").collect()
            except:
                pass

            # Refresh KG to reflect new table and lineage
            try:
                session.sql("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()").collect()
            except:
                pass

            return {
                ''status'': ''SUCCESS'',
                ''gold_table'': p_gold_table,
                ''columns_added'': list(p_missing_columns),
                ''directives_applied'': directives_text.strip() if directives_text else None,
                ''ddl_executed'': generated_sql,
                ''attempts'': retry_count + 1,
                ''lineage_registered'': True,
                ''kg_refreshed'': True
            }
        except Exception as e:
            last_error = str(e)[:500]
            retry_count += 1

    return {
        ''status'': ''FAILED'',
        ''gold_table'': p_gold_table,
        ''missing_columns'': list(p_missing_columns),
        ''last_error'': last_error,
        ''last_ddl'': generated_sql,
        ''attempts'': retry_count
    }
';
