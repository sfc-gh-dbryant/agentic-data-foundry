-- =============================================================================
-- LAYER 1: DETECT_GOLD_SCHEMA_DRIFT
-- Compares Silver columns against downstream Gold DTs using KG lineage.
-- Returns drift details per Gold table: missing columns, column types,
-- classification (passthrough vs complex).
-- =============================================================================

CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.DETECT_GOLD_SCHEMA_DRIFT(P_SILVER_TABLE VARCHAR DEFAULT NULL)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json

def run(session, p_silver_table=None):
    results = []
    has_drift = False
    total_drift_count = 0

    ignore_rows = session.sql("""
        SELECT UPPER(COLUMN_PATTERN) FROM DBAONTAP_ANALYTICS.METADATA.SCHEMA_IGNORE_COLUMNS
    """).collect()
    ignore_patterns = set(r[0] for r in ignore_rows)

    if p_silver_table:
        silver_nodes = [p_silver_table]
    else:
        rows = session.sql("""
            SELECT DISTINCT SOURCE_NODE_ID
            FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
            WHERE EDGE_TYPE = 'AGGREGATES_TO'
              AND TARGET_NODE_ID LIKE 'TABLE:DBAONTAP_ANALYTICS.GOLD.%'
              AND SOURCE_NODE_ID LIKE 'TABLE:DBAONTAP_ANALYTICS.SILVER.%'
        """).collect()
        silver_nodes = [r[0] for r in rows]

    for silver_node in silver_nodes:
        silver_name = silver_node.split('.')[-1]

        silver_cols_rows = session.sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = '{silver_name}'
              AND COLUMN_NAME NOT LIKE '_SNOWFLAKE%'
        """).collect()
        silver_cols = {r[0]: r[1] for r in silver_cols_rows}

        gold_edges = session.sql(f"""
            SELECT TARGET_NODE_ID
            FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
            WHERE EDGE_TYPE = 'AGGREGATES_TO'
              AND SOURCE_NODE_ID = '{silver_node}'
        """).collect()

        for edge in gold_edges:
            gold_fq = edge[0].replace('TABLE:', '')
            gold_name = gold_fq.split('.')[-1]

            gold_cols_rows = session.sql(f"""
                SELECT COLUMN_NAME
                FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = '{gold_name}'
            """).collect()
            gold_cols = set(r[0] for r in gold_cols_rows)

            try:
                ddl_rows = session.sql(f"SELECT GET_DDL('TABLE', '{gold_fq}')").collect()
                gold_ddl_text = ddl_rows[0][0].upper() if ddl_rows else ''
            except:
                gold_ddl_text = ''

            missing = []
            passthrough_types = {'TEXT', 'VARCHAR', 'DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'BOOLEAN', 'NUMBER'}
            for col_name, col_type in silver_cols.items():
                if col_name in gold_cols:
                    continue
                if col_name.upper() in ignore_patterns:
                    continue
                if col_name.upper() in gold_ddl_text:
                    continue
                if col_name.upper().startswith('_SNOWFLAKE'):
                    continue

                is_pt = col_type in passthrough_types
                missing.append({
                        'column_name': col_name,
                        'data_type': col_type,
                        'is_passthrough': is_pt,
                        'recommendation': 'AUTO_ADD' if is_pt else 'LLM_REVIEW'
                    })

            if missing:
                has_drift = True
                total_drift_count += len(missing)
                pt_count = sum(1 for m in missing if m['is_passthrough'])

                try:
                    ddl_rows = session.sql(f"SELECT GET_DDL('TABLE', '{gold_fq}')").collect()
                    gold_ddl = ddl_rows[0][0] if ddl_rows else None
                except:
                    gold_ddl = None

                results.append({
                    'silver_table': silver_name,
                    'gold_table': gold_name,
                    'gold_fq': gold_fq,
                    'missing_columns': [m['column_name'] for m in missing],
                    'missing_details': missing,
                    'missing_count': len(missing),
                    'passthrough_count': pt_count,
                    'complex_count': len(missing) - pt_count,
                    'current_gold_ddl': gold_ddl
                })

    return {
        'status': 'DRIFT_DETECTED' if has_drift else 'IN_SYNC',
        'total_missing_columns': total_drift_count,
        'affected_gold_tables': len(results),
        'details': results
    }
$$;


-- =============================================================================
-- LAYER 2: GOLD_AUTO_PASSTHROUGH
-- Uses LLM to safely add simple passthrough columns to existing Gold DTs.
-- Much more reliable than string manipulation for DDL rewriting.
-- =============================================================================

CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.GOLD_AUTO_PASSTHROUGH(
    P_GOLD_TABLE VARCHAR,
    P_COLUMNS ARRAY,
    P_DRY_RUN BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json

def run(session, p_gold_table, p_columns, p_dry_run=True):
    gold_name = p_gold_table.split('.')[-1]

    ddl_rows = session.sql(f"SELECT GET_DDL('TABLE', '{p_gold_table}')").collect()
    gold_ddl = ddl_rows[0][0] if ddl_rows else None
    if not gold_ddl:
        return {'status': 'FAILED', 'error': f'Could not get DDL for {p_gold_table}'}

    silver_rows = session.sql(f"""
        SELECT DISTINCT SPLIT_PART(SOURCE_NODE_ID, '.', -1) as silver_name
        FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
        WHERE EDGE_TYPE = 'AGGREGATES_TO'
          AND TARGET_NODE_ID = 'TABLE:{p_gold_table}'
          AND SOURCE_NODE_ID LIKE '%SILVER.%'
        LIMIT 1
    """).collect()
    silver_name = silver_rows[0][0] if silver_rows else None
    if not silver_name:
        return {'status': 'FAILED', 'error': 'Could not find Silver source table in KG'}

    cols_info = []
    for col in p_columns:
        type_rows = session.sql(f"""
            SELECT DATA_TYPE FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = '{silver_name}' AND COLUMN_NAME = '{col}'
        """).collect()
        col_type = type_rows[0][0] if type_rows else 'TEXT'
        cols_info.append(f"{col} ({col_type})")

    prompt = f"""You are a Snowflake SQL expert. Add these new columns to the Gold Dynamic Table.

CURRENT DDL:
{gold_ddl}

COLUMNS TO ADD (from SILVER.{silver_name}): {', '.join(cols_info)}

RULES:
1. Output ONLY the CREATE OR REPLACE DYNAMIC TABLE statement
2. Keep ALL existing columns and logic EXACTLY as-is - do not change ANY existing column
3. Add new columns as simple passthrough references using the Silver table alias from the existing DDL
4. Add new columns to GROUP BY if the query uses GROUP BY
5. Preserve TARGET_LAG, WAREHOUSE, and all other settings
6. Do NOT rename columns or add transformations
7. No comments, no explanations - ONLY the SQL

OUTPUT: The complete CREATE OR REPLACE DYNAMIC TABLE statement."""

    prompt_escaped = prompt.replace("'", "''")
    llm_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', '{prompt_escaped}')").collect()
    llm_response = llm_rows[0][0] if llm_rows else ''

    generated_sql = llm_response.strip()
    generated_sql = generated_sql.replace('```sql', '').replace('```', '').strip()
    create_idx = generated_sql.upper().find('CREATE')
    if create_idx > 0:
        generated_sql = generated_sql[create_idx:]

    if p_dry_run:
        return {
            'status': 'DRY_RUN',
            'gold_table': p_gold_table,
            'columns_to_add': list(p_columns),
            'generated_ddl': generated_sql,
            'action': 'Review and approve to execute'
        }

    try:
        session.sql(generated_sql).collect()
        return {
            'status': 'SUCCESS',
            'gold_table': p_gold_table,
            'columns_added': list(p_columns),
            'ddl_executed': generated_sql
        }
    except Exception as e:
        return {
            'status': 'FAILED',
            'gold_table': p_gold_table,
            'columns_to_add': list(p_columns),
            'error': str(e)[:500],
            'generated_ddl': generated_sql
        }
$$;


-- =============================================================================
-- LAYER 3: GOLD_AGENTIC_EXECUTOR
-- Uses LLM to generate/rebuild Gold DT DDL for complex column additions or
-- brand new Gold tables. Provides existing DDL + Silver schema as context.
-- Has retry loop with self-correction.
-- =============================================================================

CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.GOLD_AGENTIC_EXECUTOR(
    P_GOLD_TABLE VARCHAR,
    P_MISSING_COLUMNS ARRAY,
    P_DRY_RUN BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json

def run(session, p_gold_table, p_missing_columns, p_dry_run=True):
    gold_name = p_gold_table.split('.')[-1]

    try:
        ddl_rows = session.sql(f"SELECT GET_DDL('TABLE', '{p_gold_table}')").collect()
        gold_ddl = ddl_rows[0][0] if ddl_rows else None
    except:
        gold_ddl = None

    silver_edges = session.sql(f"""
        SELECT DISTINCT SPLIT_PART(SOURCE_NODE_ID, '.', -1) as silver_name
        FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
        WHERE EDGE_TYPE = 'AGGREGATES_TO'
          AND TARGET_NODE_ID = 'TABLE:{p_gold_table}'
    """).collect()

    silver_info = ''
    for row in silver_edges:
        sname = row[0]
        cols_rows = session.sql(f"""
            SELECT LISTAGG(COLUMN_NAME || ' (' || DATA_TYPE || ')', ', ')
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = '{sname}'
              AND COLUMN_NAME NOT LIKE '_SNOWFLAKE%'
            ORDER BY ORDINAL_POSITION
        """).collect()
        cols = cols_rows[0][0] if cols_rows else ''
        silver_info += f"\n- SILVER.{sname}: {cols}"

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
{gold_ddl or 'NEW TABLE - no existing DDL'}

REQUIREMENTS:
1. Output ONLY a single CREATE OR REPLACE DYNAMIC TABLE statement
2. Use TARGET_LAG = '1 hour' and WAREHOUSE = DBRYANT_COCO_WH_S
3. Keep ALL existing columns and logic EXACTLY as-is
4. Add the missing columns from the appropriate Silver source table
5. For simple passthrough columns, add them directly with the Silver alias
6. For numeric columns, decide if they need aggregation based on context
7. Add new columns to GROUP BY if the table uses GROUP BY
8. Preserve existing table alias conventions
9. Do NOT rename columns - use exact Silver column names
10. No comments or explanations - ONLY the SQL

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

        prompt_escaped = prompt.replace("'", "''")
        llm_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-7-sonnet', '{prompt_escaped}')").collect()
        llm_response = llm_rows[0][0] if llm_rows else ''

        generated_sql = llm_response.strip().replace('```sql', '').replace('```', '').strip()
        create_idx = generated_sql.upper().find('CREATE')
        if create_idx > 0:
            generated_sql = generated_sql[create_idx:]

        if p_dry_run:
            return {
                'status': 'DRY_RUN',
                'gold_table': p_gold_table,
                'missing_columns': list(p_missing_columns),
                'generated_ddl': generated_sql,
                'attempt': retry_count + 1,
                'action': 'Review and approve to execute'
            }

        try:
            session.sql(generated_sql).collect()
            return {
                'status': 'SUCCESS',
                'gold_table': p_gold_table,
                'columns_added': list(p_missing_columns),
                'ddl_executed': generated_sql,
                'attempts': retry_count + 1
            }
        except Exception as e:
            last_error = str(e)[:500]
            retry_count += 1

    return {
        'status': 'FAILED',
        'gold_table': p_gold_table,
        'missing_columns': list(p_missing_columns),
        'last_error': last_error,
        'last_ddl': generated_sql,
        'attempts': retry_count
    }
$$;


-- =============================================================================
-- ORCHESTRATOR: PROPAGATE_TO_GOLD
-- Ties all 3 layers together:
-- 1. Detect drift (Layer 1)
-- 2. Auto-passthrough simple columns (Layer 2)
-- 3. LLM for complex columns (Layer 3)
-- 4. Refresh KG + regenerate Semantic Views
-- =============================================================================

CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.PROPAGATE_TO_GOLD(
    P_SILVER_TABLE VARCHAR DEFAULT NULL,
    P_DRY_RUN BOOLEAN DEFAULT TRUE,
    P_AUTO_PASSTHROUGH BOOLEAN DEFAULT TRUE,
    P_AGENTIC_COMPLEX BOOLEAN DEFAULT TRUE,
    P_REFRESH_SEMANTIC_VIEWS BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json

def run(session, p_silver_table=None, p_dry_run=True, p_auto_passthrough=True, p_agentic_complex=True, p_refresh_semantic_views=True):
    if p_silver_table:
        drift_rows = session.sql(f"CALL AGENTS.DETECT_GOLD_SCHEMA_DRIFT('{p_silver_table}')").collect()
    else:
        drift_rows = session.sql("CALL AGENTS.DETECT_GOLD_SCHEMA_DRIFT()").collect()

    drift_result = json.loads(drift_rows[0][0]) if drift_rows else {'status': 'ERROR'}

    if drift_result.get('status') == 'IN_SYNC':
        return {
            'status': 'IN_SYNC',
            'message': 'All Gold tables are in sync with Silver. No action needed.',
            'drift_result': drift_result
        }

    passthrough_results = []
    agentic_results = []

    for detail in drift_result.get('details', []):
        gold_fq = detail['gold_fq']
        missing_details = detail.get('missing_details', [])

        passthrough_cols = [m['column_name'] for m in missing_details if m.get('is_passthrough')]
        complex_cols = [m['column_name'] for m in missing_details if not m.get('is_passthrough')]

        if p_auto_passthrough and passthrough_cols and not complex_cols:
            cols_array = "ARRAY_CONSTRUCT(" + ",".join(f"'{c}'" for c in passthrough_cols) + ")"
            dry_str = 'TRUE' if p_dry_run else 'FALSE'
            pt_rows = session.sql(f"CALL AGENTS.GOLD_AUTO_PASSTHROUGH('{gold_fq}', {cols_array}, {dry_str})").collect()
            pt_result = json.loads(pt_rows[0][0]) if pt_rows else {'status': 'ERROR'}
            passthrough_results.append(pt_result)
        elif p_agentic_complex:
            all_missing = passthrough_cols + complex_cols
            cols_array = "ARRAY_CONSTRUCT(" + ",".join(f"'{c}'" for c in all_missing) + ")"
            dry_str = 'TRUE' if p_dry_run else 'FALSE'
            ag_rows = session.sql(f"CALL AGENTS.GOLD_AGENTIC_EXECUTOR('{gold_fq}', {cols_array}, {dry_str})").collect()
            ag_result = json.loads(ag_rows[0][0]) if ag_rows else {'status': 'ERROR'}
            agentic_results.append(ag_result)

    sv_result = None
    if not p_dry_run and p_refresh_semantic_views:
        session.sql("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()").collect()
        sv_rows = session.sql("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(TRUE)").collect()
        sv_result = json.loads(sv_rows[0][0]) if sv_rows else None

    return {
        'status': 'DRY_RUN_COMPLETE' if p_dry_run else 'EXECUTED',
        'drift_detected': drift_result,
        'passthrough_results': passthrough_results,
        'agentic_results': agentic_results,
        'semantic_views_refreshed': sv_result,
        'summary': {
            'total_drift_columns': drift_result.get('total_missing_columns', 0),
            'affected_gold_tables': drift_result.get('affected_gold_tables', 0),
            'passthrough_actions': len(passthrough_results),
            'agentic_actions': len(agentic_results)
        }
    }
$$;
