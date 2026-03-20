-- Fixed Hybrid Semantic View Generator (NEW SQL Syntax)
-- Creates semantic views for all GOLD tables with KG structure + LLM enrichment
-- Uses the new SQL-native syntax instead of deprecated YAML syntax

-- Single table generator with direct execution
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_HYBRID_SV_EXECUTE(
    P_TABLE_NAME VARCHAR, 
    P_SCHEMA_NAME VARCHAR DEFAULT 'GOLD', 
    P_USE_LLM BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS $$
import json

def run(session, p_table_name, p_schema_name='GOLD', p_use_llm=True):
    table_node_id = 'TABLE:DBAONTAP_ANALYTICS.' + p_schema_name + '.' + p_table_name
    sv_name = p_table_name + '_HYBRID_SV'
    fq_table = f'DBAONTAP_ANALYTICS.{p_schema_name}.{p_table_name}'
    
    # Get table description from KG
    desc_result = session.sql(f"SELECT description FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_id = '{table_node_id}'").collect()
    table_desc = desc_result[0][0] if desc_result and desc_result[0][0] else None
    
    # LLM enrichment if needed
    if p_use_llm and not table_desc:
        cols_result = session.sql(f"SELECT ARRAY_AGG(OBJECT_CONSTRUCT('name', name, 'type', properties:data_type::VARCHAR))::VARCHAR FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'COLUMN' AND properties:table_id::VARCHAR = '{table_node_id}'").collect()
        cols_json = cols_result[0][0] if cols_result else '[]'
        
        prompt = f"Generate a 1-2 sentence business description for analytics table {p_table_name} with columns: {cols_json}. Return ONLY the description."
        prompt_escaped = prompt.replace("'", "''")
        
        llm_result = session.sql(f"SELECT TRIM(SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', '{prompt_escaped}'))").collect()
        table_desc = llm_result[0][0] if llm_result else None
    
    if not table_desc:
        table_desc = 'Analytics table for ' + p_table_name.replace('_', ' ')
    
    # Get columns from KG
    columns = session.sql(f"SELECT name, properties:data_type::VARCHAR, description, COALESCE(properties:is_primary_key::BOOLEAN, FALSE), COALESCE(properties:ordinal_position::INTEGER, 0) FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'COLUMN' AND properties:table_id::VARCHAR = '{table_node_id}' ORDER BY 5").collect()
    
    dimensions = []
    measures = []
    pk_cols = []
    
    for col in columns:
        col_name, col_type, col_desc, is_pk, _ = col[0], col[1] or 'TEXT', col[2], col[3], col[4]
        
        if col_name.startswith('_SNOWFLAKE') or col_name.startswith('METADATA'):
            continue
        
        if is_pk:
            pk_cols.append(col_name)
        
        if not col_desc:
            col_desc = 'Column: ' + col_name.replace('_', ' ').title()
        col_desc = str(col_desc).replace("'", "''").replace('\n', ' ')[:200]
        
        # Categorize: dimensions vs measures
        if col_type in ('DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIMESTAMP', 'TEXT', 'VARCHAR', 'STRING', 'CHAR', 'BOOLEAN'):
            dimensions.append({'name': col_name, 'description': col_desc})
        elif col_type in ('NUMBER', 'FLOAT', 'DECIMAL', 'DOUBLE', 'INTEGER', 'BIGINT', 'NUMERIC'):
            col_lower = col_name.lower()
            if '_id' in col_lower or col_lower.endswith('id') or '_key' in col_lower or is_pk:
                dimensions.append({'name': col_name, 'description': col_desc})
            else:
                agg = 'SUM' if any(x in col_lower for x in ['count', 'qty', 'quantity', 'total', 'amount', 'revenue']) else ('AVG' if any(x in col_lower for x in ['price', 'rate', 'avg', 'percent', 'ratio', 'score', 'rank']) else 'SUM')
                measures.append({'name': col_name, 'description': col_desc, 'agg': agg})
        else:
            dimensions.append({'name': col_name, 'description': col_desc})
    
    # Build new SQL syntax DDL
    table_desc_clean = str(table_desc).replace("'", "''").replace('\n', ' ')[:500]
    
    # Build DIMENSIONS clause
    dims_sql = ',\n  '.join([f"{p_table_name}.{d['name']} AS {d['name']} COMMENT = '{d['description']}'" for d in dimensions])
    
    # Build METRICS clause  
    metrics_sql = ',\n  '.join([f"{p_table_name}.{m['name']} AS {m['agg']}({m['name']}) COMMENT = '{m['description']}'" for m in measures])
    
    # Build PRIMARY KEY clause (use first column if none defined)
    if pk_cols:
        pk_clause = f"PRIMARY KEY ({', '.join(pk_cols)})"
    elif dimensions:
        pk_clause = f"PRIMARY KEY ({dimensions[0]['name']})"
    else:
        pk_clause = ""
    
    ddl = f"""CREATE OR REPLACE SEMANTIC VIEW DBAONTAP_ANALYTICS.GOLD.{sv_name}
TABLES (
  {fq_table}
  {pk_clause}
  COMMENT = '{table_desc_clean}'
)
DIMENSIONS (
  {dims_sql}
)
METRICS (
  {metrics_sql}
)
COMMENT = 'Hybrid: KG structure + LLM enrichment'"""
    
    try:
        session.sql(ddl).collect()
        return {
            'status': 'SUCCESS', 
            'table_name': p_table_name, 
            'semantic_view_name': sv_name, 
            'llm_enriched': p_use_llm, 
            'statistics': {
                'total_columns': len(columns), 
                'dimensions': len(dimensions), 
                'measures': len(measures)
            }
        }
    except Exception as e:
        return {
            'status': 'FAILED', 
            'table_name': p_table_name, 
            'error': str(e)[:300], 
            'ddl': ddl[:800]
        }
$$;

-- Batch generator for all GOLD tables
CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(P_USE_LLM BOOLEAN DEFAULT TRUE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS $$
import json

def run(session, p_use_llm=True):
    results = []
    success_count = 0
    fail_count = 0
    
    # Get all GOLD tables
    tables = session.sql("SELECT name FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'TABLE' AND properties:schema::VARCHAR = 'GOLD'").collect()
    
    for tbl in tables:
        table_name = tbl[0]
        try:
            result_rows = session.sql(f"CALL KNOWLEDGE_GRAPH.GENERATE_HYBRID_SV_EXECUTE('{table_name}', 'GOLD', {str(p_use_llm).upper()})").collect()
            if result_rows:
                result = result_rows[0][0]
                if isinstance(result, str):
                    result = json.loads(result)
                
                if result.get('status') == 'SUCCESS':
                    success_count += 1
                    results.append({'table': table_name, 'semantic_view': result.get('semantic_view_name'), 'status': 'CREATED', 'stats': result.get('statistics')})
                else:
                    fail_count += 1
                    results.append({'table': table_name, 'status': 'FAILED', 'error': result.get('error', 'Unknown')[:200]})
        except Exception as e:
            fail_count += 1
            results.append({'table': table_name, 'status': 'FAILED', 'error': str(e)[:200]})
    
    return {'status': 'COMPLETED', 'llm_enrichment': p_use_llm, 'success_count': success_count, 'fail_count': fail_count, 'details': results}
$$;
