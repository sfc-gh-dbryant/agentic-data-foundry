-- =============================================================================
-- HYBRID SEMANTIC VIEW GENERATOR
-- Combines Knowledge Graph structure with LLM enrichment
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE SCHEMA KNOWLEDGE_GRAPH;

-- =============================================================================
-- MAIN HYBRID PROCEDURE (Python)
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_HYBRID_SEMANTIC_VIEW(
    p_table_name VARCHAR,
    p_schema_name VARCHAR DEFAULT 'GOLD',
    p_use_llm_enrichment BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
from snowflake.snowpark import Session

def run(session, p_table_name, p_schema_name="GOLD", p_use_llm_enrichment=True):
    table_node_id = "TABLE:DBAONTAP_ANALYTICS." + p_schema_name + "." + p_table_name
    sv_name = p_table_name + "_HYBRID_SV"
    
    # Step 1: Get table description from KG
    desc_sql = "SELECT description FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_id = '" + table_node_id + "'"
    desc_result = session.sql(desc_sql).collect()
    table_desc = desc_result[0][0] if desc_result and desc_result[0][0] else None
    
    # Step 2: LLM enrichment for table description if needed
    if p_use_llm_enrichment and not table_desc:
        cols_sql = "SELECT ARRAY_AGG(OBJECT_CONSTRUCT('name', name, 'type', properties:data_type::VARCHAR))::VARCHAR FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'COLUMN' AND properties:table_id::VARCHAR = '" + table_node_id + "'"
        cols_result = session.sql(cols_sql).collect()
        cols_json = cols_result[0][0] if cols_result else "[]"
        
        prompt = "You are a data analyst. Generate a concise 1-2 sentence business description for a Gold layer analytics table named " + p_table_name + " with columns: " + str(cols_json) + ". Focus on what business questions this table answers. Return ONLY the description."
        prompt_escaped = prompt.replace("'", "''")
        
        llm_sql = "SELECT TRIM(SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', '" + prompt_escaped + "'))"
        llm_result = session.sql(llm_sql).collect()
        table_desc = llm_result[0][0] if llm_result else None
    
    if not table_desc:
        table_desc = "Analytics table for " + p_table_name.replace("_", " ")
    
    # Step 3: Get columns from KG
    columns_sql = "SELECT name AS column_name, properties:data_type::VARCHAR AS data_type, description, COALESCE(properties:is_primary_key::BOOLEAN, FALSE) AS is_primary_key, COALESCE(properties:ordinal_position::INTEGER, 0) AS ordinal_position FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'COLUMN' AND properties:table_id::VARCHAR = '" + table_node_id + "' ORDER BY ordinal_position"
    columns = session.sql(columns_sql).collect()
    
    dimensions = []
    time_dimensions = []
    measures = []
    
    for col in columns:
        col_name = col[0]
        col_type = col[1] or "TEXT"
        col_desc = col[2]
        is_pk = col[3]
        
        if col_name.startswith("_SNOWFLAKE") or col_name.startswith("METADATA$"):
            continue
        
        if not col_desc:
            col_desc = "Column: " + col_name.replace("_", " ").title()
        
        # Clean description
        col_desc = str(col_desc).replace('"', "'").replace('\n', ' ')[:200]
        
        # Categorize column
        if col_type in ("DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"):
            time_dimensions.append({
                "name": col_name.lower(),
                "label": col_name.replace("_", " ").title(),
                "description": col_desc,
                "expr": col_name,
                "data_type": col_type,
                "unique": is_pk
            })
        elif col_type in ("NUMBER", "FLOAT", "DECIMAL", "DOUBLE", "INTEGER", "BIGINT", "NUMERIC"):
            col_lower = col_name.lower()
            if "_id" in col_lower or col_lower.endswith("id") or "_key" in col_lower or is_pk:
                dimensions.append({
                    "name": col_name.lower(),
                    "label": col_name.replace("_", " ").title(),
                    "description": col_desc,
                    "expr": col_name,
                    "data_type": col_type,
                    "unique": is_pk
                })
            else:
                if any(x in col_lower for x in ["count", "qty", "quantity", "total", "amount", "revenue"]):
                    agg = "sum"
                elif any(x in col_lower for x in ["price", "rate", "avg", "percent", "ratio", "score", "rank"]):
                    agg = "avg"
                else:
                    agg = "sum"
                
                measures.append({
                    "name": col_name.lower(),
                    "label": col_name.replace("_", " ").title(),
                    "description": col_desc,
                    "expr": col_name,
                    "data_type": "NUMBER",
                    "default_aggregation": agg
                })
        else:
            dimensions.append({
                "name": col_name.lower(),
                "label": col_name.replace("_", " ").title(),
                "description": col_desc,
                "expr": col_name,
                "data_type": col_type or "TEXT",
                "unique": is_pk
            })
    
    # Build YAML sections
    def build_yaml_list(items):
        if not items:
            return " []"
        lines = []
        for item in items:
            unique_val = "true" if item.get("unique") else "false"
            line = '\n    - name: ' + item["name"] + '\n      label: "' + item["label"] + '"\n      description: "' + item["description"] + '"\n      expr: ' + item["expr"] + '\n      data_type: ' + item["data_type"] + '\n      unique: ' + unique_val
            lines.append(line)
        return "".join(lines)
    
    def build_measures_yaml(items):
        if not items:
            return " []"
        lines = []
        for item in items:
            line = '\n    - name: ' + item["name"] + '\n      label: "' + item["label"] + '"\n      description: "' + item["description"] + '"\n      expr: ' + item["expr"] + '\n      data_type: ' + item["data_type"] + '\n      default_aggregation: ' + item["default_aggregation"]
            lines.append(line)
        return "".join(lines)
    
    # Clean table description
    table_desc_clean = str(table_desc).replace('"', "'").replace('\n', ' ')[:500]
    
    # Build DDL using chr(36) for $ to avoid escaping issues
    dollar = chr(36) + chr(36)
    
    ddl = "CREATE OR REPLACE SEMANTIC VIEW DBAONTAP_ANALYTICS.GOLD." + sv_name + "\nCOMMENT = 'Hybrid: KG structure + LLM enrichment'\nAS " + dollar + '\n@semantic_model(\n  name: "' + sv_name + '"\n  description: "' + table_desc_clean + '"\n  \n  tables:\n    - name: ' + p_table_name + '\n      base_table:\n        database: DBAONTAP_ANALYTICS\n        schema: ' + p_schema_name + '\n        table: ' + p_table_name + '\n      \n      dimensions:' + build_yaml_list(dimensions) + '\n      \n      time_dimensions:' + build_yaml_list(time_dimensions) + '\n      \n      measures:' + build_measures_yaml(measures) + '\n)\n' + dollar
    
    return {
        "status": "SUCCESS",
        "table_name": p_table_name,
        "semantic_view_name": sv_name,
        "ddl": ddl,
        "llm_enriched": p_use_llm_enrichment,
        "statistics": {
            "total_columns": len(columns),
            "dimensions": len(dimensions),
            "time_dimensions": len(time_dimensions),
            "measures": len(measures)
        }
    }
$$;

-- =============================================================================
-- BATCH: Generate all GOLD tables with hybrid approach
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(
    p_use_llm BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
from snowflake.snowpark import Session

def run(session, p_use_llm=True):
    results = []
    success_count = 0
    fail_count = 0
    
    # Get all GOLD tables from KG
    tables = session.sql("SELECT name, properties:schema::VARCHAR AS schema_name FROM KNOWLEDGE_GRAPH.KG_NODE WHERE node_type = 'TABLE' AND properties:schema::VARCHAR = 'GOLD'").collect()
    
    for tbl in tables:
        table_name = tbl[0]
        schema_name = tbl[1] or "GOLD"
        
        try:
            result = session.call("KNOWLEDGE_GRAPH.GENERATE_HYBRID_SEMANTIC_VIEW", table_name, schema_name, p_use_llm)
            
            if isinstance(result, str):
                result = json.loads(result)
            
            if result.get("status") == "SUCCESS":
                ddl = result.get("ddl", "")
                try:
                    session.sql(ddl).collect()
                    success_count += 1
                    results.append({
                        "table": table_name,
                        "semantic_view": result.get("semantic_view_name"),
                        "status": "CREATED",
                        "stats": result.get("statistics")
                    })
                except Exception as exec_err:
                    fail_count += 1
                    results.append({
                        "table": table_name,
                        "status": "FAILED",
                        "error": str(exec_err)[:200]
                    })
            else:
                fail_count += 1
                results.append({
                    "table": table_name,
                    "status": "FAILED",
                    "error": result.get("error", "Unknown error")
                })
        except Exception as e:
            fail_count += 1
            results.append({
                "table": table_name,
                "status": "FAILED",
                "error": str(e)[:200]
            })
    
    return {
        "status": "COMPLETED",
        "llm_enrichment": p_use_llm,
        "success_count": success_count,
        "fail_count": fail_count,
        "details": results
    }
$$;

-- =============================================================================
-- COMPARISON VIEW: KG-only vs Hybrid
-- =============================================================================
CREATE OR REPLACE VIEW V_SEMANTIC_VIEW_COMPARISON AS
SELECT 
    n.name AS table_name,
    n.properties:schema::VARCHAR AS schema_name,
    CASE WHEN n.description IS NOT NULL AND n.description != '' THEN 'YES' ELSE 'NO' END AS has_llm_description,
    (SELECT COUNT(*) FROM KG_NODE c 
     WHERE c.node_type = 'COLUMN' 
       AND c.properties:table_id = n.node_id
       AND c.description IS NOT NULL) AS columns_with_descriptions,
    (SELECT COUNT(*) FROM KG_NODE c 
     WHERE c.node_type = 'COLUMN' 
       AND c.properties:table_id = n.node_id) AS total_columns,
    CASE 
        WHEN n.description IS NOT NULL THEN 'HYBRID_READY'
        ELSE 'KG_ONLY'
    END AS recommended_approach
FROM KG_NODE n
WHERE n.node_type = 'TABLE'
  AND n.properties:schema::VARCHAR = 'GOLD';

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================
GRANT USAGE ON PROCEDURE GENERATE_HYBRID_SEMANTIC_VIEW(VARCHAR, VARCHAR, BOOLEAN) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(BOOLEAN) TO ROLE PUBLIC;
