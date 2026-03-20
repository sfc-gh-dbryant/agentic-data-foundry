CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.REGISTER_LINEAGE_FROM_DDL(P_DDL VARCHAR, P_GOLD_TABLE VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS $$
import re
import json

def run(session, p_ddl, p_gold_table):
    gold_parts = p_gold_table.upper().replace('"', '').split('.')
    if len(gold_parts) == 3:
        gold_schema, gold_name = gold_parts[1], gold_parts[2]
    elif len(gold_parts) == 2:
        gold_schema, gold_name = gold_parts[0], gold_parts[1]
    else:
        gold_schema, gold_name = 'GOLD', gold_parts[-1]

    ddl_upper = p_ddl.upper()
    pattern = r"(?:FROM|JOIN)\s+(?:DBAONTAP_ANALYTICS\.)?(SILVER|GOLD|BRONZE)\.(\w+)"
    matches = re.findall(pattern, ddl_upper)

    silver_sources = set()
    for schema, table in matches:
        if schema == 'SILVER':
            silver_sources.add(table)

    registered = []
    for src_table in silver_sources:
        try:
            escaped_src = src_table.replace("'", "''")
            escaped_gold = gold_name.replace("'", "''")
            session.sql(f"""
                MERGE INTO DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP tgt
                USING (SELECT
                    'SILVER' AS SOURCE_SCHEMA,
                    '{escaped_src}' AS SOURCE_TABLE,
                    '{gold_schema}' AS TARGET_SCHEMA,
                    '{escaped_gold}' AS TARGET_TABLE
                ) src
                ON tgt.SOURCE_SCHEMA = src.SOURCE_SCHEMA
                   AND tgt.SOURCE_TABLE = src.SOURCE_TABLE
                   AND tgt.TARGET_SCHEMA = src.TARGET_SCHEMA
                   AND tgt.TARGET_TABLE = src.TARGET_TABLE
                WHEN NOT MATCHED THEN INSERT
                    (SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, EDGE_TYPE, RELATIONSHIP_LABEL, DESCRIPTION)
                VALUES
                    (src.SOURCE_SCHEMA, src.SOURCE_TABLE, src.TARGET_SCHEMA, src.TARGET_TABLE,
                     'AGGREGATES_TO', 'aggregate', 'Auto-registered by agentic Gold build')
            """).collect()
            registered.append(f"SILVER.{src_table} -> {gold_schema}.{escaped_gold}")
        except Exception as e:
            registered.append(f"SILVER.{src_table} -> {gold_schema}.{gold_name} FAILED: {str(e)[:200]}")

    return {"status": "SUCCESS", "registered": registered, "gold_table": p_gold_table, "silver_sources": list(silver_sources)}
$$;
