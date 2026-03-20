CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.VALIDATE_GOLD_DDL(P_DDL VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS $$
import re

def run(session, p_ddl):
    ddl_upper = p_ddl.upper()

    pattern = r"(?:FROM|JOIN)\s+(?:DBAONTAP_ANALYTICS\.)?(SILVER|GOLD|BRONZE|LANDING|SOURCE|METADATA|KNOWLEDGE_GRAPH|AGENTS)\.(\w+)"
    matches = re.findall(pattern, ddl_upper)

    if not matches:
        return {"valid": True, "message": "No schema-qualified table references found to validate", "references": []}

    existing = {}
    schemas = set(m[0] for m in matches)
    for schema in schemas:
        rows = session.sql(f"SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'").collect()
        existing[schema] = set(r[0] for r in rows)

    results = []
    invalid = []
    for schema, table in matches:
        exists = table in existing.get(schema, set())
        ref = {"schema": schema, "table": table, "exists": exists}
        results.append(ref)
        if not exists:
            valid_tables = sorted(existing.get(schema, set()))
            candidates = [t for t in valid_tables if table in t or t in table]
            ref["candidates"] = candidates[:5]
            invalid.append(ref)

    if invalid:
        msg_parts = []
        for inv in invalid:
            cands = inv.get("candidates", [])
            suggestion = f" Did you mean: {', '.join(cands)}?" if cands else ""
            msg_parts.append(f"{inv['schema']}.{inv['table']} does not exist.{suggestion}")
        return {
            "valid": False,
            "message": "DDL references non-existent tables: " + "; ".join(msg_parts),
            "references": results,
            "invalid_count": len(invalid),
            "invalid_references": invalid
        }

    return {
        "valid": True,
        "message": f"All {len(results)} table references validated successfully",
        "references": results
    }
$$;
