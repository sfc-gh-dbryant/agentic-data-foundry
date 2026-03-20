CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.BUILD_GOLD_FOR_NEW_TABLES(
    "P_DRY_RUN" BOOLEAN DEFAULT TRUE,
    "P_REFRESH_SEMANTIC_VIEWS" BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
import json

def run(session, p_dry_run=True, p_refresh_semantic_views=True):
    silver_rows = session.sql("""
        SELECT TABLE_NAME
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ''SILVER''
          AND TABLE_TYPE = ''BASE TABLE''
          AND SUBSTR(TABLE_NAME, 1, 2) != ''__''
        ORDER BY TABLE_NAME
    """).collect()
    silver_tables = [r[0] for r in silver_rows]

    gold_rows = session.sql("""
        SELECT TABLE_NAME
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ''GOLD''
          AND TABLE_TYPE = ''BASE TABLE''
        ORDER BY TABLE_NAME
    """).collect()
    gold_tables = set(r[0] for r in gold_rows)

    # --- Strategy 1: Missing Gold targets from TABLE_LINEAGE_MAP ---
    # Gold tables defined in lineage map but not yet created
    missing_gold_rows = session.sql("""
        SELECT DISTINCT lm.TARGET_TABLE
        FROM DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP lm
        WHERE lm.TARGET_SCHEMA = ''GOLD''
          AND NOT EXISTS (
              SELECT 1 FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES t
              WHERE t.TABLE_SCHEMA = ''GOLD'' AND t.TABLE_NAME = lm.TARGET_TABLE
          )
    """).collect()
    missing_gold_targets = [r[0] for r in missing_gold_rows]

    # For each missing Gold target, get its Silver sources from the lineage map
    missing_gold_builds = []
    for gold_target in missing_gold_targets:
        src_rows = session.sql(f"""
            SELECT SOURCE_TABLE
            FROM DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP
            WHERE TARGET_SCHEMA = ''GOLD'' AND TARGET_TABLE = ''{gold_target}''
              AND SOURCE_SCHEMA = ''SILVER''
        """).collect()
        silver_sources = [r[0] for r in src_rows]
        missing_gold_builds.append({
            "gold_target": gold_target,
            "silver_sources": silver_sources,
            "discovery": "lineage_map"
        })

    # --- Strategy 2: Uncovered Silver tables (no lineage mapping at all) ---
    mapped_silver = set()
    for row in session.sql("""
        SELECT DISTINCT SOURCE_TABLE
        FROM DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP
        WHERE SOURCE_SCHEMA = ''SILVER'' AND TARGET_SCHEMA = ''GOLD''
    """).collect():
        mapped_silver.add(row[0])

    uncovered_silver = [t for t in silver_tables if t not in mapped_silver]

    # Build list of all work items
    all_builds = []

    # Missing Gold targets (from lineage map)
    for item in missing_gold_builds:
        all_builds.append(item)

    # Uncovered Silver tables (truly new, no mapping)
    for silver_name in uncovered_silver:
        all_builds.append({
            "gold_target": None,
            "silver_sources": [silver_name],
            "discovery": "uncovered_silver"
        })

    if not all_builds:
        return {
            "status": "ALL_COVERED",
            "message": "All Silver tables have Gold coverage and all lineage map targets exist.",
            "silver_tables": silver_tables,
            "gold_tables": list(gold_tables),
            "lineage_mapped_silver": list(mapped_silver)
        }

    results = []
    for build in all_builds:
        silver_sources = build["silver_sources"]
        gold_target = build.get("gold_target")
        discovery = build["discovery"]

        # Gather column info for all relevant Silver tables
        all_silver_info = ""
        for st in silver_tables:
            st_cols = session.sql(f"""
                SELECT LISTAGG(COLUMN_NAME || '' ('' || DATA_TYPE || '')'', '', '')
                FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ''SILVER'' AND TABLE_NAME = ''{st}''
                  AND COLUMN_NAME NOT LIKE ''_SNOWFLAKE%''
                  AND COLUMN_NAME NOT IN (''IS_DELETED'',''INSERTED_AT'',''UPDATED_AT'',''UPDATED_AT_TS'')
                ORDER BY ORDINAL_POSITION
            """).collect()
            all_silver_info += f"\\n- SILVER.{st}: {st_cols[0][0] if st_cols else ''''}"

        existing_gold_info = ""
        for gt in gold_tables:
            gt_cols = session.sql(f"""
                SELECT LISTAGG(COLUMN_NAME || '' ('' || DATA_TYPE || '')'', '', '')
                FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ''GOLD'' AND TABLE_NAME = ''{gt}''
                ORDER BY ORDINAL_POSITION
            """).collect()
            existing_gold_info += f"\\n- GOLD.{gt}: {gt_cols[0][0] if gt_cols else ''''}"

        primary_silver = silver_sources[0] if silver_sources else silver_tables[0]
        del_col_rows = session.sql(f"""
            SELECT COLUMN_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ''SILVER'' AND TABLE_NAME = ''{primary_silver}''
              AND COLUMN_NAME IN (''_SNOWFLAKE_DELETED'',''IS_DELETED'')
        """).collect()
        del_col = del_col_rows[0][0] if del_col_rows else ''_SNOWFLAKE_DELETED''

        if gold_target:
            gold_name_instruction = f"Name it DBAONTAP_ANALYTICS.GOLD.{gold_target}"
            source_instruction = "PRIMARY SOURCE TABLES: " + ", ".join("SILVER." + s for s in silver_sources)
        else:
            gold_name_instruction = f"Name it DBAONTAP_ANALYTICS.GOLD.<meaningful_name> (e.g. for {primary_silver} -> {primary_silver}_METRICS or {primary_silver}_SUMMARY)"
            source_instruction = f"PRIMARY SOURCE TABLE: SILVER.{primary_silver}"

        # Get directives
        directives_text = ""
        for sname in silver_sources:
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

        prompt = f"""You are a Snowflake SQL expert building Gold-layer Dynamic Tables for a medallion architecture.

{source_instruction}

ALL SILVER TABLES (for JOINs):{all_silver_info}

EXISTING GOLD TABLES (for context):{existing_gold_info}

BUSINESS DIRECTIVES:{directives_text if directives_text else "No specific directives - use standard aggregation patterns"}

REQUIREMENTS:
1. Create ONE Gold Dynamic Table that provides useful business aggregations or metrics
2. {gold_name_instruction}
3. Use CREATE OR REPLACE DYNAMIC TABLE with TARGET_LAG = DOWNSTREAM and WAREHOUSE = DBRYANT_COCO_WH_S
4. Filter out deleted rows using: WHERE <alias>.{del_col} = FALSE
5. JOIN with other Silver tables if it adds business value
6. Include useful aggregations (COUNT, SUM, AVG) and GROUP BY appropriate dimensions
7. Use fully qualified table names (DBAONTAP_ANALYTICS.SILVER.xxx)
8. CRITICAL: Use ONLY table names from the ALL SILVER TABLES list above - do NOT infer or guess table names
9. No comments or explanations - ONLY the SQL

OUTPUT: Only the CREATE OR REPLACE DYNAMIC TABLE statement."""

        prompt_escaped = prompt.replace("''", "''''")
        llm_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-5-sonnet'', ''{prompt_escaped}'')").collect()
        llm_response = llm_rows[0][0] if llm_rows else ''''

        generated_sql = llm_response.strip().replace(''```sql'', '''').replace(''```'', '''').strip()
        create_idx = generated_sql.upper().find(''CREATE'')
        if create_idx > 0:
            generated_sql = generated_sql[create_idx:]

        # Validate DDL
        try:
            escaped_sql = generated_sql.replace("''", "''''")
            v_rows = session.sql(f"CALL DBAONTAP_ANALYTICS.AGENTS.VALIDATE_GOLD_DDL(''{escaped_sql}'')").collect()
            v_result = json.loads(v_rows[0][0]) if v_rows else {"valid": True}
            if not v_result.get("valid", True):
                generated_sql = f"-- VALIDATION FAILED: {v_result.get(''message'', '''')}\\n{generated_sql}"
        except:
            pass

        if p_dry_run:
            results.append({
                "status": "DRY_RUN",
                "discovery": discovery,
                "gold_target": gold_target,
                "silver_sources": silver_sources,
                "generated_ddl": generated_sql,
                "action": "Review and approve to execute"
            })
            continue

        max_retries = 3
        retry_count = 0
        last_error = None
        success = False

        while retry_count < max_retries:
            # Validate before each attempt
            try:
                escaped_sql = generated_sql.replace("''", "''''")
                v_rows = session.sql(f"CALL DBAONTAP_ANALYTICS.AGENTS.VALIDATE_GOLD_DDL(''{escaped_sql}'')").collect()
                v_result = json.loads(v_rows[0][0]) if v_rows else {"valid": True}
                if not v_result.get("valid", True):
                    last_error = f"VALIDATION_FAILED: {v_result.get(''message'', '''')}"
                    retry_count += 1
                    fix_prompt = f"""The DDL failed validation: {last_error}\\nFix the table references.\\nFAILED SQL:\\n{generated_sql}\\nOUTPUT: Only the corrected SQL."""
                    fix_escaped = fix_prompt.replace("''", "''''")
                    fix_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-5-sonnet'', ''{fix_escaped}'')").collect()
                    fix_response = fix_rows[0][0] if fix_rows else ''''
                    generated_sql = fix_response.strip().replace(''```sql'', '''').replace(''```'', '''').strip()
                    ci = generated_sql.upper().find(''CREATE'')
                    if ci > 0:
                        generated_sql = generated_sql[ci:]
                    continue
            except:
                pass

            try:
                session.sql(generated_sql).collect()

                # Auto-register lineage
                try:
                    reg_escaped = generated_sql.replace("''", "''''")
                    # Extract gold table name from DDL
                    import re
                    gold_match = re.search(r''DBAONTAP_ANALYTICS\\.GOLD\\.(\w+)'', generated_sql.upper())
                    if gold_match:
                        actual_gold_name = f"DBAONTAP_ANALYTICS.GOLD.{gold_match.group(1)}"
                        tbl_escaped = actual_gold_name.replace("''", "''''")
                        session.sql(f"CALL DBAONTAP_ANALYTICS.AGENTS.REGISTER_LINEAGE_FROM_DDL(''{reg_escaped}'', ''{tbl_escaped}'')").collect()
                except:
                    pass

                results.append({
                    "status": "SUCCESS",
                    "discovery": discovery,
                    "gold_target": gold_target,
                    "silver_sources": silver_sources,
                    "ddl_executed": generated_sql,
                    "attempts": retry_count + 1,
                    "lineage_registered": True
                })
                success = True
                break
            except Exception as e:
                last_error = str(e)[:500]
                retry_count += 1
                fix_prompt = f"""The previous DDL failed with error: {last_error}
Fix the DDL. Use fully qualified names (DBAONTAP_ANALYTICS.SILVER.xxx). The deleted column is {del_col}.
FAILED SQL:
{generated_sql}
OUTPUT: Only the corrected CREATE OR REPLACE DYNAMIC TABLE statement."""
                fix_escaped = fix_prompt.replace("''", "''''")
                fix_rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-5-sonnet'', ''{fix_escaped}'')").collect()
                fix_response = fix_rows[0][0] if fix_rows else ''''
                generated_sql = fix_response.strip().replace(''```sql'', '''').replace(''```'', '''').strip()
                ci = generated_sql.upper().find(''CREATE'')
                if ci > 0:
                    generated_sql = generated_sql[ci:]

        if not success:
            results.append({
                "status": "FAILED",
                "discovery": discovery,
                "gold_target": gold_target,
                "silver_sources": silver_sources,
                "last_error": last_error,
                "last_ddl": generated_sql,
                "attempts": retry_count
            })

    # Refresh KG and semantic views after all builds
    if not p_dry_run:
        try:
            session.sql("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()").collect()
        except:
            pass
        if p_refresh_semantic_views:
            try:
                session.sql("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(TRUE)").collect()
            except:
                pass

    return {
        "status": "DRY_RUN_COMPLETE" if p_dry_run else "EXECUTED",
        "missing_gold_targets": missing_gold_targets,
        "uncovered_silver_tables": uncovered_silver,
        "results": results,
        "summary": {
            "missing_gold_from_lineage": len(missing_gold_targets),
            "uncovered_silver": len(uncovered_silver),
            "total_builds": len(all_builds),
            "successful": sum(1 for r in results if r["status"] == "SUCCESS"),
            "failed": sum(1 for r in results if r["status"] == "FAILED"),
            "dry_run": sum(1 for r in results if r["status"] == "DRY_RUN")
        }
    }
';
