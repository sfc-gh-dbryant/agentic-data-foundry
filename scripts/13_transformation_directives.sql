-- =============================================================================
-- TRANSFORMATION DIRECTIVES: "Human in the Middle" for Agentic Workflows
-- =============================================================================
-- Provides business intent/instructions to the LLM agents so they tailor
-- transformations for specific use cases (forecasting, churn, dashboards, etc.)
--
-- Three layers of control:
--   1. Schema Contracts = structural guardrails (column names, types)
--   2. Transformation Directives = business intent (what the data is FOR)
--   3. Workflow Learnings = operational memory (what worked before)
-- =============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

-- ============================================================================
-- TABLE: TRANSFORMATION_DIRECTIVES
-- ============================================================================

CREATE TABLE IF NOT EXISTS METADATA.TRANSFORMATION_DIRECTIVES (
    directive_id       VARCHAR DEFAULT UUID_STRING(),
    source_table_pattern VARCHAR NOT NULL,
    target_layer       VARCHAR NOT NULL,
    use_case           VARCHAR NOT NULL,
    instructions       VARCHAR NOT NULL,
    priority           INTEGER DEFAULT 5,
    is_active          BOOLEAN DEFAULT TRUE,
    created_by         VARCHAR DEFAULT CURRENT_USER(),
    created_at         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (directive_id)
);

-- ============================================================================
-- SEED DATA: Example directives for the demo
-- ============================================================================

MERGE INTO METADATA.TRANSFORMATION_DIRECTIVES t
USING (
    SELECT column1 as source_table_pattern, column2 as target_layer,
           column3 as use_case, column4 as instructions, column5 as priority
    FROM VALUES
    ('ORDERS', 'GOLD', 'demand_forecasting',
     'This data feeds a demand forecasting model. Preserve daily granularity - do NOT aggregate to weekly/monthly. Create lag features: 7-day, 14-day, and 30-day rolling averages of order counts and revenue. Include day-of-week and month-of-year indicators. Ensure a continuous date spine with zero-fill for days with no orders. Partition output by customer segment.',
     8),
    ('CUSTOMERS', 'GOLD', 'churn_prediction',
     'This data feeds a churn prediction model. Calculate days-since-last-order, support-ticket frequency (tickets per 30 days), and engagement decay rate. Flag customers with no activity in 60+ days as at-risk. Include lifetime value percentile rank. Derive a recency-frequency-monetary (RFM) score.',
     8),
    ('SUPPORT_TICKETS', 'GOLD', 'service_optimization',
     'This data supports a service operations dashboard. Calculate average resolution time by priority level and by customer segment. Track ticket volume trends (daily counts with 7-day moving average). Flag SLA breaches: HIGH priority tickets open longer than 4 hours, MEDIUM longer than 24 hours. Include a first-response-time metric.',
     7),
    ('PRODUCTS', 'GOLD', 'inventory_planning',
     'This data feeds inventory planning. Calculate sell-through rate (units sold / units available per 30-day window). Identify slow-moving products (bottom 10% by units sold). Track margin trends. Include category-level aggregations alongside product-level detail.',
     6),
    ('ORDER_ITEMS', 'GOLD', 'basket_analysis',
     'This data supports market basket analysis. Preserve item-level granularity within each order. Include product category alongside item details. Calculate each items share of order total. Do not aggregate - downstream ML models need transaction-level records.',
     6),
    ('%', 'SILVER', 'general_hygiene',
     'Preserve all source columns. Do not aggregate or drop rows beyond CDC deduplication. Apply proper type casting. Handle nulls defensively with COALESCE for required business fields. All timestamp columns must use TIMESTAMP_NTZ.',
     3)
) s
ON t.source_table_pattern = s.source_table_pattern
   AND t.target_layer = s.target_layer
   AND t.use_case = s.use_case
WHEN NOT MATCHED THEN INSERT (source_table_pattern, target_layer, use_case, instructions, priority)
VALUES (s.source_table_pattern, s.target_layer, s.use_case, s.instructions, s.priority);


-- ============================================================================
-- UPDATED WORKFLOW_PLANNER: Now reads directives and injects into LLM prompt
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_PLANNER(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
DECLARE
    tables_to_process ARRAY;
    current_table VARCHAR;
    schema_info VARIANT;
    quality_info VARIANT;
    existing_learnings VARCHAR;
    directives_text VARCHAR;
    schema_contract_text VARCHAR;
    planner_prompt VARCHAR;
    llm_response VARCHAR;
    parsed_plan VARIANT;
    all_decisions ARRAY DEFAULT ARRAY_CONSTRUCT();
    i INTEGER;
    table_base_name VARCHAR;
BEGIN
    UPDATE METADATA.WORKFLOW_EXECUTIONS
    SET status = ''PLANNING'', current_phase = ''PLANNER''
    WHERE execution_id = :execution_id;

    SELECT planner_output:tables INTO :tables_to_process
    FROM METADATA.WORKFLOW_EXECUTIONS
    WHERE execution_id = :execution_id;

    SELECT LISTAGG(observation || '' -> '' || recommendation, ''; '')
    INTO :existing_learnings
    FROM METADATA.WORKFLOW_LEARNINGS
    WHERE is_active = TRUE AND confidence_score > 0.7
    LIMIT 5;

    FOR i IN 0 TO ARRAY_SIZE(tables_to_process) - 1 DO
        current_table := tables_to_process[i]::VARCHAR;
        table_base_name := UPPER(REPLACE(SPLIT_PART(current_table, ''.'', -1), ''_VARIANT'', ''''));

        schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:current_table));
        quality_info := (CALL AGENTS.ANALYZE_DATA_QUALITY(:current_table, 500));

        -- Fetch transformation directives for this table
        SELECT LISTAGG(
            ''['' || use_case || '' | priority:'' || priority || ''] '' || instructions,
            ''\n''
        ) WITHIN GROUP (ORDER BY priority DESC)
        INTO :directives_text
        FROM METADATA.TRANSFORMATION_DIRECTIVES
        WHERE is_active = TRUE
          AND (:table_base_name LIKE source_table_pattern OR source_table_pattern = ''%'')
          AND target_layer IN (''SILVER'', ''BOTH'');

        -- Fetch schema contract for this table
        SELECT naming_rules:note::VARCHAR
        INTO :schema_contract_text
        FROM METADATA.SILVER_SCHEMA_CONTRACTS
        WHERE source_table_pattern = :table_base_name;

        planner_prompt := ''
You are the PLANNER agent in an agentic data transformation workflow.

TASK: Analyze this Bronze table and decide the transformation strategy for Silver layer.

SOURCE TABLE: '' || current_table || ''

SCHEMA ANALYSIS:
'' || schema_info::VARCHAR || ''

DATA QUALITY ANALYSIS:
'' || quality_info::VARCHAR || ''

PAST LEARNINGS (apply if relevant):
'' || COALESCE(existing_learnings, ''No prior learnings'') || ''

SCHEMA CONTRACT (enforce these column names and types):
'' || COALESCE(schema_contract_text, ''No schema contract - use best judgment'') || ''

BUSINESS DIRECTIVES (from the data team - follow these instructions for HOW to shape this data):
'' || COALESCE(directives_text, ''No specific directives - apply standard transformation patterns'') || ''

AVAILABLE STRATEGIES:
1. flatten_and_type - Extract VARIANT fields, apply proper types, handle nulls
2. deduplicate - Remove duplicates based on key columns
3. scd_type2 - Slowly changing dimension with history tracking
4. aggregate - Pre-aggregate for performance
5. normalize - Split nested arrays into separate tables

OUTPUT FORMAT (JSON only, no explanation):
{
  "source_table": "...",
  "target_table": "SILVER.<name>",
  "strategy": "<strategy_name>",
  "detected_patterns": {
    "has_nested_arrays": true/false,
    "has_null_issues": true/false,
    "needs_type_casting": true/false,
    "has_duplicates": true/false
  },
  "transformations": [
    {"column": "...", "action": "...", "reason": "..."}
  ],
  "directives_applied": ["list of use_case names that influenced this plan"],
  "priority": 1-5,
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation including how directives were applied"
}'';

        SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-7-sonnet'', :planner_prompt) INTO :llm_response;

        BEGIN
            LET json_start INTEGER := POSITION(''{'' IN llm_response);
            LET json_end INTEGER := 0;
            LET brace_count INTEGER := 0;
            LET i INTEGER := json_start;
            LET response_len INTEGER := LENGTH(llm_response);

            WHILE (i <= response_len AND (json_end = 0 OR brace_count > 0)) DO
                IF (SUBSTR(llm_response, i, 1) = ''{'') THEN
                    brace_count := brace_count + 1;
                ELSEIF (SUBSTR(llm_response, i, 1) = ''}'') THEN
                    brace_count := brace_count - 1;
                    IF (brace_count = 0) THEN
                        json_end := i;
                    END IF;
                END IF;
                i := i + 1;
            END WHILE;

            IF (json_start > 0 AND json_end > json_start) THEN
                parsed_plan := PARSE_JSON(SUBSTR(llm_response, json_start, json_end - json_start + 1));
            ELSE
                parsed_plan := NULL;
            END IF;
        EXCEPTION WHEN OTHER THEN
            parsed_plan := OBJECT_CONSTRUCT(
                ''source_table'', current_table,
                ''target_table'', ''SILVER.'' || REPLACE(SPLIT_PART(current_table, ''.'', -1), ''RAW_'', ''''),
                ''strategy'', ''flatten_and_type'',
                ''confidence'', 0.5,
                ''reasoning'', ''Default strategy due to parse error: '' || SUBSTR(llm_response, 1, 500)
            );
        END;

        INSERT INTO METADATA.PLANNER_DECISIONS (
            execution_id, source_table, target_schema, transformation_strategy,
            detected_patterns, recommended_actions, priority, llm_reasoning, confidence_score
        )
        SELECT
            :execution_id,
            :current_table,
            ''SILVER'',
            :parsed_plan:strategy::VARCHAR,
            :parsed_plan:detected_patterns,
            :parsed_plan:transformations,
            COALESCE(:parsed_plan:priority::INTEGER, 3),
            :parsed_plan:reasoning::VARCHAR,
            COALESCE(:parsed_plan:confidence::FLOAT, 0.5);

        all_decisions := ARRAY_APPEND(all_decisions, parsed_plan);
    END FOR;

    UPDATE METADATA.WORKFLOW_EXECUTIONS
    SET planner_output = OBJECT_CONSTRUCT(
            ''decisions'', :all_decisions,
            ''tables_planned'', ARRAY_SIZE(:tables_to_process),
            ''completed_at'', CURRENT_TIMESTAMP()::VARCHAR
        ),
        planning_completed_at = CURRENT_TIMESTAMP(),
        current_phase = ''PLANNER_COMPLETE''
    WHERE execution_id = :execution_id;

    RETURN OBJECT_CONSTRUCT(
        ''execution_id'', execution_id,
        ''status'', ''PLANNED'',
        ''decisions_count'', ARRAY_SIZE(all_decisions),
        ''decisions'', all_decisions,
        ''next_phase'', ''EXECUTOR''
    );
END;
';


-- ============================================================================
-- UPDATED WORKFLOW_EXECUTOR: Now passes directives to execution prompt
-- ============================================================================

CREATE OR REPLACE PROCEDURE AGENTS.WORKFLOW_EXECUTOR(execution_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
DECLARE
    decisions_cursor CURSOR FOR
        SELECT decision_id, source_table, transformation_strategy, recommended_actions, llm_reasoning
        FROM METADATA.PLANNER_DECISIONS
        WHERE execution_id = ?
        ORDER BY priority ASC;

    current_decision VARIANT;
    generated_sql VARCHAR;
    execution_prompt VARCHAR;
    llm_response VARCHAR;
    retry_count INTEGER;
    max_retries INTEGER DEFAULT 3;
    execution_succeeded BOOLEAN;
    last_error VARCHAR;
    execution_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    success_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    cur_source_table VARCHAR;
    cur_strategy VARCHAR;
    cur_actions VARCHAR;
    cur_reasoning VARCHAR;
    cur_schema_info VARIANT;
    variant_column VARCHAR;
    discovered_fields VARCHAR;
    table_base_name VARCHAR;
    directives_text VARCHAR;
    schema_contract_text VARCHAR;
BEGIN
    UPDATE METADATA.WORKFLOW_EXECUTIONS
    SET status = ''EXECUTING'', current_phase = ''EXECUTOR''
    WHERE execution_id = :execution_id;

    OPEN decisions_cursor USING (execution_id);
    FOR record IN decisions_cursor DO
        cur_source_table := record.source_table;
        cur_strategy := record.transformation_strategy;
        cur_actions := record.recommended_actions::VARCHAR;
        cur_reasoning := record.llm_reasoning;
        table_base_name := UPPER(REPLACE(SPLIT_PART(cur_source_table, ''.'', -1), ''_VARIANT'', ''''));

        cur_schema_info := (CALL AGENTS.DISCOVER_SCHEMA(:cur_source_table));
        variant_column := COALESCE(cur_schema_info:variant_column::VARCHAR, ''PAYLOAD'');

        SELECT LISTAGG(key || '' ('' || value:inferred_type::VARCHAR || '')'', '', '')
        INTO :discovered_fields
        FROM TABLE(FLATTEN(input => :cur_schema_info:discovered_columns));

        -- Fetch directives for this table at Silver layer
        SELECT LISTAGG(
            ''['' || use_case || ''] '' || instructions,
            ''\n''
        ) WITHIN GROUP (ORDER BY priority DESC)
        INTO :directives_text
        FROM METADATA.TRANSFORMATION_DIRECTIVES
        WHERE is_active = TRUE
          AND (:table_base_name LIKE source_table_pattern OR source_table_pattern = ''%'')
          AND target_layer IN (''SILVER'', ''BOTH'');

        -- Fetch schema contract
        SELECT naming_rules:note::VARCHAR
        INTO :schema_contract_text
        FROM METADATA.SILVER_SCHEMA_CONTRACTS
        WHERE source_table_pattern = :table_base_name;

        retry_count := 0;
        execution_succeeded := FALSE;
        last_error := NULL;

        WHILE (retry_count < max_retries AND NOT execution_succeeded) DO

            IF (retry_count = 0) THEN
                execution_prompt := ''
Generate Snowflake Dynamic Table DDL for this transformation.

SOURCE TABLE: '' || cur_source_table || ''
STRATEGY: '' || cur_strategy || ''
ACTIONS: '' || cur_actions || ''
REASONING: '' || cur_reasoning || ''

CRITICAL SCHEMA INFO:
- The source table has a VARIANT/OBJECT column named: '' || variant_column || ''
- All data fields are INSIDE this column and must be accessed as: '' || variant_column || '':field_name
- Discovered fields: '' || COALESCE(discovered_fields, ''unknown'') || ''

SCHEMA CONTRACT (enforce these rules):
'' || COALESCE(schema_contract_text, ''No schema contract'') || ''

BUSINESS DIRECTIVES (follow these instructions):
'' || COALESCE(directives_text, ''No specific directives'') || ''

REQUIREMENTS:
1. Create a DYNAMIC TABLE in the SILVER schema (use database DBAONTAP_ANALYTICS)
2. Use TARGET_LAG = ''''1 minute''''
3. Use WAREHOUSE = DBRYANT_COCO_WH_S
4. Access all fields using '' || variant_column || '':field_name syntax
5. Cast fields to proper types using ::TYPE or CAST()
6. Handle NULL values with COALESCE where appropriate
7. Add meaningful column aliases
8. Apply business directives where specified

OUTPUT: Only the CREATE OR REPLACE DYNAMIC TABLE statement, nothing else.'';
            ELSE
                execution_prompt := ''
The previous DDL failed with error: '' || last_error || ''

Fix the DDL and try again.

SOURCE TABLE: '' || cur_source_table || ''
STRATEGY: '' || cur_strategy || ''

CRITICAL: The VARIANT column is named "'' || variant_column || ''".
Access fields as: '' || variant_column || '':field_name (NOT as direct columns!)
Discovered fields: '' || COALESCE(discovered_fields, ''unknown'') || ''

FAILED SQL:
'' || generated_sql || ''

CORRECT PATTERN:
SELECT '' || variant_column || '':field_name::TYPE AS alias FROM table;

OUTPUT: Only the corrected CREATE OR REPLACE DYNAMIC TABLE statement.'';
            END IF;

            SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-3-7-sonnet'', :execution_prompt) INTO :llm_response;

            generated_sql := TRIM(REGEXP_REPLACE(llm_response, ''```sql|```'', ''''));
            LET create_pos INTEGER := POSITION(''CREATE'' IN UPPER(generated_sql));
            IF (create_pos > 1) THEN
                generated_sql := SUBSTR(generated_sql, create_pos);
            END IF;

            BEGIN
                EXECUTE IMMEDIATE :generated_sql;
                execution_succeeded := TRUE;
                success_count := success_count + 1;

                INSERT INTO METADATA.TRANSFORMATION_LOG (
                    source_table, target_table, transformation_sql, agent_reasoning, status, executed_at
                )
                VALUES (
                    cur_source_table,
                    ''SILVER.'' || REPLACE(SPLIT_PART(cur_source_table, ''.'', -1), ''RAW_'', ''''),
                    :generated_sql,
                    CASE WHEN :retry_count > 0
                         THEN ''Self-corrected after '' || retry_count || '' retries''
                         ELSE ''Executed on first attempt''
                    END,
                    ''SUCCESS'',
                    CURRENT_TIMESTAMP()
                );

                execution_results := ARRAY_APPEND(execution_results, OBJECT_CONSTRUCT(
                    ''source_table'', cur_source_table,
                    ''status'', ''SUCCESS'',
                    ''retries'', retry_count,
                    ''sql'', generated_sql
                ));

            EXCEPTION WHEN OTHER THEN
                last_error := SQLERRM;
                retry_count := retry_count + 1;
            END;
        END WHILE;

        IF (NOT execution_succeeded) THEN
            fail_count := fail_count + 1;

            INSERT INTO METADATA.TRANSFORMATION_LOG (
                source_table, target_table, transformation_sql, agent_reasoning, status, executed_at
            )
            SELECT
                :cur_source_table,
                ''SILVER.'' || REPLACE(SPLIT_PART(:cur_source_table, ''.'', -1), ''RAW_'', ''''),
                :generated_sql,
                ''FAILED after '' || :max_retries || '' attempts: '' || :last_error,
                ''FAILED'',
                CURRENT_TIMESTAMP();

            execution_results := ARRAY_APPEND(execution_results, OBJECT_CONSTRUCT(
                ''source_table'', cur_source_table,
                ''status'', ''FAILED'',
                ''retries'', retry_count,
                ''error'', last_error,
                ''sql'', generated_sql
            ));
        END IF;
    END FOR;

    UPDATE METADATA.WORKFLOW_EXECUTIONS
    SET executor_output = OBJECT_CONSTRUCT(
            ''success_count'', :success_count,
            ''fail_count'', :fail_count,
            ''results'', :execution_results,
            ''completed_at'', CURRENT_TIMESTAMP()::VARCHAR
        ),
        execution_completed_at = CURRENT_TIMESTAMP(),
        current_phase = ''EXECUTOR_COMPLETE'',
        retry_count = (SELECT SUM(r.value:retries::INTEGER) FROM TABLE(FLATTEN(input => :execution_results)) r)
    WHERE execution_id = :execution_id;

    RETURN OBJECT_CONSTRUCT(
        ''execution_id'', execution_id,
        ''status'', CASE WHEN fail_count = 0 THEN ''ALL_SUCCEEDED'' ELSE ''PARTIAL_FAILURE'' END,
        ''success_count'', success_count,
        ''fail_count'', fail_count,
        ''results'', execution_results,
        ''next_phase'', ''VALIDATOR''
    );
END;
';


-- ============================================================================
-- UPDATED GOLD_AGENTIC_EXECUTOR: Now reads Gold-layer directives
-- ============================================================================

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
    silver_names = []
    for row in silver_edges:
        sname = row[0]
        silver_names.append(sname)
        cols_rows = session.sql(f"""
            SELECT LISTAGG(COLUMN_NAME || ' (' || DATA_TYPE || ')', ', ')
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = '{sname}'
              AND COLUMN_NAME NOT LIKE '_SNOWFLAKE%'
            ORDER BY ORDINAL_POSITION
        """).collect()
        cols = cols_rows[0][0] if cols_rows else ''
        silver_info += f"\n- SILVER.{sname}: {cols}"

    directives_text = ''
    for sname in silver_names:
        dir_rows = session.sql(f"""
            SELECT use_case, instructions, priority
            FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
            WHERE is_active = TRUE
              AND ('{sname}' LIKE source_table_pattern OR source_table_pattern = '%')
              AND target_layer IN ('GOLD', 'BOTH')
            ORDER BY priority DESC
        """).collect()
        for dr in dir_rows:
            directives_text += f"\n[{dr[0]} | priority:{dr[2]}] {dr[1]}"

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

BUSINESS DIRECTIVES (from the data team - follow these for HOW to shape the Gold output):
{directives_text if directives_text else 'No specific directives - use standard aggregation patterns'}

REQUIREMENTS:
1. Output ONLY a single CREATE OR REPLACE DYNAMIC TABLE statement
2. Use TARGET_LAG = '1 hour' and WAREHOUSE = DBRYANT_COCO_WH_S
3. Keep ALL existing columns and logic EXACTLY as-is
4. Add the missing columns from the appropriate Silver source table
5. For simple passthrough columns, add them directly with the Silver alias
6. For numeric columns, decide if they need aggregation based on context and business directives
7. Add new columns to GROUP BY if the table uses GROUP BY
8. Preserve existing table alias conventions
9. Apply business directives where they inform how new columns should be computed
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
                'directives_applied': directives_text.strip() if directives_text else None,
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
                'directives_applied': directives_text.strip() if directives_text else None,
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


-- ============================================================================
-- UPDATED GOLD_AUTO_PASSTHROUGH: Now reads Gold-layer directives
-- ============================================================================

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

    dir_rows = session.sql(f"""
        SELECT use_case, instructions
        FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
        WHERE is_active = TRUE
          AND ('{silver_name}' LIKE source_table_pattern OR source_table_pattern = '%')
          AND target_layer IN ('GOLD', 'BOTH')
        ORDER BY priority DESC
        LIMIT 3
    """).collect()
    directives_text = '\n'.join(f"[{r[0]}] {r[1]}" for r in dir_rows) if dir_rows else ''

    prompt = f"""You are a Snowflake SQL expert. Add these new columns to the Gold Dynamic Table.

CURRENT DDL:
{gold_ddl}

COLUMNS TO ADD (from SILVER.{silver_name}): {', '.join(cols_info)}

BUSINESS DIRECTIVES:
{directives_text if directives_text else 'No specific directives - add as simple passthrough columns'}

RULES:
1. Output ONLY the CREATE OR REPLACE DYNAMIC TABLE statement
2. Keep ALL existing columns and logic EXACTLY as-is - do not change ANY existing column
3. Add new columns as simple passthrough references using the Silver table alias from the existing DDL
4. Add new columns to GROUP BY if the query uses GROUP BY
5. Preserve TARGET_LAG, WAREHOUSE, and all other settings
6. Apply directives if they provide guidance on how these columns should be shaped
7. Do NOT rename columns or add transformations unless directives specify otherwise
8. No comments, no explanations - ONLY the SQL

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
            'directives_applied': directives_text if directives_text else None,
            'generated_ddl': generated_sql,
            'action': 'Review and approve to execute'
        }

    try:
        session.sql(generated_sql).collect()
        return {
            'status': 'SUCCESS',
            'gold_table': p_gold_table,
            'columns_added': list(p_columns),
            'directives_applied': directives_text if directives_text else None,
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


-- ============================================================================
-- HELPER VIEW: Show active directives with matched tables
-- ============================================================================

CREATE OR REPLACE VIEW METADATA.ACTIVE_DIRECTIVES AS
SELECT
    d.directive_id,
    d.source_table_pattern,
    d.target_layer,
    d.use_case,
    d.instructions,
    d.priority,
    d.created_by,
    d.created_at,
    CASE
        WHEN d.source_table_pattern = '%' THEN 'ALL TABLES'
        ELSE (
            SELECT COALESCE(LISTAGG(REPLACE(TABLE_NAME, '_VARIANT', ''), ', '), 'none')
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'BRONZE'
              AND REPLACE(TABLE_NAME, '_VARIANT', '') LIKE d.source_table_pattern
        )
    END as matched_tables
FROM METADATA.TRANSFORMATION_DIRECTIVES d
WHERE d.is_active = TRUE
ORDER BY d.priority DESC, d.source_table_pattern;

-- ============================================================================
-- Verification
-- ============================================================================
SELECT 'Directives loaded' as status, COUNT(*) as count
FROM METADATA.TRANSFORMATION_DIRECTIVES;

SELECT * FROM METADATA.ACTIVE_DIRECTIVES;
