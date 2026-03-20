# Agentic Data Foundry Architecture

## Overview

This demo showcases a modern data architecture combining:
- **Snowflake Managed PostgreSQL** for operational data (SOURCE + LANDING instances)
- **Openflow CDC** for real-time data replication with `_SNOWFLAKE_*` metadata columns
- **Dynamic Tables** for medallion architecture (Bronze/Silver/Gold)
- **Cortex LLM Agents** for AI-powered Silver transformations (4-phase workflow) and Gold layer generation
- **Schema Contracts** for guardrails on LLM-generated DDL
- **Knowledge Graph** for automated lineage and metadata discovery
- **Semantic Views** for natural language queries via Cortex Analyst
- **Streamlit in Snowflake** for the 11-tab demo management app

## Source Tables

| Table | Description |
|-------|-------------|
| `customers` | Customer profiles (name, email, segment) |
| `orders` | Order headers (customer_id, total, status) |
| `products` | Product catalog (name, category, price, cost) |
| `order_items` | Line items (order_id, product_id, quantity, price) |
| `support_tickets` | Support interactions (customer_id, subject, status, priority) |

## Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                DATA FLOW                                         │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────┐                                                         │
│  │   SOURCE PG          │  Snowflake Managed PostgreSQL (OLTP)                   │
│  │   5 tables:          │  • Application writes here                             │
│  │   customers, orders, │  • Logical replication enabled                         │
│  │   products,          │  • Publication: dbaontap_to_landing_pub                │
│  │   order_items,       │    (owned by snowflake_admin)                          │
│  │   support_tickets    │                                                        │
│  └──────────┬───────────┘                                                        │
│             │                                                                    │
│             │ PostgreSQL Logical Replication                                      │
│             ▼                                                                    │
│  ┌─────────────────────┐                                                         │
│  │   LANDING PG         │  Snowflake Managed PostgreSQL (CDC staging)            │
│  │   5 tables (replica) │  • Subscription: dbaontap_sub                          │
│  │                      │  • Receives changes from SOURCE                        │
│  │                      │  • Openflow reads from here                            │
│  └──────────┬───────────┘                                                        │
│             │                                                                    │
│             │ Openflow CDC (continuous)                                           │
│             │ Adds: _SNOWFLAKE_DELETED, _SNOWFLAKE_UPDATED_AT,                   │
│             │       _SNOWFLAKE_INSERTED_AT                                        │
│             ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                            SNOWFLAKE                                        │ │
│  │                                                                             │ │
│  │  "public" schema (Openflow landing, owned by OPENFLOWRUNTIMEROLE)           │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐  │ │
│  │  │customers │ │orders    │ │products  │ │order_    │ │support_tickets   │  │ │
│  │  │+ CDC cols│ │+ CDC cols│ │+ CDC cols│ │items     │ │+ CDC cols        │  │ │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────────────┘  │ │
│  │       └─────────────┼───────────┼─────────────┼────────────┘                │ │
│  │                     ▼           ▼             ▼                              │ │
│  │  BRONZE schema (Dynamic Tables, TARGET_LAG = 1 min)                         │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ 5 *_VARIANT tables + ALL_DATA_VARIANT view         │                     │ │
│  │  │ • SELECT OBJECT_CONSTRUCT(*) AS PAYLOAD            │                     │ │
│  │  │ • Schema-on-read: source schema changes don't break│                     │ │
│  │  └───────────────────────┬────────────────────────────┘                     │ │
│  │                          │                                                  │ │
│  │            ┌─────────────┤ AGENTIC SILVER WORKFLOW                          │ │
│  │            │             │ (AGENTS.RUN_AGENTIC_WORKFLOW)                     │ │
│  │            ▼             │                                                  │ │
│  │  ┌──────────────────┐   │                                                   │ │
│  │  │ SCHEMA CONTRACTS │   │  METADATA.SILVER_SCHEMA_CONTRACTS                 │ │
│  │  │ 5 contracts with │   │  • Required columns & types per table             │ │
│  │  │ naming rules     │   │  • Enforce _SNOWFLAKE_* CDC column naming         │ │
│  │  └──────────────────┘   │  • LLM reads these before generating DDL          │ │
│  │                          │                                                  │ │
│  │                          ▼                                                  │ │
│  │  SILVER schema (Dynamic Tables, TARGET_LAG = DOWNSTREAM)                    │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ 5 cleaned, typed, deduplicated tables              │                     │ │
│  │  │ • LLM-generated DDL via 4-phase agentic workflow   │                     │ │
│  │  │ • ROW_NUMBER() deduplication by PK + timestamp     │                     │ │
│  │  │ • WHERE _SNOWFLAKE_DELETED = FALSE                 │                     │ │
│  │  │ • Typed columns extracted from VARIANT             │                     │ │
│  │  │ • Derived fields (FULL_NAME, SEGMENT, etc.)        │                     │ │
│  │  └───────────────────────┬────────────────────────────┘                     │ │
│  │                          │                                                  │ │
│  │                          ▼                                                  │ │
│  │  GOLD schema (Dynamic Tables, TARGET_LAG = DOWNSTREAM)                      │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ 5 business tables:                                 │                     │ │
│  │  │ • CUSTOMER_360: RFM, LTV, loyalty, engagement     │                     │ │
│  │  │ • PRODUCT_PERFORMANCE: sales, margins, revenue     │                     │ │
│  │  │ • ORDER_SUMMARY: monthly trends by segment         │                     │ │
│  │  │ • CUSTOMER_METRICS: segment-level aggregations     │                     │ │
│  │  │ • ML_CUSTOMER_FEATURES: encoded ML features        │                     │ │
│  │  └───────────────────────┬────────────────────────────┘                     │ │
│  │                          │                                                  │ │
│  │  KNOWLEDGE_GRAPH schema  │                                                  │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ KG_NODE + KG_EDGE tables                           │                     │ │
│  │  │ • Auto-populated from INFORMATION_SCHEMA           │                     │ │
│  │  │ • Tracks lineage: Bronze→Silver→Gold               │                     │ │
│  │  │ • Edge types: TRANSFORMS_TO, AGGREGATES_TO         │                     │ │
│  │  └───────────────────────┬────────────────────────────┘                     │ │
│  │                          │                                                  │ │
│  │                          ▼                                                  │ │
│  │  GOLD schema (Semantic Views — LLM Generated)                               │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ *_SV semantic views (one per Gold table)           │                     │ │
│  │  │ • TABLES: base table + primary key                 │                     │ │
│  │  │ • FACTS: numeric columns                           │                     │ │
│  │  │ • DIMENSIONS: categorical columns                  │                     │ │
│  │  │ • METRICS: business aggregations                   │                     │ │
│  │  └───────────────────────┬────────────────────────────┘                     │ │
│  │                          │                                                  │ │
│  │                          ▼                                                  │ │
│  │  CORTEX ANALYST (AI Chat in Streamlit app)                                  │ │
│  │  ┌────────────────────────────────────────────────────┐                     │ │
│  │  │ Natural language → SQL via Semantic Views           │                     │ │
│  │  │ • "Who are our top customers by lifetime value?"   │                     │ │
│  │  │ • "What's our best-selling product by revenue?"    │                     │ │
│  │  │ • "Show me order trends by customer segment"       │                     │ │
│  │  └────────────────────────────────────────────────────┘                     │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Agentic Silver Workflow (4 Phases)

The Silver layer is generated by `AGENTS.RUN_AGENTIC_WORKFLOW()`, a 4-phase AI pipeline:

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   PLANNER    │───▶│   EXECUTOR   │───▶│  VALIDATOR   │───▶│  REFLECTOR   │
│              │    │              │    │              │    │              │
│ • Analyze    │    │ • Generate   │    │ • Execute    │    │ • Review     │
│   Bronze     │    │   DT DDL     │    │   DDL        │    │   outcomes   │
│   schema     │    │ • Apply      │    │ • Check row  │    │ • Store      │
│ • Sample     │    │   schema     │    │   counts     │    │   learnings  │
│   data       │    │   contracts  │    │ • Validate   │    │ • Update     │
│ • Check      │    │ • Handle     │    │   data       │    │   patterns   │
│   learnings  │    │   typing     │    │   quality    │    │   for next   │
│ • Decide     │    │   & dedup    │    │              │    │   run        │
│   strategy   │    │              │    │              │    │              │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

| Phase | Procedure | LLM Model | Purpose |
|-------|-----------|-----------|---------|
| Planner | `WORKFLOW_PLANNER` | claude-3-5-sonnet | Analyzes Bronze VARIANT schema, samples data, reads learnings, decides transformation strategy |
| Executor | `WORKFLOW_EXECUTOR` | claude-3-5-sonnet | Generates `CREATE DYNAMIC TABLE` DDL respecting schema contracts |
| Validator | `WORKFLOW_VALIDATOR` | — | Executes DDL, verifies row counts, checks data quality |
| Reflector | `WORKFLOW_REFLECTOR` | claude-3-5-sonnet | Reviews execution, stores learnings in `METADATA.WORKFLOW_LEARNINGS` |

## Agentic Gold Layer

The Gold layer has three operational modes:

| Mode | Procedure | Description |
|------|-----------|-------------|
| **Core Build** | Hard-coded DDLs in Streamlit app | Creates 5 standard Gold DTs (CUSTOMER_360, etc.) |
| **Agentic Build** | `AGENTS.BUILD_GOLD_FOR_NEW_TABLES(DRY_RUN, REFRESH_SVS)` | Discovers Silver tables with no Gold coverage via KG edges, uses LLM to generate Gold DT DDLs |
| **Drift Detection** | `AGENTS.PROPAGATE_TO_GOLD(TABLE, DRY_RUN)` | Detects Silver columns missing from Gold, optionally auto-remediates |

Supporting procedures:
- `DETECT_GOLD_SCHEMA_DRIFT(TABLE)` — compares Silver vs Gold columns
- `GOLD_AGENTIC_EXECUTOR(TABLE, COLUMNS, DRY_RUN)` — LLM-driven Gold DT rebuild
- `GOLD_AUTO_PASSTHROUGH(TABLE, COLUMNS, DRY_RUN)` — simple column additions without LLM

## Schema Contracts

Stored in `METADATA.SILVER_SCHEMA_CONTRACTS` (5 entries, one per source table). Each contract defines:

- `SOURCE_TABLE_PATTERN` — table name the contract applies to
- `REQUIRED_COLUMNS` — columns that must exist with specific data types
- `NAMING_RULES` — column naming standards including: CDC columns MUST use `_SNOWFLAKE_DELETED`, `_SNOWFLAKE_INSERTED_AT`, `_SNOWFLAKE_UPDATED_AT`
- `ADDITIONAL_INSTRUCTIONS` — table-specific guidance (e.g., "Column is NAME not PRODUCT_NAME")

The agentic workflow reads contracts before generating DDL, ensuring consistent output across runs.

## Key Patterns

### 1. Bronze VARIANT Pattern
```sql
CREATE OR REPLACE DYNAMIC TABLE BRONZE.CUSTOMERS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
    SELECT OBJECT_CONSTRUCT(*) AS PAYLOAD
    FROM DBAONTAP_ANALYTICS."public".customers;
```

### 2. Silver CDC Deduplication Pattern (LLM-generated)
```sql
CREATE OR REPLACE DYNAMIC TABLE SILVER.CUSTOMERS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked AS (
    SELECT
        PAYLOAD:customer_id::INTEGER AS CUSTOMER_ID,
        PAYLOAD:first_name::VARCHAR AS FIRST_NAME,
        PAYLOAD:last_name::VARCHAR AS LAST_NAME,
        PAYLOAD:_SNOWFLAKE_DELETED::BOOLEAN AS _SNOWFLAKE_DELETED,
        PAYLOAD:_SNOWFLAKE_UPDATED_AT::TIMESTAMP_NTZ AS _SNOWFLAKE_UPDATED_AT,
        PAYLOAD:_SNOWFLAKE_INSERTED_AT::TIMESTAMP_NTZ AS _SNOWFLAKE_INSERTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY PAYLOAD:customer_id
            ORDER BY PAYLOAD:_SNOWFLAKE_UPDATED_AT DESC
        ) AS RN
    FROM BRONZE.CUSTOMERS_VARIANT
)
SELECT * FROM ranked
WHERE RN = 1 AND (_SNOWFLAKE_DELETED = FALSE OR _SNOWFLAKE_DELETED IS NULL);
```

### 3. Gold Aggregation Pattern
```sql
CREATE OR REPLACE DYNAMIC TABLE GOLD.CUSTOMER_360
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT
    c.CUSTOMER_ID,
    c.FULL_NAME,
    c.SEGMENT,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) AS LIFETIME_VALUE,
    -- RFM scoring, loyalty tiers, engagement status...
FROM SILVER.CUSTOMERS c
LEFT JOIN SILVER.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    AND o._SNOWFLAKE_DELETED = FALSE
LEFT JOIN SILVER.SUPPORT_TICKETS st ON c.CUSTOMER_ID = st.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FULL_NAME, c.SEGMENT, ...;
```

### 4. Cortex LLM Call Pattern
```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    :prompt_with_schema_and_contracts
) INTO :llm_response;

EXECUTE IMMEDIATE :extracted_ddl;
```

### 5. Error Handling with Retry
```sql
FOR i IN 1 TO 3 DO
    BEGIN
        EXECUTE IMMEDIATE :ddl_sql;
        BREAK;
    EXCEPTION WHEN OTHER THEN
        LET error_msg := SQLERRM;
        -- Feed error back to LLM for correction
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
            'Fix this DDL. Error: ' || :error_msg || ' DDL: ' || :ddl_sql
        ) INTO :llm_response;
    END;
END FOR;
```

## LLM Usage

| Model | Use Case | Why |
|-------|----------|-----|
| `claude-3-5-sonnet` | Silver DT DDL generation (Planner, Executor) | High-quality SQL generation with complex typing and dedup logic |
| `claude-3-5-sonnet` | Gold DT DDL generation (Agentic Build) | Accurate multi-table joins and aggregation patterns |
| `claude-3-5-sonnet` | Semantic View generation | Excellent at inferring business context for dimensions/facts/metrics |
| `claude-3-5-sonnet` | Workflow Reflection | Pattern analysis and learning extraction |

## Components

### PostgreSQL Instances
- **SOURCE PG** (`dbaontap_source`): Application database simulating OLTP workload
- **LANDING PG** (`dbaontap_landing`): CDC staging receiving logical replication
- **Publication**: `dbaontap_to_landing_pub` (owned by `snowflake_admin`)
- **Subscription**: `dbaontap_sub`

### Snowflake Schemas

| Schema | Purpose |
|--------|---------|
| `"public"` (lowercase) | Openflow landing tables (owned by `OPENFLOWRUNTIMEROLE_SPCS1_RUNTIME1`) |
| `BRONZE` | VARIANT Dynamic Tables — schema-on-read |
| `SILVER` | Typed, deduplicated Dynamic Tables — LLM-generated |
| `GOLD` | Aggregation Dynamic Tables + Semantic Views |
| `AGENTS` | Stored procedures with Cortex LLM integration |
| `METADATA` | Workflow logs, schema contracts, learnings, planner decisions |
| `KNOWLEDGE_GRAPH` | KG_NODE/KG_EDGE tables + lineage views |

### Streamlit App (DEMO_MANAGER)

11-tab Streamlit in Snowflake app for demo orchestration:

| Tab | Purpose |
|-----|---------|
| Generate Data | Insert test data into SOURCE PG via External Access Integration |
| Pipeline Status | Monitor row counts across all layers (public → Bronze → Silver → Gold) |
| Agentic Workflow | Run 4-phase Silver workflow, view execution logs |
| Gold Layer | Build Core (5 DDLs), Agentic Build (new tables), Drift Detection |
| Schema Contracts | View/manage SILVER_SCHEMA_CONTRACTS |
| Semantic Views | Generate *_SV semantic views via KG + LLM |
| AI Chat | Natural language queries via Cortex Analyst + Semantic Views |
| Knowledge Graph | Visual lineage diagram (Bronze → Silver → Gold) |
| Logs & Errors | WORKFLOW_LOG and TRANSFORMATION_LOG viewer |
| Demo Control | Pipeline management and status |
| Reset Data | Drop all layers for clean demo restart (preserves contracts) |

Requirements:
- `EXTERNAL_ACCESS_INTEGRATIONS = (DBAONTAP_PG_EAI)` for PostgreSQL connectivity
- `SECRETS` mapping for `pg_source_creds` and `pg_landing_creds`
- `environment.yml` with `streamlit=1.51.0`, `snowflake-snowpark-python`, `psycopg2`

### Key Procedures

**AGENTS schema:**

| Procedure | Purpose |
|-----------|---------|
| `RUN_AGENTIC_WORKFLOW(trigger, tables)` | Orchestrate 4-phase Silver build |
| `WORKFLOW_PLANNER(execution_id)` | Phase 1: Analyze Bronze, decide strategy |
| `WORKFLOW_EXECUTOR(execution_id)` | Phase 2: Generate Silver DT DDL via LLM |
| `WORKFLOW_VALIDATOR(execution_id)` | Phase 3: Execute DDL, validate results |
| `WORKFLOW_REFLECTOR(execution_id)` | Phase 4: Store learnings |
| `DISCOVER_AND_ONBOARD_NEW_TABLES()` | Auto-detect new Bronze tables for Silver |
| `BUILD_GOLD_FOR_NEW_TABLES(dry_run, refresh_svs)` | Agentic Gold build for uncovered Silver tables |
| `PROPAGATE_TO_GOLD(table, dry_run)` | Schema drift detection and remediation |
| `DETECT_GOLD_SCHEMA_DRIFT(table)` | Compare Silver vs Gold columns |
| `RUN_SEMANTIC_VIEW_PIPELINE()` | Auto-generate all semantic views |
| `GENERATE_SEMANTIC_VIEW(table, schema)` | Single semantic view generation |

**KNOWLEDGE_GRAPH schema:**

| Procedure | Purpose |
|-----------|---------|
| `POPULATE_KG_FROM_INFORMATION_SCHEMA()` | Build KG nodes/edges from live metadata |
| `GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(force)` | Generate SVs for all Gold tables via KG |
| `GENERATE_HYBRID_SEMANTIC_VIEW(table)` | Single SV generation with KG context |

### Metadata Tables

| Table | Purpose |
|-------|---------|
| `WORKFLOW_EXECUTIONS` | Per-execution record with planner/executor output |
| `WORKFLOW_LOG` | Detailed step-by-step execution log |
| `WORKFLOW_LEARNINGS` | Accumulated learnings for future runs |
| `PLANNER_DECISIONS` | Planner strategy choices per table |
| `TRANSFORMATION_LOG` | Semantic view and Gold generation log |
| `SILVER_SCHEMA_CONTRACTS` | Column rules and naming standards |
| `AGENT_REFLECTIONS` | Reflector phase outputs |
| `VALIDATION_RESULTS` | Validator phase data quality checks |
| `ONBOARDED_TABLES` | Tables discovered and onboarded |
| `BRONZE_SCHEMA_REGISTRY` | Bronze table schemas for change detection |

## Verification Queries

```sql
-- Pipeline health check (all 4 layers)
SELECT 'LANDED' AS STAGE, COUNT(*) AS CNT FROM DBAONTAP_ANALYTICS."public".customers
UNION ALL SELECT 'BRONZE', COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT
UNION ALL SELECT 'SILVER', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS
UNION ALL SELECT 'GOLD',   COUNT(*) FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360;

-- All Gold tables
SELECT TABLE_NAME, ROW_COUNT
FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE '%_SV'
ORDER BY TABLE_NAME;

-- Semantic view status
SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;

-- Schema contracts
SELECT SOURCE_TABLE_PATTERN, NAMING_RULES
FROM DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS;

-- Workflow execution history
SELECT EXECUTION_ID, STATUS, STARTED_AT, COMPLETED_AT
FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
ORDER BY STARTED_AT DESC LIMIT 10;

-- Knowledge graph coverage
SELECT SOURCE_NODE, EDGE_TYPE, TARGET_NODE
FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
ORDER BY EDGE_TYPE, SOURCE_NODE;
```

## Infrastructure

| Component | Value |
|-----------|-------|
| Database | `DBAONTAP_ANALYTICS` |
| Warehouse | `DBRYANT_COCO_WH_S` |
| Role | `ACCOUNTADMIN` |
| SOURCE PG Host | `source-pg-host.example.snowflake.app` |
| LANDING PG Host | `landing-pg-host.example.snowflake.app` |
| EAI | `DBAONTAP_PG_EAI` (PostgreSQL), `DBAONTAP_CDC_EAI`, `CORTEX_API_INTEGRATION` |
| Secrets | `METADATA.PG_SOURCE_SECRET`, `METADATA.PG_LANDING_SECRET` |
| Streamlit Stage | `@DBAONTAP_ANALYTICS.METADATA.STREAMLIT_STAGE/default-content/` |
