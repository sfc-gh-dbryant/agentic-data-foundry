# Agentic Data Foundry

End-to-end CDC pipeline from PostgreSQL to Snowflake Intelligence with AI-powered transformations, a Knowledge Graph, and agentic Gold layer construction.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────────────────────────────────────┐
│  SOURCE PG      │    │  LANDING PG     │    │                  SNOWFLAKE                       │
│  (Application)  │───▶│  (Staging)      │───▶│                                                  │
│                 │    │                 │    │  ┌─────────┐  ┌─────────┐  ┌──────────────────┐  │
│  customers      │    │  customers      │    │  │ BRONZE  │  │ SILVER  │  │ GOLD (Agentic)   │  │
│  orders         │    │  orders         │    │  │ VARIANT │─▶│ CDC-    │─▶│ Aggregations,    │  │
│  products       │    │  products       │    │  │ DTs     │  │ Aware   │  │ ML Features      │  │
│  order_items    │    │  order_items    │    │  └─────────┘  └─────────┘  └────────┬─────────┘  │
│  support_tickets│    │  support_tickets│    │                                     │            │
└────────┬────────┘    └────────┬────────┘    │  ┌──────────────┐  ┌───────────────▼─────────┐  │
         │                      │             │  │ KNOWLEDGE    │  │ SEMANTIC VIEWS (AI-Gen) │  │
         │ Logical              │ Openflow    │  │ GRAPH        │  │ + VQRs                  │  │
         │ Replication          │ CDC         │  │ (Nodes/Edges)│  └───────────────┬─────────┘  │
         │ (one-time)           │ (continuous)│  └──────────────┘                  │            │
         ▼                      ▼             │  ┌──────────────┐  ┌──────────────▼──────────┐  │
    PostgreSQL             Snowflake          │  │ TABLE_       │  │ SNOWFLAKE INTELLIGENCE  │  │
    Publication            "public" schema    │  │ LINEAGE_MAP  │  │ (Cortex Analyst)        │  │
                                              │  └──────────────┘  └─────────────────────────┘  │
                                              └──────────────────────────────────────────────────┘
```

## Components

| Layer | Schema | Technology | Purpose |
|-------|--------|------------|---------|
| Source | — | Snowflake Managed PostgreSQL | Application database (OLTP) |
| Landing | — | Snowflake Managed PostgreSQL | CDC staging with logical replication |
| Openflow | — | Snowflake Openflow CDC | Continuous replication PG → Snowflake |
| Bronze | `BRONZE` | Dynamic Tables (VARIANT) | Schema-on-read, raw payload preservation |
| Silver | `SILVER` | Dynamic Tables (CDC-aware) | Deduplication, soft-delete handling |
| Gold | `GOLD` | Dynamic Tables (Aggregations) | Business metrics, ML features |
| Agents | `AGENTS` | Stored Procedures + Cortex LLM | AI-powered transformations & agentic Gold build |
| Metadata | `METADATA` | Tables & Views | Lineage map, directives, schema contracts, logs |
| Knowledge Graph | `KNOWLEDGE_GRAPH` | Node/Edge tables + SPs | Schema-aware graph of all database objects |
| Semantic Views | `GOLD` | Auto-generated via LLM | Natural language query interface |
| Intelligence | — | Snowflake Intelligence | Business user chat interface (Cortex Analyst) |

## Data Tables

| Schema | Table | Type | Description |
|--------|-------|------|-------------|
| BRONZE | CUSTOMERS_VARIANT | Dynamic Table | Raw CDC payload from PG customers |
| BRONZE | ORDERS_VARIANT | Dynamic Table | Raw CDC payload from PG orders |
| BRONZE | ORDER_ITEMS_VARIANT | Dynamic Table | Raw CDC payload from PG order_items |
| BRONZE | PRODUCTS_VARIANT | Dynamic Table | Raw CDC payload from PG products |
| BRONZE | SUPPORT_TICKETS_VARIANT | Dynamic Table | Raw CDC payload from PG support_tickets |
| SILVER | CUSTOMERS | Dynamic Table | CDC-aware, deduplicated customers |
| SILVER | ORDERS | Dynamic Table | CDC-aware orders |
| SILVER | ORDER_ITEMS | Dynamic Table | CDC-aware order items |
| SILVER | PRODUCTS_VARIANT | Dynamic Table | CDC-aware products |
| SILVER | SUPPORT_TICKETS | Dynamic Table | CDC-aware support tickets |
| GOLD | CUSTOMER_360 | Dynamic Table | Unified customer view with order history |
| GOLD | ML_CUSTOMER_FEATURES | Dynamic Table | Feature engineering for ML models |
| GOLD | ORDER_SUMMARY | Dynamic Table | Order aggregations by customer/time |
| GOLD | PRODUCT_PERFORMANCE_METRICS | Dynamic Table | Product sales performance metrics |
| GOLD | SUPPORT_METRICS | Dynamic Table | Support ticket SLA/resolution metrics (agentic) |

## Metadata Tables

| Table | Purpose |
|-------|---------|
| `METADATA.TABLE_LINEAGE_MAP` | Single source of truth for Bronze→Silver→Gold table name mappings (16 rows) |
| `METADATA.TRANSFORMATION_DIRECTIVES` | Business intent/instructions for LLM agents ("Human in the Middle") |
| `METADATA.SCHEMA_CONTRACTS` | Structural guardrails for CDC column naming conventions |
| `METADATA.TRANSFORMATION_LOG` | Agentic workflow execution history and LLM reasoning |
| `KNOWLEDGE_GRAPH.KG_NODE` | Graph nodes for all database objects (tables, columns, schemas) |
| `KNOWLEDGE_GRAPH.KG_EDGE` | Graph edges: CONTAINS, HAS_COLUMN, TRANSFORMS_TO, AGGREGATES_TO |

## Key Procedures

| Procedure | Purpose |
|-----------|---------|
| `AGENTS.RUN_AGENTIC_WORKFLOW()` | 5-phase pipeline: Trigger → Planner → Executor → Validator → Reflector |
| `AGENTS.BUILD_GOLD_FOR_NEW_TABLES(dry_run, refresh_svs)` | Agentic Gold layer build — discovers missing Gold targets from TABLE_LINEAGE_MAP + uncovered Silver tables |
| `AGENTS.GOLD_AGENTIC_EXECUTOR(ddl, table)` | Executes LLM-generated Gold DDL with validation, retry, auto lineage registration, and KG refresh |
| `AGENTS.VALIDATE_GOLD_DDL(ddl)` | Validates FROM/JOIN references against INFORMATION_SCHEMA before execution |
| `AGENTS.REGISTER_LINEAGE_FROM_DDL(ddl, gold_table)` | Auto-parses DDL to register Silver→Gold mappings in TABLE_LINEAGE_MAP |
| `KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()` | Populates KG nodes/edges from INFORMATION_SCHEMA + TABLE_LINEAGE_MAP |
| `KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(overwrite)` | LLM-generated semantic views for all Gold tables |
| `KNOWLEDGE_GRAPH.GENERATE_VQRS_FOR_ALL_SEMANTIC_VIEWS()` | Generate Verified Query Representations for semantic views |

## Streamlit App

The **Demo Manager** (`DBAONTAP_ANALYTICS.METADATA.DEMO_MANAGER`) is a Streamlit in Snowflake app with 13 tabs:

| Tab | Purpose |
|-----|---------|
| Architecture | Pipeline progress visualization (Graphviz) |
| Generate Data | Insert sample records into PostgreSQL source |
| Pipeline Status | Dynamic Table refresh status and health |
| Agentic Workflow | Run 5-phase agentic transformation pipeline |
| Gold Layer | Core Gold build, Agentic Gold build, Schema Drift Detection |
| Schema Contracts | Manage CDC column naming conventions |
| Directives | CRUD for transformation directives ("Human in the Middle") |
| Semantic Views | Generate/manage semantic views + VQRs |
| AI Chat | Cortex Analyst natural language queries |
| Knowledge Graph | KG population, data lineage visualization |
| Logs & Errors | Transformation log viewer |
| Demo Control | Bulk operations (resume/suspend/reset DTs) |
| Reset Data | Full or partial data reset |

## Prerequisites

- Snowflake account with:
  - ACCOUNTADMIN role access
  - Snowflake Managed PostgreSQL enabled
  - Openflow enabled
  - Cortex LLM access (claude-3-5-sonnet, llama3.1-8b)
- `snow` CLI installed and configured
- Connection named `CoCo-Green` (or update scripts accordingly)

## Quick Start

```bash
# 1. Set your connection
export SF_CONNECTION="CoCo-Green"

# 2. Deploy the numbered scripts in order (core infrastructure)
for script in scripts/[0-9][0-9]_*.sql; do
  snow sql -c $SF_CONNECTION -f "$script"
done

# 3. Deploy the Streamlit app
snow sql -c $SF_CONNECTION -f app/deploy_sis_complete.sql

# 4. Populate the Knowledge Graph
snow sql -c $SF_CONNECTION -q "CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA();"

# 5. Run the agentic Gold build (dry run)
snow sql -c $SF_CONNECTION -q "CALL DBAONTAP_ANALYTICS.AGENTS.BUILD_GOLD_FOR_NEW_TABLES(TRUE, FALSE);"
```

## Directory Structure

```
agentic-data-foundry/
├── README.md
├── AGENTIC_TRANSFORMATION_OBJECTS.md   # Object model specification
├── AI_FIRST_METHODOLOGY.md             # AI-first design methodology
├── app/
│   ├── streamlit_app.py                # Main Streamlit app (13 tabs)
│   ├── environment.yml                 # SiS dependencies (pinned streamlit 1.51.0)
│   ├── deploy_sis_complete.sql         # Full SiS deploy with EAI + secrets
│   ├── deploy_streamlit.sql            # Basic deploy script
│   └── deploy_with_eai.sql             # Deploy with external access
├── docs/
│   ├── architecture.md                 # Detailed architecture docs
│   ├── DEMO_RUNBOOK.md                 # Step-by-step demo guide
│   ├── DEMO_TALK_TRACK.md             # Presenter talk track
│   └── USE_CASE_CUSTOMER_INTELLIGENCE.md
├── patent/
│   └── Agentic_Data_Foundry_IDF.docx   # Invention Disclosure Form
├── scripts/
│   ├── 01_setup_schema.sql             # Database, schemas, warehouse
│   ├── 02_discovery_tools.sql          # Bronze schema analysis UDFs
│   ├── 03_transformation_agent.sql     # Core transformation agent SP
│   ├── 04_sample_data.sql              # Sample CDC data for Bronze
│   ├── 05_agentic_workflow.sql         # End-to-end workflow demo
│   ├── 06_workflow_engine.sql          # Stream-based triggers and tasks
│   ├── 07_agentic_workflow_engine.sql  # 5-phase workflow engine v2
│   ├── 08_decision_point_2.sql         # Silver existence + schema change detection
│   ├── 08b_workflow_trigger.sql        # Workflow trigger refinements
│   ├── 08c_schema_detection_tuning.sql # Schema change detection tuning
│   ├── 08d_detect_schema_changes_v2.sql
│   ├── 08e_detect_schema_changes_with_kg.sql
│   ├── 09_knowledge_graph.sql          # KG population SP (reads TABLE_LINEAGE_MAP)
│   ├── 10_kg_semantic_view_generator.sql  # KG-based semantic view generator
│   ├── 10_postgres_source.sql          # PostgreSQL source instance setup
│   ├── 11_hybrid_semantic_view_generator.sql  # Hybrid SV generator
│   ├── 11_postgres_silver_gold.sql     # PG-sourced Silver/Gold DTs
│   ├── 12_ai_consumption.sql           # AI consumption layer (Cortex Agent)
│   ├── 12_fixed_hybrid_semantic_view.sql  # Fixed SV generator (new SQL syntax)
│   ├── 13_transformation_directives.sql   # Directives table + seed data
│   ├── 14_ddl_validation.sql           # VALIDATE_GOLD_DDL SP
│   ├── 15_table_lineage_map.sql        # TABLE_LINEAGE_MAP DDL + seed (16 rows)
│   ├── 16_register_lineage.sql         # REGISTER_LINEAGE_FROM_DDL SP
│   ├── 17_gold_agentic_executor.sql    # GOLD_AGENTIC_EXECUTOR with auto-registration
│   ├── 18_build_gold_for_new_tables.sql # Two-strategy agentic Gold discovery
│   ├── 99_cleanup.sql                  # Full teardown
│   ├── 99_reset_for_testing.sql        # Reset Gold/SVs for re-demo
│   └── 99b_reset_pg_instances.sql      # Reset PostgreSQL instances
└── sql/
    └── create_pipeline_with_retry.sql  # Pipeline creation with retry logic
```

## LLMs Used

| Model | Use Case |
|-------|----------|
| claude-3-5-sonnet | Gold DDL generation, semantic view creation, VQR generation |
| llama3.1-8b | Schema inference, workflow reflection, semantic context |

## Key Design Decisions

### TABLE_LINEAGE_MAP as Single Source of Truth
All table name mappings (Bronze→Silver, Silver→Gold) are stored in `METADATA.TABLE_LINEAGE_MAP`. This eliminates hardcoded table names across the KG SP, KG visualization, agentic Gold builder, and DDL validation. When adding or renaming a table, update TABLE_LINEAGE_MAP and all consumers follow automatically.

### Three-Layer Control Model
1. **Schema Contracts** — structural guardrails (column names, types)
2. **Transformation Directives** — business intent/instructions for LLM agents
3. **Learnings** — LLM reflection memory from past executions

### Agentic Gold Build (Two-Strategy Discovery)
1. **Missing Gold targets** — Gold tables defined in TABLE_LINEAGE_MAP but not yet created
2. **Uncovered Silver** — Silver tables with no lineage mapping at all

Both strategies use Cortex LLM to generate DDL, with DDL validation, retry logic, auto lineage registration, and KG refresh.

## Verification

```sql
-- Check pipeline health
SELECT 
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS."public".customers) as landed_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT) as bronze_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS) as silver_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360) as gold_rows;

-- Check Knowledge Graph
SELECT edge_type, COUNT(*) FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE GROUP BY 1;

-- Check TABLE_LINEAGE_MAP
SELECT * FROM DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP ORDER BY EDGE_TYPE, SOURCE_TABLE;

-- Check semantic views
SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;

-- Check agent logs
SELECT status, COUNT(*) 
FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
GROUP BY status;

-- Dry run agentic Gold build
CALL DBAONTAP_ANALYTICS.AGENTS.BUILD_GOLD_FOR_NEW_TABLES(TRUE, FALSE);
```

## Troubleshooting

### Agentic Gold Build Returns ALL_COVERED
All Gold targets from TABLE_LINEAGE_MAP already exist. To test agentic build, drop a Gold table:
```sql
DROP TABLE DBAONTAP_ANALYTICS.GOLD.SUPPORT_METRICS;
CALL DBAONTAP_ANALYTICS.AGENTS.BUILD_GOLD_FOR_NEW_TABLES(TRUE, FALSE);
```

### Dynamic Table Status Shows Dropped Tables
`DYNAMIC_TABLE_REFRESH_HISTORY` retains records for dropped DTs. The Streamlit app filters these via INNER JOIN to INFORMATION_SCHEMA.TABLES.

### Knowledge Graph Missing Lineage Edges
Lineage edges require both source AND target nodes to exist in KG_NODE. Re-populate the KG after creating new tables:
```sql
CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA();
```

### Semantic View Generation Failures
```sql
SELECT target_table, transformation_sql, agent_reasoning 
FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
WHERE status = 'FAILED';
```

## License

Internal Snowflake Demo - Not for distribution
