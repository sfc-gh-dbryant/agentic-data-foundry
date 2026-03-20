# Agentic Silver Layer Demo

End-to-end CDC pipeline from PostgreSQL to Snowflake Intelligence with AI-powered transformations.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────────────────┐
│  SOURCE PG      │    │  LANDING PG     │    │           SNOWFLAKE                     │
│  (Application)  │───▶│  (Staging)      │───▶│                                         │
│                 │    │                 │    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
│  customers      │    │  customers      │    │  │ BRONZE  │  │ SILVER  │  │  GOLD   │  │
│  orders         │    │  orders         │    │  │ VARIANT │─▶│ CDC-    │─▶│ Agg     │  │
│  products       │    │  products       │    │  │ DTs     │  │ Aware   │  │ DTs     │  │
│  order_items    │    │  order_items    │    │  └─────────┘  └─────────┘  └────┬────┘  │
│  support_tickets│    │  support_tickets│    │                                 │       │
└────────┬────────┘    └────────┬────────┘    │  ┌─────────────────────────────▼────┐   │
         │                      │             │  │     SEMANTIC VIEWS (AI-Gen)      │   │
         │ Logical              │ Openflow    │  │     via AGENTS.RUN_PIPELINE()    │   │
         │ Replication          │ CDC         │  └─────────────────────────────┬────┘   │
         │ (one-time)           │ (continuous)│                                │        │
         ▼                      ▼             │  ┌─────────────────────────────▼────┐   │
    PostgreSQL             Snowflake          │  │     SNOWFLAKE INTELLIGENCE       │   │
    Publication            "public" schema    │  │     (Cortex Analyst)             │   │
                                              │  └──────────────────────────────────┘   │
                                              └─────────────────────────────────────────┘
```

## Components

| Layer | Technology | Purpose |
|-------|------------|---------|
| SOURCE | Snowflake Managed PostgreSQL | Application database (OLTP) |
| LANDING | Snowflake Managed PostgreSQL | CDC staging with logical replication |
| OPENFLOW | Snowflake Openflow CDC | Continuous replication to Snowflake |
| BRONZE | Dynamic Tables (VARIANT) | Schema-on-read, raw payload preservation |
| SILVER | Dynamic Tables (CDC-aware) | Deduplication, soft-delete handling |
| GOLD | Dynamic Tables (Aggregations) | Business metrics, ML features |
| AGENTS | Stored Procedures + Cortex LLM | AI-powered transformations |
| SEMANTIC VIEWS | Auto-generated via LLM | Natural language query interface |
| INTELLIGENCE | Snowflake Intelligence | Business user chat interface |

## Prerequisites

- Snowflake account with:
  - ACCOUNTADMIN role access
  - Snowflake Managed PostgreSQL enabled
  - Openflow enabled
  - Cortex LLM access (claude-3-5-sonnet, llama3.1-8b)
- `snow` CLI installed and configured
- Connection named `CoCo-Green` (or update scripts)

## Quick Start

```bash
# 1. Set your connection
export SF_CONNECTION="CoCo-Green"

# 2. Run all scripts in order
for script in scripts/*/setup.sql; do
  snow sql -c $SF_CONNECTION -f "$script"
done

# 3. Run the agentic semantic view pipeline
snow sql -c $SF_CONNECTION -q "CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE();"
```

## Directory Structure

```
agentic-silver-layer/
├── README.md                    # This file
├── docs/
│   └── architecture.md          # Detailed architecture docs
├── scripts/
│   ├── 01_source/              # PostgreSQL SOURCE instance
│   │   ├── setup.sql
│   │   └── seed_data.sql
│   ├── 02_landing/             # PostgreSQL LANDING instance
│   │   └── setup.sql
│   ├── 03_replication/         # Logical replication setup
│   │   └── setup.sql
│   ├── 04_openflow/            # Openflow CDC configuration
│   │   └── setup.sql
│   ├── 05_bronze/              # VARIANT Dynamic Tables
│   │   └── setup.sql
│   ├── 06_silver/              # CDC-aware Dynamic Tables
│   │   └── setup.sql
│   ├── 07_gold/                # Aggregation Dynamic Tables
│   │   └── setup.sql
│   ├── 08_agents/              # Agentic procedures
│   │   └── setup.sql
│   ├── 09_semantic_views/      # Semantic view pipeline
│   │   └── setup.sql
│   └── 10_intelligence/        # Snowflake Intelligence setup
│       └── setup.sql
```

## LLMs Used

| Model | Use Case |
|-------|----------|
| claude-3-5-sonnet | Semantic view DDL generation, semantic context inference |
| llama3.1-8b | Schema inference, workflow reflection |

## Key Procedures

| Procedure | Purpose |
|-----------|---------|
| `AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()` | Auto-discovers Gold tables, generates semantic views |
| `AGENTS.ANALYZE_GOLD_SCHEMA(table)` | Schema introspection |
| `AGENTS.INFER_SEMANTIC_CONTEXT(table)` | LLM-based semantic inference |
| `AGENTS.GENERATE_SEMANTIC_VIEW(table, name)` | Individual semantic view generation |

## Verification

```sql
-- Check pipeline health
SELECT 
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS."public".customers) as landed_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.customers_variant) as bronze_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS) as silver_rows,
  (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360) as gold_rows;

-- Check semantic views
SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;

-- Check agent logs
SELECT status, COUNT(*) 
FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
GROUP BY status;
```

## Troubleshooting

### Semantic View Generation Failures
Check the transformation log for failed DDL:
```sql
SELECT target_table, transformation_sql, agent_reasoning 
FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
WHERE status = 'FAILED';
```

### Dynamic Table Lag
Check refresh status:
```sql
SELECT name, refresh_mode, target_lag, data_timestamp 
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name LIKE '%CUSTOMER%';
```

## License

Internal Snowflake Demo - Not for distribution
