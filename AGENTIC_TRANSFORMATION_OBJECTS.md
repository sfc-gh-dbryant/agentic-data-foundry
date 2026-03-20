# Agentic Transformation Process: Complete Object Reference

This document provides a comprehensive breakdown of all database objects associated with the Agentic Transformation Layer, organized by the 5-step workflow architecture.

## Workflow Overview

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   TRIGGER    │───▶│   PLANNER    │───▶│   EXECUTOR   │───▶│  VALIDATOR   │───▶│  REFLECTOR   │
│              │    │              │    │              │    │              │    │              │
│ • Detect     │    │ • Analyze    │    │ • Generate   │    │ • Test       │    │ • Learn      │
│ • Discover   │    │ • Strategize │    │ • Execute    │    │ • Compare    │    │ • Optimize   │
│ • Onboard    │    │ • Plan       │    │ • Retry      │    │ • Alert      │    │ • Persist    │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
        │                  │                                                            │
        │                  │                                                            │
        ▼                  ▼                                                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    KNOWLEDGE GRAPH                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ Similar Tables  │  │ Lineage Impact  │  │ Table Metadata  │  │ Pattern Storage │             │
│  │ (Semantic)      │  │ (Downstream)    │  │ (Descriptions)  │  │ (Learnings)     │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Openflow CDC Table Conversion

This section describes how tables land via Openflow CDC and are converted through the medallion architecture.

### End-to-End Data Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    
│  SOURCE PG      │    │  LANDING PG     │    │    OPENFLOW     │    
│  (Application)  │───▶│  (CDC Staging)  │───▶│   CDC Connector │───┐
│                 │    │                 │    │                 │   │
│  customers      │    │  Logical        │    │  • pgoutput     │   │
│  orders         │    │  Replication    │    │  • CDC columns  │   │
│  products       │    │  Publication    │    │  • Continuous   │   │
│  order_items    │    │                 │    │    streaming    │   │
│  support_tickets│    │                 │    │                 │   │
└─────────────────┘    └─────────────────┘    └─────────────────┘   │
                                                                    │
    ┌───────────────────────────────────────────────────────────────┘
    │
    │   ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
    │   │                                    SNOWFLAKE                                                 │
    │   │                                                                                              │
    │   │  ┌─────────────────────────────────────────────────────────────────────────────────────────┐ │
    │   │  │                           DECISION POINT 1: NEW TABLE DETECTION                        │ │
    │   │  │                           (DISCOVER_AND_ONBOARD_NEW_TABLES)                            │ │
    │   │  │                                                                                         │ │
    ▼   │  │    ┌─────────────────┐         Does Bronze VARIANT          ┌─────────────────┐        │ │
────────│──│───▶│  "public"       │────────────table exist?─────────────▶│  Bronze VARIANT │        │ │
        │  │    │  Landing Tables │                │                     │  EXISTS         │        │ │
        │  │    │  (from Openflow)│                │ NO                  │  (skip onboard) │        │ │
        │  │    └─────────────────┘                ▼                     └────────┬────────┘        │ │
        │  │                            ┌─────────────────────┐                   │                 │ │
        │  │                            │  AGENTIC ONBOARDING │                   │                 │ │
        │  │                            │                     │                   │                 │ │
        │  │                            │  1. Create Stream   │                   │                 │ │
        │  │                            │  2. Create Bronze   │                   │                 │ │
        │  │                            │     VARIANT DT      │                   │                 │ │
        │  │                            │  3. Log to METADATA │                   │                 │ │
        │  │                            └──────────┬──────────┘                   │                 │ │
        │  │                                       │                              │                 │ │
        │  │                                       ▼                              │                 │ │
        │  │                            ┌─────────────────────┐                   │                 │ │
        │  │                            │  NEW Bronze VARIANT │◀──────────────────┘                 │ │
        │  │                            │  Dynamic Table      │                                     │ │
        │  │                            └──────────┬──────────┘                                     │ │
        │  └───────────────────────────────────────┼─────────────────────────────────────────────────┘ │
        │                                          │                                                   │
        │  ┌───────────────────────────────────────┼─────────────────────────────────────────────────┐ │
        │  │                           DECISION POINT 2: SILVER TRANSFORMATION                      │ │
        │  │                           (WORKFLOW_TRIGGER stream detection)                          │ │
        │  │                                       │                                                 │ │
        │  │                                       ▼                                                 │ │
        │  │                        ┌─────────────────────────────┐                                  │ │
        │  │                        │   Does Silver DT exist?     │                                  │ │
        │  │                        │   Schema changed?           │                                  │ │
        │  │                        └──────────────┬──────────────┘                                  │ │
        │  │                                       │                                                 │ │
        │  │            ┌──────────────────────────┼──────────────────────────┐                      │ │
        │  │            │                          │                          │                      │ │
        │  │            ▼                          ▼                          ▼                      │ │
        │  │   ┌────────────────┐       ┌────────────────┐       ┌────────────────┐                  │ │
        │  │   │ SILVER EXISTS  │       │ NEW TABLE OR   │       │ SCHEMA CHANGE  │                  │ │
        │  │   │ No Changes     │       │ NO SILVER DT   │       │ DETECTED       │                  │ │
        │  │   └───────┬────────┘       └───────┬────────┘       └───────┬────────┘                  │ │
        │  │           │                        │                        │                           │ │
        │  │           ▼                        └────────────┬───────────┘                           │ │
        │  │  ┌────────────────────┐                         │                                       │ │
        │  │  │  AUTO-REFRESH      │                         ▼                                       │ │
        │  │  │  via TARGET_LAG    │            ┌────────────────────────────────┐                   │ │
        │  │  │  (Standard DT)     │            │     AGENTIC WORKFLOW           │                   │ │
        │  │  └─────────┬──────────┘            │                                │                   │ │
        │  │            │                       │  TRIGGER → PLANNER → EXECUTOR  │                   │ │
        │  │            │                       │      → VALIDATOR → REFLECTOR   │                   │ │
        │  │            │                       │                                │                   │ │
        │  │            │                       │  (LLM generates Silver DDL)    │                   │ │
        │  │            │                       └───────────────┬────────────────┘                   │ │
        │  │            │                                       │                                    │ │
        │  │            └──────────────────┬────────────────────┘                                    │ │
        │  │                               │                                                         │ │
        │  └───────────────────────────────┼─────────────────────────────────────────────────────────┘ │
        │                                  ▼                                                           │
        │                       ┌─────────────────────┐                                                │
        │                       │      SILVER         │                                                │
        │                       │   CDC-Aware DTs     │                                                │
        │                       └──────────┬──────────┘                                                │
        │                                  │                                                           │
        │                                  ▼                                                           │
        │                       ┌─────────────────────┐                                                │
        │                       │       GOLD          │                                                │
        │                       │  Aggregation DTs    │                                                │
        │                       └─────────────────────┘                                                │
        └──────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Two-Phase Decision Model

The agentic workflow operates at **two distinct decision points**:

| Decision Point | Location | Detection Method | Action |
|----------------|----------|------------------|--------|
| **Decision Point 1** | BEFORE Bronze | `DISCOVER_AND_ONBOARD_NEW_TABLES()` compares `public.*` tables to `BRONZE.*_VARIANT` tables | Creates Stream + Bronze VARIANT DT for new tables |
| **Decision Point 2** | AFTER Bronze | Stream detection + schema comparison | Triggers full agentic workflow for Silver transformation |

### Decision Point 1: New Table Onboarding (Before Bronze)

When Openflow lands a new table in the `public` schema, the system detects it has no corresponding Bronze table:

```sql
-- Discovery logic in DISCOVER_AND_ONBOARD_NEW_TABLES()
SELECT ARRAY_AGG(landing.TABLE_CATALOG || '.' || landing.TABLE_SCHEMA || '.' || landing.TABLE_NAME)
FROM INFORMATION_SCHEMA.TABLES landing
WHERE landing.TABLE_SCHEMA = 'public'
  AND landing.TABLE_TYPE = 'BASE TABLE'
  AND landing.TABLE_NAME NOT LIKE '%JOURNAL%'  -- Exclude Openflow journals
  AND NOT EXISTS (
      SELECT 1 FROM INFORMATION_SCHEMA.TABLES bronze
      WHERE bronze.TABLE_SCHEMA = 'BRONZE'
        AND bronze.TABLE_NAME = UPPER(landing.TABLE_NAME) || '_VARIANT'
  );
```

**Onboarding Actions (AUTO_ONBOARD_TABLE procedure):**
1. **Create Stream**: `CREATE STREAM AGENTS.<TABLE>_LANDING_STREAM ON TABLE public.<table>`
2. **Create Bronze DT**: `CREATE DYNAMIC TABLE BRONZE.<TABLE>_VARIANT AS SELECT OBJECT_CONSTRUCT(*) as payload...`
3. **Log to Metadata**: Insert record into `METADATA.ONBOARDED_TABLES`

### Decision Point 2: Silver Transformation (After Bronze) ✅ IMPLEMENTED

Once Bronze tables exist, the system monitors for changes requiring Silver transformation:

| Scenario | Detection | Path |
|----------|-----------|------|
| **Data arrived, Silver exists** | `SYSTEM$STREAM_HAS_DATA()` = TRUE, Silver DT exists | Standard DT refresh (auto via `TARGET_LAG`) |
| **New Bronze table, no Silver** | Bronze exists, no matching Silver DT | Agentic Workflow → LLM generates Silver DDL |
| **Schema change** | Bronze payload keys ≠ Silver columns | Agentic Workflow → LLM regenerates Silver DDL |
| **Quality threshold breach** | Validation variance > 5% | Agentic Workflow → Self-correction |

#### Implementation: Schema Change Detection with Ignore Patterns

The `DETECT_SCHEMA_CHANGES` procedure compares Bronze VARIANT keys to Silver columns while ignoring known derived and system columns:

```sql
-- Check if schema changes require agentic workflow
CALL AGENTS.DETECT_SCHEMA_CHANGES('DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT');

-- Returns:
{
  "bronze_table": "DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT",
  "silver_table": "DBAONTAP_ANALYTICS.SILVER.CUSTOMERS",
  "silver_exists": true,
  "schema_changed": false,  -- No changes = skip agentic workflow
  "new_columns": [],
  "dropped_columns": [],
  "ignored_columns": {
    "new_ignored": ["phone", "_SNOWFLAKE_DELETED", "_SNOWFLAKE_UPDATED_AT", "_SNOWFLAKE_INSERTED_AT"],
    "dropped_ignored": ["INSERTED_AT", "UPDATED_AT_SYSTEM", "PHONE_STANDARDIZED", "IS_DELETED"]
  }
}
```

#### Configuration: Schema Ignore Patterns

The `METADATA.SCHEMA_IGNORE_COLUMNS` table configures which columns to exclude from schema diff:

| Column Pattern | Ignore Type | Description |
|----------------|-------------|-------------|
| `PHONE_STANDARDIZED` | SILVER_DERIVED | Derived from phone column |
| `UPDATED_AT_SYSTEM` | SYSTEM_COLUMN | System-generated update timestamp |
| `INSERTED_AT` | SYSTEM_COLUMN | System-generated insert timestamp |
| `IS_DELETED` | CDC_METADATA | CDC soft delete flag |
| `CDC_TIMESTAMP` | CDC_METADATA | CDC operation timestamp |
| `RN` | SYSTEM_COLUMN | Row number for deduplication |
| `INGESTED_AT` | SYSTEM_COLUMN | Ingestion timestamp |
| `SOURCE_TABLE` | SYSTEM_COLUMN | Source table reference |
| `PHONE` | SOURCE_FOR_DERIVED | Source column transformed to PHONE_STANDARDIZED |

Snowflake internal columns (`_SNOWFLAKE%`) are automatically excluded via pattern matching.

### Dual-Path Processing Model

After data lands in Bronze VARIANT tables, the system follows one of two paths:

| Scenario | Detection Method | Processing Path | Outcome |
|----------|-----------------|-----------------|---------|
| **Existing table, no schema change** | Stream has data, Silver DT exists | Direct Dynamic Table refresh | Silver DT auto-refreshes via `TARGET_LAG` |
| **New table landed** | `DISCOVER_AND_ONBOARD_NEW_TABLES()` finds unmatched table | Agentic Workflow (full 5-step) | New Silver DT created with LLM-generated transformation |
| **Schema change detected** | Column mismatch in Bronze vs Silver | Agentic Workflow (full 5-step) | Silver DT recreated with updated schema |
| **Data quality threshold breached** | Validation fails (>5% variance) | Agentic Workflow (triggered alert) | Self-correction via EXECUTOR retry |

### Agentic Workflow Trigger Points

The agentic workflow activates after Bronze in these scenarios:

```
                              BRONZE VARIANT TABLE
                                      │
                                      ▼
                        ┌─────────────────────────────┐
                        │   STREAM DETECTION          │
                        │   SYSTEM$STREAM_HAS_DATA()  │
                        └──────────────┬──────────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            │                          │                          │
            ▼                          ▼                          ▼
   ┌────────────────┐       ┌────────────────┐       ┌────────────────┐
   │ NEW TABLE      │       │ SCHEMA CHANGE  │       │ DATA ARRIVED   │
   │ DETECTED       │       │ DETECTED       │       │ (Known Table)  │
   │                │       │                │       │                │
   │ No matching    │       │ Bronze columns │       │ Silver DT      │
   │ Silver DT      │       │ ≠ Silver cols  │       │ exists         │
   └───────┬────────┘       └───────┬────────┘       └───────┬────────┘
           │                        │                        │
           │                        │                        │
           ▼                        ▼                        ▼
   ┌─────────────────────────────────────────┐    ┌────────────────────┐
   │         AGENTIC WORKFLOW                │    │  STANDARD DT       │
   │                                         │    │  REFRESH           │
   │  TRIGGER: Identify transformation need  │    │                    │
   │      ↓                                  │    │  TARGET_LAG='1 min'│
   │  PLANNER: LLM analyzes Bronze schema    │    │  Auto-refresh      │
   │      ↓                                  │    │                    │
   │  EXECUTOR: Generate & execute DDL       │    └─────────┬──────────┘
   │      ↓                                  │              │
   │  VALIDATOR: Compare row counts          │              │
   │      ↓                                  │              │
   │  REFLECTOR: Capture learnings           │              │
   └────────────────────┬────────────────────┘              │
                        │                                   │
                        └──────────────┬────────────────────┘
                                       │
                                       ▼
                              SILVER CDC-AWARE TABLE
```

---

## Leveraging Row Timestamps (METADATA$ROW_LAST_COMMIT_TIME)

Snowflake's [Row Timestamps](https://docs.snowflake.com/en/user-guide/data-engineering/row-timestamps) feature provides definitive commit timestamps that can significantly enhance the agentic workflow's reliability and observability.

### What Are Row Timestamps?

When enabled, tables expose `METADATA$ROW_LAST_COMMIT_TIME` - a system-managed column that records the exact timestamp when each row was committed. Unlike CDC timestamps from source systems, this is a **server-side, guaranteed chronological** timestamp.

```sql
-- Enable row timestamps on a table
ALTER TABLE BRONZE.CUSTOMERS_VARIANT SET ROW_TIMESTAMP = TRUE;

-- Query with row timestamps
SELECT METADATA$ROW_LAST_COMMIT_TIME as commit_time, *
FROM BRONZE.CUSTOMERS_VARIANT
ORDER BY commit_time DESC;
```

### Integration Points in the Agentic Workflow

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           ROW TIMESTAMP INTEGRATION POINTS                                          │
├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                     │
│   ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐                │
│   │  LANDING  │───▶│  BRONZE   │───▶│  SILVER   │───▶│   GOLD    │───▶│ SEMANTIC  │                │
│   │  public.* │    │  VARIANT  │    │  CDC-Aware│    │  Agg DTs  │    │  VIEWS    │                │
│   └─────┬─────┘    └─────┬─────┘    └─────┬─────┘    └─────┬─────┘    └───────────┘                │
│         │                │                │                │                                        │
│         ▼                ▼                ▼                ▼                                        │
│   ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐                                 │
│   │ ROW_TS=T  │    │ ROW_TS=T  │    │ ROW_TS=T  │    │ ROW_TS=T  │                                 │
│   │           │    │           │    │           │    │           │                                 │
│   │ Ingest    │    │ Bronze    │    │ Transform │    │ Aggregate │                                 │
│   │ Latency   │    │ Latency   │    │ Latency   │    │ Latency   │                                 │
│   └───────────┘    └───────────┘    └───────────┘    └───────────┘                                 │
│         │                │                │                │                                        │
│         └────────────────┴────────────────┴────────────────┘                                        │
│                                    │                                                                │
│                                    ▼                                                                │
│                          ┌─────────────────────┐                                                    │
│                          │  PIPELINE LATENCY   │                                                    │
│                          │  OBSERVABILITY      │                                                    │
│                          │  (End-to-End)       │                                                    │
│                          └─────────────────────┘                                                    │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Enhancement 1: Enable Row Timestamps on All Pipeline Tables

Add to schema setup to enable row timestamps by default:

```sql
-- Enable row timestamps for all new tables in pipeline schemas
ALTER SCHEMA DBAONTAP_ANALYTICS."public" SET ROW_TIMESTAMP_DEFAULT = TRUE;
ALTER SCHEMA DBAONTAP_ANALYTICS.BRONZE SET ROW_TIMESTAMP_DEFAULT = TRUE;
ALTER SCHEMA DBAONTAP_ANALYTICS.SILVER SET ROW_TIMESTAMP_DEFAULT = TRUE;
ALTER SCHEMA DBAONTAP_ANALYTICS.GOLD SET ROW_TIMESTAMP_DEFAULT = TRUE;

-- Bulk enable on existing tables
SELECT SYSTEM$SET_ROW_TIMESTAMP_ON_ALL_SUPPORTED_TABLES('schema', 'DBAONTAP_ANALYTICS.BRONZE');
SELECT SYSTEM$SET_ROW_TIMESTAMP_ON_ALL_SUPPORTED_TABLES('schema', 'DBAONTAP_ANALYTICS.SILVER');
SELECT SYSTEM$SET_ROW_TIMESTAMP_ON_ALL_SUPPORTED_TABLES('schema', 'DBAONTAP_ANALYTICS.GOLD');
```

### Enhancement 2: Improved CDC Deduplication in Silver

Replace reliance on `_SNOWFLAKE_UPDATED_AT` (source CDC timestamp) with `METADATA$ROW_LAST_COMMIT_TIME` (Snowflake commit timestamp) for more reliable ordering:

```sql
-- CURRENT: Using source CDC timestamp (can have clock skew issues)
CREATE OR REPLACE DYNAMIC TABLE SILVER.CUSTOMERS
AS
WITH ranked_changes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY payload:customer_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC  -- Source timestamp
        ) as rn
    FROM BRONZE.CUSTOMERS_VARIANT
)
SELECT * FROM ranked_changes WHERE rn = 1;

-- ENHANCED: Using Snowflake row timestamp (guaranteed ordering)
CREATE OR REPLACE DYNAMIC TABLE SILVER.CUSTOMERS
AS
WITH ranked_changes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY payload:customer_id 
            ORDER BY METADATA$ROW_LAST_COMMIT_TIME DESC  -- Snowflake commit time
        ) as rn
    FROM BRONZE.CUSTOMERS_VARIANT
)
SELECT * FROM ranked_changes WHERE rn = 1;
```

**Benefits:**
- Eliminates clock skew between source system and Snowflake
- Handles backfilled/late-arriving data correctly
- Provides definitive ordering for regulatory compliance

### Enhancement 3: Pipeline Latency Monitoring

Create a monitoring view to track end-to-end latency across pipeline stages:

```sql
-- New monitoring procedure for pipeline latency
CREATE OR REPLACE PROCEDURE AGENTS.MEASURE_PIPELINE_LATENCY(table_name VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    landing_ts TIMESTAMP_LTZ;
    bronze_ts TIMESTAMP_LTZ;
    silver_ts TIMESTAMP_LTZ;
    gold_ts TIMESTAMP_LTZ;
BEGIN
    -- Get latest commit times from each layer
    EXECUTE IMMEDIATE 'SELECT MAX(METADATA$ROW_LAST_COMMIT_TIME) FROM DBAONTAP_ANALYTICS."public".' || :table_name INTO :landing_ts;
    EXECUTE IMMEDIATE 'SELECT MAX(METADATA$ROW_LAST_COMMIT_TIME) FROM DBAONTAP_ANALYTICS.BRONZE.' || UPPER(:table_name) || '_VARIANT' INTO :bronze_ts;
    EXECUTE IMMEDIATE 'SELECT MAX(METADATA$ROW_LAST_COMMIT_TIME) FROM DBAONTAP_ANALYTICS.SILVER.' || UPPER(:table_name) INTO :silver_ts;
    
    RETURN OBJECT_CONSTRUCT(
        'table', table_name,
        'measured_at', CURRENT_TIMESTAMP(),
        'landing_latest', landing_ts,
        'bronze_latest', bronze_ts,
        'silver_latest', silver_ts,
        'landing_to_bronze_ms', TIMESTAMPDIFF('millisecond', landing_ts, bronze_ts),
        'bronze_to_silver_ms', TIMESTAMPDIFF('millisecond', bronze_ts, silver_ts),
        'total_latency_ms', TIMESTAMPDIFF('millisecond', landing_ts, silver_ts)
    );
END;
$$;

-- Pipeline latency dashboard view
CREATE OR REPLACE VIEW METADATA.PIPELINE_LATENCY_DASHBOARD AS
SELECT 
    'CUSTOMERS' as table_name,
    MAX(l.METADATA$ROW_LAST_COMMIT_TIME) as landing_commit,
    MAX(b.METADATA$ROW_LAST_COMMIT_TIME) as bronze_commit,
    MAX(s.METADATA$ROW_LAST_COMMIT_TIME) as silver_commit,
    TIMESTAMPDIFF('second', MAX(l.METADATA$ROW_LAST_COMMIT_TIME), MAX(s.METADATA$ROW_LAST_COMMIT_TIME)) as total_latency_sec
FROM DBAONTAP_ANALYTICS."public".customers l
JOIN DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT b ON TRUE
JOIN DBAONTAP_ANALYTICS.SILVER.CUSTOMERS s ON TRUE
-- UNION ALL for other tables...
;
```

### Enhancement 4: Validator Phase Improvement

Use row timestamps to detect stale data and validate freshness:

```sql
-- Enhanced validation in WORKFLOW_VALIDATOR
CREATE OR REPLACE PROCEDURE AGENTS.VALIDATE_DATA_FRESHNESS(
    source_table VARCHAR,
    target_table VARCHAR,
    max_latency_seconds INTEGER DEFAULT 300  -- 5 minute SLA
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    source_latest TIMESTAMP_LTZ;
    target_latest TIMESTAMP_LTZ;
    latency_seconds INTEGER;
    is_fresh BOOLEAN;
BEGIN
    EXECUTE IMMEDIATE 'SELECT MAX(METADATA$ROW_LAST_COMMIT_TIME) FROM ' || :source_table INTO :source_latest;
    EXECUTE IMMEDIATE 'SELECT MAX(METADATA$ROW_LAST_COMMIT_TIME) FROM ' || :target_table INTO :target_latest;
    
    latency_seconds := TIMESTAMPDIFF('second', source_latest, target_latest);
    is_fresh := latency_seconds <= max_latency_seconds;
    
    -- Log validation result
    INSERT INTO METADATA.VALIDATION_RESULTS (
        execution_id, source_table, target_table, validation_type,
        expected_value, actual_value, passed, variance_pct, details
    ) VALUES (
        UUID_STRING(), :source_table, :target_table, 'data_freshness',
        :max_latency_seconds::VARIANT, :latency_seconds::VARIANT, :is_fresh, NULL,
        'Source latest: ' || :source_latest || ', Target latest: ' || :target_latest
    );
    
    RETURN OBJECT_CONSTRUCT(
        'source_table', source_table,
        'target_table', target_table,
        'source_latest_commit', source_latest,
        'target_latest_commit', target_latest,
        'latency_seconds', latency_seconds,
        'sla_seconds', max_latency_seconds,
        'is_fresh', is_fresh
    );
END;
$$;
```

### Enhancement 5: Incremental Processing with Row Timestamps

Replace stream-based change detection with row timestamp queries for more flexible incremental processing:

```sql
-- Track last processed timestamp per table
CREATE TABLE IF NOT EXISTS METADATA.INCREMENTAL_CHECKPOINTS (
    table_name VARCHAR PRIMARY KEY,
    last_processed_commit_time TIMESTAMP_LTZ,
    rows_processed INTEGER,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Incremental processing using row timestamps
CREATE OR REPLACE PROCEDURE AGENTS.PROCESS_INCREMENTAL_CHANGES(source_table VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    last_checkpoint TIMESTAMP_LTZ;
    new_rows INTEGER;
BEGIN
    -- Get last processed timestamp
    SELECT last_processed_commit_time INTO :last_checkpoint
    FROM METADATA.INCREMENTAL_CHECKPOINTS
    WHERE table_name = :source_table;
    
    -- If no checkpoint, start from beginning
    IF (last_checkpoint IS NULL) THEN
        last_checkpoint := '1970-01-01'::TIMESTAMP_LTZ;
    END IF;
    
    -- Count new rows since checkpoint
    EXECUTE IMMEDIATE '
        SELECT COUNT(*) FROM ' || :source_table || '
        WHERE METADATA$ROW_LAST_COMMIT_TIME > ''' || :last_checkpoint || '''
    ' INTO :new_rows;
    
    IF (new_rows > 0) THEN
        -- Process new rows... (trigger agentic workflow)
        -- Update checkpoint
        MERGE INTO METADATA.INCREMENTAL_CHECKPOINTS t
        USING (SELECT :source_table as table_name) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET 
            last_processed_commit_time = CURRENT_TIMESTAMP(),
            rows_processed = :new_rows,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (table_name, last_processed_commit_time, rows_processed)
            VALUES (:source_table, CURRENT_TIMESTAMP(), :new_rows);
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'table', source_table,
        'last_checkpoint', last_checkpoint,
        'new_rows_found', new_rows,
        'processed', new_rows > 0
    );
END;
$$;
```

### Summary: Row Timestamp Benefits for Agentic Workflow

| Current Approach | Enhanced with Row Timestamps | Benefit |
|------------------|------------------------------|---------|
| `_SNOWFLAKE_UPDATED_AT` from source CDC | `METADATA$ROW_LAST_COMMIT_TIME` | Guaranteed ordering, no clock skew |
| Stream-based change detection | Row timestamp checkpoint queries | More flexible, supports backfill |
| No latency visibility | End-to-end latency measurement | Pipeline observability |
| Row count validation only | Freshness SLA validation | Data quality assurance |
| Implicit ordering assumptions | Explicit commit-time ordering | Regulatory compliance (SCD2) |

### New Objects for Row Timestamp Integration

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `MEASURE_PIPELINE_LATENCY` | Stored Procedure | `AGENTS` | Measures latency across pipeline stages |
| `VALIDATE_DATA_FRESHNESS` | Stored Procedure | `AGENTS` | Validates data freshness against SLA |
| `PROCESS_INCREMENTAL_CHANGES` | Stored Procedure | `AGENTS` | Row timestamp-based incremental processing |
| `INCREMENTAL_CHECKPOINTS` | Table | `METADATA` | Tracks last processed commit time per table |
| `PIPELINE_LATENCY_DASHBOARD` | View | `METADATA` | Real-time latency monitoring |

---

### Schema Change Detection Logic

The agentic workflow detects schema changes by comparing Bronze payload keys to existing Silver columns:

```sql
-- Pseudo-logic in WORKFLOW_TRIGGER
WITH bronze_schema AS (
    SELECT DISTINCT f.key as column_name
    FROM BRONZE.TABLE_VARIANT,
         LATERAL FLATTEN(input => payload) f
    LIMIT 1000
),
silver_schema AS (
    SELECT COLUMN_NAME as column_name
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = 'TABLE'
)
SELECT 
    CASE 
        WHEN silver.column_name IS NULL THEN 'NEW_COLUMN'
        WHEN bronze.column_name IS NULL THEN 'DROPPED_COLUMN'
        ELSE 'MATCH'
    END as change_type,
    COALESCE(bronze.column_name, silver.column_name) as column_name
FROM bronze_schema bronze
FULL OUTER JOIN silver_schema silver ON bronze.column_name = silver.column_name
WHERE silver.column_name IS NULL OR bronze.column_name IS NULL;
```

### Workflow Orchestration for Schema Evolution

When schema changes are detected, the agentic workflow:

1. **TRIGGER**: Marks table for reprocessing, logs change type
2. **PLANNER**: LLM analyzes new schema structure, determines migration strategy
3. **EXECUTOR**: Generates `CREATE OR REPLACE DYNAMIC TABLE` with new columns
4. **VALIDATOR**: Ensures row counts match, validates new columns populated
5. **REFLECTOR**: Logs schema evolution pattern for future reference

```sql
-- Example: Agentic response to new column "loyalty_tier" in customers
-- PLANNER decision:
{
  "source_table": "BRONZE.CUSTOMERS_VARIANT",
  "target_table": "SILVER.CUSTOMERS",
  "strategy": "schema_evolution",
  "detected_patterns": {
    "new_columns": ["loyalty_tier"],
    "dropped_columns": [],
    "type_changes": []
  },
  "transformations": [
    {"column": "loyalty_tier", "action": "ADD", "type": "VARCHAR", "reason": "New field in source"}
  ],
  "recommendation": "Recreate Silver DT with new column extraction"
}
```

### PostgreSQL Source Tables

The following tables originate in the SOURCE PostgreSQL instance (`DBAONTAP_SOURCE.PUBLIC.SOURCE_PG`):

| Source Table | Primary Key | Key Columns | CDC Publication |
|--------------|-------------|-------------|-----------------|
| `customers` | `customer_id` (SERIAL) | first_name, last_name, email, company_name, industry | `dbaontap_pub` |
| `orders` | `order_id` (SERIAL) | customer_id (FK), order_date, status, total_amount | `dbaontap_pub` |
| `products` | `product_id` (SERIAL) | product_name, category, price, cost, sku | `dbaontap_pub` |
| `order_items` | `item_id` (SERIAL) | order_id (FK), product_id (FK), quantity, unit_price | `dbaontap_pub` |
| `support_tickets` | `ticket_id` (SERIAL) | customer_id (FK), subject, priority, status | `dbaontap_pub` |

### Openflow CDC Configuration

Openflow replicates data from LANDING PostgreSQL to Snowflake with CDC metadata columns:

| Configuration | Value |
|---------------|-------|
| Source Host | `<LANDING_PG_HOST>` |
| Source Port | `5432` |
| Source Database | `dbaontap` |
| Replication Plugin | `pgoutput` |
| Slot Name | `openflow_slot` |
| Target Database | `DBAONTAP_ANALYTICS` |
| Target Schema | `public` |
| Target Warehouse | `DBRYANT_COCO_WH_S` |

### CDC Metadata Columns Added by Openflow

Openflow automatically adds these CDC columns to landed tables:

| Column | Type | Description |
|--------|------|-------------|
| `_SNOWFLAKE_DELETED` | BOOLEAN | Soft-delete flag (TRUE when row deleted in source) |
| `_SNOWFLAKE_UPDATED_AT` | TIMESTAMP | CDC timestamp for change ordering |

### Table Conversion Pipeline

#### Stage 1: Landing Tables (`public` schema)

Openflow creates landing tables that mirror the PostgreSQL source structure plus CDC columns:

```sql
-- Example: public.customers (landed by Openflow)
customer_id INTEGER,
first_name VARCHAR,
last_name VARCHAR,
email VARCHAR,
phone VARCHAR,
company_name VARCHAR,
industry VARCHAR,
created_at TIMESTAMP,
updated_at TIMESTAMP,
_SNOWFLAKE_DELETED BOOLEAN,      -- CDC: soft delete flag
_SNOWFLAKE_UPDATED_AT TIMESTAMP  -- CDC: change timestamp
```

#### Stage 2: Bronze VARIANT Tables (`BRONZE` schema)

The agentic workflow converts landing tables to VARIANT format for schema-on-read flexibility:

```sql
-- Conversion: public.customers → BRONZE.CUSTOMERS_VARIANT
CREATE OR REPLACE DYNAMIC TABLE BRONZE.CUSTOMERS_VARIANT
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT 
    OBJECT_CONSTRUCT(*) as payload,    -- All columns as JSON object
    'customers' as source_table,
    CURRENT_TIMESTAMP() as ingested_at
FROM "public"."customers";
```

**Result Schema:**
```
payload VARIANT          -- Contains: {customer_id, first_name, ..., _SNOWFLAKE_DELETED, _SNOWFLAKE_UPDATED_AT}
source_table VARCHAR     -- 'customers'
ingested_at TIMESTAMP    -- Ingestion timestamp
```

#### Stage 3: Silver CDC-Aware Tables (`SILVER` schema)

Silver tables apply CDC deduplication using `ROW_NUMBER()` partitioned by primary key:

```sql
-- Conversion: BRONZE.CUSTOMERS_VARIANT → SILVER.CUSTOMERS
CREATE OR REPLACE DYNAMIC TABLE SILVER.CUSTOMERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = DBRYANT_COCO_WH_S
AS
WITH ranked_changes AS (
    SELECT 
        payload:customer_id::INTEGER as customer_id,
        payload:first_name::VARCHAR as first_name,
        payload:last_name::VARCHAR as last_name,
        payload:email::VARCHAR as email,
        -- ... other fields extracted from VARIANT
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        TRY_TO_TIMESTAMP(payload:_SNOWFLAKE_UPDATED_AT::VARCHAR) as cdc_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY payload:customer_id 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM BRONZE.CUSTOMERS_VARIANT
)
SELECT * FROM ranked_changes
WHERE rn = 1                                          -- Latest version only
  AND (is_deleted = FALSE OR is_deleted IS NULL);    -- Exclude soft-deletes
```

### Complete Table Lineage

| Source (PostgreSQL) | Landing (Openflow) | Bronze (VARIANT) | Silver (CDC-Aware) | Gold (Aggregated) |
|---------------------|-------------------|------------------|-------------------|-------------------|
| `customers` | `public.customers` | `BRONZE.CUSTOMERS_VARIANT` | `SILVER.CUSTOMERS` | `GOLD.CUSTOMER_360` |
| `orders` | `public.orders` | `BRONZE.ORDERS_VARIANT` | `SILVER.ORDERS` | `GOLD.ORDER_SUMMARY` |
| `products` | `public.products` | `BRONZE.PRODUCTS_VARIANT` | `SILVER.PRODUCTS` | `GOLD.PRODUCT_PERFORMANCE` |
| `order_items` | `public.order_items` | `BRONZE.ORDER_ITEMS_VARIANT` | `SILVER.ORDER_ITEMS` | (joined in Gold) |
| `support_tickets` | `public.support_tickets` | `BRONZE.SUPPORT_TICKETS_VARIANT` | `SILVER.SUPPORT_TICKETS` | (joined in CUSTOMER_360) |

### Auto-Discovery and Onboarding

When new tables are added to the `public` schema via Openflow, the agentic workflow automatically:

1. **Discovers** new tables (excludes Openflow journal tables `*JOURNAL*`)
2. **Creates Stream** on landing table for CDC detection
3. **Creates Bronze Dynamic Table** with `OBJECT_CONSTRUCT(*)` pattern
4. **Logs onboarding** to `METADATA.ONBOARDED_TABLES`

```sql
-- Auto-onboarding procedure
CALL AGENTS.DISCOVER_AND_ONBOARD_NEW_TABLES();

-- Returns:
{
  "status": "DISCOVERY_COMPLETE",
  "new_tables_found": 2,
  "onboarded": [
    {"status": "ONBOARDED", "landing_table": "public.new_table", "bronze_table": "BRONZE.NEW_TABLE_VARIANT"},
    ...
  ]
}
```

---

## Database & Schema Structure

| Database | Schema | Purpose |
|----------|--------|---------|
| `DBAONTAP_ANALYTICS` | `public` | Landing zone for Openflow CDC data |
| `DBAONTAP_ANALYTICS` | `BRONZE` | VARIANT Dynamic Tables (schema-on-read) |
| `DBAONTAP_ANALYTICS` | `SILVER` | CDC-aware, deduplicated Dynamic Tables |
| `DBAONTAP_ANALYTICS` | `GOLD` | Aggregation Dynamic Tables & Semantic Views |
| `DBAONTAP_ANALYTICS` | `AGENTS` | Stored procedures, UDFs, streams, tasks |
| `DBAONTAP_ANALYTICS` | `METADATA` | Workflow tracking, learnings, logging |

---

## Step 1: TRIGGER Phase

**Purpose**: Detect events that initiate transformation workflows, auto-discover new landing tables, and create Bronze layer objects.

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_TRIGGER` | Stored Procedure | `AGENTS` | Main trigger procedure that detects events and determines tables to process |
| `DISCOVER_AND_ONBOARD_NEW_TABLES` | Stored Procedure | `AGENTS` | Auto-discovers new landing tables and creates corresponding Bronze objects |
| `AUTO_ONBOARD_TABLE` | Stored Procedure | `AGENTS` | Onboards a single landing table (creates stream + Bronze Dynamic Table) |

### Trigger Types
- `manual` - User-initiated via `RUN_AGENTIC_WORKFLOW('manual')`
- `stream` - Triggered when streams have new CDC data
- `scheduled` - Processes all Bronze tables on schedule
- `quality_alert` - Triggered when quality thresholds are breached

### Metadata Tables

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `ONBOARDED_TABLES` | Table | `METADATA` | Tracks tables that have been auto-onboarded |
| `WORKFLOW_EXECUTIONS` | Table | `METADATA` | Master workflow execution tracking |

**ONBOARDED_TABLES Schema:**
```sql
table_name VARCHAR PRIMARY KEY,
landing_table VARCHAR,
stream_name VARCHAR,
bronze_table VARCHAR,
onboarded_at TIMESTAMP_LTZ,
onboarded_by VARCHAR
```

### Streams (Auto-Created per Table)

| Object | Type | Location | Source Table |
|--------|------|----------|--------------|
| `CUSTOMERS_LANDING_STREAM` | Stream | `AGENTS` | `public.customers` |
| `ORDERS_LANDING_STREAM` | Stream | `AGENTS` | `public.orders` |
| `ORDER_ITEMS_LANDING_STREAM` | Stream | `AGENTS` | `public.order_items` |
| `PRODUCTS_LANDING_STREAM` | Stream | `AGENTS` | `public.products` |
| `SUPPORT_TICKETS_LANDING_STREAM` | Stream | `AGENTS` | `public.support_tickets` |

### Bronze Dynamic Tables (Created by Trigger)

| Object | Type | Location | Target Lag | Description |
|--------|------|----------|------------|-------------|
| `CUSTOMERS_VARIANT` | Dynamic Table | `BRONZE` | 1 minute | Schema-on-read customer data |
| `ORDERS_VARIANT` | Dynamic Table | `BRONZE` | 1 minute | Schema-on-read order data |
| `ORDER_ITEMS_VARIANT` | Dynamic Table | `BRONZE` | 1 minute | Schema-on-read order items |
| `PRODUCTS_VARIANT` | Dynamic Table | `BRONZE` | 1 minute | Schema-on-read product data |
| `SUPPORT_TICKETS_VARIANT` | Dynamic Table | `BRONZE` | 1 minute | Schema-on-read support tickets |
| `ALL_DATA_VARIANT` | View | `BRONZE` | N/A | Unified view of all Bronze tables |

**Bronze Table Schema Pattern:**
```sql
payload VARIANT,           -- OBJECT_CONSTRUCT(*) from source
source_table VARCHAR,      -- Source table name
ingested_at TIMESTAMP_LTZ  -- Ingestion timestamp
```

---

## Step 2: PLANNER Phase

**Purpose**: Use LLM to analyze Bronze tables, detect patterns, and decide transformation strategies for Silver layer.

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_PLANNER` | Stored Procedure | `AGENTS` | Main planner that analyzes schemas and decides strategies |
| `DISCOVER_SCHEMA` | UDF | `AGENTS` | Extracts schema structure from VARIANT columns |
| `ANALYZE_DATA_QUALITY` | Stored Procedure | `AGENTS` | Analyzes data quality issues in Bronze tables |
| `CORTEX_INFER_SCHEMA` | Stored Procedure | `AGENTS` | LLM-powered schema inference |

### LLM Integration
- **Model**: `claude-3-5-sonnet`
- **Purpose**: Analyze schemas and determine optimal transformation strategies

### Transformation Strategies
| Strategy | Description | Use Case |
|----------|-------------|----------|
| `flatten_and_type` | Extract VARIANT fields, apply types | Default for most tables |
| `deduplicate` | Remove duplicates by key columns | Tables with CDC overwrites |
| `scd_type2` | Slowly changing dimension tracking | Dimension tables needing history |
| `aggregate` | Pre-aggregate for performance | High-volume tables |
| `normalize` | Split nested arrays | Complex JSON structures |

### Metadata Tables

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `PLANNER_DECISIONS` | Table | `METADATA` | Logs all planner decisions with LLM reasoning |
| `BRONZE_SCHEMA_REGISTRY` | Table | `METADATA` | Registry of analyzed Bronze schemas |

**PLANNER_DECISIONS Schema:**
```sql
decision_id VARCHAR,
execution_id VARCHAR,
source_table VARCHAR,
target_schema VARCHAR,
transformation_strategy VARCHAR,
detected_patterns VARIANT,
recommended_actions ARRAY,
priority INTEGER,
llm_reasoning TEXT,
confidence_score FLOAT,
created_at TIMESTAMP_LTZ
```

---

## Step 3: EXECUTOR Phase

**Purpose**: Execute planned transformations with LLM-powered SQL generation and automatic retry/self-correction.

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_EXECUTOR` | Stored Procedure | `AGENTS` | Executes transformations with retry logic |
| `GENERATE_TRANSFORMATION` | Stored Procedure | `AGENTS` | Generates Dynamic Table DDL |

### Key Features
- **Self-Correction**: On failure, LLM re-analyzes error and regenerates SQL
- **Max Retries**: 3 attempts before marking as failed
- **Logging**: All executions logged to `TRANSFORMATION_LOG`

### LLM Integration
- **Model**: `claude-3-5-sonnet`
- **Purpose**: Generate Dynamic Table DDL with proper VARIANT field extraction

### Silver Dynamic Tables (Created by Executor)

| Object | Type | Location | Target Lag | Key Features |
|--------|------|----------|------------|--------------|
| `CUSTOMERS` | Dynamic Table | `SILVER` | 1 minute | CDC deduplication, soft-delete handling, segment derivation |
| `ORDERS` | Dynamic Table | `SILVER` | 1 minute | CDC deduplication, order_month derivation |
| `PRODUCTS` | Dynamic Table | `SILVER` | 1 minute | CDC deduplication, margin calculations |
| `ORDER_ITEMS` | Dynamic Table | `SILVER` | 1 minute | CDC deduplication, line total calculations |
| `SUPPORT_TICKETS` | Dynamic Table | `SILVER` | 1 minute | CDC deduplication, resolution_hours derivation |

**Silver CDC Pattern:**
```sql
WITH ranked_changes AS (
    SELECT 
        payload:field::TYPE as field,
        payload:_SNOWFLAKE_DELETED::BOOLEAN as is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY payload:pk_column 
            ORDER BY payload:_SNOWFLAKE_UPDATED_AT DESC
        ) as rn
    FROM BRONZE.TABLE_VARIANT
)
SELECT * FROM ranked_changes
WHERE rn = 1 AND (is_deleted = FALSE OR is_deleted IS NULL);
```

### Metadata Tables

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `TRANSFORMATION_LOG` | Table | `METADATA` | Logs all transformation executions with SQL and status |

**TRANSFORMATION_LOG Schema:**
```sql
transformation_id VARCHAR,
source_table VARCHAR,
target_table VARCHAR,
transformation_sql TEXT,
agent_reasoning TEXT,
status VARCHAR,  -- 'SUCCESS', 'FAILED'
created_at TIMESTAMP_LTZ,
executed_at TIMESTAMP_LTZ,
error_message TEXT
```

---

## Step 4: VALIDATOR Phase

**Purpose**: Validate transformation results by comparing source and target data.

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_VALIDATOR` | Stored Procedure | `AGENTS` | Validates transformations (row counts, schema match) |

### Validation Types
| Type | Description | Pass Threshold |
|------|-------------|----------------|
| `row_count` | Compare source vs target row counts | < 5% variance |
| `schema_match` | Verify column types match | Exact match |
| `data_quality` | Check for nulls, type mismatches | Configurable |
| `referential_integrity` | Verify FK relationships | 100% match |

### Metadata Tables

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `VALIDATION_RESULTS` | Table | `METADATA` | Stores all validation outcomes |

**VALIDATION_RESULTS Schema:**
```sql
validation_id VARCHAR,
execution_id VARCHAR,
source_table VARCHAR,
target_table VARCHAR,
validation_type VARCHAR,
expected_value VARIANT,
actual_value VARIANT,
passed BOOLEAN,
variance_pct FLOAT,
details TEXT,
created_at TIMESTAMP_LTZ
```

---

## Step 5: REFLECTOR Phase

**Purpose**: Analyze workflow results, extract learnings, and optimize future runs.

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_REFLECTOR` | Stored Procedure | `AGENTS` | Analyzes workflow and persists learnings |

### LLM Integration
- **Model**: `claude-3-5-sonnet`
- **Purpose**: Analyze workflow results and extract actionable learnings

### Learning Types
| Type | Description |
|------|-------------|
| `success_pattern` | Patterns that led to successful transformations |
| `failure_pattern` | Patterns that caused failures (for avoidance) |
| `optimization` | Performance improvement recommendations |
| `schema_pattern` | Discovered schema conventions |

### Metadata Tables

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_LEARNINGS` | Table | `METADATA` | Persisted learnings with confidence scores |
| `WORKFLOW_LOG` | Table | `METADATA` | Detailed workflow debugging/monitoring |

**WORKFLOW_LEARNINGS Schema:**
```sql
learning_id VARCHAR,
execution_id VARCHAR,
learning_type VARCHAR,
source_context VARCHAR,
pattern_signature VARCHAR,  -- Hash for pattern matching
observation TEXT,
recommendation TEXT,
times_observed INTEGER,
last_observed_at TIMESTAMP_LTZ,
confidence_score FLOAT,
is_active BOOLEAN
```

---

## Master Orchestrator

### Procedures

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `RUN_AGENTIC_WORKFLOW` | Stored Procedure | `AGENTS` | Orchestrates all 5 phases in sequence |

**Usage:**
```sql
-- Manual run (all Bronze tables)
CALL AGENTS.RUN_AGENTIC_WORKFLOW('manual');

-- Manual run (specific tables)
CALL AGENTS.RUN_AGENTIC_WORKFLOW('manual', 
    ARRAY_CONSTRUCT('DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT'));

-- Stream-triggered (checks for new data)
CALL AGENTS.RUN_AGENTIC_WORKFLOW('stream');
```

### Tasks

| Object | Type | Location | Schedule | Description |
|--------|------|----------|----------|-------------|
| `AGENTIC_WORKFLOW_TRIGGER_TASK` | Task | `AGENTS` | 1 minute | Auto-runs workflow, checks streams, discovers new tables |

---

## Gold Layer Objects

### Dynamic Tables

| Object | Type | Location | Target Lag | Description |
|--------|------|----------|------------|-------------|
| `CUSTOMER_360` | Dynamic Table | `GOLD` | 5 minutes | Customer 360 view with RFM features |
| `PRODUCT_PERFORMANCE` | Dynamic Table | `GOLD` | 5 minutes | Product analytics with sales metrics |
| `ORDER_SUMMARY` | Dynamic Table | `GOLD` | 5 minutes | Time-based order aggregations |
| `CUSTOMER_METRICS` | Dynamic Table | `GOLD` | 5 minutes | Segment/industry metrics |
| `ML_CUSTOMER_FEATURES` | Dynamic Table | `GOLD` | 5 minutes | ML feature engineering for churn prediction |

### Semantic View Pipeline

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `RUN_SEMANTIC_VIEW_PIPELINE` | Stored Procedure | `AGENTS` | Auto-generates semantic views for all Gold tables |

**Generated Semantic Views:**
- `CUSTOMER_360_SV`
- `PRODUCT_PERFORMANCE_SV`
- `ORDER_SUMMARY_SV`
- `CUSTOMER_METRICS_SV`
- `ML_CUSTOMER_FEATURES_SV`

---

## Monitoring Views

| Object | Type | Location | Description |
|--------|------|----------|-------------|
| `WORKFLOW_DASHBOARD` | View | `METADATA` | Real-time workflow execution status |
| `ACTIVE_LEARNINGS` | View | `METADATA` | Current active learnings by confidence |

**WORKFLOW_DASHBOARD Columns:**
```sql
execution_id, workflow_name, trigger_type, status, current_phase,
tables_triggered, executions_succeeded, executions_failed,
validations_passed, validations_failed, learnings_captured,
retry_count, duration_seconds, started_at, completed_at, last_error
```

---

## LLM Models Used

| Model | Use Case |
|-------|----------|
| `claude-3-5-sonnet` | Semantic view DDL generation, transformation planning, workflow reflection |
| `llama3.1-8b` | Schema inference (lighter weight tasks) |
| `e5-base-v2` | Embedding model for Knowledge Graph semantic search |

---

## Knowledge Graph Integration ✅ IMPLEMENTED

The Knowledge Graph (KG) enhances the agentic workflow with lineage awareness and semantic search capabilities.

### KG Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              KNOWLEDGE GRAPH INTEGRATION                                            │
├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                                   KNOWLEDGE_GRAPH SCHEMA                                     │   │
│  │                                                                                              │   │
│  │   ┌─────────────────┐           ┌─────────────────┐           ┌─────────────────┐           │   │
│  │   │    KG_NODE      │           │    KG_EDGE      │           │   EMBEDDINGS    │           │   │
│  │   │                 │           │                 │           │                 │           │   │
│  │   │ • DATABASE      │◀─────────▶│ • CONTAINS      │           │ VECTOR(768)     │           │   │
│  │   │ • SCHEMA        │           │ • HAS_COLUMN    │           │ via e5-base-v2  │           │   │
│  │   │ • TABLE         │           │ • TRANSFORMS_TO │           │                 │           │   │
│  │   │ • COLUMN        │           │ • AGGREGATES_TO │           │ Enables:        │           │   │
│  │   │                 │           │                 │           │ SEARCH_TABLES   │           │   │
│  │   └─────────────────┘           └─────────────────┘           └─────────────────┘           │   │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                                      │
│              ┌───────────────────────────────┴───────────────────────────────┐                      │
│              │                                                               │                      │
│              ▼                                                               ▼                      │
│  ┌───────────────────────────────────────┐              ┌───────────────────────────────────────┐  │
│  │      DECISION POINT 1                 │              │      DECISION POINT 2                 │  │
│  │      New Table Onboarding             │              │      Silver Transformation            │  │
│  ├───────────────────────────────────────┤              ├───────────────────────────────────────┤  │
│  │                                       │              │                                       │  │
│  │  NEW TABLE DETECTED                   │              │  SCHEMA CHANGE DETECTED               │  │
│  │         │                             │              │         │                             │  │
│  │         ▼                             │              │         ▼                             │  │
│  │  ┌─────────────────────┐              │              │  ┌─────────────────────┐              │  │
│  │  │ SEARCH_TABLES_SP()  │              │              │  │ GET_TABLE_LINEAGE() │              │  │
│  │  │                     │              │              │  │                     │              │  │
│  │  │ Find similar tables │              │              │  │ Query downstream:   │              │  │
│  │  │ by semantic search  │              │              │  │ Silver → Gold deps  │              │  │
│  │  └──────────┬──────────┘              │              │  └──────────┬──────────┘              │  │
│  │             │                         │              │             │                         │  │
│  │             ▼                         │              │             ▼                         │  │
│  │  ┌─────────────────────┐              │              │  ┌─────────────────────┐              │  │
│  │  │ PATTERN REUSE       │              │              │  │ IMPACT ANALYSIS     │              │  │
│  │  │                     │              │              │  │                     │              │  │
│  │  │ • Copy transformation│             │              │  │ • Count affected    │              │  │
│  │  │   from similar table│              │              │  │   Gold tables       │              │  │
│  │  │ • Infer column types│              │              │  │ • Generate warning  │              │  │
│  │  │ • Reuse CDC patterns│              │              │  │ • Inform LLM prompt │              │  │
│  │  └──────────┬──────────┘              │              │  └──────────┬──────────┘              │  │
│  │             │                         │              │             │                         │  │
│  └─────────────┼─────────────────────────┘              └─────────────┼─────────────────────────┘  │
│                │                                                      │                            │
│                └──────────────────────────┬───────────────────────────┘                            │
│                                           │                                                        │
│                                           ▼                                                        │
│                              ┌─────────────────────────┐                                           │
│                              │   ENRICHED LLM PROMPT   │                                           │
│                              │                         │                                           │
│                              │ Context includes:       │                                           │
│                              │ • Table descriptions    │                                           │
│                              │ • Lineage relationships │                                           │
│                              │ • Similar table patterns│                                           │
│                              │ • Downstream impact     │                                           │
│                              └─────────────────────────┘                                           │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Entity-Relationship Model

```
                                    ┌─────────────┐
                                    │  DATABASE   │
                                    │ DBAONTAP_   │
                                    │ ANALYTICS   │
                                    └──────┬──────┘
                                           │ CONTAINS
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
             ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
             │   SCHEMA    │        │   SCHEMA    │        │   SCHEMA    │
             │   BRONZE    │        │   SILVER    │        │    GOLD     │
             └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
                    │ CONTAINS             │ CONTAINS             │ CONTAINS
                    ▼                      ▼                      ▼
             ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
             │   TABLE     │        │   TABLE     │        │   TABLE     │
             │ CUSTOMERS_  │───────▶│  CUSTOMERS  │───────▶│ CUSTOMER_   │
             │  VARIANT    │TRANSF. │             │ AGGREG.│    360      │
             └──────┬──────┘        └──────┬──────┘        └─────────────┘
                    │ HAS_COLUMN           │ HAS_COLUMN
                    ▼                      ▼
             ┌─────────────┐        ┌─────────────┐
             │   COLUMN    │        │   COLUMN    │
             │  customer_  │        │ CUSTOMER_ID │
             │     id      │        │ FIRST_NAME  │
             │  payload    │        │   ...       │
             └─────────────┘        └─────────────┘
```

### KG Schema: `KNOWLEDGE_GRAPH`

| Object | Type | Description |
|--------|------|-------------|
| `KG_NODE` | Table | Universal entity storage (TABLE, COLUMN, SCHEMA nodes) |
| `KG_EDGE` | Table | Relationships (CONTAINS, TRANSFORMS_TO, AGGREGATES_TO) |
| `V_TABLES` | View | All table nodes with properties |
| `V_COLUMNS` | View | All column nodes |
| `V_TABLE_LINEAGE` | View | Bronze → Silver → Gold lineage |
| `V_COLUMN_RELATIONSHIPS` | View | FK relationships |

### KG Procedures & Functions

| Object | Type | Description |
|--------|------|-------------|
| `POPULATE_KG_FROM_INFORMATION_SCHEMA` | Procedure | Auto-populates KG from catalog |
| `ENRICH_TABLE_DESCRIPTIONS` | Procedure | LLM-generates table descriptions |
| `GENERATE_NODE_EMBEDDINGS` | Procedure | Creates embeddings for semantic search |
| `GET_RELATED_TABLES` | Function | Find 1-hop neighbors of a table |
| `GET_TABLE_LINEAGE` | Function | Recursive lineage (upstream/downstream) |
| `SEARCH_TABLES_SP` | Procedure | Semantic search for similar tables |

### KG-Enhanced Decision Point 2

The `DETECT_SCHEMA_CHANGES` procedure now includes `kg_insight`:

```json
{
  "schema_changed": false,
  "kg_insight": {
    "downstream_count": 3,
    "downstream_tables": [
      {"table_name": "CUSTOMER_360", "schema": "GOLD", "hop_level": 1},
      {"table_name": "CUSTOMER_METRICS", "schema": "GOLD", "hop_level": 1},
      {"table_name": "ML_CUSTOMER_FEATURES", "schema": "GOLD", "hop_level": 1}
    ],
    "impact_warning": "Schema change will impact 3 downstream Gold tables"
  }
}
```

### KG Benefits

| Decision Point | Enhancement |
|----------------|-------------|
| **DP1: New Table** | `SEARCH_TABLES_SP` finds similar tables for pattern reuse |
| **DP2: Schema Change** | `GET_TABLE_LINEAGE` shows downstream impact before changes |
| **LLM Prompts** | Enriched with descriptions, relationships, similar patterns |

---

## Quick Reference: All Objects by Schema

### AGENTS Schema
```
Procedures:
├── WORKFLOW_TRIGGER
├── WORKFLOW_PLANNER
├── WORKFLOW_EXECUTOR
├── WORKFLOW_VALIDATOR
├── WORKFLOW_REFLECTOR
├── RUN_AGENTIC_WORKFLOW
├── DISCOVER_AND_ONBOARD_NEW_TABLES
├── AUTO_ONBOARD_TABLE
├── DISCOVER_SCHEMA (UDF)
├── ANALYZE_DATA_QUALITY
├── GENERATE_TRANSFORMATION
├── CORTEX_INFER_SCHEMA
└── RUN_SEMANTIC_VIEW_PIPELINE

Streams:
├── CUSTOMERS_LANDING_STREAM
├── ORDERS_LANDING_STREAM
├── ORDER_ITEMS_LANDING_STREAM
├── PRODUCTS_LANDING_STREAM
└── SUPPORT_TICKETS_LANDING_STREAM

Tasks:
└── AGENTIC_WORKFLOW_TRIGGER_TASK
```

### METADATA Schema
```
Tables:
├── WORKFLOW_EXECUTIONS
├── PLANNER_DECISIONS
├── VALIDATION_RESULTS
├── WORKFLOW_LEARNINGS
├── WORKFLOW_LOG
├── ONBOARDED_TABLES
├── BRONZE_SCHEMA_REGISTRY
├── TRANSFORMATION_LOG
└── SCHEMA_IGNORE_COLUMNS        -- ✅ NEW: DP2 config for schema detection

Views:
├── WORKFLOW_DASHBOARD
└── ACTIVE_LEARNINGS
```

### AGENTS Schema (Decision Point 2)
```
Procedures:
└── DETECT_SCHEMA_CHANGES        -- ✅ KG-enhanced: Schema diff + lineage impact
```

### KNOWLEDGE_GRAPH Schema ✅ NEW
```
Tables:
├── KG_NODE                       -- Entity storage with embeddings
└── KG_EDGE                       -- Relationships between entities

Views:
├── V_TABLES
├── V_COLUMNS
├── V_TABLE_LINEAGE
└── V_COLUMN_RELATIONSHIPS

Procedures:
├── POPULATE_KG_FROM_INFORMATION_SCHEMA
├── ENRICH_TABLE_DESCRIPTIONS
├── GENERATE_NODE_EMBEDDINGS
└── SEARCH_TABLES_SP

Functions:
├── GET_RELATED_TABLES
└── GET_TABLE_LINEAGE
```

### BRONZE Schema
```
Dynamic Tables:
├── CUSTOMERS_VARIANT
├── ORDERS_VARIANT
├── ORDER_ITEMS_VARIANT
├── PRODUCTS_VARIANT
└── SUPPORT_TICKETS_VARIANT

Views:
└── ALL_DATA_VARIANT
```

### SILVER Schema
```
Dynamic Tables:
├── CUSTOMERS
├── ORDERS
├── ORDER_ITEMS
├── PRODUCTS
└── SUPPORT_TICKETS
```

### GOLD Schema
```
Dynamic Tables:
├── CUSTOMER_360
├── PRODUCT_PERFORMANCE
├── ORDER_SUMMARY
├── CUSTOMER_METRICS
└── ML_CUSTOMER_FEATURES

Semantic Views (Generated):
├── CUSTOMER_360_SV
├── PRODUCT_PERFORMANCE_SV
├── ORDER_SUMMARY_SV
├── CUSTOMER_METRICS_SV
└── ML_CUSTOMER_FEATURES_SV
```

---

## File References

| Script | Purpose |
|--------|---------|
| `scripts/01_setup_schema.sql` | Database and schema creation |
| `scripts/02_discovery_tools.sql` | Discovery UDFs and procedures |
| `scripts/03_transformation_agent.sql` | Cortex Agent definition |
| `scripts/05_bronze/setup.sql` | Bronze Dynamic Tables |
| `scripts/06_silver/setup.sql` | Silver Dynamic Tables |
| `scripts/07_gold/setup.sql` | Gold Dynamic Tables |
| `scripts/07_agentic_workflow_engine.sql` | Complete workflow engine |
| `scripts/08c_schema_detection_tuning.sql` | Schema ignore patterns config |
| `scripts/08d_detect_schema_changes_v2.sql` | Base schema detection procedure |
| `scripts/08e_detect_schema_changes_with_kg.sql` | KG-enhanced schema detection |
| `scripts/09_knowledge_graph.sql` | Knowledge Graph schema & procedures |
| `scripts/09_semantic_views/setup.sql` | Semantic view pipeline |
