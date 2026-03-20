# Agentic Transformation in Practice

## Real-World Example: New Data Source Arrives

### Scenario

**Monday 9:00 AM** — Sales ops uploads a new Salesforce export to the data lake. It's a JSON file with opportunity data they need for pipeline reporting by end of day.

---

## Traditional Approach (Days to Weeks)

```
9:00 AM   Sales uploads file to S3
9:15 AM   Creates Jira ticket: "Need opportunity data in warehouse"
          
... waits 3 days for data engineering sprint planning ...

Day 4     Engineer examines file, maps 47 fields manually
Day 5     Writes SQL transformation, handles edge cases
Day 6     Tests, finds nulls breaking reports, fixes
Day 7     Deploys to production
Day 8     Sales finally gets their report
```

**Result**: 8 days, frustrated stakeholders, brittle pipeline

---

## Agentic Approach (Minutes to Hours)

```
9:00 AM   Sales uploads file to Bronze landing zone
          
          ┌─────────────────────────────────────────────────────────┐
          │  🤖 AGENT DETECTS NEW DATA                              │
          │                                                         │
          │  "New file detected in BRONZE.RAW_OPPORTUNITIES"        │
          │  "Initiating discovery workflow..."                     │
          └─────────────────────────────────────────────────────────┘

9:01 AM   DISCOVER PHASE
          ┌─────────────────────────────────────────────────────────┐
          │  Agent analyzes 1000 sample rows:                       │
          │                                                         │
          │  • 47 fields detected                                   │
          │  • opportunity_id: VARCHAR (unique key)                 │
          │  • amount: NUMBER (currency, 3% nulls)                  │
          │  • close_date: VARCHAR (needs DATE cast)                │
          │  • stage: VARCHAR (7 distinct values)                   │
          │  • owner.name: nested object (needs flatten)            │
          └─────────────────────────────────────────────────────────┘

9:02 AM   REASON PHASE
          ┌─────────────────────────────────────────────────────────┐
          │  Agent determines transformation strategy:              │
          │                                                         │
          │  "Source contains nested JSON with sales opportunities. │
          │   Recommended approach:                                 │
          │   1. Flatten nested owner object                        │
          │   2. Cast close_date to DATE                            │
          │   3. COALESCE nulls in amount to 0                      │
          │   4. Derive 'is_won' boolean from stage                 │
          │   5. Create as Dynamic Table with 1hr refresh"          │
          └─────────────────────────────────────────────────────────┘

9:03 AM   EXECUTE PHASE
          ┌─────────────────────────────────────────────────────────┐
          │  Agent generates and executes SQL:                      │
          │                                                         │
          │  CREATE DYNAMIC TABLE SILVER.OPPORTUNITIES              │
          │      TARGET_LAG = '1 hour'                              │
          │  AS                                                     │
          │  SELECT                                                 │
          │      raw:opportunity_id::VARCHAR as opportunity_id,     │
          │      raw:account_name::VARCHAR as account_name,         │
          │      COALESCE(raw:amount::NUMBER, 0) as amount,         │
          │      TRY_TO_DATE(raw:close_date) as close_date,         │
          │      raw:stage::VARCHAR as stage,                       │
          │      raw:stage IN ('Closed Won') as is_won,             │
          │      raw:owner.name::VARCHAR as owner_name,             │
          │      raw:owner.email::VARCHAR as owner_email            │
          │  FROM BRONZE.RAW_OPPORTUNITIES;                         │
          │                                                         │
          │  ✓ Dynamic Table created successfully                   │
          │  ✓ 12,847 rows transformed                              │
          └─────────────────────────────────────────────────────────┘

9:04 AM   REFLECT PHASE
          ┌─────────────────────────────────────────────────────────┐
          │  Agent logs transformation metadata:                    │
          │                                                         │
          │  • Source: BRONZE.RAW_OPPORTUNITIES                     │
          │  • Target: SILVER.OPPORTUNITIES                         │
          │  • Transformation: flatten_and_type                     │
          │  • Quality: 3% nulls handled, 0 type errors             │
          │  • Lineage: Updated in METADATA.TRANSFORMATION_LOG      │
          │                                                         │
          │  "Pattern saved: Salesforce opportunity ingestion"      │
          └─────────────────────────────────────────────────────────┘

9:05 AM   Sales asks Cortex Analyst:
          
          USER: "What's our pipeline value by stage this quarter?"
          
          AGENT: Queries SILVER.OPPORTUNITIES, returns:
          
          | STAGE           | PIPELINE_VALUE  |
          |-----------------|-----------------|
          | Qualification   | $2,450,000      |
          | Proposal        | $5,820,000      |
          | Negotiation     | $3,100,000      |
          | Closed Won      | $8,750,000      |
```

**Result**: 5 minutes, self-service, auto-refreshing pipeline

---

## What Happens Next Week?

### Schema Changes Automatically

```
Tuesday   Salesforce admin adds new field "competitor_name"
          
          ┌─────────────────────────────────────────────────────────┐
          │  🤖 AGENT DETECTS SCHEMA CHANGE                         │
          │                                                         │
          │  "New field detected: competitor_name (VARCHAR)"        │
          │  "Updating SILVER.OPPORTUNITIES transformation..."      │
          │  "Added column, no breaking changes"                    │
          │                                                         │
          │  ✓ Schema evolved automatically                         │
          └─────────────────────────────────────────────────────────┘
```

### Quality Issues Caught Proactively

```
Wednesday Bad data from integration error
          
          ┌─────────────────────────────────────────────────────────┐
          │  🤖 AGENT DETECTS ANOMALY                               │
          │                                                         │
          │  "Alert: 45% null rate in 'amount' field"               │
          │  "Normal baseline: 3%"                                  │
          │  "Action: Quarantined batch, notified data team"        │
          │                                                         │
          │  ⚠️ Slack notification sent to #data-quality            │
          └─────────────────────────────────────────────────────────┘
```

---

## The Contrast

| Metric | Traditional | Agentic |
|--------|-------------|---------|
| Time to first query | 8 days | 5 minutes |
| Schema change response | Pipeline breaks | Auto-adapts |
| Quality issue detection | User complaints | Proactive alert |
| Engineering effort | 16 hours | 0 hours |
| Business user access | IT ticket | Natural language |

---

## How This Maps to the Demo

This example demonstrates the workflow implemented in the demo scripts:

| Phase | Demo Implementation |
|-------|---------------------|
| **DISCOVER** | `02_discovery_tools.sql` → `DISCOVER_SCHEMA()`, `CORTEX_INFER_SCHEMA()` |
| **REASON** | `03_transformation_agent.sql` → Agent with `analyze_quality` tool |
| **EXECUTE** | `05_agentic_workflow.sql` → Dynamic Table generation |
| **REFLECT** | `06_workflow_engine.sql` → `REFLECT_ON_WORKFLOW()`, lineage logging |

### Try It Yourself

```sql
-- 1. Simulate new data arrival
INSERT INTO BRONZE.RAW_CUSTOMERS (raw_payload) 
SELECT PARSE_JSON('{"id": "NEW01", "first_name": "Test", "last_name": "User", "email": "test@example.com", "segment": "smb", "annual_revenue": 50000}');

-- 2. Watch the Stream detect it
SELECT SYSTEM$STREAM_HAS_DATA('METADATA.BRONZE_CUSTOMERS_STREAM');

-- 3. Run the transformation workflow
CALL AGENTS.RUN_TRANSFORMATION_WORKFLOW('AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS', 'flatten_and_type');

-- 4. Check what the agent did
SELECT * FROM METADATA.WORKFLOW_STATE ORDER BY started_at DESC LIMIT 1;

-- 5. Query the result
SELECT * FROM SILVER.CUSTOMERS WHERE customer_id = 'NEW01';
```

---

## Key Takeaway

> **Traditional ETL**: Humans write code, pipelines break, users wait  
> **Agentic Transformation**: AI discovers data, generates pipelines, users query immediately

The agent doesn't replace data engineers—it handles the repetitive discovery and transformation work so engineers can focus on complex business logic and architecture.
