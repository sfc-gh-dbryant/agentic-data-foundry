# Agentic Data Foundry — Demo Talk Track

**Duration:** 12-15 min (core) | 20-25 min (with Show & Tell deep dives)
**Audience:** Data leaders, architects, engineers evaluating Snowflake's AI and data platform capabilities

> **Tip:** Show & Tell sections are optional. Use them for technical audiences or when someone asks "how does that work?" For executive audiences, skip them and keep the narrative flowing.

---

## Opening (60 seconds)

> "What I'm going to show you is an end-to-end data platform built entirely on Snowflake. We start with an operational PostgreSQL database — managed by Snowflake — and in about 10 minutes we'll have production-ready analytics that business users can query in plain English. The key differentiator: the Silver and Gold transformations are driven by AI agents using Cortex LLM, not hand-coded ETL."

> "Here's the journey we'll take:
>
> 1. **Generate Data** — We spin up a live PostgreSQL database and populate it with realistic business data
> 2. **Pipeline Status** — Watch Openflow CDC replicate changes in real time, and see Bronze Dynamic Tables wrap everything into schema-flexible VARIANT
> 3. **Agentic Workflow** — This is the star of the show. An AI agent analyzes raw data, generates Silver transformations, validates quality, and learns from every run
> 4. **Gold Layer** — We build business-ready aggregations — four by hand, then let the AI discover what's missing and build the rest autonomously
> 5. **Semantic Views** — The AI generates business-friendly metadata so anyone can understand what the data means
> 6. **AI Chat** — The payoff. Business users ask questions in plain English and get answers backed by governed, production-quality data
> 7. **Knowledge Graph** — A live, self-updating map of the entire platform — every table, every relationship, every lineage path
>
> Everything you'll see — the CDC, the agents, the transformations, the chat — runs natively inside Snowflake. No external tools, no data movement, no glue code. Let's go."

---

## Tab 2: Generate Data (1 min)

> "We start with a simulated OLTP application. This is Snowflake Managed PostgreSQL — a fully managed Postgres instance running inside Snowflake's ecosystem. We're generating realistic business data: customers, orders, products, order items, and support tickets.

> The app connects directly to PostgreSQL using External Access Integrations — this Streamlit app running in Snowflake can securely reach out to the Postgres instances using stored credentials.

> Behind the scenes, there are two Postgres instances: a SOURCE that simulates the application, and a LANDING that receives changes via PostgreSQL logical replication. This is the same pattern you'd see in production — source database stays clean, CDC happens from a replica."

**[Click Generate Data]**

### Show & Tell: PostgreSQL Connection Architecture
> *"Let me show you what's under the hood..."*
- **Show** the `get_pg_connection()` function in the Streamlit code — `_snowflake.get_username_password()` retrieves credentials securely without exposing them in code
- **Show** the `CREATE STREAMLIT` statement with `EXTERNAL_ACCESS_INTEGRATIONS` and `SECRETS` — this is how SiS apps get network access
- **Data check:** Query the SOURCE Postgres directly: `SELECT COUNT(*) FROM customers` — prove the data is real

---

## Tab 3: Pipeline Status (1 min)

> "Now let's see what happened. Openflow — Snowflake's built-in CDC engine — is continuously replicating changes from the Landing Postgres into Snowflake. It adds CDC metadata columns: `_SNOWFLAKE_DELETED`, `_SNOWFLAKE_UPDATED_AT` — so we always know the state of every record.

> The data lands in the `public` schema as raw tables. From there, Dynamic Tables take over. Our Bronze layer wraps everything into VARIANT using `OBJECT_CONSTRUCT(*)` — this gives us schema-on-read flexibility. If the source schema changes, Bronze doesn't break.

> All of this is declarative. No Airflow, no scheduler, no orchestration code. Dynamic Tables handle the refresh automatically with a 1-minute target lag."

### Show & Tell: Bronze VARIANT Pattern
> *"Let me show you what a Bronze table actually looks like..."*
- **Show the DDL:** `SELECT GET_DDL('TABLE', 'DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT')` — reveal the actual Dynamic Table definition:
  ```sql
  CREATE OR REPLACE DYNAMIC TABLE CUSTOMERS_VARIANT(
      PAYLOAD, SOURCE_TABLE, INGESTED_AT
  ) TARGET_LAG = '1 minute'
    REFRESH_MODE = AUTO
    WAREHOUSE = DBRYANT_COCO_WH_S
  AS
  SELECT
      OBJECT_CONSTRUCT(*) AS PAYLOAD,
      'CUSTOMERS' AS SOURCE_TABLE,
      CURRENT_TIMESTAMP() AS INGESTED_AT
  FROM DBAONTAP_ANALYTICS."public"."customers";
  ```
- **Explain:** "Three lines of SQL. `OBJECT_CONSTRUCT(*)` wraps the entire row — including CDC metadata — into a single VARIANT column. If the source adds a column tomorrow, this table doesn't break."
- **Query:** `SELECT * FROM BRONZE.CUSTOMERS_VARIANT LIMIT 3` — show the PAYLOAD variant column containing the full row as JSON
- **Query:** `SELECT PAYLOAD:customer_id, PAYLOAD:first_name, PAYLOAD:_SNOWFLAKE_DELETED FROM BRONZE.CUSTOMERS_VARIANT LIMIT 3` — show how you extract typed fields from VARIANT

---

## Tab 4: Agentic Workflow (2 min) — THE STAR OF THE SHOW

> "This is where it gets interesting. Traditional medallion architectures require a data engineer to hand-code every Silver transformation — column typing, deduplication, null handling, naming conventions. That's the bottleneck.

> Our agentic workflow replaces that with a five-phase AI pipeline:

> 1. **Trigger** — Detects which Bronze tables need processing.
> 2. **Planner** — An LLM agent examines each Bronze table's VARIANT schema, samples data quality, checks past learnings, and decides the transformation strategy.
> 3. **Executor** — The LLM generates the actual `CREATE DYNAMIC TABLE` DDL for each Silver table, respecting schema contracts we've defined. Includes self-correction if DDL fails.
> 4. **Validator** — Runs the DDL, checks row counts, validates data quality.
> 5. **Reflector** — Reviews what happened and stores learnings for next time.

> The schema contracts are the guardrails. They enforce column naming standards — for example, CDC columns must use `_SNOWFLAKE_DELETED`, not `IS_DELETED`. The LLM proposes, the contracts constrain."

**[Click Run Agentic Workflow]**

> "Watch the phases execute. The LLM is calling Claude via Cortex — all within Snowflake's security perimeter. No data leaves the platform."

### Show & Tell: The Agentic Brain (Best for Technical Audiences)
> *"Let me pull back the curtain on what the LLM is actually doing..."*
- **Show** the `WORKFLOW_PLANNER` procedure — highlight the prompt construction that feeds schema analysis + data quality + past learnings to the LLM
- **Show** a sample planner decision from `METADATA.WORKFLOW_EXECUTIONS`:
  ```sql
  SELECT EXECUTION_ID, PLANNER_OUTPUT
  FROM METADATA.WORKFLOW_EXECUTIONS
  ORDER BY STARTED_AT DESC LIMIT 1
  ```
- **Highlight:** The LLM chose `flatten_and_type` strategy, identified the primary key, decided on deduplication logic — all from analyzing the raw VARIANT data
- **Show** the Schema Contract: `SELECT * FROM METADATA.SILVER_SCHEMA_CONTRACTS WHERE SOURCE_TABLE_PATTERN = 'CUSTOMERS'` — "This is why the LLM uses `_SNOWFLAKE_DELETED` instead of making up its own column name"

### Show & Tell: Data Quality Guardrails
> *"The agents don't just generate SQL and hope for the best — there are built-in quality checks..."*
- **Planner-side (upstream):** Before any DDL is generated, the Planner calls `ANALYZE_DATA_QUALITY(table, 500)` which samples 500 rows and feeds a quality report to the LLM — nulls, duplicates, type casting issues are detected *before* the transformation strategy is chosen
- **Validator-side (downstream):** After DDL executes, the Validator compares row counts between Bronze source and Silver target:
  ```sql
  count_variance := ABS(source_count - target_count) * 100.0 / NULLIF(source_count, 0);
  -- Passes if variance < 5%
  ```
- **Show** validation results: `SELECT source_table, target_table, passed, variance_pct, details FROM METADATA.VALIDATION_RESULTS WHERE EXECUTION_ID = (SELECT MAX(EXECUTION_ID) FROM METADATA.WORKFLOW_EXECUTIONS)`
- **Explain:** "If Silver loses more than 5% of rows compared to Bronze, the validation fails. Every run is logged to `VALIDATION_RESULTS` so you have a full audit trail."

### Show & Tell: Active Learnings (The Memory)
> *"The agents don't just run and forget — they remember..."*
- **Scroll down** to the "🧠 Active Learnings" section (below the workflow dashboard table)
- **Click each 💡 expander** to reveal what the Reflector learned — each shows a learning type, observation, recommendation, and confidence score
- **Explain:** "After every run, the Reflector phase reviews what happened and stores patterns. Next time the workflow runs, the Planner reads these learnings and applies them. The system gets smarter over time — if it learned that PRODUCTS has a column called NAME not PRODUCT_NAME, it won't make that mistake again."
- **Key point for the audience:** "This is the difference between AI-generated and AI-*agentic*. Generated is one-shot. Agentic learns, adapts, and improves."

### Show & Tell: Silver Data Quality
> *"Let's verify the output is actually correct..."*
- **Query:** `SELECT * FROM SILVER.CUSTOMERS LIMIT 5` — show clean, typed columns extracted from VARIANT
- **Query:** Compare counts: `SELECT COUNT(*) FROM BRONZE.CUSTOMERS_VARIANT` vs `SELECT COUNT(*) FROM SILVER.CUSTOMERS` — prove deduplication worked
- **Query:** `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = 'CUSTOMERS'` — show the LLM correctly typed every column

---

## Checkpoint: Knowledge Graph (30 seconds)

> "Before we build Gold, let me show you something. Flip to the Knowledge Graph tab."

**[Click Knowledge Graph tab]**

> "This graph is built dynamically from live metadata — it's not a static diagram. Right now you can see Bronze→Silver lineage for all 5 tables. After we build Gold, new edges will appear automatically showing Silver→Gold relationships. Watch for that."

> *"This is the system's self-awareness — it knows its own structure."*

**[Click back to Gold Layer tab]**

---

## Tab 5: Gold Layer (2 min)

> "Silver gives us clean, typed, deduplicated tables. Gold gives us business value. We start by building four core Gold Dynamic Tables:

> - **CUSTOMER_360** — RFM analysis, lifetime value, engagement scoring
> - **PRODUCT_PERFORMANCE** — margins, units sold, revenue by product
> - **ORDER_SUMMARY** — monthly trends by customer segment
> - **ML_CUSTOMER_FEATURES** — encoded features ready for model training

> These are all Dynamic Tables with `TARGET_LAG = DOWNSTREAM`, meaning they refresh only when something downstream needs them. Zero wasted compute."

**[Click Build Core Gold Layer]**

> "Now here's where it gets interesting. We built four Gold tables — but we have *five* Silver tables. Let's see if the AI can figure out what's missing."

**[Click Agentic Gold Build]**

> "The agentic builder scans every Silver table and checks whether it has dedicated Gold-layer coverage. It's not just checking names — it's reasoning about what's covered and what's not."

> *Wait for the agentic build to complete (~30-60 seconds).*

> "Look at what it found — Silver tables that are used in joins inside other Gold tables but don't have their own dedicated Gold aggregation. The AI recognized those gaps and built new tables autonomously — for example, a **CUSTOMER_METRICS** table aggregating support and order data per customer.

> This is the 'aha' moment. A human engineer might not catch that — they'd see the data flowing into joins and assume it's covered. The AI looked at coverage *per source table* and said 'these deserve their own analytical surface.' That's what happens when a new source table appears in production — you don't hand-code the Gold layer, the AI builds it."

> *Note: The AI generates table names and structures dynamically based on what it finds. The exact tables created may vary between runs.*

### Show & Tell: What the AI Actually Built
> *"Let me show you the DDL the AI generated..."*
- **Query:** `SHOW DYNAMIC TABLES IN SCHEMA DBAONTAP_ANALYTICS.GOLD` — identify the AI-generated tables (any beyond the 4 core tables were built by the agent)
- **Query:** `SELECT GET_DDL('TABLE', 'DBAONTAP_ANALYTICS.GOLD.<AI_GENERATED_TABLE>')` — show the AI-generated aggregation logic
- **Key point:** "Notice the AI chose the right deleted-row filter for each table, picked meaningful aggregations, and even joined with related tables for business context — all autonomously."

### Show & Tell: Customer 360 Deep Dive
> *"Let me show you what a Gold table looks like in practice..."*
- **Query:** `SELECT FULL_NAME, SEGMENT, LOYALTY_TIER, TOTAL_ORDERS, LIFETIME_VALUE, REVENUE_TIER, ENGAGEMENT_STATUS FROM GOLD.CUSTOMER_360 ORDER BY LIFETIME_VALUE DESC LIMIT 10`
- **Explain:** "This single table joins Customers, Orders, and Support Tickets. It computes RFM scores, revenue tiers, and engagement status — all as a Dynamic Table that refreshes automatically."
- **Query:** `SELECT PRODUCT_NAME, CATEGORY, MARGIN_PERCENT, UNITS_SOLD, TOTAL_REVENUE FROM GOLD.PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC LIMIT 5` — "Product margin analysis, ready for a dashboard."

---

## Tab 6: Schema Contracts (30 seconds)

> "Quick look at the contracts. These persist across resets — they're the institutional knowledge of your data team encoded as rules. Each table has required columns, data types, and naming conventions. The agentic workflow reads these before generating any DDL."

### Show & Tell: Contract Enforcement (If Asked)
- **Show** a contract: `SELECT SOURCE_TABLE_PATTERN, NAMING_RULES:note::VARCHAR FROM METADATA.SILVER_SCHEMA_CONTRACTS`
- **Explain:** "The PRODUCTS contract says 'Column is NAME not PRODUCT_NAME.' Without this, the LLM might rename columns inconsistently between runs. Contracts make the output deterministic."

---

## Tab 7: Semantic Views (1 min)

> "Now we bridge the gap between data engineering and business users. Semantic Views are Snowflake's way of describing your data in business terms — dimensions, facts, metrics, and relationships.

> The app offers three approaches: pure Agentic (LLM-only), Knowledge Graph (rule-based from metadata), or Hybrid (recommended) which uses the KG structure as a base and enriches it with LLM-generated descriptions and synonyms."

**[Click ⭐ Generate Hybrid]**

> "The Hybrid approach first populates a Knowledge Graph from live schema metadata, then the LLM enriches each Semantic View with business-friendly descriptions and column synonyms. This gives us the best of both worlds — deterministic structure with intelligent context."

### Show & Tell: What a Semantic View Looks Like
> *"Let me show you what the LLM actually generated..."*
- **Query:** `SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD` — list all generated SVs (named `*_HYBRID_SV`)
- **Query:** `SELECT GET_DDL('SEMANTIC VIEW', 'DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360_HYBRID_SV')` — show the full Semantic View DDL with TABLES, FACTS, DIMENSIONS, METRICS sections
- **Explain:** "The LLM looked at the CUSTOMER_360 table, understood that SEGMENT is a dimension, LIFETIME_VALUE is a fact, and 'Average Order Value' is a meaningful metric. The synonyms help Cortex Analyst understand alternate ways users might ask about the same data. This is what powers the natural language querying."

---

## Tab 8: AI Chat (1 min) — THE PAYOFF

> "And here's the payoff. Business users can now ask questions in plain English and get answers backed by governed, production-quality data."

**Suggested questions to ask live:**
1. *"Who are our top 5 customers by lifetime value?"* — proves the pipeline works end-to-end
2. *"What's our best-selling product by revenue?"* — shows cross-table intelligence
3. *"Show me order trends by customer segment"* — demonstrates aggregation awareness

> "This is Cortex Analyst under the hood, powered by the Semantic Views we just generated. The SQL is generated, executed, and results returned — all within Snowflake."

### Show & Tell: SQL Behind the Answer
> *"Let me show you what's happening behind the scenes..."*
- After asking a question, **show the generated SQL** — "Cortex Analyst translated plain English into this SQL query, using the Semantic View as its guide"
- **Explain:** "The Semantic View told the AI that 'lifetime value' maps to the LIFETIME_VALUE column in CUSTOMER_360, and that it should be aggregated with SUM. That's why the answer is accurate."

---

## Tab 9: Knowledge Graph (30 seconds)

> "The Knowledge Graph tab shows the full data lineage — Bronze to Silver to Gold — rendered dynamically based on what actually exists. This is live metadata, not a static diagram."

### Show & Tell: Dynamic Lineage
- **Point out** that the graph shows actual tables from INFORMATION_SCHEMA, not hard-coded names
- **Explain:** "If we added a new source table, the graph would update automatically after the agentic workflow runs."

---

## Closing (30 seconds)

> "So what did we just do? In about 10 minutes, we went from an empty database to a fully operational analytics platform. PostgreSQL source, real-time CDC, AI-driven transformations with guardrails, business-ready aggregations, and natural language querying.

> Every component is native Snowflake — no external tools, no separate compute, no data leaving the platform.

> The agentic pattern is the key innovation here. When your source schema changes or a new table appears, the AI agents adapt. That's the future of data engineering — not replacing engineers, but letting them focus on business logic while AI handles the plumbing."

---

## Appendix: Key Talking Points by Audience

| Audience | Emphasize | Show & Tell Focus |
|----------|-----------|-------------------|
| **CTO / VP Data** | Speed to value, no external tools, governed AI | Skip most code, show Chat results |
| **Data Architects** | Dynamic Tables, CDC pattern, medallion architecture | Bronze VARIANT, Silver dedup, Gold aggregations |
| **Data Engineers** | Agentic workflow, schema contracts, LLM guardrails | Planner prompts, contract enforcement, DDL generation |
| **Analytics / BI** | Semantic Views, natural language querying | Semantic View DDL, Chat SQL generation |
| **Security / Compliance** | EAI, secrets management, data never leaves Snowflake | CREATE STREAMLIT with SECRETS, Cortex in-platform |

---

## Quick Reference: Demo Flow

```
Generate Data ──→ Pipeline Status ──→ Agentic Workflow ──→ Gold Layer
     │                   │                    │                  │
     │                   │                    │                  │
  [Show:PG]        [Show:Bronze]      [Show:Planner+DDL]  [Show:360 data]
                                                                 │
                                                                 ▼
Schema Contracts ──→ Semantic Views ──→ AI Chat ──→ Knowledge Graph
     │                    │                │               │
  [Show:Rules]      [Show:SV DDL]   [Show:SQL]       [Show:Lineage]
```
