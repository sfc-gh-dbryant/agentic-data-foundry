# The Agentic Data Foundry: From Building Pipelines to Describing Intent

**A Whitepaper on AI-Native Data Engineering**

*March 2026*

---

## Abstract

The data engineering discipline is hitting a wall. For two decades, we've tricked ourselves into thinking that hand-coding ETL scripts and manually mapping schemas was "engineering." In reality, it was just expensive plumbing. As we enter the era of the Agentic Enterprise, this manual approach isn't just slow — it's a systemic liability.

Snowflake has provided the world's most powerful engine with Cortex AI and Project SnowWork, but these tools are only as effective as the data they consume. If your "Back-of-House" data supply chain is still a web of brittle, human-dependent pipes, your AI agents will inevitably fail.

The Agentic Data Foundry is the missing architectural layer. It represents a fundamental shift from **Building** to **Describing**. By leveraging Snowflake's native capabilities — Openflow for ingestion, Dynamic Tables for orchestration, and Cortex for reasoning — we have built a system that autonomously discovers, transforms, and validates data. In this paradigm, the data engineer doesn't write code; they **curate intent**. This is the blueprint for an autonomous data lifecycle that moves at the speed of business thought, ensuring that Snowflake is the central **System of Action** for the modern enterprise.

---

## 1. Introduction: The Case for Change — Escaping the Maintenance Trap

Every modern data leader is currently paying a "plumbing tax" that is bankrupting their roadmap. We've spent two decades perfecting progressive refinement — staging data through capture, conformance, and consumption layers — yet we're still stuck in a cycle of manual labor: a source DB changes a column name, a pipeline breaks, an executive sees a blank dashboard, and an engineer spends Friday night debugging SQL.

We don't have a data problem; we have a **coordination problem**. Project SnowWork and the era of "Agentic Enterprises" promise a world where business users just "ask and get." But if your underlying data infrastructure still relies on hand-coded brittle pipes, those agents are just going to hallucinate at scale. The Agentic Data Foundry isn't just a new tool; it's the mandatory "back-of-house" engine that makes the agentic future possible. We are moving the engineer from the engine room to the bridge, where they don't turn the gears — they set the course.

Modern data teams spend an estimated 40-60% of their time on pipeline maintenance rather than value creation [1]. Schema changes break downstream transformations. New data sources require weeks of manual onboarding. Quality issues propagate silently through layers until they surface in executive dashboards. The progressive refinement pattern (Bronze → Silver → Gold) provided a useful organizational model, but the *implementation* of that pattern remains overwhelmingly manual.

Meanwhile, the AI landscape has shifted dramatically. Gartner predicts that 40% of enterprise applications will feature task-specific AI agents by 2026, up from less than 5% in 2025 [2]. McKinsey's research on agentic AI identifies autonomous task completion as a defining capability of the next wave of enterprise AI adoption [3]. Snowflake's own platform evolution — with Cortex AI, Dynamic Tables, and Cortex Agents — has created the infrastructure for AI-native data engineering [4].

Snowflake's strategic direction validates this trajectory. In March 2026, the company announced Project SnowWork — an autonomous enterprise AI platform designed to help business users "simply ask for what they need" and have AI "securely complete multi-step tasks based on conversational prompts" [14]. CEO Sridhar Ramaswamy described the vision: "We are entering the era of the agentic enterprise... embedding intelligence directly into the operating fabric of the enterprise." Industry analyst Sanjeev Mohan (SanjMo) observed that Snowflake is "extending its platform from a system of insight to a system of action, which is where measurable business value is ultimately realized" [14]. The Agentic Data Foundry embodies this same architectural principle — applied specifically to data engineering — where governed data, shared business definitions, and autonomous execution converge to replace manual pipeline construction with intent-driven automation.

The Agentic Data Foundry synthesizes these trends into a working system. It is not a theoretical framework but a deployed platform that has autonomously discovered tables, generated transformation logic, validated results, and learned from its own execution history — all guided by human-defined intent rather than human-written code.

---

## 2. The Core Thesis: Describe, Don't Build

### 2.1 The Traditional Model

In conventional data engineering, the practitioner's primary artifact is *code*: SQL scripts, Python transformations, orchestration DAGs, and configuration files. The data engineer must:

1. Understand the source schema
2. Design the target schema
3. Write transformation logic
4. Handle edge cases (nulls, type mismatches, CDC semantics)
5. Deploy, schedule, and monitor the pipeline
6. Debug failures and adapt to schema changes

Each step requires deep technical knowledge and intimate familiarity with the specific data. The result is brittle, person-dependent pipelines where the "how" overwhelms the "what."

### 2.2 The Agentic Model

The Agentic Data Foundry inverts this relationship. The human practitioner's primary artifacts become three types of *metadata*:

| Artifact | Purpose | Example |
|----------|---------|---------|
| **Schema Contracts** | Define structural constraints — column names, types, required fields | `CUSTOMERS` must have `CUSTOMER_ID (INTEGER)`, `EMAIL (VARCHAR)`, `IS_DELETED (BOOLEAN)` |
| **Transformation Directives** | Declare business intent — what the data is *for* | "This data feeds a churn prediction model. Preserve daily granularity. Create 7/14/30 day rolling averages." |
| **Learnings** | Capture accumulated knowledge from past executions | "Tables with `_SNOWFLAKE_UPDATED_AT` require CDC deduplication via `ROW_NUMBER()` partitioned by primary key." |

The AI agents then autonomously execute the full pipeline lifecycle: discovery, schema inference, transformation generation, validation, and optimization. The human remains "in the middle" — not writing the pipeline, but *describing* what the pipeline should achieve and *constraining* how it should behave.

This shift mirrors a broader industry trend. As Databricks observed, "AI is transforming data engineering" by automating schema inference, anomaly detection, and pipeline generation [5]. Capgemini's research on AI-driven data integration similarly concludes that the future lies in "AI-powered orchestration of data flows rather than manual pipeline construction" [6].

---

## 3. Architecture: The Five Layers

### 3.1 Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    AGENTIC DATA FOUNDRY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │                  │
│  │ Activated│    │ Agentic  │    │ Agentic  │                  │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘                  │
│       │               │               │                        │
│  CDC Ingestion   LLM-Generated   LLM-Generated                │
│  Schema-on-Read  Dynamic Tables  Aggregation DTs               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  CONTROL PLANE                                                  │
│  Schema Contracts │ Directives │ Learnings │ Knowledge Graph   │
├─────────────────────────────────────────────────────────────────┤
│  AGENT LAYER                                                    │
│  Trigger → Planner → Executor → Validator → Reflector          │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Intent-to-Insight: A Single Data Point's Journey

To illustrate the end-to-end autonomous path, consider a single CDC event — a customer's `updated_at` timestamp changes in the PostgreSQL source — and trace it through to a Gold-layer "Churn Prediction" feature, with zero human-written transformation code:

1. **PostgreSQL SOURCE** → Logical replication propagates the row change to LANDING_PG
2. **Openflow CDC** → Row appears in Snowflake `public.customers` with `_SNOWFLAKE_UPDATED_AT` set
3. **Bronze DT** → `OBJECT_CONSTRUCT(*)` absorbs the change into `BRONZE.CUSTOMERS_VARIANT` (VARIANT payload)
4. **Stream fires** → `CUSTOMERS_LANDING_STREAM` triggers `AGENTIC_WORKFLOW_TRIGGER_TASK`
5. **Planner** → LLM reads Schema Contract + Directive ("churn prediction: compute RFM features") + Learnings
6. **Executor** → LLM generates `CREATE OR REPLACE DYNAMIC TABLE SILVER.CUSTOMERS` with `ROW_NUMBER()` CDC dedup + derived `RECENCY_DAYS`, `SEGMENT`
7. **Validator** → Row count ±5%, null rates compared, contract columns verified
8. **Silver DT refreshes** → `GOLD.ML_CUSTOMER_FEATURES` auto-refreshes via Dynamic Table dependency chain, computing `IS_CHURNED`, `LIFETIME_VALUE`, `FREQUENCY`
9. **KG + Semantic View** → Cortex Analyst receives the updated Semantic View; business user asks: "Which customers are at risk of churning?"

Total human code written: **zero**. Total human artifacts: one Schema Contract, one Transformation Directive.

### 3.3 The Bronze Layer: AI-Activated Ingestion

The Bronze layer is the entry point where raw data arrives from source systems. In the Agentic Data Foundry, Bronze is not merely a landing zone — it is an *activation layer* where AI begins adding value from the first moment data enters the platform.

**Openflow CDC Ingestion.** Source tables from PostgreSQL arrive via Openflow's Change Data Capture (CDC) connector, which adds system columns (`_SNOWFLAKE_DELETED`, `_SNOWFLAKE_UPDATED_AT`) for change tracking. Landing tables appear in a `public` schema that mirrors the source database structure.

**Autonomous Discovery and Onboarding.** The `DISCOVER_AND_ONBOARD_NEW_TABLES` procedure continuously scans for new landing tables that lack corresponding Bronze representations. When a new table is detected, the system autonomously:

1. Creates a Stream on the landing table for CDC event detection
2. Creates a Bronze Dynamic Table using `OBJECT_CONSTRUCT(*)` to preserve the full payload as a VARIANT column
3. Registers the onboarding event in metadata

This pattern — storing raw data as schema-on-read VARIANT — provides a critical advantage: the Bronze layer absorbs schema changes without breaking. New columns appear automatically in the VARIANT payload. Dropped columns simply stop appearing. The system never needs to `ALTER TABLE` to accommodate source changes.

**AI in the Bronze Layer.** The concept of embedding AI early in the data pipeline, rather than only at the analytics layer, provides what Heisler and Frere describe as a "mechanical advantage, akin to moving the fulcrum on a lever closer to the load" [7]. By enriching data with AI-derived signals at ingestion time, downstream consumers benefit without additional effort. In the Agentic Data Foundry, this manifests as:

- LLM-powered schema inference at discovery time
- Semantic similarity search to find analogous tables for pattern reuse
- Automatic classification of data sensitivity and business domain

### 3.4 The Silver Layer: Agentic Transformation

The Silver layer is where the agentic paradigm most visibly departs from traditional data engineering. Rather than human-authored transformation scripts, Silver tables are generated by an autonomous 5-phase workflow. Each phase is a distinct agent responsibility with defined inputs, outputs, and failure modes.

#### 3.4.1 Phase 1: The Trigger — Event-Driven Activation

The Trigger is the system's nervous system. It doesn't poll on a schedule — it *reacts* to structural events in the data platform. Three event classes activate the agentic workflow:

1. **New Table Detection.** When `DISCOVER_AND_ONBOARD_NEW_TABLES` finds a landing table with no corresponding Bronze DT, it creates the Bronze infrastructure and fires a Stream. The Stream's `SYSTEM$STREAM_HAS_DATA()` check activates `AGENTIC_WORKFLOW_TRIGGER_TASK`, which scans for Bronze tables lacking Silver counterparts.

2. **Schema Drift Detection.** The system compares the current Bronze VARIANT payload keys against the last-known schema snapshot stored in metadata. New keys trigger a re-planning cycle; dropped keys trigger a downstream impact assessment via the Knowledge Graph before any action is taken.

3. **Quality Threshold Breach.** If a scheduled validation detects that an existing Silver table has drifted beyond acceptable tolerances (>5% row count variance, null rate spike, or distribution anomaly), the Trigger initiates a regeneration cycle rather than patching the existing table.

The Trigger's output is a *work order* — a JSON payload containing the Bronze table name, the detected event type, the current schema snapshot, and any relevant downstream dependencies. This work order is the Planner's input.

#### 3.4.2 Phase 2: The Planner — Context Assembly and Strategy

The Planner is the most LLM-intensive phase and the one where the system's intelligence is most visible. Its job is not to write SQL — it is to *decide what SQL should be written*. The Planner assembles a rich context window from four sources:

1. **Schema Context.** The Bronze table's VARIANT keys, inferred types, sample values, and cardinality estimates. For a `CUSTOMERS_VARIANT` table, this includes the knowledge that `CUSTOMER_ID` is an integer primary key, `EMAIL` contains valid email addresses, and `_SNOWFLAKE_UPDATED_AT` is the CDC timestamp.

2. **Contract and Directive Context.** Any active Schema Contract for the target Silver table (column names, types, required flags) and any matching Transformation Directives (business intent like "churn prediction: compute RFM features"). The contract constrains the output shape; the directive guides the transformation logic.

3. **Knowledge Graph Context.** Vector similarity search finds analogous tables that have been successfully transformed before. If `INVOICES_VARIANT` arrives and `ORDERS_VARIANT` was previously transformed with a specific CDC dedup pattern, that pattern is surfaced as a candidate strategy. The KG also provides downstream dependency information — who consumes the Silver table and what they expect.

4. **Learnings Context.** All active learnings matching the table's pattern signature are injected. This includes both positive patterns ("CDC tables with `_SNOWFLAKE_UPDATED_AT` require `ROW_NUMBER()` dedup") and negative patterns ("Window functions on SUPPORT_TICKETS cause timeout due to partition skew").

The Planner's output is a *transformation strategy* — not SQL, but a structured plan: "Use ROW_NUMBER() dedup on `_SNOWFLAKE_UPDATED_AT`, extract and type-cast these 12 columns, derive `RECENCY_DAYS` from `LAST_ORDER_DATE`, apply the churn prediction directive by computing RFM features." This strategy document is the Executor's input.

#### 3.4.3 Phase 3: The Executor — SQL Generation with Self-Correction

The Executor is where strategy becomes DDL. The LLM (Claude 3.5 Sonnet) takes the Planner's strategy and generates a complete `CREATE OR REPLACE DYNAMIC TABLE` statement with:

- CDC-aware deduplication using `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY _SNOWFLAKE_UPDATED_AT DESC)`
- Type-safe column extraction from the VARIANT payload (`SRC:customer_id::INTEGER`)
- Derived columns specified by the Transformation Directive
- `TARGET_LAG` set to `'1 minute'` for near-real-time refresh
- Soft-delete filtering (`WHERE _SNOWFLAKE_DELETED = FALSE`)

**The Self-Correction Loop.** If the generated DDL fails compilation, the Executor doesn't give up — it learns from the error. The Snowflake error message is injected back into the LLM prompt alongside the failed DDL: *"Your DDL failed with: 'SQL compilation error: invalid identifier SRC:full_name'. The VARIANT payload contains 'first_name' and 'last_name' but not 'full_name'. Fix the DDL."* The system retries up to 3 times, with each attempt receiving the accumulated error context from all previous failures.

For Gold-layer DDL, the Executor additionally runs through the `VALIDATE_GOLD_DDL` procedure, which parses FROM/JOIN references and verifies each table exists in `INFORMATION_SCHEMA` — catching the 15-20% of first-attempt DDLs where the LLM hallucinates a table name.

#### 3.4.4 Phase 4: The Validator — Deterministic Trust Boundary

This is the phase that separates a toy demo from a production system. The Validator is the hard boundary between AI-generated optimism and production reality. We treat every agent-generated SQL statement as **guilty until proven innocent**.

The Validator isn't an AI "vibe check." It is a battery of deterministic, hard-coded guardrails:

1. **Row Count Parity.** The Silver table's row count must be within ±5% of the source Bronze count (after dedup). A 10% delta means something was silently dropped or duplicated — the DDL is rejected.

2. **Schema Contract Enforcement.** Every column declared in the Schema Contract must be present with the correct data type. If the contract says `CUSTOMER_ID INTEGER` and the DDL produced `CUSTOMER_ID VARCHAR`, that's a failure — regardless of whether the data "looks fine."

3. **Statistical Profiling.** Null rates, distinct counts, and value distributions are compared column-by-column against the Bronze source. A Silver column with 40% nulls when the Bronze source has 0% triggers an investigation — either the type cast is wrong or the dedup logic is filtering incorrectly.

4. **Referential Integrity.** Foreign key columns are checked against parent tables. `CUSTOMER_ID` in ORDERS must exist in CUSTOMERS. Orphaned references indicate a join or filter error in the generated DDL.

5. **Semantic Assertions.** The LLM generates domain-specific checks based on column semantics and accumulated Learnings: monetary values are non-negative, dates are not in the future, email formats match regex patterns, status fields contain only valid enum values.

If a generated pipeline fails any of these checks, it is **rejected before it ever touches production**. We don't ask the AI to be "perfect"; we build a system that makes it impossible for the AI to be "wrong" in a way that affects the business. Validation failures feed back into the Executor's self-correction loop with specific diagnostic information.

#### 3.4.5 Phase 5: The Reflector — Institutional Memory

The Reflector is what transforms the Agentic Data Foundry from a stateless LLM wrapper into a system that *gets smarter over time*. After every workflow execution — whether successful or failed — an LLM analyzes the complete execution trace and extracts three categories of learning:

1. **Success Patterns.** "Tables with temporal data benefit from `ORDER_MONTH` derivation." "CDC tables with compound primary keys require multi-column `PARTITION BY`." These positive patterns seed future Planner prompts, improving first-attempt accuracy.

2. **Failure Patterns (Negative Learnings).** "Nested VARIANT arrays require `LATERAL FLATTEN` before extraction — direct path access returns NULL." "`LEAD()`/`LAG()` on `SUPPORT_TICKETS` causes timeout due to partition skew." These anti-patterns prevent the Planner from repeating expensive mistakes.

3. **Optimization Recommendations.** "Consider clustering on `CUSTOMER_ID` for join performance." "Pre-aggregate by date before applying window functions on high-cardinality tables." These improve query performance without changing correctness.

Each learning is persisted with a confidence score (increases with repeated observation), a pattern signature (hash for matching against future scenarios), and an active/inactive flag (humans can override incorrect learnings). The Reflector phase generates learnings; the Planner phase consumes them — creating a feedback loop where the system improves with every execution. When a senior engineer corrects an agent's join logic, that "scar tissue" is saved forever in the customer's Snowflake metadata.

### 3.5 The Gold Layer: Agentic Aggregation

The Gold layer extends the agentic pattern to business-ready aggregations. The `BUILD_GOLD_FOR_NEW_TABLES` procedure employs a two-strategy discovery approach:

**Strategy 1: Missing Gold Targets.** The `TABLE_LINEAGE_MAP` metadata table declares the intended mapping from Silver to Gold tables (e.g., `SILVER.CUSTOMERS → GOLD.CUSTOMER_360` via `AGGREGATES_TO`). The system identifies Gold targets that exist in the lineage map but not yet in the database.

**Strategy 2: Uncovered Silver Tables.** The system identifies Silver tables that have no lineage mapping at all — tables that arrived after the initial lineage was defined. For these, the LLM proposes appropriate Gold aggregations based on the table's schema and content.

In both cases, the generated Gold Dynamic Tables are validated, their lineage is registered, and the Knowledge Graph is refreshed — all autonomously.

### 3.6 The Ephemeral Nature of Silver and Gold

A defining property of the Agentic Data Foundry is that Silver and Gold layers are *ephemeral* — they are derived, not primary. Because every Silver and Gold table is:

1. Defined by a Dynamic Table DDL (stored in transformation logs)
2. Guided by Schema Contracts and Directives (stored in metadata)
3. Populated from Bronze (the immutable source of truth)

...the entire Silver and Gold layers can be regenerated from scratch at any time. This has profound implications:

- **Schema evolution becomes trivial**: When source schemas change, the agentic workflow regenerates affected tables rather than patching them
- **Environment provisioning is instant**: Dev, staging, and production environments diverge only in their metadata, not their pipeline code
- **Disaster recovery simplifies**: Bronze + metadata = complete system reconstruction
- **Technical debt cannot accumulate**: There is no legacy transformation code to maintain

This aligns with the broader industry movement toward treating analytics layers as *materialized views of intent* rather than independently maintained data stores. As InfoQ noted in their analysis of progressive refinement evolution, "The future of the layered pattern lies in making intermediate layers regenerable rather than permanent" [8].

---

## 4. The Three-Layer Control Model

The Agentic Data Foundry's human-AI interface is organized into three complementary control mechanisms.

### 4.1 Schema Contracts: Structural Guardrails

Schema Contracts define *what the output must look like* — column names, data types, and required fields. They are stored in `METADATA.SILVER_SCHEMA_CONTRACTS` and enforced during the Executor phase.

Without a contract, the LLM freely chooses column names. This non-determinism is problematic: one execution might produce `FULL_NAME`, another `FIRST_NAME` and `LAST_NAME`, breaking downstream consumers. Schema Contracts eliminate this ambiguity.

```json
[
  {"name": "CUSTOMER_ID", "type": "INTEGER", "required": true},
  {"name": "FIRST_NAME", "type": "VARCHAR", "required": true},
  {"name": "LAST_NAME", "type": "VARCHAR", "required": true},
  {"name": "EMAIL", "type": "VARCHAR", "required": true},
  {"name": "IS_DELETED", "type": "BOOLEAN", "required": true}
]
```

Schema Contracts can be:
- Authored manually by data engineers
- Generated from existing Silver tables (reverse-engineering the current state)
- Proposed by an LLM from Bronze schema analysis (with human review)

### 4.2 Transformation Directives: Business Intent

Directives declare *why the data exists* — the business purpose that should guide transformation decisions. They are stored in `METADATA.TRANSFORMATION_DIRECTIVES` and injected into LLM prompts during the Planner phase.

A directive is not a transformation specification. It is a statement of intent:

> "This data feeds a demand forecasting model. Preserve daily granularity. Create 7/14/30 day rolling averages. Exclude test accounts (industry = 'TEST'). The model requires at minimum 90 days of history."

The LLM interprets this intent and generates appropriate SQL. If the directive says "churn prediction," the LLM knows to compute recency, frequency, and monetary (RFM) features. If the directive says "executive dashboard," the LLM knows to pre-aggregate to weekly or monthly granularity.

Directives support:
- **Source table patterns** — apply to specific tables or wildcards
- **Target layers** — Silver, Gold, or both
- **Priority weighting** — higher priority directives override lower ones
- **LLM-assisted generation** — the system can draft directives from a use case description

### 4.3 Learnings: Accumulated Memory

Learnings capture patterns discovered during execution and persist them for future use. They function as the system's institutional memory — the equivalent of tribal knowledge that typically exists only in senior engineers' heads.

Each learning has:
- A **pattern signature** (hash for matching against future scenarios)
- An **observation** ("CDC tables require IS_DELETED filtering")
- A **recommendation** ("Always add `WHERE is_deleted = FALSE OR is_deleted IS NULL`")
- A **confidence score** (increases with repeated observation)
- An **active/inactive flag** (humans can override incorrect learnings)

The Reflector phase generates learnings; the Planner phase consumes them. This creates a feedback loop where the system improves with every execution.

**Negative Learning Example.** Not all learnings encode success. The system also captures anti-patterns:

> **Learning #47** (confidence: 0.92, observed: 3 executions)  
> **Observation:** "Using `LEAD()`/`LAG()` window functions on `SUPPORT_TICKETS` causes query timeout due to high micro-partition skew on `CUSTOMER_ID`. Snowflake's adaptive optimization cannot resolve the skew within a 1-minute Dynamic Table `TARGET_LAG`."  
> **Recommendation:** "For ticket-level temporal analysis, use self-joins with date range predicates instead of window functions. Pre-aggregate by `CUSTOMER_ID` before applying temporal calculations."  
> **Pattern Signature:** `window_func_skewed_partition_timeout`

This negative learning prevents the Planner from repeatedly generating window-function-based strategies for skewed tables — a mistake that would cost credits and time with each failed attempt.

---

## 5. The Knowledge Graph: Lineage as Infrastructure

The Agentic Data Foundry maintains a Knowledge Graph (`KNOWLEDGE_GRAPH` schema) that encodes the relationships between all database objects:

- **Nodes**: Databases, schemas, tables, columns — each with LLM-generated descriptions and vector embeddings
- **Edges**: `CONTAINS` (schema→table), `HAS_COLUMN` (table→column), `TRANSFORMS_TO` (Bronze→Silver), `AGGREGATES_TO` (Silver→Gold)

The Knowledge Graph serves three critical functions:

**1. Lineage-Aware Impact Analysis.** Before regenerating a Silver table after a schema change, the system queries downstream dependencies. If `SILVER.CUSTOMERS` changes, the Knowledge Graph reveals that `GOLD.CUSTOMER_360`, `GOLD.ML_CUSTOMER_FEATURES`, and `GOLD.CUSTOMER_METRICS` are all impacted — enabling the system to regenerate the full dependency chain.

**2. Semantic Pattern Reuse.** When a new table arrives (e.g., `INVOICES`), the system uses vector similarity search to find analogous tables (e.g., `ORDERS`). The transformation pattern from the similar table seeds the LLM prompt, dramatically improving first-attempt accuracy.

**3. Enriched LLM Context.** Every LLM prompt in the agentic workflow is enriched with Knowledge Graph context: table descriptions, lineage relationships, similar table patterns, and downstream impact assessments. This grounding prevents the LLM from generating transformations in isolation.

### 5.1 Knowledge Graph Cold-Start: Greenfield Bootstrapping

A common concern with KG-dependent architectures is the cold-start problem: how does the graph get initialized for a brand-new enterprise deployment?

The Agentic Data Foundry solves this through a three-phase bootstrap:

1. **Structural Discovery** — `POPULATE_KG_FROM_INFORMATION_SCHEMA()` scans every schema, table, and column in the database and creates DATABASE, SCHEMA, TABLE, and COLUMN nodes with CONTAINS and HAS_COLUMN edges. This runs in seconds and provides the complete structural skeleton with zero human input.

2. **Lineage Seeding** — The human architect populates `TABLE_LINEAGE_MAP` with known Bronze→Silver and Silver→Gold intent. For a greenfield deployment, this is typically 2-3 rows per source table — a task measured in minutes, not days.

3. **Semantic Enrichment** — The KG population procedure calls `CORTEX.COMPLETE()` to generate natural language descriptions for every table and column node, and `EMBED_TEXT_768('e5-base-v2')` to create vector embeddings. These embeddings enable semantic similarity search from the first execution — even with a single table, the system can find analogous patterns in future tables.

The key insight is that the KG does not require a "critical mass" of data to be useful. Even a single Bronze→Silver→Gold chain provides enough context for the agentic workflow to operate. The KG's value compounds as more tables flow through the system, but the minimum viable KG is remarkably small.

---

## 6. The TABLE_LINEAGE_MAP: Single Source of Truth

At the center of the Agentic Data Foundry's metadata layer is the `TABLE_LINEAGE_MAP` — a living registry of all table-to-table relationships that is both **human-seeded** and **agent-populated**:

| SOURCE_TABLE | TARGET_TABLE | RELATIONSHIP_TYPE | ORIGIN |
|-------------|-------------|-------------------|--------|
| CUSTOMERS_VARIANT | CUSTOMERS | TRANSFORMS_TO | Seed |
| ORDERS_VARIANT | ORDERS | TRANSFORMS_TO | Seed |
| CUSTOMERS | CUSTOMER_360 | AGGREGATES_TO | Seed |
| ORDERS | ORDER_SUMMARY | AGGREGATES_TO | Agent |
| CUSTOMERS | ML_CUSTOMER_FEATURES | AGGREGATES_TO | Agent |

**Dual Population Model.** The lineage map is not a static configuration file — it is a dynamic registry that grows through two mechanisms:

1. **Human-Seeded Entries.** Data engineers declare known, intentional relationships — for example, that `CUSTOMERS_VARIANT` should produce a typed `CUSTOMERS` Silver table, or that `CUSTOMERS` + `ORDERS` + `SUPPORT_TICKETS` should feed a `CUSTOMER_360` Gold view. These entries express *architectural intent* before any agent executes.

2. **Agent-Populated Entries.** When the agentic Gold builder (`BUILD_GOLD_FOR_NEW_TABLES`) generates and executes a new Gold Dynamic Table, the `REGISTER_LINEAGE_FROM_DDL` procedure automatically parses the generated SQL, extracts all Silver table references from `FROM` and `JOIN` clauses, and inserts new lineage entries via `MERGE`. These entries are tagged as auto-registered, creating an audit trail that distinguishes human intent from agent discovery.

This dual model means the lineage map starts with a human-defined skeleton and grows organically as agents discover new relationships. The agentic build processes consult this map to identify gaps using two strategies: (1) Gold targets declared in the map but not yet materialized, and (2) Silver tables with no downstream mapping at all — "uncovered" tables that the agents autonomously build Gold aggregations for and then register back into the map.

The result is a self-expanding lineage registry where human architects define the initial vision and autonomous agents fill in the details, with every relationship — whether human-authored or agent-discovered — tracked in a single source of truth.

---

## 7. The Evolving Role of the Data Engineer

The Agentic Data Foundry does not eliminate the data engineer — it elevates the role. The new data engineer operates as:

**Architect of Intent.** Rather than writing `CREATE DYNAMIC TABLE` statements, the data engineer writes Directives that express business requirements in natural language. The specificity of the directive determines the quality of the autonomous output.

**Curator of Contracts.** The data engineer defines and maintains Schema Contracts that constrain LLM behavior. This requires deep understanding of downstream consumers — which columns are required, what naming conventions must be preserved, what data types are acceptable.

**Reviewer of Agents.** The agentic system generates transformation logic, but the data engineer reviews, approves, and occasionally overrides. The system provides multiple intervention points:

- **Pre-Execution Review** — Using `dry_run=TRUE`, the engineer can inspect all generated DDLs before any table is materialized. The Streamlit Gold Layer tab displays each DDL with side-by-side source schema comparison.
- **Post-Validation Override** — When the Validator flags a marginal result (e.g., 4.8% row variance, just under the 5% threshold), the engineer can reject or accept via the Agentic Workflow tab.
- **Learning Curation** — The engineer can deactivate incorrect learnings or manually inject learnings from domain expertise, directly steering future LLM behavior without touching any SQL.
- **Directive Tuning** — The Directives tab provides real-time feedback: the engineer writes a directive, triggers a dry run, reviews the LLM's interpretation, and refines the directive iteratively.

Every agent decision is logged with full LLM reasoning chains, making review efficient — the engineer reads *why* the agent chose a strategy, not just *what* it produced.

**Steward of Learnings.** The data engineer monitors the system's accumulated learnings, deactivating incorrect patterns and reinforcing correct ones. Over time, the system requires less human intervention — but never zero.

This evolution mirrors broader industry trends. As the Data Engineering Academy observes, "The future data engineer is less a builder of pipelines and more a designer of data systems — defining the 'what' and 'why' while AI handles the 'how'" [9]. Forbes' 2026 AI predictions similarly identify "the shift from manual engineering to AI-orchestrated data workflows" as one of the defining trends of the year [10].

---

## 8. Implementation: A Working System

The Agentic Data Foundry is not a conceptual framework. It is a deployed system running on Snowflake with the following concrete components:

| Component | Technology | Count |
|-----------|-----------|-------|
| Bronze Dynamic Tables | Snowflake Dynamic Tables (VARIANT) | 5 |
| Silver Dynamic Tables | LLM-generated, CDC-aware | 5 |
| Gold Dynamic Tables | LLM-generated, multi-source aggregation | 4+ |
| Stored Procedures | Python + SQL in Snowflake | 20+ |
| Metadata Tables | Schema Contracts, Directives, Learnings, Lineage | 12+ |
| Knowledge Graph | Nodes, Edges, Embeddings (e5-base-v2) | 200+ nodes |
| LLM Models | Claude 3.5 Sonnet (planning), Llama 3.1-8b (inference) | 2 |
| Streamlit Application | 13-tab management interface | 1 |

The Streamlit application (`DEMO_MANAGER`) provides a comprehensive management interface including:
- Real-time pipeline status monitoring
- Schema Contract and Directive management (CRUD with LLM-assisted generation)
- Knowledge Graph visualization
- Agentic build execution and monitoring
- AI-powered natural language chat over Gold data
- Dynamic Table health dashboards
- Transformation log inspection

The system has demonstrated autonomous end-to-end execution: from detecting a new PostgreSQL source table landing via Openflow CDC, through Bronze onboarding, Silver transformation, Gold aggregation, Knowledge Graph enrichment, and Semantic View generation — with zero human-written transformation code.

---

## 9. Principles and Design Decisions

### 9.1 Bronze Is Immutable

The Bronze layer uses `OBJECT_CONSTRUCT(*)` to capture the complete source payload as a VARIANT column. This design ensures:
- Schema changes in the source never break Bronze
- Historical payloads are preserved exactly as received
- The agentic system always has the full context for transformation decisions

### 9.2 Dynamic Tables Over Stored Procedures for Materialization

Silver and Gold tables are implemented as Dynamic Tables with `TARGET_LAG` rather than stored procedure-based ETL. This provides:
- Declarative refresh semantics (Snowflake manages scheduling)
- Automatic dependency tracking (downstream DTs refresh when upstream changes)
- Built-in observability (DT refresh history, lag monitoring)

### 9.3 Hallucination Guardrails: Multi-Layer DDL Validation

LLM hallucination in SQL generation is the single greatest risk to trust in agentic data engineering. The Agentic Data Foundry addresses this through a three-layer validation pipeline that executes *before* any generated DDL touches production data:

1. **Syntactic Compilation** — Every generated DDL is compiled via `EXECUTE IMMEDIATE` in a dry-run mode (using Snowflake's ability to parse SQL without executing). This catches syntax errors and keyword misuse.

2. **Semantic Reference Check** — `VALIDATE_GOLD_DDL` parses the generated SQL to extract all table references from `FROM` and `JOIN` clauses, then verifies each reference exists in `INFORMATION_SCHEMA`. When the LLM hallucinates a table name (e.g., `SILVER.PRODUCTS` when the actual table is `SILVER.PRODUCTS_VARIANT`), the validator detects the missing reference and suggests the closest match using Levenshtein distance. This was implemented after observing that 15-20% of first-attempt Gold DDLs contained at least one hallucinated table name.

3. **Column Existence Verification** — The validator checks that all referenced columns in `SELECT`, `WHERE`, `GROUP BY`, and `JOIN ON` clauses actually exist in the referenced tables. A common hallucination pattern is the LLM inventing plausible but non-existent columns (e.g., `CUSTOMER_NAME` when the actual column is `FULL_NAME`).

Validation failures are not terminal — they feed back into the LLM's retry loop with specific error messages, enabling self-correction. The combination of compilation + reference checking + column verification catches the vast majority of hallucinated SQL before execution.

### 9.4 Security and Governance in Agentic DDL

A critical concern for enterprise adoption is whether AI agents respect data governance policies during autonomous DDL generation. The Agentic Data Foundry operates within Snowflake's native security model:

- **Role-Based Access Control (RBAC)** — All agent-generated DDL executes under the calling role's privileges. If the role lacks `SELECT` on a table, the LLM-generated query fails at execution — the agent cannot bypass access controls regardless of what SQL it generates.
- **Row-Level Security & Masking** — Snowflake's row access policies and dynamic data masking policies are enforced at query time, not at DDL definition time. This means agent-generated Dynamic Tables automatically respect RLS and masking policies applied to their source tables — the policies propagate through the DT dependency chain.
- **Audit Trail** — Every LLM prompt, generated DDL, validation result, and execution outcome is logged to `METADATA.TRANSFORMATION_LOG` with timestamps, execution IDs, and the generating LLM model. This provides a complete audit chain from human directive to materialized table.
- **DDL Review Gate** — The system supports a `dry_run` mode where generated DDLs are logged but not executed, allowing human review before production materialization.

### 9.5 Metadata Is the Product

In the Agentic Data Foundry, the primary deliverable of human work is *metadata* — contracts, directives, lineage maps — not code. This inverts the traditional relationship where metadata is an afterthought generated from code. Here, code is generated from metadata.

---

## 10. Challenges and Limitations

### 10.1 LLM Non-Determinism

The same Bronze schema may produce slightly different Silver DDL across executions. Schema Contracts mitigate but do not eliminate this. Future work includes deterministic DDL templates with LLM-powered parameterization.

### 10.2 Cost: Human Latency for Instant Consumption

The most frequent objection to agentic workflows is the cost of LLM inference. Critics point to the Snowflake credits consumed by Cortex AI during the planning and validation phases.

**The Human Reality:** We are no longer trading SQL credits for LLM credits; we are trading **Human Latency** for **Instant Consumption**.

- The real cost in a modern enterprise isn't the $2.00 in compute to map a schema; it's the $20,000 in engineering salary wasted on "tribal knowledge" while a data product sits in a 6-week JIRA queue.
- By using **Ephemeral Derived Layers**, we actually reduce long-term TCO by eliminating the "storage tax" on unused, stagnant data. We only pay for the data the business actually needs, exactly when they need it.

Traditional pipeline development carries substantial hidden costs that are rarely attributed to the pipeline itself:

| Cost Category | Traditional (Manual) | Agentic |
|---|---|---|
| New table onboarding | Days to weeks of engineer time | Minutes (autonomous) |
| Schema change response | Manual investigation, code change, test, deploy | Automatic regeneration from metadata |
| Pipeline maintenance | 40-60% of data engineering time [1] | Near-zero (ephemeral, regenerable) |
| Knowledge transfer | Tribal knowledge, person-dependent | Encoded in Learnings, Contracts, Directives |
| Quality issue diagnosis | Reactive debugging after downstream failure | Proactive validation at generation time |
| Time to production | Weeks to months per new data product | Hours to days |

Industry data supports the economic case for AI-driven automation. IDC reports an average ROI of 3.7× per dollar invested in generative AI projects, with top-performing organizations achieving up to 10.3× [11]. Deloitte's 2026 State of AI survey found that 66% of enterprises report measurable productivity gains from AI adoption, with an average 21% productivity improvement and 15% cost reduction [12]. NVIDIA's 2026 State of AI report — surveying 3,200+ enterprises — found that 88% reported AI increased annual revenue and 87% reported AI reduced annual costs, with 53% citing improved employee productivity as the single biggest operational impact [13].

For data engineering specifically, the cost calculus shifts further in favor of agentic approaches as pipeline count grows. Each manually maintained pipeline carries a compounding maintenance burden — schema drift, quality regression, documentation decay. Agentic pipelines, regenerated from metadata, carry a fixed cost per regeneration and zero ongoing maintenance cost. The economic advantage grows with every pipeline added.

### 10.3 Beyond Hallucinations: The Zero-Trust Model

The fear of "AI-generated garbage" is valid if you treat an LLM as a black box. The Foundry does the opposite.

**The Human Reality:** We treat every agent-generated SQL statement as **guilty until proven innocent**.

- Our **Validator Phase** isn't an AI "vibe check." It is a battery of deterministic, hard-coded guardrails.
- If a generated pipeline fails a row-count parity test or violates a null-check constraint, it is rejected before it ever touches production. We don't ask the AI to be "perfect"; we build a system that makes it impossible for the AI to be "wrong" in a way that affects the business.

The three-layer hallucination guardrail pipeline (Syntactic Compilation → Semantic Reference Check → Column Verification) catches the vast majority of hallucinated SQL before execution. The remaining edge cases are caught by the Validator's deterministic checks. Trust is not assumed — it is earned through verifiable evidence at every step.

### 10.4 The "Scar Tissue" Moat

Finally, there is the concern of "Platform Dependency."

**The Human Reality:** By using the **Learnings Registry**, we are turning ephemeral engineering efforts into **Institutional Memory**.

- When a senior engineer corrects an agent's join logic, that "scar tissue" is saved forever in the customer's Snowflake metadata.
- This creates a defensive moat that a generic, off-the-shelf LLM can never replicate. Your Snowflake account literally becomes "smarter" the more you use it, making the Foundry the central brain of your data operations.

### 10.5 Trust and Auditability

Production data teams don't adopt tools on faith — they adopt tools they can audit. The Agentic Data Foundry treats auditability as a first-class architectural concern, not an afterthought.

Every LLM interaction is logged to `METADATA.TRANSFORMATION_LOG` with full provenance: the prompt sent, the model used, the raw response, the generated DDL, the validation result, and the final execution outcome — all tied to a unique execution ID and timestamp. This means a compliance officer can trace any Gold table column back through the exact LLM reasoning chain that produced it, the directive that guided it, and the contract that constrained it.

The system also supports a `dry_run` mode where the entire agentic workflow executes — Trigger through Reflector — but no DDL is materialized. The generated SQL is logged, validated, and available for human review in the Streamlit interface before a single table is touched. This is not a "trust me" system; it's a "show your work" system.

That said, organizational trust in AI-generated transformations is still developing. Early adopters report a predictable trust curve: initial skepticism, followed by cautious adoption with heavy dry-run usage, followed by growing confidence as the audit trail demonstrates consistent accuracy. The Foundry is designed to meet teams wherever they are on that curve.

### 10.6 Complex Business Logic

Not everything belongs in an LLM prompt. Fiscal calendar calculations that follow a 4-4-5 retail pattern, regulatory compliance transformations governed by HIPAA or SOX, multi-entity consolidation rules with intercompany elimination — these are domains where precision is non-negotiable and the cost of a subtle error is catastrophic.

The Agentic Data Foundry handles this through a deliberate escape hatch: **manual overrides**. A data engineer can define a Schema Contract and Transformation Directive that says, in effect, "for this table, use this exact DDL" — bypassing the LLM entirely while still benefiting from the Trigger, Validator, and Reflector phases. The override DDL is version-controlled in metadata, validated by the same deterministic guardrails, and tracked in the same audit trail.

This hybrid model is intentional. The goal is not to replace every SQL statement with an LLM call — it's to eliminate the 80% of pipeline work that is repetitive, pattern-based, and mechanical, freeing engineers to focus on the 20% that genuinely requires domain expertise. The boundary between "agentic" and "manual" is a configuration choice, not an architectural limitation.

---

## 11. Conclusion: Bridging the \"Context Gap\"

Data engineering as we know it \u2014 the era of the \"manual plumber\" \u2014 is over. It has to be. You cannot scale an Agentic Enterprise one SQL script at a time. The Agentic Data Foundry demonstrates that the transition from imperative to declarative engineering is not only possible but practical.

By combining AI-activated discovery, agentic transformation, and a Knowledge Graph that captures \"institutional memory,\" we've created a system where the gap between \"data arrives\" and \"insight is actionable\" is bridged by autonomous agents guided by human intent.

The Agentic Data Foundry demonstrates that the transition from imperative to declarative data engineering is not only possible but practical. By combining:

- **AI in the Bronze Layer** for autonomous discovery and onboarding
- **Agentic Transformation** for LLM-powered Silver and Gold generation
- **Ephemeral Derived Layers** that can be regenerated from metadata + Bronze
- **Intent-Driven Directives** that replace pipeline code with business purpose
- **Schema Contracts** that provide deterministic guardrails for LLM behavior
- **A Knowledge Graph** that gives agents lineage awareness and institutional memory

...the system achieves something that would have seemed improbable even two years ago: a data platform where the gap between "data arrives" and "business insight is available" is bridged not by human engineering effort but by autonomous AI agents guided by human intent.

The data engineer's role evolves from builder to describer, from coder to curator, from reactive debugger to proactive architect. This is not the end of data engineering — it is the beginning of data engineering's most productive era.

The trajectory is clear. Snowflake's March 2026 launch of Project SnowWork — an autonomous AI platform that enables business users to move "from intent to actions and outcomes" on governed data [14] — demonstrates that the agentic paradigm is not a research concept but an emerging product category.

The Agentic Data Foundry occupies a specific and critical position in this landscape. Consider the architectural hand-off:

| Layer | Responsibility | Owner |
|-------|---------------|-------|
| **Back-of-House** (Foundry) | Ingest → Transform → Validate → Materialize governed Gold tables + Semantic Views | Data Engineering (Agentic) |
| **Hand-off Point** | Semantic Views with dimensions, measures, synonyms, verified queries | Shared Contract |
| **Front-of-House** (SnowWork) | Natural language → SQL → Results → Actions → Outcomes | Business Users (Agentic) |

The Foundry produces the governed semantic layer that Project SnowWork then consumes for business end-users. Without the Foundry, SnowWork operates on manually curated Gold tables — recreating the same bottleneck that agentic engineering was designed to eliminate. With the Foundry, the entire path from raw CDC event to business user insight is autonomous: agents build the data, agents serve the data, and humans govern at both ends through intent rather than code.

The Agentic Enterprise isn't a future state; with the Foundry on Snowflake, it's the current reality. **Let's stop building pipes and start describing the future.**

---

## References

[1] Anaconda, "State of Data Science 2022," Anaconda, 2022. Available: https://www.anaconda.com/state-of-data-science-report-2022

[2] Gartner, "Gartner Predicts 40% of Enterprise Apps Will Feature Task-Specific AI Agents by 2026," Gartner Newsroom, August 2025. Available: https://www.gartner.com/en/newsroom/press-releases/2025-08-26-gartner-predicts-40-percent-of-enterprise-apps-will-feature-task-specific-ai-agents-by-2026

[3] McKinsey & Company, "Agentic AI Advances," McKinsey Featured Insights, 2025. Available: https://www.mckinsey.com/featured-insights/week-in-charts/agentic-ai-advances

[4] Snowflake, "Democratizing Enterprise AI: Snowflake's New AI Capabilities Further Accelerate and Simplify Data-Driven Innovation," Snowflake Blog, June 2025. Available: https://www.snowflake.com/en/blog/agentic-ai-ready-enterprise-data/

[5] Databricks, "How AI Is Transforming Data Engineering," Databricks Blog, 2025. Available: https://www.databricks.com/blog/how-ai-transforming-data-engineering

[6] Capgemini, "From Data Pipelines to AI-Driven Integration: The Future of Data Automation," Capgemini Insights, 2025. Available: https://www.capgemini.com/us-en/insights/expert-perspectives/from-data-pipelines-to-ai-driven-integration-the-future-of-data-automation/

[7] J. Heisler and G. Frere, "AI-Infused Pipelines with Snowflake Cortex," Snowflake Builders Blog (Medium), October 2024. Available: https://medium.com/snowflake/ai-infused-pipelines-with-snowflake-cortex-6a7954f2078d

[8] InfoQ, "The End of the Bronze Age: Rethinking the Medallion Architecture," InfoQ, 2025. Available: https://www.infoq.com/articles/rethinking-medallion-architecture/

[9] Data Engineering Academy, "The Future of Data Engineering in an AI-Driven World," 2025. Available: https://dataengineeracademy.com/blog/the-future-of-data-engineering-in-an-ai-driven-world/

[10] M. Minevich, "Agentic AI Takes Over — 11 Shocking 2026 Predictions," Forbes, December 2025. Available: https://www.forbes.com/sites/markminevich/2025/12/31/agentic-ai-takes-over-11-shocking-2026-predictions/

[11] IDC, "Business Opportunity of AI: Generative AI Delivering New Business Value and Increasing ROI," IDC InfoBrief, 2025. Available: https://www.idc.com

[12] Deloitte, "The State of AI in the Enterprise," Deloitte AI Institute, 2026. Available: https://www.deloitte.com/us/en/what-we-do/capabilities/applied-artificial-intelligence/content/state-of-ai-in-the-enterprise.html

[13] NVIDIA, "State of AI Report 2026: How AI Is Driving Revenue, Cutting Costs and Boosting Productivity," NVIDIA Blog, March 2026. Available: https://blogs.nvidia.com/blog/state-of-ai-report-2026/

[14] Snowflake, "Snowflake Launches Project SnowWork, Bringing Outcome-Driven AI to Every Business User," Snowflake Press Release, March 2026. Available: https://www.snowflake.com/en/news/press-releases/snowflake-launches-project-snowwork-bringing-outcome-driven-ai-to-every-business-user/

---

*This whitepaper describes the Agentic Data Foundry reference implementation available at https://github.com/dbaontap/agentic-data-foundry. The system is built entirely on Snowflake using Cortex AI, Dynamic Tables, Openflow, and Streamlit in Snowflake.*
