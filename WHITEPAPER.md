# The Agentic Data Foundry: From Building Pipelines to Describing Intent

**A Whitepaper on AI-Native Data Engineering**

*March 2026*

---

## Abstract

The data engineering discipline stands at an inflection point. For two decades, data engineers have hand-coded ETL scripts, manually mapped schemas, and reactively debugged pipeline failures. The Agentic Data Foundry represents a paradigm shift: a system where AI agents autonomously discover, transform, validate, and optimize data pipelines while human practitioners evolve from pipeline builders to pipeline *describers*. This whitepaper presents the architecture, principles, and working implementation of an agentic data engineering platform built on Snowflake, demonstrating how large language models (LLMs), declarative metadata, and autonomous workflows can replace imperative pipeline code with intent-driven data engineering.

---

## 1. Introduction: The Case for Change

Modern data teams spend an estimated 40-60% of their time on pipeline maintenance rather than value creation [1]. Schema changes break downstream transformations. New data sources require weeks of manual onboarding. Quality issues propagate silently through layers until they surface in executive dashboards. The medallion architecture (Bronze → Silver → Gold) provided a useful organizational pattern, but the *implementation* of that pattern remains overwhelmingly manual.

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

### 3.2 The Bronze Layer: AI-Activated Ingestion

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

### 3.3 The Silver Layer: Agentic Transformation

The Silver layer is where the agentic paradigm most visibly departs from traditional data engineering. Rather than human-authored transformation scripts, Silver tables are generated by an autonomous 5-phase workflow.

**Phase 1: Trigger.** The system detects events requiring transformation:
- A new Bronze table with no corresponding Silver table
- A schema change (new or dropped columns in the Bronze payload)
- A data quality threshold breach (>5% row count variance)

**Phase 2: Planner.** An LLM (Claude 3.5 Sonnet) analyzes the Bronze schema structure and determines the optimal transformation strategy. The planner considers:
- Column types and naming conventions
- CDC deduplication requirements
- Whether similar tables have been transformed before (via Knowledge Graph)
- Active Schema Contracts and Directives for the table
- Historical Learnings from previous transformations

**Phase 3: Executor.** The LLM generates a complete `CREATE OR REPLACE DYNAMIC TABLE` DDL statement. The generated SQL includes CDC-aware deduplication using `ROW_NUMBER()` partitioned by primary key and ordered by `_SNOWFLAKE_UPDATED_AT`. If the DDL fails compilation, the Executor captures the error and retries with the error context injected into the LLM prompt — a self-correction loop with up to 3 attempts.

**Phase 4: Validator.** The system compares source and target:
- Row count comparison (must be within 5% tolerance)
- Schema validation (all contract-required columns present)
- Data type verification
- Referential integrity checks

**Phase 5: Reflector.** An LLM analyzes the complete workflow execution and extracts learnings:
- Success patterns ("Tables with temporal data benefit from `ORDER_MONTH` derivation")
- Failure patterns ("Nested VARIANT arrays require `LATERAL FLATTEN` before extraction")
- Optimization recommendations ("Consider clustering on `CUSTOMER_ID` for join performance")

These learnings are persisted with confidence scores and fed back into future Planner prompts, creating a continuously improving system.

### 3.4 The Gold Layer: Agentic Aggregation

The Gold layer extends the agentic pattern to business-ready aggregations. The `BUILD_GOLD_FOR_NEW_TABLES` procedure employs a two-strategy discovery approach:

**Strategy 1: Missing Gold Targets.** The `TABLE_LINEAGE_MAP` metadata table declares the intended mapping from Silver to Gold tables (e.g., `SILVER.CUSTOMERS → GOLD.CUSTOMER_360` via `AGGREGATES_TO`). The system identifies Gold targets that exist in the lineage map but not yet in the database.

**Strategy 2: Uncovered Silver Tables.** The system identifies Silver tables that have no lineage mapping at all — tables that arrived after the initial lineage was defined. For these, the LLM proposes appropriate Gold aggregations based on the table's schema and content.

In both cases, the generated Gold Dynamic Tables are validated, their lineage is registered, and the Knowledge Graph is refreshed — all autonomously.

### 3.5 The Ephemeral Nature of Silver and Gold

A defining property of the Agentic Data Foundry is that Silver and Gold layers are *ephemeral* — they are derived, not primary. Because every Silver and Gold table is:

1. Defined by a Dynamic Table DDL (stored in transformation logs)
2. Guided by Schema Contracts and Directives (stored in metadata)
3. Populated from Bronze (the immutable source of truth)

...the entire Silver and Gold layers can be regenerated from scratch at any time. This has profound implications:

- **Schema evolution becomes trivial**: When source schemas change, the agentic workflow regenerates affected tables rather than patching them
- **Environment provisioning is instant**: Dev, staging, and production environments diverge only in their metadata, not their pipeline code
- **Disaster recovery simplifies**: Bronze + metadata = complete system reconstruction
- **Technical debt cannot accumulate**: There is no legacy transformation code to maintain

This aligns with the broader industry movement toward treating analytics layers as *materialized views of intent* rather than independently maintained data stores. As InfoQ noted in their analysis of medallion architecture evolution, "The future of the medallion pattern lies in making intermediate layers regenerable rather than permanent" [8].

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

---

## 5. The Knowledge Graph: Lineage as Infrastructure

The Agentic Data Foundry maintains a Knowledge Graph (`KNOWLEDGE_GRAPH` schema) that encodes the relationships between all database objects:

- **Nodes**: Databases, schemas, tables, columns — each with LLM-generated descriptions and vector embeddings
- **Edges**: `CONTAINS` (schema→table), `HAS_COLUMN` (table→column), `TRANSFORMS_TO` (Bronze→Silver), `AGGREGATES_TO` (Silver→Gold)

The Knowledge Graph serves three critical functions:

**1. Lineage-Aware Impact Analysis.** Before regenerating a Silver table after a schema change, the system queries downstream dependencies. If `SILVER.CUSTOMERS` changes, the Knowledge Graph reveals that `GOLD.CUSTOMER_360`, `GOLD.ML_CUSTOMER_FEATURES`, and `GOLD.CUSTOMER_METRICS` are all impacted — enabling the system to regenerate the full dependency chain.

**2. Semantic Pattern Reuse.** When a new table arrives (e.g., `INVOICES`), the system uses vector similarity search to find analogous tables (e.g., `ORDERS`). The transformation pattern from the similar table seeds the LLM prompt, dramatically improving first-attempt accuracy.

**3. Enriched LLM Context.** Every LLM prompt in the agentic workflow is enriched with Knowledge Graph context: table descriptions, lineage relationships, similar table patterns, and downstream impact assessments. This grounding prevents the LLM from generating transformations in isolation.

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

**Reviewer of Agents.** The agentic system generates transformation logic, but the data engineer reviews, approves, and occasionally overrides. The system logs every LLM decision with full reasoning, making review efficient.

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

### 9.3 LLM-Generated DDL Is Validated Before Execution

Every DDL generated by an LLM passes through a `VALIDATE_GOLD_DDL` step that compiles the SQL without executing it. This catches syntax errors, invalid column references, and type mismatches before any data is written.

### 9.4 Metadata Is the Product

In the Agentic Data Foundry, the primary deliverable of human work is *metadata* — contracts, directives, lineage maps — not code. This inverts the traditional relationship where metadata is an afterthought generated from code. Here, code is generated from metadata.

---

## 10. Challenges and Limitations

**LLM Non-Determinism.** The same Bronze schema may produce slightly different Silver DDL across executions. Schema Contracts mitigate but do not eliminate this. Future work includes deterministic DDL templates with LLM-powered parameterization.

**Cost: Inference vs. Total Cost of Ownership.** LLM inference for transformation planning is more expensive per-execution than static SQL. The initial transformation of a new table incurs meaningful Cortex AI credits, and the system amortizes this through cached learnings and pattern reuse. However, evaluating agentic data engineering solely on inference cost misses the broader economic picture. The relevant comparison is not *LLM inference vs. SQL execution* — it is *total cost of agentic pipeline ownership vs. total cost of manual pipeline ownership*.

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

For data engineering specifically, the cost calculus shifts further in favor of agentic approaches as pipeline count grows. Each manually maintained pipeline carries a compounding maintenance burden — schema drift, quality regression, documentation decay. Agentic pipelines, regenerated from metadata, carry a fixed cost per regeneration and zero ongoing maintenance cost.

**Trust and Auditability.** Production data teams require full audit trails. The system logs every LLM prompt, response, generated DDL, and validation result — but organizational trust in AI-generated transformations is still developing.

**Complex Business Logic.** Highly specific business rules (e.g., fiscal calendar calculations, regulatory compliance transformations) may exceed what an LLM can reliably generate from a natural language directive. The system supports manual overrides for these cases.

---

## 11. Conclusion

The Agentic Data Foundry demonstrates that the transition from imperative to declarative data engineering is not only possible but practical. By combining:

- **AI in the Bronze Layer** for autonomous discovery and onboarding
- **Agentic Transformation** for LLM-powered Silver and Gold generation
- **Ephemeral Derived Layers** that can be regenerated from metadata + Bronze
- **Intent-Driven Directives** that replace pipeline code with business purpose
- **Schema Contracts** that provide deterministic guardrails for LLM behavior
- **A Knowledge Graph** that gives agents lineage awareness and institutional memory

...the system achieves something that would have seemed improbable even two years ago: a data platform where the gap between "data arrives" and "business insight is available" is bridged not by human engineering effort but by autonomous AI agents guided by human intent.

The data engineer's role evolves from builder to describer, from coder to curator, from reactive debugger to proactive architect. This is not the end of data engineering — it is the beginning of data engineering's most productive era.

The trajectory is clear. Snowflake's March 2026 launch of Project SnowWork — an autonomous AI platform that enables business users to move "from intent to actions and outcomes" on governed data [14] — demonstrates that the agentic paradigm is not a research concept but an emerging product category. The Agentic Data Foundry occupies a specific and critical position in this landscape: where Project SnowWork brings agentic intelligence to business users consuming data, the Agentic Data Foundry brings the same principles to the data engineering teams *producing* it. Together, they represent the full arc of the agentic enterprise — from data creation to data consumption, from pipeline to insight to action, all governed by intent rather than code.

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
