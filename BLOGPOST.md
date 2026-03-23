# The Agentic Data Foundry: Stop Building Pipes, Start Describing the Future

#OnTapToday — a concept that's been consuming my brain for the better part of a year. I want to talk about why the way we've been doing data engineering is fundamentally broken, and what I think comes next.

---

#### The Plumbing Tax

I'm going to say something that might ruffle some feathers: **we've been doing expensive plumbing and calling it engineering.**

For two decades, the data engineering discipline has been stuck in a loop. A source database changes a column name. A pipeline breaks. An executive sees a blank dashboard on Monday morning. An engineer spends Friday night debugging SQL that someone else wrote three years ago and never documented. Sound familiar?

This is the "plumbing tax" — and every modern data team is paying it. Industry research tells us that data engineers spend an estimated 40-60% of their time on **pipeline maintenance** rather than value creation. Schema changes break downstream transformations. New data sources require weeks of manual onboarding. Quality issues propagate silently through layers until they blow up in a board meeting.

We don't have a data problem. We have a **coordination problem.**

#### The Agentic Enterprise Is Coming — Is Your Data Ready?

Here's what keeps me up at night. We are entering the era of the Agentic Enterprise. Gartner predicts 40% of enterprise applications will feature task-specific AI agents by 2026. Snowflake just announced Project SnowWork — an autonomous AI platform where business users can "simply ask for what they need." The "Front-of-House" is going agentic whether we like it or not.

But here's the thing nobody is talking about: **if your "Back-of-House" data supply chain is still a web of brittle, human-dependent pipes, your AI agents are just going to hallucinate at scale.**

You can't scale an Agentic Enterprise one SQL script at a time.

#### The Core Idea: Describe, Don't Build

The Agentic Data Foundry is a concept — and a working system — built around a simple inversion: **what if data engineers stopped writing transformation code and started describing what the data should look like and what it's for?**

In the traditional model, the engineer's primary artifact is *code*: SQL scripts, Python transformations, orchestration DAGs, config files. You have to understand the source, design the target, write the logic, handle edge cases, deploy, monitor, and debug. Every step requires deep tribal knowledge. The result? Brittle, person-dependent pipelines where the "how" overwhelms the "what."

In the agentic model, the engineer's primary artifacts become three types of **metadata**:

- **Schema Contracts** — Define structural constraints. Column names, types, required fields. This is the guardrail.
- **Transformation Directives** — Declare business intent in natural language. "This data feeds a churn prediction model. Preserve daily granularity. Create 7/14/30 day rolling averages." This is the *why*.
- **Learnings** — Captured knowledge from past executions. "Tables with CDC timestamps require deduplication partitioned by primary key." This is institutional memory.

AI agents then autonomously execute the full pipeline lifecycle: discovery, schema inference, transformation generation, validation, and optimization. The human stays in the middle — not writing the pipeline, but *describing* what the pipeline should achieve and *constraining* how it should behave.

#### Progressive Refinement — Same Pattern, New Engine

If you've worked with layered data architectures, the pattern is familiar: **Capture** (Bronze) raw data as-is, **Conform** (Silver) it into typed and validated structures, **Consume** (Gold) it as business-ready aggregations.

What changes is *who does the work at each layer*:

- **Bronze** — AI-activated ingestion. New source tables are discovered automatically. Schema-on-read captures the full payload as a semi-structured column. Source changes never break this layer.
- **Silver** — Agentic transformation. An LLM reads your Schema Contract and Directive, generates the typed transformation DDL, and self-corrects through a retry loop when validation fails.
- **Gold** — Agentic aggregation. The same pattern, now stitching multiple Silver tables into business-ready views.

The schema names don't change. The mental model doesn't change. What changes is that the engine driving each layer is an AI agent rather than a human engineer hand-coding SQL.

#### The Five Agentic Phases

Under the hood, each agentic action follows a five-phase lifecycle. Think of it as a structured "thought process" for the AI:

1. **Trigger** — An event fires. A new table lands. A schema changes. A scheduled task kicks off. The system detects *something happened* and emits a structured work order.

2. **Planner** — The agent assembles context. It reads the Schema Contract for structural constraints, the Directive for business intent, past Learnings for patterns that worked (or didn't), and the Knowledge Graph for lineage awareness. Then it formulates a strategy: which tables to transform, what approach to take, what pitfalls to avoid.

3. **Executor** — The agent generates SQL. But here's the key: it doesn't just fire and forget. It runs a **self-correction loop** — if the generated DDL fails validation, the error message is injected back into the LLM prompt, and it retries. In practice, we've seen first-attempt success rates around 80-85%, climbing to 95%+ after self-correction.

4. **Validator** — This is the **zero-trust boundary**. Every generated DDL goes through syntactic compilation (does it parse?), semantic reference checks (do the tables it references actually exist?), and column verification (are those column names real or hallucinated?). The philosophy: **guilty until proven innocent.** The LLM doesn't get the benefit of the doubt.

5. **Reflector** — After execution, the system captures what happened as a Learning. What worked, what failed, what patterns emerged. This is the "scar tissue" that becomes a moat — the more the system runs, the smarter it gets. Institutional memory that doesn't walk out the door when someone leaves.

#### The $2 vs. $20,000 Argument

The most frequent objection I hear: "But LLM inference costs money!" Yes. It does. But let's be honest about what we're really comparing.

The real cost in a modern enterprise isn't the **$2.00 in compute** to map a schema. It's the **$20,000 in engineering salary** wasted while a data product sits in a six-week JIRA queue because the one person who understands the source system is on vacation.

We're not trading SQL credits for LLM credits. We're trading **Human Latency** for **Instant Consumption.**

And when you factor in ephemeral derived layers — Gold tables that can be regenerated on demand from metadata and Bronze — you eliminate the "storage tax" on unused, stagnant data. You only pay for the data the business actually needs, exactly when they need it.

#### Trust: Show Your Work, Don't Trust Me

I won't pretend LLM-generated SQL should be blindly trusted. It shouldn't. That's the whole point of the Validator phase.

Every LLM interaction — the prompt, the model, the raw response, the generated DDL, the validation result, and the execution outcome — is logged with full provenance. A compliance officer can trace any table back through the exact reasoning chain that produced it. There's a dry-run mode where the entire workflow executes but nothing is materialized until a human approves it.

This is not a "trust me" system. It's a **"show your work"** system.

And the real kicker: the agent can't bypass your security. All generated DDL executes under the calling role's privileges. Row access policies and masking propagate through the dependency chain. If the role can't see the data, the agent can't either.

#### Not Everything Belongs in an LLM Prompt

Let me be clear: the Agentic Data Foundry is not about replacing every SQL statement with an LLM call. Fiscal calendar calculations following a 4-4-5 retail pattern? Regulatory compliance transformations governed by HIPAA? Multi-entity consolidation with intercompany elimination? Those need precision that an LLM shouldn't be trusted with.

The goal is the **80/20 rule**: eliminate the 80% of pipeline work that is repetitive, pattern-based, and mechanical. Free the engineers to focus on the 20% that genuinely requires domain expertise. The boundary between "agentic" and "manual" is a configuration choice, not an architectural limitation.

#### The Data Engineer Evolves

The Agentic Data Foundry doesn't eliminate the data engineer. It elevates the role:

- From **builder** to **describer**
- From **coder** to **curator**
- From **reactive debugger** to **proactive architect**

You stop writing transformation SQL and start writing Schema Contracts and Directives. You stop debugging pipelines at 2 AM and start reviewing agent-generated logic in a dry-run interface. You stop being the bottleneck and start being the architect of intent.

This isn't the end of data engineering. It's the beginning of data engineering's most productive era.

#### The Handoff That Makes It All Work

Here's the architectural picture that ties it all together:

| Layer | Responsibility | Who |
|-------|---------------|-----|
| **Back-of-House** (Foundry) | Ingest, Transform, Validate, Materialize governed data + Semantic Views | Data Engineering (Agentic) |
| **Hand-off Point** | Semantic Views with dimensions, measures, synonyms | Shared Contract |
| **Front-of-House** (SnowWork) | Natural language to SQL to Results to Actions | Business Users (Agentic) |

Without the Back-of-House running autonomously, the Front-of-House operates on manually curated data — recreating the exact same bottleneck that agentic engineering was designed to eliminate. With it, the entire path from raw event to business insight is autonomous. Agents build the data. Agents serve the data. Humans govern at both ends through intent rather than code.

#### Wrapping Up

Data engineering as we know it — the era of the "manual plumber" — is over. It has to be. The Agentic Enterprise demands a Back-of-House that moves at the same speed as the Front-of-House. The Agentic Data Foundry is the architectural concept — and a real, working system — that bridges that gap.

**Let's stop building pipes and start describing the future.**

If you want the deep dive, I've published a full whitepaper with architecture details, design decisions, and references: [The Agentic Data Foundry Whitepaper](https://github.com/dbaontap/agentic-data-foundry)

Enjoy!

---

*Categories: Snowflake, AI, Data Engineering, cloud*
*Tags: agentic AI, data engineering, LLM, Snowflake, Cortex AI, Dynamic Tables, progressive refinement*
