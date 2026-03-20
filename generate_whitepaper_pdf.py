#!/usr/bin/env python3
"""Generate a professional PDF from the Agentic Data Foundry whitepaper."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
import os
import re

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "WHITEPAPER.pdf")

SF_BLUE = HexColor("#29B5E8")
SF_DARK = HexColor("#0f3460")
SF_LIGHT_BG = HexColor("#F0F8FF")
SF_ACCENT = HexColor("#16537e")
SF_GREEN = HexColor("#198754")
SF_GRAY = HexColor("#6C757D")
SF_LIGHT_GRAY = HexColor("#E9ECEF")
SF_WHITE = white
TABLE_HEADER_BG = HexColor("#0f3460")
TABLE_ALT_ROW = HexColor("#F8FAFC")
CODE_BG = HexColor("#F1F3F5")


def build_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='CoverTitle', fontName='Helvetica-Bold', fontSize=26, leading=32,
        textColor=SF_WHITE, alignment=TA_LEFT,
    ))
    styles.add(ParagraphStyle(
        name='CoverSubtitle', fontName='Helvetica', fontSize=13, leading=18,
        textColor=HexColor("#B0D4F1"), alignment=TA_LEFT,
    ))
    styles.add(ParagraphStyle(
        name='SectionTitle', fontName='Helvetica-Bold', fontSize=16, leading=22,
        textColor=SF_DARK, spaceBefore=18, spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name='SubsectionTitle', fontName='Helvetica-Bold', fontSize=12, leading=16,
        textColor=SF_ACCENT, spaceBefore=12, spaceAfter=6,
    ))
    styles.add(ParagraphStyle(
        name='SubsubTitle', fontName='Helvetica-Bold', fontSize=10.5, leading=14,
        textColor=HexColor("#1a6fb0"), spaceBefore=10, spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name='BodyText2', fontName='Helvetica', fontSize=9.5, leading=13.5,
        textColor=HexColor("#333333"), spaceBefore=3, spaceAfter=3,
        alignment=TA_JUSTIFY,
    ))
    styles.add(ParagraphStyle(
        name='BulletItem', fontName='Helvetica', fontSize=9.5, leading=13,
        textColor=HexColor("#333333"), leftIndent=18, bulletIndent=6,
        spaceBefore=2, spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        name='CodeBlock', fontName='Courier', fontSize=7.5, leading=10,
        textColor=HexColor("#212529"), backColor=CODE_BG,
        borderPadding=(6, 6, 6, 6), spaceBefore=4, spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name='SmallNote', fontName='Helvetica-Oblique', fontSize=8, leading=11,
        textColor=SF_GRAY, spaceBefore=2, spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        name='TableHeader', fontName='Helvetica-Bold', fontSize=8, leading=10,
        textColor=SF_WHITE, alignment=TA_CENTER,
    ))
    styles.add(ParagraphStyle(
        name='TableCell', fontName='Helvetica', fontSize=8, leading=10,
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='TableCellBold', fontName='Helvetica-Bold', fontSize=8, leading=10,
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='RefText', fontName='Helvetica', fontSize=8, leading=11,
        textColor=HexColor("#333333"), spaceBefore=2, spaceAfter=2,
        leftIndent=18,
    ))
    styles.add(ParagraphStyle(
        name='AbstractText', fontName='Helvetica-Oblique', fontSize=9.5, leading=13.5,
        textColor=HexColor("#444444"), spaceBefore=3, spaceAfter=3,
        alignment=TA_JUSTIFY,
    ))
    styles.add(ParagraphStyle(
        name='BlockquoteText', fontName='Helvetica-Oblique', fontSize=9, leading=12.5,
        textColor=SF_DARK, leftIndent=14, spaceBefore=4, spaceAfter=4,
    ))
    return styles


def make_table(headers, rows, col_widths, styles, first_col_bold=False):
    s = styles
    header_row = [Paragraph(h, s['TableHeader']) for h in headers]
    data = [header_row]
    for row in rows:
        processed = []
        for i, cell in enumerate(row):
            st = s['TableCellBold'] if (i == 0 and first_col_bold) else s['TableCell']
            processed.append(Paragraph(str(cell), st))
        data.append(processed)
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER_BG),
        ('TEXTCOLOR', (0, 0), (-1, 0), SF_WHITE),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('TOPPADDING', (0, 1), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('GRID', (0, 0), (-1, -1), 0.5, HexColor("#DEE2E6")),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [SF_WHITE, TABLE_ALT_ROW]),
    ]))
    return t


def add_callout(elements, text, styles):
    tbl = Table(
        [[Paragraph(text, styles['BodyText2'])]],
        colWidths=[6.3 * inch],
    )
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SF_LIGHT_BG),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
    ]))
    elements.append(tbl)


def add_code_block(elements, code_text, styles):
    lines = code_text.strip().split('\n')
    formatted = '<br/>'.join(
        line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace(' ', '&nbsp;')
        for line in lines
    )
    tbl = Table(
        [[Paragraph(formatted, styles['CodeBlock'])]],
        colWidths=[6.3 * inch],
    )
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), CODE_BG),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [4, 4, 4, 4]),
    ]))
    elements.append(tbl)
    elements.append(Spacer(1, 4))


def esc(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def build_cover(elements, styles):
    cover_bg = Table(
        [
            [Spacer(1, 1.2 * inch)],
            [Paragraph("The Agentic<br/>Data Foundry", styles['CoverTitle'])],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("From Building Pipelines to Describing Intent", styles['CoverSubtitle'])],
            [Spacer(1, 0.3 * inch)],
            [Paragraph("A Whitepaper on AI-Native Data Engineering", styles['CoverSubtitle'])],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("March 2026", styles['CoverSubtitle'])],
            [Spacer(1, 2.0 * inch)],
        ],
        colWidths=[6.5 * inch],
    )
    cover_bg.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SF_DARK),
        ('LEFTPADDING', (0, 0), (-1, -1), 40),
        ('RIGHTPADDING', (0, 0), (-1, -1), 40),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    elements.append(cover_bg)
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(Paragraph(
        '<font color="#6C757D"><i>Built on Snowflake  |  Cortex AI  |  Dynamic Tables  |  Openflow  |  Streamlit</i></font>',
        ParagraphStyle('conf', fontName='Helvetica-Oblique', fontSize=9, alignment=TA_CENTER, textColor=SF_GRAY)
    ))
    elements.append(PageBreak())


def build_abstract(elements, styles):
    elements.append(Paragraph("Abstract", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The data engineering discipline stands at an inflection point. For two decades, data engineers have hand-coded "
        "ETL scripts, manually mapped schemas, and reactively debugged pipeline failures. The Agentic Data Foundry "
        "represents a paradigm shift: a system where AI agents autonomously discover, transform, validate, and optimize "
        "data pipelines while human practitioners evolve from pipeline builders to pipeline <i>describers</i>. This "
        "whitepaper presents the architecture, principles, and working implementation of an agentic data engineering "
        "platform built on Snowflake, demonstrating how large language models (LLMs), declarative metadata, and autonomous "
        "workflows can replace imperative pipeline code with intent-driven data engineering.",
        styles['AbstractText']))
    elements.append(Spacer(1, 12))


def build_section1(elements, styles):
    elements.append(Paragraph("1. Introduction: The Case for Change", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "Modern data teams spend an estimated 40-60% of their time on pipeline maintenance rather than value creation [1]. "
        "Schema changes break downstream transformations. New data sources require weeks of manual onboarding. Quality "
        "issues propagate silently through layers until they surface in executive dashboards. The medallion architecture "
        "(Bronze \u2192 Silver \u2192 Gold) provided a useful organizational pattern, but the <i>implementation</i> of that pattern "
        "remains overwhelmingly manual.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Meanwhile, the AI landscape has shifted dramatically. Gartner predicts that 40% of enterprise applications will "
        "feature task-specific AI agents by 2026, up from less than 5% in 2025 [2]. McKinsey's research on agentic AI "
        "identifies autonomous task completion as a defining capability of the next wave of enterprise AI adoption [3]. "
        "Snowflake's own platform evolution \u2014 with Cortex AI, Dynamic Tables, and Cortex Agents \u2014 has created the "
        "infrastructure for AI-native data engineering [4].",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Snowflake's strategic direction validates this trajectory. In March 2026, the company announced "
        "<b>Project SnowWork</b> \u2014 an autonomous enterprise AI platform designed to help business users "
        "\"simply ask for what they need\" and have AI \"securely complete multi-step tasks based on conversational "
        "prompts\" [14]. CEO Sridhar Ramaswamy described the vision: \"We are entering the era of the agentic "
        "enterprise... embedding intelligence directly into the operating fabric of the enterprise.\" Industry analyst "
        "Sanjeev Mohan (SanjMo) observed that Snowflake is \"extending its platform from a system of insight to a "
        "system of action, which is where measurable business value is ultimately realized\" [14]. The Agentic Data "
        "Foundry embodies this same architectural principle \u2014 applied specifically to data engineering \u2014 where "
        "governed data, shared business definitions, and autonomous execution converge to replace manual pipeline "
        "construction with intent-driven automation.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The Agentic Data Foundry synthesizes these trends into a working system. It is not a theoretical framework but "
        "a deployed platform that has autonomously discovered tables, generated transformation logic, validated results, "
        "and learned from its own execution history \u2014 all guided by human-defined intent rather than human-written code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section2(elements, styles):
    elements.append(Paragraph("2. The Core Thesis: Describe, Don't Build", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("2.1 The Traditional Model", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "In conventional data engineering, the practitioner's primary artifact is <i>code</i>: SQL scripts, Python "
        "transformations, orchestration DAGs, and configuration files. The data engineer must understand the source schema, "
        "design the target schema, write transformation logic, handle edge cases (nulls, type mismatches, CDC semantics), "
        "deploy, schedule, monitor, debug failures, and adapt to schema changes. Each step requires deep technical knowledge. "
        "The result is brittle, person-dependent pipelines where the \"how\" overwhelms the \"what.\"",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("2.2 The Agentic Model", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Agentic Data Foundry inverts this relationship. The human practitioner's primary artifacts become three "
        "types of <b>metadata</b>:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    t = make_table(
        ["Artifact", "Purpose", "Example"],
        [
            ["Schema Contracts", "Define structural constraints \u2014 column names, types, required fields",
             "CUSTOMERS must have CUSTOMER_ID (INTEGER), EMAIL (VARCHAR), IS_DELETED (BOOLEAN)"],
            ["Transformation\nDirectives", "Declare business intent \u2014 what the data is for",
             "\"This data feeds a churn prediction model. Preserve daily granularity. Create rolling averages.\""],
            ["Learnings", "Capture accumulated knowledge from past executions",
             "\"Tables with _SNOWFLAKE_UPDATED_AT require CDC deduplication via ROW_NUMBER().\""],
        ],
        [1.1*inch, 1.8*inch, 3.4*inch], styles, first_col_bold=True
    )
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        "The AI agents then autonomously execute the full pipeline lifecycle: discovery, schema inference, transformation "
        "generation, validation, and optimization. The human remains \"in the middle\" \u2014 not writing the pipeline, but "
        "<i>describing</i> what the pipeline should achieve and <i>constraining</i> how it should behave.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "This shift mirrors a broader industry trend. As Databricks observed, \"AI is transforming data engineering\" by "
        "automating schema inference, anomaly detection, and pipeline generation [5]. Capgemini's research on AI-driven "
        "data integration similarly concludes that the future lies in \"AI-powered orchestration of data flows rather than "
        "manual pipeline construction\" [6].",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section3(elements, styles):
    elements.append(Paragraph("3. Architecture: The Five Layers", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    add_code_block(elements, """                    AGENTIC DATA FOUNDRY
    +----------+    +----------+    +----------+
    |  BRONZE  |--->|  SILVER  |--->|   GOLD   |
    | Activated|    | Agentic  |    | Agentic  |
    +----------+    +----------+    +----------+
    CDC Ingestion   LLM-Generated   LLM-Generated
    Schema-on-Read  Dynamic Tables  Aggregation DTs

    CONTROL PLANE
    Schema Contracts | Directives | Learnings | Knowledge Graph

    AGENT LAYER
    Trigger -> Planner -> Executor -> Validator -> Reflector""", styles)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("3.1 The Bronze Layer: AI-Activated Ingestion", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Bronze layer is the entry point where raw data arrives from source systems. In the Agentic Data Foundry, "
        "Bronze is not merely a landing zone \u2014 it is an <b>activation layer</b> where AI begins adding value from the "
        "first moment data enters the platform.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "<b>Openflow CDC Ingestion.</b> Source tables from PostgreSQL arrive via Openflow's Change Data Capture (CDC) "
        "connector, which adds system columns (<font face='Courier' size='8'>_SNOWFLAKE_DELETED</font>, "
        "<font face='Courier' size='8'>_SNOWFLAKE_UPDATED_AT</font>) for change tracking.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "<b>Autonomous Discovery and Onboarding.</b> The <font face='Courier' size='8'>DISCOVER_AND_ONBOARD_NEW_TABLES</font> "
        "procedure continuously scans for new landing tables that lack corresponding Bronze representations. When a new "
        "table is detected, the system autonomously creates a Stream, a Bronze Dynamic Table using "
        "<font face='Courier' size='8'>OBJECT_CONSTRUCT(*)</font>, and registers the event in metadata.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "This schema-on-read VARIANT pattern provides a critical advantage: the Bronze layer absorbs schema changes "
        "without breaking. New columns appear automatically. The system never needs ALTER TABLE.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_callout(elements,
        "<b>AI in the Bronze Layer:</b> Embedding AI early in the pipeline provides what Heisler and Frere describe as a "
        "\"mechanical advantage, akin to moving the fulcrum on a lever closer to the load\" [7]. By enriching data with "
        "AI-derived signals at ingestion time, downstream consumers benefit without additional effort.",
        styles)
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.2 The Silver Layer: Agentic Transformation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Silver layer is where the agentic paradigm most visibly departs from traditional data engineering. Rather "
        "than human-authored transformation scripts, Silver tables are generated by an autonomous 5-phase workflow:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    phases = [
        ["Phase 1: Trigger", "Detects events: new Bronze table, schema change, quality threshold breach"],
        ["Phase 2: Planner", "LLM (Claude 3.5 Sonnet) analyzes schema, considers contracts/directives/learnings, determines strategy"],
        ["Phase 3: Executor", "LLM generates CREATE OR REPLACE DYNAMIC TABLE DDL with CDC deduplication; self-corrects on failure (3 retries)"],
        ["Phase 4: Validator", "Compares source/target: row counts (\u00b15%), schema match, type verification, referential integrity"],
        ["Phase 5: Reflector", "LLM extracts learnings (success patterns, failure patterns, optimizations) with confidence scores"],
    ]
    t = make_table(["Phase", "Description"], phases, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.3 The Gold Layer: Agentic Aggregation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Gold layer extends the agentic pattern to business-ready aggregations. The "
        "<font face='Courier' size='8'>BUILD_GOLD_FOR_NEW_TABLES</font> procedure employs a two-strategy discovery: "
        "(1) Missing Gold targets from the TABLE_LINEAGE_MAP, and (2) Uncovered Silver tables with no lineage mapping. "
        "In both cases, the LLM generates aggregation DDL, validates it, registers lineage, and refreshes the Knowledge Graph.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.4 The Ephemeral Nature of Silver and Gold", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "A defining property of the Agentic Data Foundry is that Silver and Gold layers are <b>ephemeral</b> \u2014 they are "
        "derived, not primary. Because every Silver and Gold table is defined by a Dynamic Table DDL (stored in logs), "
        "guided by Schema Contracts and Directives (stored in metadata), and populated from Bronze (the immutable source "
        "of truth), the entire Silver and Gold layers can be regenerated from scratch at any time.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    implications = [
        ["Schema evolution", "Agentic workflow regenerates affected tables rather than patching them"],
        ["Environment provisioning", "Dev, staging, production diverge only in metadata, not pipeline code"],
        ["Disaster recovery", "Bronze + metadata = complete system reconstruction"],
        ["Technical debt", "Cannot accumulate \u2014 there is no legacy transformation code to maintain"],
    ]
    t = make_table(["Implication", "Impact"], implications, [1.5*inch, 4.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "This aligns with the broader industry movement toward treating analytics layers as materialized views of intent "
        "rather than independently maintained data stores [8].",
        styles['BodyText2']))
    elements.append(Spacer(1, 16))


def build_section4(elements, styles):
    elements.append(Paragraph("4. The Three-Layer Control Model", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("4.1 Schema Contracts: Structural Guardrails", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Schema Contracts define <i>what the output must look like</i> \u2014 column names, data types, and required fields. "
        "They are stored in <font face='Courier' size='8'>METADATA.SILVER_SCHEMA_CONTRACTS</font> and enforced during the "
        "Executor phase. Without a contract, the LLM freely chooses column names \u2014 one execution might produce "
        "<font face='Courier' size='8'>FULL_NAME</font>, another <font face='Courier' size='8'>FIRST_NAME</font> and "
        "<font face='Courier' size='8'>LAST_NAME</font>. Schema Contracts eliminate this non-determinism.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_code_block(elements, """[
  {"name": "CUSTOMER_ID", "type": "INTEGER", "required": true},
  {"name": "FIRST_NAME",  "type": "VARCHAR", "required": true},
  {"name": "LAST_NAME",   "type": "VARCHAR", "required": true},
  {"name": "EMAIL",       "type": "VARCHAR", "required": true},
  {"name": "IS_DELETED",  "type": "BOOLEAN", "required": true}
]""", styles)
    elements.append(Paragraph(
        "Contracts can be authored manually, generated from existing Silver tables, or proposed by an LLM from Bronze "
        "schema analysis with human review.",
        styles['BodyText2']))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("4.2 Transformation Directives: Business Intent", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Directives declare <i>why the data exists</i> \u2014 the business purpose that guides transformation decisions. "
        "They are stored in <font face='Courier' size='8'>METADATA.TRANSFORMATION_DIRECTIVES</font> and injected into "
        "LLM prompts during the Planner phase. A directive is not a transformation specification \u2014 it is a statement of intent:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_callout(elements,
        "<i>\"This data feeds a demand forecasting model. Preserve daily granularity. Create 7/14/30 day rolling averages. "
        "Exclude test accounts (industry = 'TEST'). The model requires at minimum 90 days of history.\"</i>",
        styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The LLM interprets this intent and generates appropriate SQL. If the directive says \"churn prediction,\" the LLM "
        "computes RFM features. If it says \"executive dashboard,\" the LLM pre-aggregates to weekly granularity. "
        "Directives support source table patterns, target layers, priority weighting, and LLM-assisted generation.",
        styles['BodyText2']))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("4.3 Learnings: Accumulated Memory", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Learnings capture patterns discovered during execution and persist them for future use. They function as the "
        "system's institutional memory \u2014 the equivalent of tribal knowledge that typically exists only in senior "
        "engineers' heads. Each learning has a pattern signature, observation, recommendation, confidence score, and "
        "active/inactive flag. The Reflector phase generates learnings; the Planner phase consumes them, creating a "
        "feedback loop where the system improves with every execution.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section5(elements, styles):
    elements.append(Paragraph("5. The Knowledge Graph: Lineage as Infrastructure", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Agentic Data Foundry maintains a Knowledge Graph (KNOWLEDGE_GRAPH schema) that encodes relationships between "
        "all database objects \u2014 nodes (databases, schemas, tables, columns) with LLM-generated descriptions and vector "
        "embeddings, and edges (CONTAINS, HAS_COLUMN, TRANSFORMS_TO, AGGREGATES_TO). The Knowledge Graph serves three "
        "critical functions:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    funcs = [
        ["Lineage-Aware\nImpact Analysis", "Before regenerating a Silver table, the system queries downstream dependencies. "
         "If SILVER.CUSTOMERS changes, the KG reveals GOLD.CUSTOMER_360, GOLD.ML_CUSTOMER_FEATURES, and GOLD.CUSTOMER_METRICS "
         "are all impacted."],
        ["Semantic Pattern\nReuse", "When a new table arrives (e.g., INVOICES), vector similarity search finds analogous "
         "tables (e.g., ORDERS). The transformation pattern seeds the LLM prompt, improving first-attempt accuracy."],
        ["Enriched LLM\nContext", "Every LLM prompt is enriched with KG context: table descriptions, lineage relationships, "
         "similar table patterns, and downstream impact assessments."],
    ]
    t = make_table(["Function", "Description"], funcs, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


def build_section6(elements, styles):
    elements.append(Paragraph("6. The TABLE_LINEAGE_MAP: Single Source of Truth", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "At the center of the metadata layer is the TABLE_LINEAGE_MAP \u2014 a living registry of all table-to-table "
        "relationships that is both <b>human-seeded</b> and <b>agent-populated</b>:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    lineage = [
        ["CUSTOMERS_VARIANT", "CUSTOMERS", "TRANSFORMS_TO", "Seed"],
        ["ORDERS_VARIANT", "ORDERS", "TRANSFORMS_TO", "Seed"],
        ["CUSTOMERS", "CUSTOMER_360", "AGGREGATES_TO", "Seed"],
        ["ORDERS", "ORDER_SUMMARY", "AGGREGATES_TO", "Agent"],
        ["CUSTOMERS", "ML_CUSTOMER_FEATURES", "AGGREGATES_TO", "Agent"],
    ]
    t = make_table(["SOURCE_TABLE", "TARGET_TABLE", "RELATIONSHIP_TYPE", "ORIGIN"], lineage,
                   [1.7*inch, 1.7*inch, 1.5*inch, 0.7*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("Dual Population Model", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The lineage map is not a static configuration file \u2014 it is a dynamic registry that grows through two mechanisms:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        "<b>1. Human-Seeded Entries.</b> Data engineers declare known, intentional relationships \u2014 for example, "
        "that CUSTOMERS_VARIANT should produce a typed CUSTOMERS Silver table, or that CUSTOMERS + ORDERS + "
        "SUPPORT_TICKETS should feed a CUSTOMER_360 Gold view. These entries express <i>architectural intent</i> "
        "before any agent executes.",
        styles['BulletItem']))
    elements.append(Spacer(1, 2))
    elements.append(Paragraph(
        "<b>2. Agent-Populated Entries.</b> When the agentic Gold builder "
        "(<font face='Courier' size='8'>BUILD_GOLD_FOR_NEW_TABLES</font>) generates and executes a new Gold Dynamic "
        "Table, the <font face='Courier' size='8'>REGISTER_LINEAGE_FROM_DDL</font> procedure automatically parses the "
        "generated SQL, extracts all Silver table references from FROM and JOIN clauses, and inserts new lineage entries "
        "via MERGE. These entries are tagged as auto-registered, creating an audit trail that distinguishes human intent "
        "from agent discovery.",
        styles['BulletItem']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        "This dual model means the lineage map starts with a human-defined skeleton and grows organically as agents "
        "discover new relationships. The agentic build processes consult this map to identify gaps using two strategies: "
        "(1) Gold targets declared in the map but not yet materialized, and (2) Silver tables with no downstream "
        "mapping at all \u2014 \"uncovered\" tables that the agents autonomously build Gold aggregations for and then "
        "register back into the map.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_callout(elements,
        "<b>Self-Expanding Registry:</b> Human architects define the initial vision and autonomous agents fill in the "
        "details. Every relationship \u2014 whether human-authored or agent-discovered \u2014 is tracked in a single "
        "source of truth.",
        styles)
    elements.append(Spacer(1, 10))


def build_section7(elements, styles):
    elements.append(Paragraph("7. The Evolving Role of the Data Engineer", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Agentic Data Foundry does not eliminate the data engineer \u2014 it elevates the role:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    roles = [
        ["Architect of Intent", "Writes Directives expressing business requirements in natural language instead of DDL"],
        ["Curator of Contracts", "Defines Schema Contracts constraining LLM behavior; requires understanding of downstream consumers"],
        ["Reviewer of Agents", "Reviews and approves LLM-generated transformation logic; the system logs every decision with full reasoning"],
        ["Steward of Learnings", "Monitors accumulated learnings, deactivating incorrect patterns and reinforcing correct ones"],
    ]
    t = make_table(["New Role", "Description"], roles, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "This evolution mirrors broader industry trends. As the Data Engineering Academy observes, \"The future data "
        "engineer is less a builder of pipelines and more a designer of data systems\" [9]. Forbes' 2026 AI predictions "
        "similarly identify \"the shift from manual engineering to AI-orchestrated data workflows\" as a defining trend [10].",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section8(elements, styles):
    elements.append(Paragraph("8. Implementation: A Working System", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Agentic Data Foundry is a deployed system running on Snowflake:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    components = [
        ["Bronze Dynamic Tables", "Snowflake Dynamic Tables (VARIANT)", "5"],
        ["Silver Dynamic Tables", "LLM-generated, CDC-aware", "5"],
        ["Gold Dynamic Tables", "LLM-generated, multi-source aggregation", "4+"],
        ["Stored Procedures", "Python + SQL in Snowflake", "20+"],
        ["Metadata Tables", "Contracts, Directives, Learnings, Lineage", "12+"],
        ["Knowledge Graph", "Nodes, Edges, Embeddings (e5-base-v2)", "200+"],
        ["LLM Models", "Claude 3.5 Sonnet, Llama 3.1-8b", "2"],
        ["Streamlit Application", "13-tab management interface", "1"],
    ]
    t = make_table(["Component", "Technology", "Count"], components,
                   [1.5*inch, 3.0*inch, 0.7*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "The system has demonstrated autonomous end-to-end execution: from detecting a new PostgreSQL source table "
        "landing via Openflow CDC, through Bronze onboarding, Silver transformation, Gold aggregation, Knowledge Graph "
        "enrichment, and Semantic View generation \u2014 with zero human-written transformation code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section9(elements, styles):
    elements.append(Paragraph("9. Design Decisions", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    decisions = [
        ["Bronze Is Immutable", "OBJECT_CONSTRUCT(*) captures the full payload as VARIANT. Schema changes in the source never break Bronze."],
        ["Dynamic Tables Over\nStored Procedures", "Declarative refresh semantics, automatic dependency tracking, and built-in observability via TARGET_LAG."],
        ["LLM DDL Is Validated\nBefore Execution", "Every LLM-generated DDL passes through VALIDATE_GOLD_DDL that compiles without executing."],
        ["Metadata Is the Product", "The primary deliverable of human work is metadata (contracts, directives, lineage maps), not code. Code is generated from metadata."],
    ]
    t = make_table(["Decision", "Rationale"], decisions, [1.5*inch, 4.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


def build_section10(elements, styles):
    elements.append(Paragraph("10. Challenges and Limitations", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("LLM Non-Determinism", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The same Bronze schema may produce slightly different Silver DDL across executions. Schema Contracts mitigate "
        "but do not eliminate this. Future work includes deterministic DDL templates with LLM-powered parameterization.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Cost: Inference vs. Total Cost of Ownership", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "LLM inference for transformation planning is more expensive per-execution than static SQL. The initial "
        "transformation of a new table incurs meaningful Cortex AI credits, and the system amortizes this through cached "
        "learnings and pattern reuse. However, evaluating agentic data engineering solely on inference cost misses the "
        "broader economic picture. The relevant comparison is not <i>LLM inference vs. SQL execution</i> \u2014 it is "
        "<b>total cost of agentic pipeline ownership vs. total cost of manual pipeline ownership</b>.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        "Traditional pipeline development carries substantial hidden costs that are rarely attributed to the pipeline itself:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    tco_data = [
        ["New table onboarding", "Days to weeks of\nengineer time", "Minutes\n(autonomous)"],
        ["Schema change\nresponse", "Manual investigation,\ncode change, test, deploy", "Automatic regeneration\nfrom metadata"],
        ["Pipeline maintenance", "40-60% of data\nengineering time [1]", "Near-zero (ephemeral,\nregenerable)"],
        ["Knowledge transfer", "Tribal knowledge,\nperson-dependent", "Encoded in Learnings,\nContracts, Directives"],
        ["Quality issue\ndiagnosis", "Reactive debugging after\ndownstream failure", "Proactive validation\nat generation time"],
        ["Time to production", "Weeks to months per\nnew data product", "Hours to days"],
    ]
    t = make_table(["Cost Category", "Traditional (Manual)", "Agentic"], tco_data,
                   [1.4*inch, 2.3*inch, 2.3*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        "Industry data supports the economic case for AI-driven automation. IDC reports an average ROI of <b>3.7\u00d7 per "
        "dollar invested</b> in generative AI projects, with top-performing organizations achieving up to 10.3\u00d7 [11]. "
        "Deloitte's 2026 State of AI survey found that <b>66% of enterprises report measurable productivity gains</b> "
        "from AI adoption, with an average 21% productivity improvement and 15% cost reduction [12]. NVIDIA's 2026 State "
        "of AI report \u2014 surveying 3,200+ enterprises \u2014 found that <b>88% reported AI increased annual revenue</b> "
        "and 87% reported AI reduced annual costs, with 53% citing improved employee productivity as the single biggest "
        "operational impact [13].",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    add_callout(elements,
        "<b>The Compounding Advantage:</b> Each manually maintained pipeline carries a compounding maintenance "
        "burden \u2014 schema drift, quality regression, documentation decay. Agentic pipelines, regenerated from "
        "metadata, carry a fixed cost per regeneration and zero ongoing maintenance cost. The economic advantage "
        "grows with every pipeline added.",
        styles)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("Trust and Auditability", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Production data teams require full audit trails. The system logs every LLM prompt, response, generated DDL, "
        "and validation result \u2014 but organizational trust in AI-generated transformations is still developing.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Complex Business Logic", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Highly specific business rules (e.g., fiscal calendar calculations, regulatory compliance) may exceed what an "
        "LLM can reliably generate from a natural language directive. The system supports manual overrides for these cases.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section11(elements, styles):
    elements.append(Paragraph("11. Conclusion", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Agentic Data Foundry demonstrates that the transition from imperative to declarative data engineering is "
        "not only possible but practical. By combining <b>AI in the Bronze Layer</b> for autonomous discovery and onboarding, "
        "<b>Agentic Transformation</b> for LLM-powered Silver and Gold generation, <b>Ephemeral Derived Layers</b> that can "
        "be regenerated from metadata + Bronze, <b>Intent-Driven Directives</b> that replace pipeline code with business "
        "purpose, <b>Schema Contracts</b> that provide deterministic guardrails, and a <b>Knowledge Graph</b> that gives "
        "agents lineage awareness and institutional memory, the system achieves something that would have seemed improbable "
        "even two years ago: a data platform where the gap between \"data arrives\" and \"business insight is available\" is "
        "bridged by autonomous AI agents guided by human intent.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "The data engineer's role evolves from builder to describer, from coder to curator, from reactive debugger to "
        "proactive architect. This is not the end of data engineering \u2014 it is the beginning of data engineering's "
        "most productive era.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "The trajectory is clear. Snowflake's March 2026 launch of Project SnowWork \u2014 an autonomous AI platform "
        "that enables business users to move \"from intent to actions and outcomes\" on governed data [14] \u2014 "
        "demonstrates that the agentic paradigm is not a research concept but an emerging product category. The Agentic "
        "Data Foundry occupies a specific and critical position in this landscape: where Project SnowWork brings agentic "
        "intelligence to business users <i>consuming</i> data, the Agentic Data Foundry brings the same principles to "
        "the data engineering teams <i>producing</i> it. Together, they represent the full arc of the agentic enterprise "
        "\u2014 from data creation to data consumption, from pipeline to insight to action, all governed by intent "
        "rather than code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 16))


def build_references(elements, styles):
    elements.append(Paragraph("References", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=SF_LIGHT_GRAY, spaceAfter=8))

    refs = [
        '[1] Anaconda, "State of Data Science 2022," 2022. anaconda.com',
        '[2] Gartner, "Gartner Predicts 40% of Enterprise Apps Will Feature Task-Specific AI Agents by 2026," Aug 2025. gartner.com',
        '[3] McKinsey &amp; Company, "Agentic AI Advances," McKinsey Featured Insights, 2025. mckinsey.com',
        '[4] Snowflake, "Democratizing Enterprise AI: New AI Capabilities," Snowflake Blog, Jun 2025. snowflake.com',
        '[5] Databricks, "How AI Is Transforming Data Engineering," Databricks Blog, 2025. databricks.com',
        '[6] Capgemini, "From Data Pipelines to AI-Driven Integration," Capgemini Insights, 2025. capgemini.com',
        '[7] J. Heisler and G. Frere, "AI-Infused Pipelines with Snowflake Cortex," Snowflake Builders Blog, Oct 2024. medium.com/snowflake',
        '[8] InfoQ, "The End of the Bronze Age: Rethinking the Medallion Architecture," 2025. infoq.com',
        '[9] Data Engineering Academy, "The Future of Data Engineering in an AI-Driven World," 2025. dataengineeracademy.com',
        '[10] M. Minevich, "Agentic AI Takes Over \u2014 11 Shocking 2026 Predictions," Forbes, Dec 2025. forbes.com',
        '[11] IDC, "Business Opportunity of AI: GenAI Delivering New Business Value and Increasing ROI," 2025. idc.com',
        '[12] Deloitte, "The State of AI in the Enterprise," Deloitte AI Institute, 2026. deloitte.com',
        '[13] NVIDIA, "State of AI Report 2026: How AI Is Driving Revenue, Cutting Costs and Boosting Productivity," Mar 2026. blogs.nvidia.com',
        '[14] Snowflake, "Snowflake Launches Project SnowWork, Bringing Outcome-Driven AI to Every Business User," Mar 2026. snowflake.com',
    ]
    for ref in refs:
        elements.append(Paragraph(ref, styles['RefText']))
    elements.append(Spacer(1, 16))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=SF_LIGHT_GRAY, spaceAfter=6))
    elements.append(Paragraph(
        '<i>This whitepaper describes the Agentic Data Foundry reference implementation available at '
        'github.com/dbaontap/agentic-data-foundry. Built on Snowflake using Cortex AI, Dynamic Tables, Openflow, '
        'and Streamlit in Snowflake.</i>',
        styles['SmallNote']))


def header_footer(canvas, doc):
    canvas.saveState()
    if doc.page > 1:
        canvas.setFont('Helvetica', 7)
        canvas.setFillColor(SF_GRAY)
        canvas.drawString(0.75 * inch, 0.5 * inch,
                          "The Agentic Data Foundry  |  AI-Native Data Engineering  |  March 2026")
        canvas.drawRightString(7.75 * inch, 0.5 * inch, f"Page {doc.page - 1}")
        canvas.setStrokeColor(SF_LIGHT_GRAY)
        canvas.setLineWidth(0.5)
        canvas.line(0.75 * inch, 0.6 * inch, 7.75 * inch, 0.6 * inch)
    canvas.restoreState()


def main():
    doc = SimpleDocTemplate(
        OUTPUT_PDF,
        pagesize=letter,
        topMargin=0.7 * inch,
        bottomMargin=0.8 * inch,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
    )

    styles = build_styles()
    elements = []

    build_cover(elements, styles)
    build_abstract(elements, styles)
    build_section1(elements, styles)
    build_section2(elements, styles)
    build_section3(elements, styles)
    build_section4(elements, styles)
    build_section5(elements, styles)
    build_section6(elements, styles)
    build_section7(elements, styles)
    build_section8(elements, styles)
    build_section9(elements, styles)
    build_section10(elements, styles)
    build_section11(elements, styles)
    build_references(elements, styles)

    doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)
    print(f"PDF generated: {OUTPUT_PDF}")


if __name__ == "__main__":
    main()
