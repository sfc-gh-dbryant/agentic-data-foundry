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
import sys
import re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagram_helpers import DrawingFlowable
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.lib.colors import Color

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
            [Paragraph("Danny Bryant", styles['CoverSubtitle'])],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("March 2026", styles['CoverSubtitle'])],
            [Spacer(1, 1.7 * inch)],
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
        "The data engineering discipline is hitting a wall. For two decades, we've tricked ourselves into thinking that "
        "hand-coding ETL scripts and manually mapping schemas was \"engineering.\" In reality, it was just expensive plumbing. "
        "As we enter the era of the Agentic Enterprise, this manual approach isn't just slow \u2014 it's a systemic liability.",
        styles['AbstractText']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Snowflake has provided the world's most powerful engine with <b>Cortex AI</b> and <b>Project SnowWork</b>, but "
        "these tools are only as effective as the data they consume. If your \"Back-of-House\" data supply chain is still a "
        "web of brittle, human-dependent pipes, your AI agents will inevitably fail.",
        styles['AbstractText']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The <b>Agentic Data Foundry</b> is the missing architectural layer. It represents a fundamental shift from "
        "<b>Building</b> to <b>Describing</b>. By leveraging Snowflake's native capabilities \u2014 Openflow for ingestion, "
        "Dynamic Tables for orchestration, and Cortex for reasoning \u2014 we have built a system that autonomously discovers, "
        "transforms, and validates data. In this paradigm, the data engineer doesn't write code; they <b>curate intent</b>. "
        "This is the blueprint for an autonomous data lifecycle that moves at the speed of business thought, ensuring that "
        "Snowflake is the central <b>System of Action</b> for the modern enterprise.",
        styles['AbstractText']))
    elements.append(Spacer(1, 12))


def build_section1(elements, styles):
    elements.append(Paragraph("1. Introduction: The Case for Change \u2014 Escaping the Maintenance Trap", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "Every modern data leader is currently paying a \"plumbing tax\" that is bankrupting their roadmap. We've spent "
        "two decades perfecting progressive refinement \u2014 staging data through capture, conformance, and consumption "
        "layers \u2014 yet we're still stuck in a cycle of manual labor: a source DB changes a column name, a pipeline "
        "breaks, an executive sees a blank dashboard, and an engineer spends Friday night debugging SQL.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "We don't have a data problem; we have a <b>coordination problem</b>. Project SnowWork and the era of \"Agentic "
        "Enterprises\" promise a world where business users just \"ask and get.\" But if your underlying data infrastructure "
        "still relies on hand-coded brittle pipes, those agents are just going to hallucinate at scale. The Agentic Data "
        "Foundry isn't just a new tool; it's the mandatory \"back-of-house\" engine that makes the agentic future possible. "
        "We are moving the engineer from the engine room to the bridge, where they don't turn the gears \u2014 they set the course.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Modern data teams spend an estimated 40-60% of their time on pipeline maintenance rather than value creation [1]. "
        "Schema changes break downstream transformations. New data sources require weeks of manual onboarding. Quality "
        "issues propagate silently through layers until they surface in executive dashboards. The progressive refinement "
        "pattern (Bronze \u2192 Silver \u2192 Gold) provided a useful organizational model, but the <i>implementation</i> of that "
        "pattern remains overwhelmingly manual.",
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

    elements.append(Paragraph("3.1 Intent-to-Insight: A Single Data Point\u2019s Journey", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "To illustrate the end-to-end autonomous path, consider a single CDC event \u2014 a customer's "
        "<font face='Courier' size='8'>updated_at</font> timestamp changes in PostgreSQL \u2014 traced through to a "
        "Gold-layer churn prediction feature with zero human-written transformation code:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    journey = [
        ["1", "PostgreSQL SOURCE", "Logical replication propagates row change to LANDING_PG"],
        ["2", "Openflow CDC", "Row appears in Snowflake public.customers with _SNOWFLAKE_UPDATED_AT set"],
        ["3", "Bronze DT", "OBJECT_CONSTRUCT(*) absorbs change into CUSTOMERS_VARIANT (VARIANT)"],
        ["4", "Stream fires", "CUSTOMERS_LANDING_STREAM triggers AGENTIC_WORKFLOW_TRIGGER_TASK"],
        ["5", "Planner", "LLM reads Schema Contract + Directive (\"churn prediction: compute RFM features\") + Learnings"],
        ["6", "Executor", "LLM generates SILVER.CUSTOMERS DDL with ROW_NUMBER() dedup + derived RECENCY_DAYS, SEGMENT"],
        ["7", "Validator", "Row count \u00b15%, null rates compared, contract columns verified"],
        ["8", "Gold DT refresh", "ML_CUSTOMER_FEATURES auto-refreshes, computing IS_CHURNED, LIFETIME_VALUE, FREQUENCY"],
        ["9", "AI Consumption", "Business user asks Cortex Analyst: \"Which customers are at risk of churning?\""],
    ]
    t = make_table(["#", "Stage", "What Happens"], journey, [0.3*inch, 1.3*inch, 4.5*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>Total human code written: zero.</b> Total human artifacts: one Schema Contract, one Transformation Directive.",
        styles)
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.2 The Bronze Layer: AI-Activated Ingestion", styles['SubsectionTitle']))
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

    elements.append(Paragraph("3.3 The Silver Layer: Agentic Transformation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Silver layer is where the agentic paradigm most visibly departs from traditional data engineering. Rather "
        "than human-authored transformation scripts, Silver tables are generated by an autonomous 5-phase workflow. "
        "Each phase is a distinct agent responsibility with defined inputs, outputs, and failure modes.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.3.1 Phase 1: The Trigger \u2014 Event-Driven Activation", styles['SubsubTitle']))
    elements.append(Paragraph(
        "The Trigger is the system's nervous system. It doesn't poll on a schedule \u2014 it <i>reacts</i> to structural events. "
        "Three event classes activate the agentic workflow:",
        styles['BodyText2']))
    elements.append(Spacer(1, 2))
    trigger_rows = [
        ["New Table Detection", "DISCOVER_AND_ONBOARD_NEW_TABLES finds a landing table with no Bronze DT. Creates Bronze "
         "infrastructure, fires a Stream. AGENTIC_WORKFLOW_TRIGGER_TASK scans for Bronze tables lacking Silver counterparts."],
        ["Schema Drift", "Current Bronze VARIANT payload keys are compared against the last-known schema snapshot. "
         "New keys trigger re-planning; dropped keys trigger downstream impact assessment via the Knowledge Graph."],
        ["Quality Breach", "Scheduled validation detects >5% row count variance, null rate spike, or distribution anomaly. "
         "Initiates a regeneration cycle rather than patching the existing table."],
    ]
    t = make_table(["Event Class", "Behavior"], trigger_rows, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The Trigger\u2019s output is a <i>work order</i> \u2014 a JSON payload containing the Bronze table name, event type, "
        "current schema snapshot, and downstream dependencies. This work order is the Planner\u2019s input.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.3.2 Phase 2: The Planner \u2014 Context Assembly and Strategy", styles['SubsubTitle']))
    elements.append(Paragraph(
        "The Planner is the most LLM-intensive phase. Its job is not to write SQL \u2014 it is to <i>decide what SQL should "
        "be written</i>. The Planner assembles a rich context window from four sources:",
        styles['BodyText2']))
    elements.append(Spacer(1, 2))
    planner_rows = [
        ["Schema Context", "Bronze VARIANT keys, inferred types, sample values, cardinality estimates. For CUSTOMERS_VARIANT: "
         "CUSTOMER_ID is integer PK, EMAIL contains valid addresses, _SNOWFLAKE_UPDATED_AT is the CDC timestamp."],
        ["Contract + Directive", "Any active Schema Contract (column names, types, required flags) and matching Transformation "
         "Directives (business intent like \"churn prediction: compute RFM features\"). Contract constrains shape; directive guides logic."],
        ["Knowledge Graph", "Vector similarity search finds analogous tables transformed before. If INVOICES_VARIANT arrives "
         "and ORDERS_VARIANT used a specific CDC dedup pattern, that pattern is surfaced. Also provides downstream dependency info."],
        ["Learnings", "All active learnings matching the table's pattern signature \u2014 both positive (\"CDC tables need "
         "ROW_NUMBER() dedup\") and negative (\"Window functions on SUPPORT_TICKETS cause timeout due to skew\")."],
    ]
    t = make_table(["Context Source", "What It Provides"], planner_rows, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The Planner\u2019s output is a <i>transformation strategy</i> \u2014 not SQL, but a structured plan: \"Use ROW_NUMBER() "
        "dedup on _SNOWFLAKE_UPDATED_AT, extract 12 columns, derive RECENCY_DAYS, apply churn prediction directive by "
        "computing RFM features.\" This strategy document is the Executor\u2019s input.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.3.3 Phase 3: The Executor \u2014 SQL Generation with Self-Correction", styles['SubsubTitle']))
    elements.append(Paragraph(
        "The Executor is where strategy becomes DDL. The LLM (Claude 3.5 Sonnet) takes the Planner\u2019s strategy and "
        "generates a complete CREATE OR REPLACE DYNAMIC TABLE statement with CDC-aware deduplication using ROW_NUMBER(), "
        "type-safe column extraction from VARIANT, derived columns from Directives, TARGET_LAG of 1 minute, and soft-delete filtering.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>The Self-Correction Loop:</b> If the DDL fails compilation, the Snowflake error message is injected back into "
        "the LLM prompt: <i>\"Your DDL failed with: 'invalid identifier SRC:full_name'. The VARIANT payload contains "
        "'first_name' and 'last_name' but not 'full_name'. Fix the DDL.\"</i> The system retries up to 3 times, with each "
        "attempt receiving accumulated error context from all previous failures. For Gold DDL, the Executor additionally runs "
        "VALIDATE_GOLD_DDL \u2014 catching the 15-20% of first attempts where the LLM hallucinates a table name.",
        styles)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.3.4 Phase 4: The Validator \u2014 Deterministic Trust Boundary", styles['SubsubTitle']))
    elements.append(Paragraph(
        "This is the phase that separates a toy demo from a production system. The Validator is the hard boundary between "
        "AI-generated optimism and production reality. We treat every agent-generated SQL statement as <b>guilty until "
        "proven innocent</b>. The Validator isn't an AI \"vibe check.\" It is a battery of deterministic, hard-coded guardrails:",
        styles['BodyText2']))
    elements.append(Spacer(1, 2))
    validator_rows = [
        ["Row Count Parity", "Silver count must be within \u00b15% of source Bronze (after dedup). A 10% delta means something "
         "was silently dropped or duplicated \u2014 the DDL is rejected."],
        ["Contract Enforcement", "Every column in the Schema Contract must be present with correct type. CUSTOMER_ID INTEGER "
         "produced as VARCHAR = failure, regardless of whether the data \"looks fine.\""],
        ["Statistical Profiling", "Null rates, distinct counts, and distributions compared column-by-column against Bronze. "
         "A Silver column with 40% nulls when Bronze has 0% triggers investigation."],
        ["Referential Integrity", "Foreign key columns checked against parent tables. CUSTOMER_ID in ORDERS must exist in "
         "CUSTOMERS. Orphaned references indicate join/filter errors."],
        ["Semantic Assertions", "LLM-generated domain checks: monetary values non-negative, dates not in future, email formats "
         "match regex, status fields contain only valid enum values."],
    ]
    t = make_table(["Check", "Description"], validator_rows, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "If a generated pipeline fails any check, it is <b>rejected before it ever touches production</b>. We don\u2019t ask "
        "the AI to be \"perfect\"; we build a system that makes it impossible for the AI to be \"wrong\" in a way that affects "
        "the business. Failures feed back into the Executor\u2019s self-correction loop with specific diagnostics.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.3.5 Phase 5: The Reflector \u2014 Institutional Memory", styles['SubsubTitle']))
    elements.append(Paragraph(
        "The Reflector transforms the Foundry from a stateless LLM wrapper into a system that <i>gets smarter over time</i>. "
        "After every execution \u2014 success or failure \u2014 the LLM extracts three categories of learning:",
        styles['BodyText2']))
    elements.append(Spacer(1, 2))
    reflector_rows = [
        ["Success Patterns", "\"Tables with temporal data benefit from ORDER_MONTH derivation.\" \"CDC tables with compound "
         "primary keys require multi-column PARTITION BY.\" Seeds future Planner prompts."],
        ["Failure Patterns", "\"Nested VARIANT arrays require LATERAL FLATTEN.\" \"LEAD()/LAG() on SUPPORT_TICKETS causes "
         "timeout due to partition skew.\" Prevents repeating expensive mistakes."],
        ["Optimizations", "\"Cluster on CUSTOMER_ID for join performance.\" \"Pre-aggregate by date before window functions "
         "on high-cardinality tables.\" Improves performance without changing correctness."],
    ]
    t = make_table(["Category", "Examples"], reflector_rows, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Each learning is persisted with a confidence score, pattern signature, and active/inactive flag. When a senior "
        "engineer corrects an agent\u2019s join logic, that \"scar tissue\" is saved forever in the customer\u2019s Snowflake "
        "metadata \u2014 creating institutional memory that a generic LLM can never replicate.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.4 The Gold Layer: Agentic Aggregation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Gold layer extends the agentic pattern to business-ready aggregations. The "
        "<font face='Courier' size='8'>BUILD_GOLD_FOR_NEW_TABLES</font> procedure employs a two-strategy discovery: "
        "(1) Missing Gold targets from the TABLE_LINEAGE_MAP, and (2) Uncovered Silver tables with no lineage mapping. "
        "In both cases, the LLM generates aggregation DDL, validates it, registers lineage, and refreshes the Knowledge Graph.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("3.5 The Ephemeral Nature of Silver and Gold", styles['SubsectionTitle']))
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
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("4.3.1 Negative Learning Example", styles['SubsubTitle']))
    add_callout(elements,
        "<b>Learning #47</b> (confidence: 0.92, observed: 3 executions)<br/><br/>"
        "<b>Observation:</b> \"Using LEAD()/LAG() window functions on SUPPORT_TICKETS causes query timeout due to high "
        "micro-partition skew on CUSTOMER_ID. Snowflake\u2019s adaptive optimization cannot resolve the skew within a "
        "1-minute Dynamic Table TARGET_LAG.\"<br/><br/>"
        "<b>Recommendation:</b> \"For ticket-level temporal analysis, use self-joins with date range predicates instead "
        "of window functions. Pre-aggregate by CUSTOMER_ID before applying temporal calculations.\"<br/><br/>"
        "<b>Pattern Signature:</b> <font face='Courier' size='7.5'>window_func_skewed_partition_timeout</font>",
        styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "This negative learning prevents the Planner from repeatedly generating window-function-based strategies for "
        "skewed tables \u2014 a mistake that would cost credits and time with each failed attempt.",
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
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("5.1 Knowledge Graph Cold-Start: Greenfield Bootstrapping", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "A common concern with KG-dependent architectures is the cold-start problem: how does the graph get initialized "
        "for a brand-new enterprise deployment? The Agentic Data Foundry solves this through a three-phase bootstrap:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    bootstrap = [
        ["1. Structural\nDiscovery", "POPULATE_KG_FROM_INFORMATION_SCHEMA() scans every schema, table, and column. "
         "Creates DATABASE, SCHEMA, TABLE, COLUMN nodes with CONTAINS/HAS_COLUMN edges. Runs in seconds with zero human input."],
        ["2. Lineage\nSeeding", "Human architect populates TABLE_LINEAGE_MAP with known Bronze\u2192Silver and Silver\u2192Gold intent. "
         "Typically 2-3 rows per source table \u2014 measured in minutes, not days."],
        ["3. Semantic\nEnrichment", "CORTEX.COMPLETE() generates descriptions for every node; EMBED_TEXT_768('e5-base-v2') creates "
         "vector embeddings enabling similarity search from the first execution."],
    ]
    t = make_table(["Phase", "Description"], bootstrap, [1.1*inch, 5.2*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The KG does not require a \"critical mass\" of data to be useful. Even a single Bronze\u2192Silver\u2192Gold chain "
        "provides enough context for the agentic workflow to operate. The KG\u2019s value compounds as more tables flow "
        "through the system, but the minimum viable KG is remarkably small.",
        styles['BodyText2']))
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
        ["Reviewer of Agents", "Reviews and approves LLM-generated logic via dry_run mode, post-validation override, and learning curation. Every agent decision is logged with full LLM reasoning chains."],
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
    elements.append(Paragraph("9. Principles and Design Decisions", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("9.1 Bronze Is Immutable", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Bronze layer uses <font face='Courier' size='8'>OBJECT_CONSTRUCT(*)</font> to capture the complete source "
        "payload as a VARIANT column. This design ensures schema changes in the source never break Bronze, historical "
        "payloads are preserved exactly as received, and the agentic system always has the full context for transformation decisions.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("9.2 Dynamic Tables Over Stored Procedures", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Silver and Gold tables are implemented as Dynamic Tables with <font face='Courier' size='8'>TARGET_LAG</font> "
        "rather than stored procedure-based ETL. This provides declarative refresh semantics (Snowflake manages scheduling), "
        "automatic dependency tracking (downstream DTs refresh when upstream changes), and built-in observability "
        "(DT refresh history, lag monitoring).",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("9.3 Hallucination Guardrails: Multi-Layer DDL Validation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "LLM hallucination in SQL generation is the single greatest risk to trust in agentic data engineering. "
        "The system addresses this through a three-layer validation pipeline that executes <i>before</i> any generated "
        "DDL touches production data:",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    guardrails = [
        ["1. Syntactic\nCompilation", "Every generated DDL is compiled via EXECUTE IMMEDIATE in dry-run mode. Catches syntax errors and keyword misuse."],
        ["2. Semantic\nReference Check", "VALIDATE_GOLD_DDL parses SQL, extracts FROM/JOIN table references, verifies each exists in INFORMATION_SCHEMA. "
         "Suggests closest match via Levenshtein distance when LLM hallucinates a table name (observed in 15-20% of first attempts)."],
        ["3. Column\nVerification", "Checks that all referenced columns in SELECT, WHERE, GROUP BY, and JOIN ON clauses exist. "
         "Catches the common hallucination of plausible but non-existent columns (e.g., CUSTOMER_NAME vs FULL_NAME)."],
    ]
    t = make_table(["Layer", "Description"], guardrails, [1.1*inch, 5.2*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("9.4 Security and Governance in Agentic DDL", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "All agent-generated DDL executes under the calling role\u2019s privileges \u2014 agents cannot bypass RBAC. "
        "Snowflake\u2019s row access policies and dynamic data masking propagate through the Dynamic Table dependency chain, "
        "meaning agent-generated tables automatically respect governance policies applied to source tables. "
        "Every LLM prompt, generated DDL, and validation result is logged to TRANSFORMATION_LOG with full audit trail. "
        "A <font face='Courier' size='8'>dry_run</font> mode allows human review of all generated DDLs before production execution.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("9.5 Metadata Is the Product", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "In the Agentic Data Foundry, the primary deliverable of human work is <i>metadata</i> \u2014 contracts, directives, "
        "lineage maps \u2014 not code. This inverts the traditional relationship where metadata is an afterthought generated "
        "from code. Here, code is generated from metadata.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


def build_section10(elements, styles):
    elements.append(Paragraph("10. Challenges and Limitations", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("10.1 LLM Non-Determinism", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The same Bronze schema may produce slightly different Silver DDL across executions. Schema Contracts mitigate "
        "but do not eliminate this. Future work includes deterministic DDL templates with LLM-powered parameterization.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("10.2 Cost: Human Latency for Instant Consumption", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The most frequent objection to agentic workflows is the cost of LLM inference. Critics point to the Snowflake "
        "credits consumed by Cortex AI during the planning and validation phases.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>The Human Reality:</b> We are no longer trading SQL credits for LLM credits; we are trading "
        "<b>Human Latency</b> for <b>Instant Consumption</b>.",
        styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "\u2022 The real cost in a modern enterprise isn\u2019t the <b>$2.00 in compute</b> to map a schema; it\u2019s the "
        "<b>$20,000 in engineering salary</b> wasted on \"tribal knowledge\" while a data product sits in a 6-week JIRA queue.",
        styles['BulletItem']))
    elements.append(Spacer(1, 2))
    elements.append(Paragraph(
        "\u2022 By using <b>Ephemeral Derived Layers</b>, we reduce long-term TCO by eliminating the \"storage tax\" "
        "on unused, stagnant data. We only pay for the data the business actually needs, exactly when they need it.",
        styles['BulletItem']))
    elements.append(Spacer(1, 4))

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

    elements.append(Paragraph("10.3 Beyond Hallucinations: The Zero-Trust Model", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The fear of \"AI-generated garbage\" is valid if you treat an LLM as a black box. The Foundry does the opposite.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>The Human Reality:</b> We treat every agent-generated SQL statement as <b>guilty until proven innocent</b>.",
        styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "\u2022 Our <b>Validator Phase</b> isn\u2019t an AI \"vibe check.\" It is a battery of deterministic, hard-coded guardrails.",
        styles['BulletItem']))
    elements.append(Spacer(1, 2))
    elements.append(Paragraph(
        "\u2022 If a generated pipeline fails a row-count parity test or violates a null-check constraint, it is rejected "
        "before it ever touches production. We don\u2019t ask the AI to be \"perfect\"; we build a system that makes it "
        "impossible for the AI to be \"wrong\" in a way that affects the business.",
        styles['BulletItem']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The three-layer hallucination guardrail pipeline (Syntactic Compilation \u2192 Semantic Reference Check \u2192 Column "
        "Verification) catches the vast majority of hallucinated SQL before execution. Trust is not assumed \u2014 it is "
        "earned through verifiable evidence at every step.",
        styles['BodyText2']))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("10.4 The \"Scar Tissue\" Moat", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Finally, there is the concern of \"Platform Dependency.\"",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>The Human Reality:</b> By using the <b>Learnings Registry</b>, we are turning ephemeral engineering efforts "
        "into <b>Institutional Memory</b>.",
        styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "\u2022 When a senior engineer corrects an agent\u2019s join logic, that \"scar tissue\" is saved forever in the "
        "customer\u2019s Snowflake metadata.",
        styles['BulletItem']))
    elements.append(Spacer(1, 2))
    elements.append(Paragraph(
        "\u2022 This creates a defensive moat that a generic, off-the-shelf LLM can never replicate. Your Snowflake "
        "account literally becomes \"smarter\" the more you use it, making the Foundry the central brain of your "
        "data operations.",
        styles['BulletItem']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("10.5 Trust and Auditability", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Production data teams don\u2019t adopt tools on faith \u2014 they adopt tools they can audit. The Agentic Data Foundry "
        "treats auditability as a first-class architectural concern, not an afterthought.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Every LLM interaction is logged to <font face='Courier' size='8'>METADATA.TRANSFORMATION_LOG</font> with full "
        "provenance: the prompt sent, the model used, the raw response, the generated DDL, the validation result, and the "
        "final execution outcome \u2014 all tied to a unique execution ID and timestamp. A compliance officer can trace any "
        "Gold table column back through the exact LLM reasoning chain that produced it, the directive that guided it, and "
        "the contract that constrained it.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The system also supports a <font face='Courier' size='8'>dry_run</font> mode where the entire agentic workflow "
        "executes \u2014 Trigger through Reflector \u2014 but no DDL is materialized. The generated SQL is logged, validated, "
        "and available for human review in the Streamlit interface before a single table is touched. This is not a "
        "\"trust me\" system; it\u2019s a \"show your work\" system.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "Organizational trust in AI-generated transformations is still developing. Early adopters report a predictable "
        "trust curve: initial skepticism, followed by cautious adoption with heavy dry-run usage, followed by growing "
        "confidence as the audit trail demonstrates consistent accuracy. The Foundry is designed to meet teams wherever "
        "they are on that curve.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("10.6 Complex Business Logic", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Not everything belongs in an LLM prompt. Fiscal calendar calculations that follow a 4-4-5 retail pattern, "
        "regulatory compliance transformations governed by HIPAA or SOX, multi-entity consolidation rules with "
        "intercompany elimination \u2014 these are domains where precision is non-negotiable and the cost of a subtle "
        "error is catastrophic.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "The Agentic Data Foundry handles this through a deliberate escape hatch: <b>manual overrides</b>. A data "
        "engineer can define a Schema Contract and Transformation Directive that says, in effect, \"for this table, use "
        "this exact DDL\" \u2014 bypassing the LLM entirely while still benefiting from the Trigger, Validator, and "
        "Reflector phases. The override DDL is version-controlled in metadata, validated by the same deterministic "
        "guardrails, and tracked in the same audit trail.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    add_callout(elements,
        "<b>The 80/20 Rule:</b> The goal is not to replace every SQL statement with an LLM call \u2014 it\u2019s to "
        "eliminate the 80% of pipeline work that is repetitive, pattern-based, and mechanical, freeing engineers to focus "
        "on the 20% that genuinely requires domain expertise. The boundary between \"agentic\" and \"manual\" is a "
        "configuration choice, not an architectural limitation.",
        styles)
    elements.append(Spacer(1, 10))


def build_section11(elements, styles):
    elements.append(Paragraph("11. Conclusion: Bridging the \"Context Gap\"", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "Data engineering as we know it \u2014 the era of the \"manual plumber\" \u2014 is over. It has to be. You cannot "
        "scale an Agentic Enterprise one SQL script at a time. The Agentic Data Foundry demonstrates that the transition "
        "from imperative to declarative engineering is not only possible but practical.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "By combining AI-activated discovery, agentic transformation, and a Knowledge Graph that captures \"institutional "
        "memory,\" we\u2019ve created a system where the gap between \"data arrives\" and \"insight is actionable\" is "
        "bridged by autonomous agents guided by human intent.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))
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
        "demonstrates that the agentic paradigm is not a research concept but an emerging product category.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    snowwork = [
        ["Back-of-House\n(Foundry)", "Ingest \u2192 Transform \u2192 Validate \u2192 Materialize governed Gold tables + Semantic Views", "Data Engineering\n(Agentic)"],
        ["Hand-off Point", "Semantic Views with dimensions, measures, synonyms, verified queries", "Shared Contract"],
        ["Front-of-House\n(SnowWork)", "Natural language \u2192 SQL \u2192 Results \u2192 Actions \u2192 Outcomes", "Business Users\n(Agentic)"],
    ]
    t = make_table(["Layer", "Responsibility", "Owner"], snowwork, [1.3*inch, 3.5*inch, 1.3*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        "The Foundry produces the governed semantic layer that Project SnowWork consumes for business end-users. "
        "Without the Foundry, SnowWork operates on manually curated Gold tables \u2014 recreating the same bottleneck "
        "that agentic engineering was designed to eliminate. With the Foundry, the entire path from raw CDC event to "
        "business user insight is autonomous: agents build the data, agents serve the data, and humans govern at "
        "both ends through intent rather than code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "The Agentic Enterprise isn\u2019t a future state; with the Foundry on Snowflake, it\u2019s the current reality. "
        "<b>Let\u2019s stop building pipes and start describing the future.</b>",
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
