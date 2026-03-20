#!/usr/bin/env python3
"""Generate a detailed technical description PDF of the Agentic Data Foundry."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "DESCRIPTION.pdf")

SF_BLUE = HexColor("#29B5E8")
SF_DARK = HexColor("#0f3460")
SF_LIGHT_BG = HexColor("#F0F8FF")
SF_ACCENT = HexColor("#16537e")
SF_GREEN = HexColor("#198754")
SF_ORANGE = HexColor("#E67E22")
SF_RED = HexColor("#DC3545")
SF_PURPLE = HexColor("#6F42C1")
SF_GRAY = HexColor("#6C757D")
SF_LIGHT_GRAY = HexColor("#E9ECEF")
SF_WHITE = white
TABLE_HEADER_BG = HexColor("#0f3460")
TABLE_ALT_ROW = HexColor("#F8FAFC")
CODE_BG = HexColor("#F1F3F5")
BRONZE_COLOR = HexColor("#CD7F32")
SILVER_COLOR = HexColor("#708090")
GOLD_COLOR = HexColor("#DAA520")


def build_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='CoverTitle', fontName='Helvetica-Bold', fontSize=28, leading=34,
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
        name='TableHeader', fontName='Helvetica-Bold', fontSize=7.5, leading=10,
        textColor=SF_WHITE, alignment=TA_CENTER,
    ))
    styles.add(ParagraphStyle(
        name='TableCell', fontName='Helvetica', fontSize=7.5, leading=10,
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='TableCellBold', fontName='Helvetica-Bold', fontSize=7.5, leading=10,
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='TableCellCode', fontName='Courier', fontSize=7, leading=9.5,
        textColor=HexColor("#333333"),
    ))
    styles.add(ParagraphStyle(
        name='RefText', fontName='Helvetica', fontSize=8, leading=11,
        textColor=HexColor("#333333"), spaceBefore=2, spaceAfter=2,
        leftIndent=18,
    ))
    styles.add(ParagraphStyle(
        name='TOCEntry', fontName='Helvetica', fontSize=10, leading=16,
        textColor=SF_DARK, spaceBefore=2, spaceAfter=2,
        leftIndent=12,
    ))
    styles.add(ParagraphStyle(
        name='TOCEntryBold', fontName='Helvetica-Bold', fontSize=10, leading=16,
        textColor=SF_DARK, spaceBefore=4, spaceAfter=2,
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
        ('FONTSIZE', (0, 0), (-1, 0), 7.5),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
        ('TOPPADDING', (0, 0), (-1, 0), 5),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 7.5),
        ('TOPPADDING', (0, 1), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 3),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('GRID', (0, 0), (-1, -1), 0.5, HexColor("#DEE2E6")),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [SF_WHITE, TABLE_ALT_ROW]),
    ]))
    return t


def add_callout(elements, text, styles, bg=None):
    tbl = Table(
        [[Paragraph(text, styles['BodyText2'])]],
        colWidths=[6.3 * inch],
    )
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), bg or SF_LIGHT_BG),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
    ]))
    elements.append(tbl)
    elements.append(Spacer(1, 4))


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


def add_diagram(elements, text, styles, title=None):
    items = []
    if title:
        items.append(Paragraph(f"<b>{title}</b>", ParagraphStyle(
            'DiagTitle', fontName='Helvetica-Bold', fontSize=8.5, leading=12,
            textColor=SF_DARK, alignment=TA_CENTER, spaceBefore=2, spaceAfter=4,
        )))
    lines = text.strip().split('\n')
    formatted = '<br/>'.join(
        line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace(' ', '&nbsp;')
        for line in lines
    )
    items.append(Paragraph(formatted, ParagraphStyle(
        'DiagCode', fontName='Courier', fontSize=7, leading=9,
        textColor=SF_DARK, alignment=TA_LEFT,
    )))
    inner = Table([[item] for item in items], colWidths=[6.3 * inch])
    inner.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), HexColor("#F0F4F8")),
        ('LEFTPADDING', (0, 0), (-1, -1), 14),
        ('RIGHTPADDING', (0, 0), (-1, -1), 14),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('BOX', (0, 0), (-1, -1), 1, HexColor("#B0C4DE")),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
    ]))
    elements.append(inner)
    elements.append(Spacer(1, 6))


def esc(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def footer(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(SF_LIGHT_GRAY)
    canvas.setLineWidth(0.5)
    canvas.line(54, 42, 558, 42)
    canvas.setFont('Helvetica', 7.5)
    canvas.setFillColor(SF_GRAY)
    canvas.drawString(54, 30, "The Agentic Data Foundry  |  Technical Description  |  March 2026")
    canvas.drawRightString(558, 30, f"Page {doc.page}")
    canvas.restoreState()


# ============================================================
# COVER
# ============================================================
def build_cover(elements, styles):
    cover_bg = Table(
        [
            [Spacer(1, 1.0 * inch)],
            [Paragraph("The Agentic<br/>Data Foundry", styles['CoverTitle'])],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("Detailed Technical Description", ParagraphStyle(
                'CoverSub1', fontName='Helvetica', fontSize=15, leading=20,
                textColor=HexColor("#B0D4F1"), alignment=TA_LEFT,
            ))],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("Architecture, Components, Data Flow &amp; Object Inventory",
                        styles['CoverSubtitle'])],
            [Spacer(1, 0.15 * inch)],
            [Paragraph("March 2026", styles['CoverSubtitle'])],
            [Spacer(1, 1.5 * inch)],
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


# ============================================================
# TABLE OF CONTENTS
# ============================================================
def build_toc(elements, styles):
    elements.append(Paragraph("Table of Contents", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=10))
    toc = [
        ("1.", "System Overview"),
        ("2.", "End-to-End Data Flow"),
        ("3.", "Source Layer: PostgreSQL &amp; CDC Replication"),
        ("4.", "Bronze Layer: Schema-on-Read Ingestion"),
        ("5.", "Silver Layer: Agentic Transformation Engine"),
        ("6.", "Gold Layer: Agentic Aggregation"),
        ("7.", "Three-Layer Control Model"),
        ("8.", "Knowledge Graph"),
        ("9.", "TABLE_LINEAGE_MAP: Self-Expanding Registry"),
        ("10.", "Semantic Views &amp; AI Consumption"),
        ("11.", "Streamlit Management Application"),
        ("12.", "Complete Object Inventory"),
        ("13.", "Script Dependency Map"),
        ("14.", "Snowflake Technologies Used"),
    ]
    for num, title in toc:
        elements.append(Paragraph(f"<b>{num}</b>&nbsp;&nbsp;{title}", styles['TOCEntry']))
    elements.append(Spacer(1, 16))


# ============================================================
# SECTION 1: SYSTEM OVERVIEW
# ============================================================
def build_section1(elements, styles):
    elements.append(Paragraph("1. System Overview", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Agentic Data Foundry is a deployed AI-native data engineering platform built entirely on Snowflake. "
        "It replaces hand-coded ETL pipelines with autonomous AI agents that discover, transform, validate, and "
        "optimize data \u2014 guided by human-defined metadata rather than human-written code. The system implements "
        "a medallion architecture (Bronze \u2192 Silver \u2192 Gold) where the Silver and Gold layers are <b>ephemeral</b>: "
        "fully regenerable from Bronze data plus metadata at any time.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_diagram(elements, """
+------------------+     +------------------+     +------------------+
|   PostgreSQL     |     |   PostgreSQL     |     |   Snowflake      |
|   SOURCE         |---->|   LANDING        |---->|   "public"       |
|   (OLTP App DB)  |     |   (CDC Staging)  |     |   (Openflow CDC) |
+------------------+     +------------------+     +------------------+
   Logical Replication       Subscription         _SNOWFLAKE_DELETED
                                                  _SNOWFLAKE_UPDATED_AT
                                    |
                                    v
  +---------------------------------------------------------------+
  |                    DBAONTAP_ANALYTICS                          |
  |                                                               |
  |   +----------+       +----------+       +----------+          |
  |   |  BRONZE  |------>|  SILVER  |------>|   GOLD   |          |
  |   | VARIANT  |       | CDC-Aware|       | Aggregate|          |
  |   | DTs (5)  |       |  DTs (5) |       |  DTs (5) |          |
  |   +----------+       +----------+       +----------+          |
  |        |                   |                  |               |
  |   OBJECT_CONSTRUCT   LLM-Generated      LLM-Generated        |
  |   Schema-on-Read     ROW_NUMBER()       Multi-Source          |
  |                      Deduplication      Aggregation           |
  |                                                               |
  |   +-----------+  +-----------+  +-----------+  +----------+  |
  |   |  METADATA |  |  AGENTS   |  |KNOWLEDGE  |  | SEMANTIC |  |
  |   |  Contracts|  |  20+ SPs  |  |  GRAPH    |  |  VIEWS   |  |
  |   |  Direct.  |  |  Workflow |  |  200+nodes|  |  Cortex  |  |
  |   |  Learnings|  |  Engine   |  |  Lineage  |  |  Analyst |  |
  |   +-----------+  +-----------+  +-----------+  +----------+  |
  +---------------------------------------------------------------+""", styles, title="System Architecture Overview")

    t = make_table(
        ["Component", "Technology", "Count"],
        [
            ["Database", "DBAONTAP_ANALYTICS", "1"],
            ["Schemas", "public, BRONZE, SILVER, GOLD, AGENTS, METADATA, KNOWLEDGE_GRAPH", "7"],
            ["Bronze Dynamic Tables", "VARIANT schema-on-read", "5"],
            ["Silver Dynamic Tables", "LLM-generated, CDC-aware", "5"],
            ["Gold Dynamic Tables", "LLM-generated aggregation", "5"],
            ["Stored Procedures", "Python + SQL (Cortex AI)", "27+"],
            ["Metadata Tables", "Contracts, Directives, Learnings, Lineage, Logs", "14+"],
            ["Knowledge Graph", "KG_NODE + KG_EDGE with embeddings", "200+ nodes"],
            ["Semantic Views", "Auto-generated from KG + LLM", "5"],
            ["LLM Models", "Claude 3.5 Sonnet (planning), Llama 3.1-8b (inference), e5-base-v2 (embeddings)", "3"],
            ["Streamlit Application", "13-tab management UI (DEMO_MANAGER)", "1"],
            ["Streams", "CDC event detection on landing tables", "5"],
            ["Tasks", "Scheduled agentic workflow trigger", "1"],
        ],
        [1.5*inch, 3.5*inch, 1.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 2: END-TO-END DATA FLOW
# ============================================================
def build_section2(elements, styles):
    elements.append(Paragraph("2. End-to-End Data Flow", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "Data flows through nine distinct stages, from OLTP application database to natural language analytics. "
        "Each stage is autonomous \u2014 once Bronze exists, the agentic workflow can generate every downstream layer "
        "without human-written transformation code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_diagram(elements, """
 STAGE 1       STAGE 2        STAGE 3         STAGE 4         STAGE 5
 PG Source ---> PG Landing ---> Snowflake ---> BRONZE ---------> SILVER
 (OLTP)     Logical Repl.    "public" via    VARIANT DTs      CDC-Aware DTs
 5 tables   (Subscription)    Openflow CDC   OBJECT_CONSTRUCT  ROW_NUMBER()
                                              (*)              Dedup

                              STAGE 6         STAGE 7         STAGE 8
                              GOLD ---------> KNOWLEDGE -----> SEMANTIC
                              Aggregation     GRAPH            VIEWS
                              DTs (multi-     KG_NODE/EDGE    Hybrid SV
                              source joins)   Lineage Map     (KG + LLM)

                                              STAGE 9
                                              AI CONSUMPTION
                                              Cortex Analyst
                                              Natural Language
                                              --> SQL --> Results""", styles, title="Nine-Stage Pipeline")

    stages = [
        ["1. PG Source", "PostgreSQL managed instance (SOURCE_PG)", "5 application tables with seed data, dbaontap_pub publication"],
        ["2. PG Landing", "PostgreSQL managed instance (LANDING_PG)", "Mirror tables, logical replication subscription"],
        ["3. Snowflake Landing", 'Openflow CDC connector \u2192 "public" schema', "Adds _SNOWFLAKE_DELETED, _SNOWFLAKE_UPDATED_AT columns"],
        ["4. Bronze", "Dynamic Tables with OBJECT_CONSTRUCT(*)", "Schema-on-read VARIANT; absorbs schema changes without ALTER TABLE"],
        ["5. Silver", "LLM-generated Dynamic Tables (5-phase workflow)", "CDC deduplication, type extraction, derived columns, contract-enforced"],
        ["6. Gold", "LLM-generated aggregation Dynamic Tables", "Multi-source joins, business metrics, ML features"],
        ["7. Knowledge Graph", "KG_NODE + KG_EDGE + TABLE_LINEAGE_MAP", "Lineage, impact analysis, semantic similarity search"],
        ["8. Semantic Views", "Auto-generated from KG structure + LLM enrichment", "Dimensions, measures, synonyms for Cortex Analyst"],
        ["9. AI Consumption", "Cortex Analyst via Semantic Views", "Natural language \u2192 SQL \u2192 Results, auto-routing across views"],
    ]
    t = make_table(["Stage", "Mechanism", "Key Detail"], stages, [1.3*inch, 2.2*inch, 2.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 3: SOURCE LAYER
# ============================================================
def build_section3(elements, styles):
    elements.append(Paragraph("3. Source Layer: PostgreSQL &amp; CDC Replication", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The source system is a Snowflake Managed PostgreSQL instance acting as the OLTP application database. "
        "Data arrives via PostgreSQL logical replication through a two-hop architecture: SOURCE \u2192 LANDING \u2192 "
        "Snowflake (via Openflow CDC).",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("3.1 Source Tables (PostgreSQL)", styles['SubsectionTitle']))
    tables = [
        ["customers", "customer_id SERIAL PK, first_name, last_name, email UNIQUE, phone, company_name, industry, created_at, updated_at", "10"],
        ["products", "product_id SERIAL PK, product_name, category, price NUMERIC, cost NUMERIC, sku UNIQUE, active BOOLEAN, created_at", "6"],
        ["orders", "order_id SERIAL PK, customer_id FK, order_date, status, total_amount NUMERIC, shipping_address, notes", "11"],
        ["order_items", "item_id SERIAL PK, order_id FK, product_id FK, quantity INT, unit_price NUMERIC, discount_percent NUMERIC", "15"],
        ["support_tickets", "ticket_id SERIAL PK, customer_id FK, subject, description, priority, status, created_at, resolved_at", "5"],
    ]
    t = make_table(["Table", "Columns", "Seed Rows"], tables, [1.2*inch, 4.3*inch, 0.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("3.2 Replication Architecture", styles['SubsectionTitle']))
    add_diagram(elements, """
  SOURCE_PG                    LANDING_PG                 Snowflake
  +-------------------+        +-------------------+      +-------------------+
  | customers         |        | customers         |      | "public".customers|
  | products          | -----> | products          | ---> | "public".products |
  | orders            | Logical| orders            | Open | "public".orders   |
  | order_items       | Replic.| order_items       | flow | "public".order_   |
  | support_tickets   |        | support_tickets   | CDC  |   items           |
  +-------------------+        +-------------------+      | "public".support_ |
  dbaontap_pub                 dbaontap_sub               |   tickets         |
  (Publication)                (Subscription)              +-------------------+
                                                          + _SNOWFLAKE_DELETED
                                                          + _SNOWFLAKE_UPDATED_AT""", styles, title="Two-Hop CDC Replication")

    elements.append(Paragraph(
        "<b>Openflow CDC enrichment:</b> When data arrives in Snowflake, the Openflow connector appends two system "
        "columns to every row: <font face='Courier' size='7.5'>_SNOWFLAKE_DELETED</font> (BOOLEAN, true when source "
        "row is deleted) and <font face='Courier' size='7.5'>_SNOWFLAKE_UPDATED_AT</font> (TIMESTAMP, last "
        "modification time). These columns are essential for downstream CDC deduplication in Silver.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 4: BRONZE LAYER
# ============================================================
def build_section4(elements, styles):
    elements.append(Paragraph("4. Bronze Layer: Schema-on-Read Ingestion", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "Bronze is not a passive landing zone \u2014 it is an <b>activation layer</b> where AI begins adding value "
        "from the moment data enters the platform. Every source table is stored as a VARIANT column via "
        "<font face='Courier' size='7.5'>OBJECT_CONSTRUCT(*)</font>, providing complete schema-on-read flexibility.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("4.1 Bronze Dynamic Tables", styles['SubsectionTitle']))
    bronze = [
        ["BRONZE.CUSTOMERS_VARIANT", "OBJECT_CONSTRUCT(*) AS payload", '"public"."customers"', "1 minute"],
        ["BRONZE.ORDERS_VARIANT", "OBJECT_CONSTRUCT(*) AS payload", '"public"."orders"', "1 minute"],
        ["BRONZE.ORDER_ITEMS_VARIANT", "OBJECT_CONSTRUCT(*) AS payload", '"public"."order_items"', "1 minute"],
        ["BRONZE.PRODUCTS_VARIANT", "OBJECT_CONSTRUCT(*) AS payload", '"public"."products"', "1 minute"],
        ["BRONZE.SUPPORT_TICKETS_VARIANT", "OBJECT_CONSTRUCT(*) AS payload", '"public"."support_tickets"', "1 minute"],
    ]
    t = make_table(["Dynamic Table", "Select Pattern", "Source", "Target Lag"],
                   bronze, [2.0*inch, 1.8*inch, 1.6*inch, 0.9*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("4.2 Autonomous Discovery &amp; Onboarding", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The <font face='Courier' size='7.5'>DISCOVER_AND_ONBOARD_NEW_TABLES</font> procedure continuously scans "
        "for new landing tables that lack Bronze representations. When a new table is detected, it autonomously:",
        styles['BodyText2']))
    for step in [
        "Creates a <b>Stream</b> on the landing table for CDC event detection",
        "Creates a <b>Bronze Dynamic Table</b> using OBJECT_CONSTRUCT(*) to preserve the full payload as VARIANT",
        "Registers the onboarding event in <font face='Courier' size='7.5'>METADATA.ONBOARDED_TABLES</font>",
    ]:
        elements.append(Paragraph(f"\u2022 {step}", styles['BulletItem']))

    add_callout(elements,
        "<b>Schema Resilience:</b> Because Bronze stores raw VARIANT payloads, source schema changes (new columns, "
        "dropped columns, type changes) are absorbed automatically. The system never needs ALTER TABLE at the "
        "Bronze layer. New columns simply appear in the VARIANT payload; dropped columns stop appearing.",
        styles, bg=HexColor("#FFF8E1"))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 5: SILVER LAYER
# ============================================================
def build_section5(elements, styles):
    elements.append(Paragraph("5. Silver Layer: Agentic Transformation Engine", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Silver layer is where the agentic paradigm most visibly departs from traditional data engineering. "
        "Rather than human-authored transformation scripts, Silver tables are generated by an autonomous "
        "<b>5-phase workflow engine</b> orchestrated by the "
        "<font face='Courier' size='7.5'>RUN_AGENTIC_WORKFLOW</font> stored procedure.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("5.1 The Five-Phase Workflow", styles['SubsectionTitle']))
    add_diagram(elements, """
  +----------+    +-----------+    +-----------+    +-----------+    +-----------+
  | PHASE 1  |    | PHASE 2   |    | PHASE 3   |    | PHASE 4   |    | PHASE 5   |
  | TRIGGER  |--->| PLANNER   |--->| EXECUTOR  |--->| VALIDATOR |--->| REFLECTOR |
  |          |    |           |    |           |    |           |    |           |
  | Detect   |    | LLM       |    | LLM       |    | Row count |    | LLM       |
  | events:  |    | analyzes  |    | generates |    | comparison|    | extracts  |
  | -New tbl |    | schema +  |    | CREATE OR |    | (<5% tol.)|    | learnings:|
  | -Schema  |    | quality + |    | REPLACE   |    | Schema    |    | -Success  |
  |  change  |    | learnings |    | DYNAMIC   |    | validation|    |  patterns |
  | -Quality |    | +directs. |    | TABLE DDL |    | Type      |    | -Failure  |
  |  breach  |    | Outputs:  |    | 3-retry   |    | checks    |    |  patterns |
  |          |    | strategy  |    | self-corr.|    |           |    | -Optimiz. |
  +----------+    +-----------+    +-----------+    +-----------+    +-----------+
       |               |               |                |                |
  WORKFLOW_        PLANNER_        TRANSFORMATION_  VALIDATION_     WORKFLOW_
  EXECUTIONS       DECISIONS       LOG              RESULTS         LEARNINGS""", styles, title="5-Phase Agentic Workflow Engine")

    elements.append(Paragraph("5.2 Phase Details", styles['SubsectionTitle']))

    elements.append(Paragraph("<b>Phase 1: TRIGGER</b> \u2014 <font face='Courier' size='7.5'>WORKFLOW_TRIGGER(type, tables)</font>", styles['SubsubTitle']))
    elements.append(Paragraph(
        "Runs auto-discovery (<font face='Courier' size='7.5'>DISCOVER_AND_ONBOARD_NEW_TABLES</font>), "
        "determines candidate tables by trigger type (manual/stream/scheduled), then applies Decision Point 2 \u2014 "
        "the <font face='Courier' size='7.5'>FILTER_TABLES_FOR_AGENTIC_WORKFLOW</font> procedure that classifies "
        "each table as NEW_TABLE, SCHEMA_CHANGED, or SILVER_EXISTS_NO_CHANGES. Tables where Silver already exists "
        "and the schema hasn't changed are skipped. Schema change detection uses "
        "<font face='Courier' size='7.5'>DETECT_SCHEMA_CHANGES</font> which compares Bronze VARIANT keys against "
        "Silver INFORMATION_SCHEMA columns, excluding system/derived columns via "
        "<font face='Courier' size='7.5'>SCHEMA_IGNORE_COLUMNS</font> patterns.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("<b>Phase 2: PLANNER</b> \u2014 <font face='Courier' size='7.5'>WORKFLOW_PLANNER(execution_id)</font>", styles['SubsubTitle']))
    elements.append(Paragraph(
        "For each candidate table: calls DISCOVER_SCHEMA (UDTF that FLATTENs VARIANT) + ANALYZE_DATA_QUALITY, "
        "builds an LLM prompt including the full column inventory, data quality metrics, Schema Contracts, "
        "Transformation Directives, and accumulated Learnings. Sends to <b>Claude 3.5 Sonnet</b>. Parses the JSON "
        "response (using a brace-counting parser for robustness) and logs the strategy to "
        "<font face='Courier' size='7.5'>PLANNER_DECISIONS</font>.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("<b>Phase 3: EXECUTOR</b> \u2014 <font face='Courier' size='7.5'>WORKFLOW_EXECUTOR(execution_id)</font>", styles['SubsubTitle']))
    elements.append(Paragraph(
        "For each planner decision: gathers schema info and the VARIANT column name, builds an execution prompt "
        "requesting a complete <font face='Courier' size='7.5'>CREATE OR REPLACE DYNAMIC TABLE</font> DDL with "
        "CDC deduplication. Sends to <b>Claude 3.5 Sonnet</b>. Strips markdown fences from the response. "
        "Executes via <font face='Courier' size='7.5'>EXECUTE IMMEDIATE</font> with a <b>3-retry self-correction "
        "loop</b>: if the DDL fails, the error message is fed back to the LLM in the next attempt.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("<b>Phase 4: VALIDATOR</b> \u2014 <font face='Courier' size='7.5'>WORKFLOW_VALIDATOR(execution_id)</font>", styles['SubsubTitle']))
    elements.append(Paragraph(
        "For each successfully executed table: compares source and target row counts. Passes if within "
        "<b>\u00b15% tolerance</b>. Logs results (PASS/FAIL with variance percentage) to "
        "<font face='Courier' size='7.5'>VALIDATION_RESULTS</font>.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("<b>Phase 5: REFLECTOR</b> \u2014 <font face='Courier' size='7.5'>WORKFLOW_REFLECTOR(execution_id)</font>", styles['SubsubTitle']))
    elements.append(Paragraph(
        "Builds a reflection prompt from all workflow data (tables processed, strategies used, DDLs generated, "
        "validation outcomes). Sends to the LLM, which returns a JSON array of learnings \u2014 each with an "
        "observation, recommendation, pattern_signature, and confidence_score. <b>MERGEs</b> into "
        "<font face='Courier' size='7.5'>WORKFLOW_LEARNINGS</font> (upsert by pattern_signature, averaging "
        "confidence scores). These learnings feed back into future Planner prompts, creating a "
        "<b>continuously improving system</b>.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("5.3 Silver Dynamic Tables", styles['SubsectionTitle']))
    silver = [
        ["SILVER.CUSTOMERS", "customer_id, first_name, last_name, full_name (derived), email, phone, company_name, industry, segment (derived), created_at, updated_at, cdc_timestamp", "CUSTOMERS_VARIANT"],
        ["SILVER.ORDERS", "order_id, customer_id, order_date, status, total_amount, shipping_address, notes, order_month (derived), days_since_order (derived), cdc_timestamp", "ORDERS_VARIANT"],
        ["SILVER.PRODUCTS_VARIANT", "product_id, product_name, category, price, cost, margin (derived), margin_percent (derived), sku, active, created_at, cdc_timestamp", "PRODUCTS_VARIANT"],
        ["SILVER.ORDER_ITEMS", "item_id, order_id, product_id, quantity, unit_price, discount_percent, line_total (derived), line_total_after_discount (derived), cdc_timestamp", "ORDER_ITEMS_VARIANT"],
        ["SILVER.SUPPORT_TICKETS", "ticket_id, customer_id, subject, description, priority, status, created_at, resolved_at, resolution_hours (derived), cdc_timestamp", "SUPPORT_TICKETS_VARIANT"],
    ]
    t = make_table(["Silver Table", "Columns (including derived)", "Bronze Source"],
                   silver, [1.5*inch, 3.5*inch, 1.3*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))

    add_callout(elements,
        "<b>CDC Deduplication Pattern:</b> Every Silver table uses: "
        "<font face='Courier' size='7'>WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY &lt;PK&gt; "
        "ORDER BY _SNOWFLAKE_UPDATED_AT DESC) AS rn FROM ...) SELECT ... WHERE rn = 1 AND "
        "_SNOWFLAKE_DELETED = FALSE</font>. This ensures only the latest version of each record survives, "
        "and soft-deleted records are excluded.",
        styles)
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 6: GOLD LAYER
# ============================================================
def build_section6(elements, styles):
    elements.append(Paragraph("6. Gold Layer: Agentic Aggregation", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Gold layer extends the agentic pattern to business-ready aggregations. Gold tables join multiple "
        "Silver sources into analytical models using a two-strategy discovery approach.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("6.1 Gold Dynamic Tables", styles['SubsectionTitle']))
    gold = [
        ["CUSTOMER_360", "CUSTOMERS + ORDERS + SUPPORT_TICKETS", "customer_id, full_name, email, segment, loyalty_tier, total_orders, lifetime_value, avg_order_value, recency_days, frequency, monetary_total, revenue_tier, engagement_status, total_tickets, open_tickets"],
        ["PRODUCT_PERFORMANCE_METRICS", "PRODUCTS_VARIANT + ORDER_ITEMS + ORDERS", "product_id, product_name, category, list_price, cost_price, margin_percent, orders_count, units_sold, total_revenue, unique_customers"],
        ["ORDER_SUMMARY", "ORDERS + CUSTOMERS", "order_month, customer_segment, order_count, unique_customers, total_revenue, avg_order_value, completed/pending/processing counts"],
        ["ML_CUSTOMER_FEATURES", "CUSTOMERS + CUSTOMER_360", "customer_id, segment, loyalty_tier, total_orders, lifetime_value, avg_order_value, recency_days, total_tickets, segment_encoded, tier_encoded, is_churned"],
        ["SUPPORT_METRICS", "SUPPORT_TICKETS (agentic)", "Agentic-generated: ticket metrics, resolution stats, priority distribution (LLM determines optimal aggregation)"],
    ]
    t = make_table(["Gold Table", "Silver Sources", "Key Columns"],
                   gold, [1.5*inch, 1.7*inch, 3.1*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("6.2 Two-Strategy Discovery Algorithm", styles['SubsectionTitle']))
    add_diagram(elements, """
  BUILD_GOLD_FOR_NEW_TABLES(dry_run, refresh_svs)
  |
  +-- STRATEGY 1: Missing Gold Targets
  |   Query TABLE_LINEAGE_MAP for AGGREGATES_TO edges where
  |   target Gold table does NOT exist in INFORMATION_SCHEMA
  |   Example: MAP says SUPPORT_TICKETS -> SUPPORT_METRICS,
  |            but GOLD.SUPPORT_METRICS doesn't exist yet
  |
  +-- STRATEGY 2: Uncovered Silver Tables
  |   Find Silver tables with NO AGGREGATES_TO mapping at all
  |   These are "orphan" tables the agents build Gold for autonomously
  |
  +-- For each work item:
      |-- Gather all Silver column info + existing Gold context
      |-- Detect deleted-column naming (_SNOWFLAKE_DELETED vs IS_DELETED)
      |-- Fetch matching Transformation Directives
      |-- Build LLM prompt -> Claude 3.5 Sonnet
      |-- VALIDATE_GOLD_DDL (compile check before execution)
      |-- Execute with 3-retry loop (errors fed back to LLM)
      |-- REGISTER_LINEAGE_FROM_DDL (auto-register new edges)
      |-- Refresh KG + optionally regenerate Semantic Views""", styles, title="Gold Build Algorithm")

    elements.append(Paragraph("6.3 Schema Drift Remediation", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "When Silver schemas change (new columns from source), the Gold layer detects and remedies drift via "
        "<font face='Courier' size='7.5'>PROPAGATE_TO_GOLD</font>:",
        styles['BodyText2']))
    drift_steps = [
        "<b>DETECT_GOLD_SCHEMA_DRIFT</b> \u2014 Compares Silver columns vs Gold columns for all linked Gold "
        "tables via KG edges. Classifies missing columns as <i>passthrough</i> (simple types, can be added "
        "mechanically) or <i>complex</i> (needs LLM reasoning).",
        "<b>GOLD_AUTO_PASSTHROUGH</b> \u2014 For simple passthrough columns: LLM adds columns preserving existing "
        "logic.",
        "<b>GOLD_AGENTIC_EXECUTOR</b> \u2014 For complex columns: full LLM reasoning with retry loop and "
        "error feedback.",
    ]
    for step in drift_steps:
        elements.append(Paragraph(f"\u2022 {step}", styles['BulletItem']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 7: THREE-LAYER CONTROL MODEL
# ============================================================
def build_section7(elements, styles):
    elements.append(Paragraph("7. Three-Layer Control Model", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The human-AI interface is organized into three complementary control mechanisms that together define "
        "how the autonomous agents behave without requiring the human to write transformation code.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_diagram(elements, """
  +-------------------------+    +-------------------------+    +-------------------------+
  | SCHEMA CONTRACTS        |    | TRANSFORMATION          |    | LEARNINGS               |
  | (Structure)             |    | DIRECTIVES (Intent)     |    | (Memory)                |
  |                         |    |                         |    |                         |
  | "What must the output   |    | "Why does this data     |    | "What has worked in     |
  |  look like?"            |    |  exist?"                |    |  the past?"             |
  |                         |    |                         |    |                         |
  | - Column names & types  |    | - Business purpose      |    | - Success patterns      |
  | - Required fields       |    | - Use case context      |    | - Failure patterns      |
  | - Naming conventions    |    | - Granularity needs     |    | - Optimizations         |
  | - Enforced at Executor  |    | - Injected at Planner   |    | - Confidence scores     |
  +-------------------------+    +-------------------------+    +-------------------------+
  SILVER_SCHEMA_CONTRACTS        TRANSFORMATION_DIRECTIVES       WORKFLOW_LEARNINGS""", styles, title="Three-Layer Control Model")

    elements.append(Paragraph("7.1 Schema Contracts", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Stored in <font face='Courier' size='7.5'>METADATA.SILVER_SCHEMA_CONTRACTS</font>. Define column names, "
        "types, and required flags. Without contracts, the LLM non-deterministically chooses column names "
        "(e.g., FULL_NAME vs FIRST_NAME + LAST_NAME), breaking downstream consumers. Contracts can be authored "
        "manually, generated from existing Silver tables, or proposed by LLM from Bronze analysis.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("7.2 Transformation Directives", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Stored in <font face='Courier' size='7.5'>METADATA.TRANSFORMATION_DIRECTIVES</font>. Declare <i>why</i> "
        "the data exists and what business purpose it serves. The Planner injects matching directives into LLM "
        "prompts. Directives support source table patterns, target layers, priority weighting, and LLM-assisted "
        "generation.",
        styles['BodyText2']))
    directives = [
        ["demand_forecasting", "ORDERS", "GOLD", "8", "Preserve daily granularity, create 7/14/30 day rolling averages"],
        ["churn_prediction", "CUSTOMERS", "GOLD", "8", "Compute RFM features, recency/frequency/monetary"],
        ["service_optimization", "SUPPORT_TICKETS", "GOLD", "7", "Resolution time analysis, priority distribution"],
        ["inventory_planning", "PRODUCTS", "GOLD", "6", "Stock levels, category performance, margin analysis"],
        ["basket_analysis", "ORDER_ITEMS", "GOLD", "6", "Product co-occurrence, cross-sell patterns"],
        ["general_hygiene", "%", "SILVER", "3", "Standardize nulls, remove duplicates, enforce types"],
    ]
    t = make_table(["Use Case", "Source Pattern", "Target Layer", "Priority", "Intent (abbreviated)"],
                   directives, [1.2*inch, 1.0*inch, 0.8*inch, 0.6*inch, 2.7*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("7.3 Learnings", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Stored in <font face='Courier' size='7.5'>METADATA.WORKFLOW_LEARNINGS</font>. The Reflector phase (Phase 5) "
        "generates learnings; the Planner phase (Phase 2) consumes them. Each learning has a pattern_signature "
        "(hash for matching), observation, recommendation, confidence_score (increases with repeated observation), "
        "and active flag (humans can deactivate incorrect learnings). This creates a <b>feedback loop</b> where the "
        "system improves with every execution.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 8: KNOWLEDGE GRAPH
# ============================================================
def build_section8(elements, styles):
    elements.append(Paragraph("8. Knowledge Graph", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The Knowledge Graph (schema <font face='Courier' size='7.5'>KNOWLEDGE_GRAPH</font>) encodes all "
        "relationships between database objects using a node-edge model with LLM-generated descriptions "
        "and vector embeddings (<b>e5-base-v2</b>).",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("8.1 Node and Edge Types", styles['SubsectionTitle']))
    nodes = [
        ["DATABASE", "DBAONTAP_ANALYTICS", "1"],
        ["SCHEMA", "BRONZE, SILVER, GOLD, AGENTS, METADATA", "5+"],
        ["TABLE", "All Bronze, Silver, Gold tables", "15+"],
        ["COLUMN", "All columns across all tables", "150+"],
    ]
    t = make_table(["Node Type", "Examples", "Count"], nodes, [1.2*inch, 3.5*inch, 0.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 4))

    edges = [
        ["CONTAINS", "Schema \u2192 Table", "Schema contains table"],
        ["HAS_COLUMN", "Table \u2192 Column", "Table has column"],
        ["TRANSFORMS_TO", "Bronze \u2192 Silver", "Bronze variant transforms to typed Silver table"],
        ["AGGREGATES_TO", "Silver \u2192 Gold", "Silver table aggregates into Gold analytical model"],
    ]
    t = make_table(["Edge Type", "Direction", "Meaning"], edges, [1.4*inch, 1.5*inch, 3.4*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("8.2 Three Critical Functions", styles['SubsectionTitle']))
    funcs = [
        "<b>Lineage-Aware Impact Analysis:</b> Before regenerating a Silver table, the system queries downstream "
        "dependencies. If SILVER.CUSTOMERS changes, the KG reveals that GOLD.CUSTOMER_360, "
        "GOLD.ML_CUSTOMER_FEATURES, and GOLD.CUSTOMER_METRICS are all impacted.",
        "<b>Semantic Pattern Reuse:</b> When a new table arrives (e.g., INVOICES), vector similarity search "
        "(<font face='Courier' size='7.5'>VECTOR_COSINE_SIMILARITY</font> + "
        "<font face='Courier' size='7.5'>EMBED_TEXT_768('e5-base-v2')</font>) finds the 3 most similar existing "
        "Silver tables. Their transformation patterns seed the LLM prompt.",
        "<b>Enriched LLM Context:</b> Every LLM prompt in the agentic workflow is enriched with KG context: "
        "table descriptions, lineage relationships, similar table patterns, and downstream impact assessments.",
    ]
    for f in funcs:
        elements.append(Paragraph(f"\u2022 {f}", styles['BulletItem']))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("8.3 Population", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "<font face='Courier' size='7.5'>POPULATE_KG_FROM_INFORMATION_SCHEMA()</font> truncates and rebuilds "
        "KG_NODE and KG_EDGE from: (1) INFORMATION_SCHEMA for database/schema/table/column nodes and CONTAINS/"
        "HAS_COLUMN edges, and (2) TABLE_LINEAGE_MAP for TRANSFORMS_TO/AGGREGATES_TO lineage edges.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 9: TABLE_LINEAGE_MAP
# ============================================================
def build_section9(elements, styles):
    elements.append(Paragraph("9. TABLE_LINEAGE_MAP: Self-Expanding Registry", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "At the center of the metadata layer is <font face='Courier' size='7.5'>METADATA.TABLE_LINEAGE_MAP</font> "
        "\u2014 a living registry of all table-to-table relationships that grows through two mechanisms.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_diagram(elements, """
  TABLE_LINEAGE_MAP (Dual Population Model)
  +-----------+-----------+-----------+------------------+---------+
  | SOURCE    | TARGET    | EDGE      | RELATIONSHIP     | ORIGIN  |
  | SCHEMA    | SCHEMA    | TYPE      | LABEL            |         |
  +-----------+-----------+-----------+------------------+---------+
  | BRONZE    | SILVER    | TRANSFORMS| Bronze->Silver   | Seed    |  <-- Human-seeded
  | BRONZE    | SILVER    | TRANSFORMS| Bronze->Silver   | Seed    |      (architectural
  | ...       | ...       | ...       | ...              | Seed    |       intent)
  +-----------+-----------+-----------+------------------+---------+
  | SILVER    | GOLD      | AGGREGATES| Silver->Gold     | Seed    |  <-- Human-seeded
  | SILVER    | GOLD      | AGGREGATES| Silver->Gold     | Agent   |  <-- Agent-populated
  | ...       | ...       | ...       | ...              | Agent   |      (auto-registered
  +-----------+-----------+-----------+------------------+---------+       via MERGE)

  Consumers:
  - POPULATE_KG_FROM_INFORMATION_SCHEMA (builds KG edges)
  - BUILD_GOLD_FOR_NEW_TABLES (discovers gaps)
  - Schema drift detection (downstream impact)
  - KG visualization (Streamlit lineage diagrams)""", styles, title="TABLE_LINEAGE_MAP: Single Source of Truth")

    elements.append(Paragraph(
        "<b>Human-Seeded Entries:</b> Data engineers declare known relationships (e.g., CUSTOMERS_VARIANT \u2192 "
        "CUSTOMERS, CUSTOMERS \u2192 CUSTOMER_360). These express architectural intent before any agent executes.",
        styles['BodyText2']))
    elements.append(Paragraph(
        "<b>Agent-Populated Entries:</b> When BUILD_GOLD_FOR_NEW_TABLES generates a new Gold DT, "
        "<font face='Courier' size='7.5'>REGISTER_LINEAGE_FROM_DDL</font> parses the SQL, extracts all "
        "FROM/JOIN references to Silver tables, and inserts new edges via MERGE. An audit trail distinguishes "
        "human intent from agent discovery.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 10: SEMANTIC VIEWS & AI CONSUMPTION
# ============================================================
def build_section10(elements, styles):
    elements.append(Paragraph("10. Semantic Views &amp; AI Consumption", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The consumption layer bridges Gold analytical tables to natural language querying via Snowflake Cortex "
        "Analyst. Semantic Views are auto-generated using three approaches.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    approaches = [
        ["Agentic (Pure LLM)", "RUN_SEMANTIC_VIEW_PIPELINE", "LLM generates complete Semantic View DDL from INFORMATION_SCHEMA column list. Fast but may miss relationships."],
        ["Knowledge Graph", "GENERATE_ALL_SEMANTIC_VIEWS_FROM_KG", "Rule-based from KG metadata: categorizes columns as dimensions/measures by data type. Structural but lacks business context."],
        ["Hybrid (Recommended)", "GENERATE_ALL_HYBRID_SEMANTIC_VIEWS", "KG structure + LLM enrichment: KG provides column inventory and relationships, LLM adds descriptions, synonyms, and sample values."],
    ]
    t = make_table(["Approach", "Procedure", "Description"],
                   approaches, [1.3*inch, 2.2*inch, 2.8*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("10.1 Cortex Analyst Chat", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "The Streamlit AI Chat tab provides a natural language interface backed by Cortex Analyst. It sends all "
        "Semantic View FQNs for automatic view selection, supports chat history, and includes a 3-retry "
        "auto-correction loop: if generated SQL fails, the error is fed back to Cortex Analyst for self-correction. "
        "Example questions are LLM-generated from Gold table schemas.",
        styles['BodyText2']))
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 11: STREAMLIT APPLICATION
# ============================================================
def build_section11(elements, styles):
    elements.append(Paragraph("11. Streamlit Management Application", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The <font face='Courier' size='7.5'>DEMO_MANAGER</font> Streamlit in Snowflake application provides "
        "a 13-tab management interface for the entire platform. It connects to both Snowflake (native) and "
        "PostgreSQL (via External Access Integration + psycopg2).",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    tabs = [
        ["0. Architecture", "Live Graphviz pipeline diagram that auto-detects 9 stages (PG Source \u2192 AI Chat). Progress bar, manual overrides."],
        ["1. Generate Data", "AI-powered (Cortex LLM) or rule-based synthetic data generation. Targets PG SOURCE, PG LANDING, or Snowflake Landing. Configurable row counts."],
        ["2. Pipeline Status", "Real-time PG SOURCE \u2192 LANDING sync %, Snowflake layer counts (Landing/Bronze/Silver/Gold), DT refresh history."],
        ["3. Agentic Workflow", "5-phase visual pipeline with live state updates. Trigger type selection, per-table control. History/learnings drill-down."],
        ["4. Gold Layer", "Core Gold build (4 hardcoded DDLs with validation), Agentic Gold build (LLM-generated), Schema drift detection and auto-fix."],
        ["5. Schema Contracts", "CRUD for SILVER_SCHEMA_CONTRACTS. Add from manual entry, existing Silver schema, or LLM-generated from Bronze."],
        ["5b. Directives", "CRUD for TRANSFORMATION_DIRECTIVES. LLM-assisted directive drafting from use case description."],
        ["6. Semantic Views", "Three generation approaches (Agentic/KG/Hybrid). Parsed dimension/fact/metric display. Raw DDL inspection."],
        ["7. AI Chat", "Cortex Analyst chatbot with auto-routing across semantic views. LLM-generated example questions. 3-retry self-correction."],
        ["8. Knowledge Graph", "Three Graphviz visualizations: Data Lineage, Table Relationships, Full Graph. KG statistics and management."],
        ["9. Logs &amp; Errors", "Filterable TRANSFORMATION_LOG view. Summary metrics, failed item drill-down with SQL/reasoning."],
        ["10. Demo Control", "Resume/suspend all automation (Tasks, Dynamic Tables, Streams). Granular per-object controls."],
        ["11. Reset Data", "Layer-by-layer reset. Force DT refresh. PG truncation. Hard/soft reset. SV management. Full reset with confirmation."],
    ]
    t = make_table(["Tab", "Functionality"], tabs, [1.3*inch, 5.0*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 12: OBJECT INVENTORY
# ============================================================
def build_section12(elements, styles):
    elements.append(Paragraph("12. Complete Object Inventory", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph("12.1 Dynamic Tables (15)", styles['SubsectionTitle']))
    dts = [
        ["BRONZE", "CUSTOMERS_VARIANT", "1 min", "OBJECT_CONSTRUCT(*) from public.customers"],
        ["BRONZE", "ORDERS_VARIANT", "1 min", "OBJECT_CONSTRUCT(*) from public.orders"],
        ["BRONZE", "ORDER_ITEMS_VARIANT", "1 min", "OBJECT_CONSTRUCT(*) from public.order_items"],
        ["BRONZE", "PRODUCTS_VARIANT", "1 min", "OBJECT_CONSTRUCT(*) from public.products"],
        ["BRONZE", "SUPPORT_TICKETS_VARIANT", "1 min", "OBJECT_CONSTRUCT(*) from public.support_tickets"],
        ["SILVER", "CUSTOMERS", "1 min", "CDC dedup, type extraction, derived columns"],
        ["SILVER", "ORDERS", "1 min", "CDC dedup, order_month, days_since_order"],
        ["SILVER", "ORDER_ITEMS", "1 min", "CDC dedup, line_total, line_total_after_discount"],
        ["SILVER", "PRODUCTS_VARIANT", "1 min", "CDC dedup, margin, margin_percent"],
        ["SILVER", "SUPPORT_TICKETS", "1 min", "CDC dedup, resolution_hours"],
        ["GOLD", "CUSTOMER_360", "DOWNSTREAM", "Multi-join: CUSTOMERS+ORDERS+SUPPORT_TICKETS"],
        ["GOLD", "PRODUCT_PERFORMANCE_METRICS", "DOWNSTREAM", "Multi-join: PRODUCTS+ORDER_ITEMS+ORDERS"],
        ["GOLD", "ORDER_SUMMARY", "DOWNSTREAM", "Multi-join: ORDERS+CUSTOMERS"],
        ["GOLD", "ML_CUSTOMER_FEATURES", "DOWNSTREAM", "Multi-join: CUSTOMERS+CUSTOMER_360"],
        ["GOLD", "SUPPORT_METRICS", "DOWNSTREAM", "Agentic-generated from SUPPORT_TICKETS"],
    ]
    t = make_table(["Schema", "Table", "Target Lag", "Description"],
                   dts, [0.8*inch, 2.0*inch, 0.9*inch, 2.6*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("12.2 Metadata Tables (14+)", styles['SubsectionTitle']))
    meta = [
        ["METADATA", "WORKFLOW_EXECUTIONS", "Master workflow state (execution_id PK, status, phases completed, timestamps)"],
        ["METADATA", "PLANNER_DECISIONS", "LLM planning output per table (strategy, reasoning, JSON payload)"],
        ["METADATA", "VALIDATION_RESULTS", "Row count comparisons, pass/fail, variance %"],
        ["METADATA", "WORKFLOW_LEARNINGS", "Accumulated patterns (pattern_signature, observation, recommendation, confidence)"],
        ["METADATA", "WORKFLOW_LOG", "Event log for all workflow phases"],
        ["METADATA", "ONBOARDED_TABLES", "Registry of auto-discovered Bronze tables"],
        ["METADATA", "TRANSFORMATION_LOG", "DDL execution history (source, target, SQL, status, reasoning)"],
        ["METADATA", "WORKFLOW_STATE", "Live workflow state tracking"],
        ["METADATA", "AGENT_REFLECTIONS", "Detailed reflection outputs per workflow"],
        ["METADATA", "SILVER_SCHEMA_CONTRACTS", "Column name/type/required constraints per table"],
        ["METADATA", "TRANSFORMATION_DIRECTIVES", "Business intent declarations per table pattern"],
        ["METADATA", "SCHEMA_IGNORE_COLUMNS", "Patterns to exclude from schema drift detection"],
        ["METADATA", "TABLE_LINEAGE_MAP", "Source-target lineage registry (human + agent populated)"],
        ["KNOWLEDGE_GRAPH", "KG_NODE", "Graph nodes: databases, schemas, tables, columns with embeddings"],
        ["KNOWLEDGE_GRAPH", "KG_EDGE", "Graph edges: CONTAINS, HAS_COLUMN, TRANSFORMS_TO, AGGREGATES_TO"],
    ]
    t = make_table(["Schema", "Table", "Purpose"],
                   meta, [1.2*inch, 2.0*inch, 3.1*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("12.3 Stored Procedures (27+)", styles['SubsectionTitle']))
    sps = [
        ["AGENTS", "RUN_AGENTIC_WORKFLOW", "SQL", "Orchestrator: TRIGGER\u2192PLANNER\u2192EXECUTOR\u2192VALIDATOR\u2192REFLECTOR"],
        ["AGENTS", "WORKFLOW_TRIGGER", "SQL", "Phase 1: auto-discovery + Decision Point 2 filtering"],
        ["AGENTS", "WORKFLOW_PLANNER", "SQL", "Phase 2: LLM-powered strategy planning"],
        ["AGENTS", "WORKFLOW_EXECUTOR", "SQL", "Phase 3: LLM DDL generation, 3-retry self-correction"],
        ["AGENTS", "WORKFLOW_VALIDATOR", "SQL", "Phase 4: row count + schema validation"],
        ["AGENTS", "WORKFLOW_REFLECTOR", "SQL", "Phase 5: learning extraction + MERGE"],
        ["AGENTS", "DISCOVER_AND_ONBOARD_NEW_TABLES", "SQL", "Auto-create Bronze DTs for new landing tables"],
        ["AGENTS", "AUTO_ONBOARD_TABLE", "SQL", "Create stream + Bronze DT for single table"],
        ["AGENTS", "DETECT_SCHEMA_CHANGES", "SQL", "Compare Bronze VARIANT vs Silver columns"],
        ["AGENTS", "FILTER_TABLES_FOR_AGENTIC_WORKFLOW", "SQL", "Decision Point 2: classify tables"],
        ["AGENTS", "BUILD_GOLD_FOR_NEW_TABLES", "Python", "Two-strategy Gold discovery + LLM generation"],
        ["AGENTS", "VALIDATE_GOLD_DDL", "Python", "Compile-check DDL, find invalid references"],
        ["AGENTS", "REGISTER_LINEAGE_FROM_DDL", "Python", "Parse DDL \u2192 auto-register lineage edges"],
        ["AGENTS", "DETECT_GOLD_SCHEMA_DRIFT", "Python", "Compare Silver vs Gold columns for drift"],
        ["AGENTS", "GOLD_AUTO_PASSTHROUGH", "Python", "LLM adds simple passthrough columns to Gold"],
        ["AGENTS", "GOLD_AGENTIC_EXECUTOR", "Python", "LLM adds complex columns with retry loop"],
        ["AGENTS", "PROPAGATE_TO_GOLD", "Python", "Orchestrate drift detection + remediation"],
        ["AGENTS", "RUN_SEMANTIC_VIEW_PIPELINE", "SQL", "Agentic SV generation from INFORMATION_SCHEMA"],
        ["KG", "POPULATE_KG_FROM_INFORMATION_SCHEMA", "SQL", "Rebuild KG from INFORMATION_SCHEMA + lineage map"],
        ["KG", "GENERATE_ALL_HYBRID_SEMANTIC_VIEWS", "Python", "Hybrid SV generation (KG + LLM)"],
        ["KG", "GENERATE_HYBRID_SV_EXECUTE", "Python", "Single hybrid SV generation + execution"],
    ]
    t = make_table(["Schema", "Procedure", "Lang", "Purpose"],
                   sps, [0.6*inch, 2.3*inch, 0.5*inch, 2.9*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("12.4 Views, Functions, Streams &amp; Tasks", styles['SubsectionTitle']))
    other = [
        ["View", "BRONZE.ALL_DATA_VARIANT", "UNION ALL of all 5 Bronze tables"],
        ["View", "METADATA.WORKFLOW_DASHBOARD", "Aggregated workflow execution statistics"],
        ["View", "METADATA.ACTIVE_LEARNINGS", "Currently active learnings"],
        ["View", "METADATA.ACTIVE_DIRECTIVES", "Active directives with matched tables"],
        ["UDTF", "AGENTS.DISCOVER_SCHEMA", "FLATTEN VARIANT rows, infer types, count distinct"],
        ["UDF", "AGENTS.SILVER_DT_EXISTS", "Check if Silver DT exists for a Bronze table"],
        ["UDTF", "KG.GET_TABLE_COLUMNS_FROM_KG", "Get columns with types from KG for a table"],
        ["UDTF", "KG.SUGGEST_SEMANTIC_VIEW_JOINS", "Suggest joins based on KG column references"],
        ["Stream", "AGENTS.*_LANDING_STREAM (5)", "APPEND_ONLY streams on 5 public landing tables"],
        ["Task", "AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK", "1-minute schedule: RUN_AGENTIC_WORKFLOW('stream')"],
    ]
    t = make_table(["Type", "Name", "Description"],
                   other, [0.7*inch, 2.5*inch, 3.1*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 13: SCRIPT DEPENDENCY MAP
# ============================================================
def build_section13(elements, styles):
    elements.append(Paragraph("13. Script Dependency Map", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))
    elements.append(Paragraph(
        "The SQL scripts in the <font face='Courier' size='7.5'>scripts/</font> directory build the system "
        "incrementally. The numbered subdirectory scripts (01_source through 10_intelligence) form the "
        "production pipeline. The evolutionary scripts (01\u201318) add features iteratively.",
        styles['BodyText2']))
    elements.append(Spacer(1, 4))

    add_diagram(elements, """
  PRODUCTION PIPELINE (Numbered Subdirectories)
  ==============================================
  01_source/setup.sql -> 01_source/seed_data.sql
                           |
  02_landing/setup.sql -> 02_landing/create_tables.sql
                           |
  04_openflow/setup.sql (creates DBAONTAP_ANALYTICS + 7 schemas)
       |
       +-> 05_bronze/setup.sql (5 Bronze DTs)
       |       |
       |       +-> 06_silver/setup.sql (5 Silver DTs)
       |               |
       |               +-> 07_gold/setup.sql (5 Gold DTs)
       |                       |
       |                       +-> 09_semantic_views/setup.sql (SV pipeline SP)
       |                               |
       |                               +-> 10_intelligence/setup.sql (permissions)
       |
       +-> 08_agents/setup.sql (metadata tables + utility SPs)

  EVOLUTIONARY SCRIPTS (Feature Development)
  ===========================================
  07_agentic_workflow_engine.sql (5-phase workflow)
    +-> 08_decision_point_2.sql (schema change detection)
    |     +-> 08c_schema_detection_tuning.sql (ignore patterns)
    |           +-> 08e_detect_schema_changes_with_kg.sql (KG-enhanced)
    +-> 13_transformation_directives.sql (three-layer control)

  15_table_lineage_map.sql (foundational metadata)
    +-> 09_knowledge_graph.sql (KG population from lineage map)
    |     +-> 11_hybrid_semantic_view_generator.sql
    |     +-> 12_fixed_hybrid_semantic_view.sql
    |     +-> gold_schema_propagation.sql
    +-> 16_register_lineage.sql (auto-register lineage from DDL)
    +-> 14_ddl_validation.sql (compile-check DDL)
          +-> 17_gold_agentic_executor.sql (validated Gold execution)
                +-> 18_build_gold_for_new_tables.sql (two-strategy discovery)""", styles, title="Script Dependency Tree")

    elements.append(Spacer(1, 10))


# ============================================================
# SECTION 14: SNOWFLAKE TECHNOLOGIES
# ============================================================
def build_section14(elements, styles):
    elements.append(Paragraph("14. Snowflake Technologies Used", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    tech = [
        ["Dynamic Tables", "Bronze, Silver, Gold layers", "Declarative refresh, automatic dependency tracking, lag monitoring"],
        ["Cortex AI (COMPLETE)", "Planner, Executor, Reflector, data gen", "Claude 3.5 Sonnet for DDL generation + planning; Llama 3.1-8b for schema inference + reflection"],
        ["Cortex AI (EMBED_TEXT_768)", "Knowledge Graph similarity", "e5-base-v2 embeddings for semantic pattern reuse"],
        ["Cortex Analyst", "AI Chat consumption", "Natural language \u2192 SQL via Semantic Views"],
        ["Semantic Views", "Gold layer consumption", "Dimensions, measures, synonyms for text-to-SQL"],
        ["Openflow (CDC)", "Source \u2192 Snowflake ingestion", "Change Data Capture with _SNOWFLAKE_DELETED/_UPDATED_AT"],
        ["Managed PostgreSQL", "Source + Landing instances", "Snowflake-managed PG for OLTP simulation"],
        ["Streams", "CDC event detection", "APPEND_ONLY streams on 5 landing tables"],
        ["Tasks", "Scheduled automation", "1-minute trigger for agentic workflow"],
        ["Streamlit in Snowflake", "Management UI", "13-tab application with External Access Integration"],
        ["External Access Integration", "PostgreSQL connectivity", "psycopg2 via EAI for PG data generation/monitoring"],
        ["Python Stored Procedures", "Complex agent logic", "Gold build, DDL validation, lineage registration, drift detection"],
        ["VARIANT Data Type", "Bronze schema-on-read", "OBJECT_CONSTRUCT(*) for schema-flexible ingestion"],
        ["EXECUTE IMMEDIATE", "Dynamic DDL execution", "Agent-generated DDL executed at runtime"],
        ["INFORMATION_SCHEMA", "Metadata discovery", "Table/column/DT introspection for KG population"],
        ["VECTOR_COSINE_SIMILARITY", "Semantic search", "Find similar tables for pattern reuse"],
    ]
    t = make_table(["Technology", "Used For", "Details"],
                   tech, [1.6*inch, 1.8*inch, 2.9*inch], styles, first_col_bold=True)
    elements.append(t)
    elements.append(Spacer(1, 16))

    elements.append(HRFlowable(width="100%", thickness=0.5, color=SF_LIGHT_GRAY, spaceAfter=8))
    elements.append(Paragraph(
        "<i>This document describes the Agentic Data Foundry reference implementation available at "
        "github.com/dbaontap/agentic-data-foundry. Built on Snowflake using Cortex AI, Dynamic Tables, "
        "Openflow, and Streamlit in Snowflake.</i>",
        styles['SmallNote']))


# ============================================================
# MAIN
# ============================================================
def main():
    doc = SimpleDocTemplate(
        OUTPUT_PDF,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.7 * inch,
    )
    styles = build_styles()
    elements = []

    build_cover(elements, styles)
    build_toc(elements, styles)
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
    build_section12(elements, styles)
    build_section13(elements, styles)
    build_section14(elements, styles)

    doc.build(elements, onFirstPage=footer, onLaterPages=footer)
    print(f"PDF generated: {OUTPUT_PDF}")


if __name__ == "__main__":
    main()
