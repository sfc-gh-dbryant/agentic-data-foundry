#!/usr/bin/env python3
"""Generate a professional Snowflake-branded PDF: Agentic Data Foundry Demo Talk Track."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, white
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether, CondPageBreak
)
from reportlab.platypus.flowables import Flowable
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "agentic-data-foundry-talk-track.pdf")

SF_BLUE = HexColor("#29B5E8")
SF_DARK = HexColor("#1B2A4A")
SF_LIGHT_BG = HexColor("#F0F8FF")
SF_GREEN = HexColor("#198754")
SF_ORANGE = HexColor("#E8742A")
SF_PURPLE = HexColor("#6F42C1")
SF_GRAY = HexColor("#6C757D")
SF_LIGHT_GRAY = HexColor("#E9ECEF")
SF_WHITE = white
TABLE_HEADER_BG = HexColor("#1B2A4A")
TABLE_ALT_ROW = HexColor("#F8FAFC")
CODE_BG = HexColor("#F1F3F5")
CALLOUT_BLUE_BG = HexColor("#E3F2FD")
CALLOUT_GREEN_BG = HexColor("#E8F5E9")
CALLOUT_ORANGE_BG = HexColor("#FFF3E0")
CALLOUT_PURPLE_BG = HexColor("#F3E8FF")
SHOW_TELL_BG = HexColor("#FFF8E1")
SHOW_TELL_ACCENT = HexColor("#FFA000")


class StatBox(Flowable):
    def __init__(self, width, height, color, label, value):
        Flowable.__init__(self)
        self.box_width = width
        self.box_height = height
        self.color = color
        self.label = label
        self.value = value
        self.width = width
        self.height = height

    def draw(self):
        self.canv.setFillColor(self.color)
        self.canv.roundRect(0, 0, self.box_width, self.box_height, 6, fill=1, stroke=0)
        self.canv.setFillColor(white)
        self.canv.setFont('Helvetica-Bold', 14)
        self.canv.drawCentredString(self.box_width / 2, self.box_height / 2 + 2, self.value)
        self.canv.setFont('Helvetica', 7)
        self.canv.setFillColor(HexColor("#B0D4F1"))
        self.canv.drawCentredString(self.box_width / 2, self.box_height / 2 - 14, self.label)


def build_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle('CoverTitle', fontName='Helvetica-Bold', fontSize=28, leading=34, textColor=SF_WHITE, alignment=TA_LEFT))
    styles.add(ParagraphStyle('CoverSubtitle', fontName='Helvetica', fontSize=14, leading=20, textColor=HexColor("#B0D4F1"), alignment=TA_LEFT))
    styles.add(ParagraphStyle('SectionTitle', fontName='Helvetica-Bold', fontSize=16, leading=22, textColor=SF_DARK, spaceBefore=16, spaceAfter=8))
    styles.add(ParagraphStyle('SubsectionTitle', fontName='Helvetica-Bold', fontSize=12, leading=16, textColor=SF_DARK, spaceBefore=10, spaceAfter=4))
    styles.add(ParagraphStyle('Sub2Title', fontName='Helvetica-Bold', fontSize=10, leading=13, textColor=HexColor("#444444"), spaceBefore=8, spaceAfter=3))
    styles.add(ParagraphStyle('BodyText2', fontName='Helvetica', fontSize=9.5, leading=13, textColor=HexColor("#333333"), spaceBefore=3, spaceAfter=3))
    styles.add(ParagraphStyle('SayThis', fontName='Helvetica-Oblique', fontSize=9.5, leading=13.5, textColor=HexColor("#1B2A4A"), leftIndent=14, rightIndent=14, spaceBefore=3, spaceAfter=3, borderPadding=(4, 4, 4, 4), backColor=CALLOUT_BLUE_BG))
    styles.add(ParagraphStyle('BulletItem', fontName='Helvetica', fontSize=9.5, leading=13, textColor=HexColor("#333333"), leftIndent=18, bulletIndent=6, spaceBefore=2, spaceAfter=2))
    styles.add(ParagraphStyle('NumberedItem', fontName='Helvetica', fontSize=9.5, leading=13, textColor=HexColor("#333333"), leftIndent=22, bulletIndent=6, spaceBefore=2, spaceAfter=2))
    styles.add(ParagraphStyle('CodeBlock', fontName='Courier', fontSize=7.5, leading=10, textColor=HexColor("#212529"), backColor=CODE_BG, borderPadding=(6, 6, 6, 6), spaceBefore=4, spaceAfter=4))
    styles.add(ParagraphStyle('SmallNote', fontName='Helvetica-Oblique', fontSize=8.5, leading=11, textColor=SF_GRAY, spaceBefore=4, spaceAfter=4))
    styles.add(ParagraphStyle('TimeBadge', fontName='Helvetica-Bold', fontSize=8, leading=10, textColor=SF_ORANGE))
    styles.add(ParagraphStyle('ActionCue', fontName='Helvetica-Bold', fontSize=9, leading=12, textColor=SF_GREEN, spaceBefore=4, spaceAfter=4))
    styles.add(ParagraphStyle('TableHeader', fontName='Helvetica-Bold', fontSize=8, leading=10, textColor=SF_WHITE, alignment=TA_CENTER))
    styles.add(ParagraphStyle('TableCell', fontName='Helvetica', fontSize=8, leading=10, textColor=HexColor("#333333")))
    styles.add(ParagraphStyle('TableCellCenter', fontName='Helvetica', fontSize=8, leading=10, textColor=HexColor("#333333"), alignment=TA_CENTER))
    styles.add(ParagraphStyle('TableCellBold', fontName='Helvetica-Bold', fontSize=8, leading=10, textColor=HexColor("#333333")))
    styles.add(ParagraphStyle('CalloutText', fontName='Helvetica', fontSize=9, leading=12, textColor=SF_DARK, leftIndent=10, spaceBefore=6, spaceAfter=6))
    styles.add(ParagraphStyle('ShowTellTitle', fontName='Helvetica-Bold', fontSize=9.5, leading=12, textColor=SHOW_TELL_ACCENT, spaceBefore=6, spaceAfter=2))
    styles.add(ParagraphStyle('ShowTellText', fontName='Helvetica', fontSize=8.5, leading=11.5, textColor=HexColor("#333333"), leftIndent=10, spaceBefore=1, spaceAfter=1))
    return styles


def make_table(headers, rows, col_widths, styles, first_col_bold=False):
    s = styles
    header_row = [Paragraph(h, s['TableHeader']) for h in headers]
    data = [header_row]
    for row in rows:
        processed = []
        for i, cell in enumerate(row):
            if i == 0 and first_col_bold:
                processed.append(Paragraph(str(cell), s['TableCellBold']))
            elif i > 0 and len(headers) > 2:
                processed.append(Paragraph(str(cell), s['TableCellCenter']))
            else:
                processed.append(Paragraph(str(cell), s['TableCell']))
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


def add_callout(elements, text, styles, accent_color=SF_BLUE, bg_color=SF_LIGHT_BG):
    tbl = Table([[Paragraph(text, styles['CalloutText'])]], colWidths=[6.5 * inch])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), bg_color),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
        ('LINEBEFORETABLE', (0, 0), (0, -1), 3, accent_color),
    ]))
    elements.append(tbl)


def add_code_block(elements, code_text, styles):
    lines = code_text.strip().split('\n')
    formatted = '<br/>'.join(
        line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace(' ', '&nbsp;')
        for line in lines
    )
    tbl = Table([[Paragraph(formatted, styles['CodeBlock'])]], colWidths=[6.5 * inch])
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


def add_say_this(elements, text, styles):
    tbl = Table([[Paragraph(text, styles['SayThis'])]], colWidths=[6.5 * inch])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), CALLOUT_BLUE_BG),
        ('LEFTPADDING', (0, 0), (-1, -1), 14),
        ('RIGHTPADDING', (0, 0), (-1, -1), 14),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [4, 4, 4, 4]),
        ('LINEBEFORETABLE', (0, 0), (0, -1), 3, SF_BLUE),
    ]))
    elements.append(tbl)
    elements.append(Spacer(1, 4))


def add_show_tell(elements, title, bullets, styles, sql=None):
    items = []
    items.append(Paragraph(f"Show &amp; Tell: {title}", styles['ShowTellTitle']))
    for b in bullets:
        items.append(Paragraph(f"  {b}", styles['ShowTellText']))
    if sql:
        lines = sql.strip().split('\n')
        formatted = '<br/>'.join(
            line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace(' ', '&nbsp;')
            for line in lines
        )
        items.append(Spacer(1, 2))
        code_tbl = Table([[Paragraph(formatted, styles['CodeBlock'])]], colWidths=[6.2 * inch])
        code_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), CODE_BG),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('ROUNDEDCORNERS', [4, 4, 4, 4]),
        ]))
        items.append(code_tbl)
    wrapper_tbl = Table([[items_to_flowable(items)]], colWidths=[6.5 * inch])
    wrapper_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SHOW_TELL_BG),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('ROUNDEDCORNERS', [4, 4, 4, 4]),
        ('LINEBEFORETABLE', (0, 0), (0, -1), 3, SHOW_TELL_ACCENT),
    ]))
    elements.append(wrapper_tbl)
    elements.append(Spacer(1, 6))


def items_to_flowable(items):
    from reportlab.platypus import Table as T, TableStyle as TS
    data = [[item] for item in items]
    t = T(data, colWidths=[6.2 * inch])
    t.setStyle(TS([
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    return t


def section_header(elements, styles, title, duration):
    elements.append(CondPageBreak(2 * inch))
    elements.append(Paragraph(title, styles['SectionTitle']))
    row = [
        [Paragraph(f"Duration: {duration}", styles['TimeBadge'])]
    ]
    dur_tbl = Table(row, colWidths=[6.5 * inch])
    dur_tbl.setStyle(TableStyle([
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(dur_tbl)
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=6))


# === PAGE BUILDERS ===

def build_cover(elements, styles):
    cover_bg = Table(
        [
            [Spacer(1, 1.4 * inch)],
            [Paragraph("Agentic Data Foundry", styles['CoverTitle'])],
            [Spacer(1, 0.1 * inch)],
            [Paragraph("Demo Talk Track", ParagraphStyle('CoverTitle2', fontName='Helvetica-Bold', fontSize=22, leading=28, textColor=SF_BLUE, alignment=TA_LEFT))],
            [Spacer(1, 0.2 * inch)],
            [Paragraph("End-to-End AI-Driven Data Platform on Snowflake", styles['CoverSubtitle'])],
            [Spacer(1, 0.08 * inch)],
            [Paragraph("March 2026  |  Snowflake Professional Services", styles['CoverSubtitle'])],
            [Spacer(1, 0.5 * inch)],
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

    elements.append(Spacer(1, 0.25 * inch))
    box_w = 1.28 * inch
    box_h = 0.6 * inch
    stat_row = Table(
        [[StatBox(box_w, box_h, SF_BLUE, "CORE DEMO", "12-15 min"),
          StatBox(box_w, box_h, SF_ORANGE, "WITH DEEP DIVES", "20-25 min"),
          StatBox(box_w, box_h, SF_GREEN, "DEMO TABS", "8"),
          StatBox(box_w, box_h, SF_PURPLE, "SHOW & TELLS", "10"),
          StatBox(box_w, box_h, SF_DARK, "AI PHASES", "5")]],
        colWidths=[box_w + 6] * 5,
    )
    stat_row.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(stat_row)

    elements.append(Spacer(1, 0.3 * inch))
    add_callout(elements,
                "<b>Tip:</b> Show &amp; Tell sections (marked in amber) are optional. Use them for technical audiences "
                "or when someone asks \"how does that work?\" For executive audiences, skip them and keep the narrative flowing.",
                styles, accent_color=SHOW_TELL_ACCENT, bg_color=SHOW_TELL_BG)
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph(
        '<font color="#6C757D">INTERNAL - Snowflake Confidential</font>',
        ParagraphStyle('conf', fontName='Helvetica-Oblique', fontSize=9, alignment=TA_CENTER, textColor=SF_GRAY)
    ))
    elements.append(PageBreak())


def build_overview(elements, styles):
    elements.append(Paragraph("Demo Overview", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    elements.append(Paragraph(
        "This talk track guides you through the Agentic Data Foundry demo - an end-to-end data platform "
        "built entirely on Snowflake. The demo takes an audience from an operational PostgreSQL database "
        "to production-ready analytics queryable in plain English, with AI agents handling the Silver and Gold transformations.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Audience", styles['SubsectionTitle']))
    elements.append(Paragraph(
        "Data leaders, architects, and engineers evaluating Snowflake's AI and data platform capabilities.",
        styles['BodyText2']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Demo Journey", styles['SubsectionTitle']))
    headers = ["#", "Tab", "Duration", "What Happens"]
    rows = [
        ["1", "Opening", "60s", "Set the stage - what we'll build and why it matters"],
        ["2", "Generate Data", "1 min", "Spin up live PostgreSQL, populate with business data"],
        ["3", "Pipeline Status", "1 min", "Watch Openflow CDC + Bronze Dynamic Tables in action"],
        ["4", "Agentic Workflow", "2 min", "AI agent analyzes, generates, validates Silver transformations"],
        ["-", "Knowledge Graph", "30s", "Checkpoint - show live metadata lineage"],
        ["5", "Gold Layer", "2 min", "Build business aggregations + autonomous gap discovery"],
        ["6", "Schema Contracts", "30s", "The guardrails that keep AI output deterministic"],
        ["7", "Semantic Views", "1 min", "AI generates business-friendly metadata layer"],
        ["8", "AI Chat", "1 min", "Business users query data in plain English"],
        ["9", "Knowledge Graph", "30s", "Full lineage from source to analytics"],
        ["-", "Closing", "30s", "Key takeaways and discussion"],
    ]
    elements.append(make_table(headers, rows, [0.4 * inch, 1.4 * inch, 0.7 * inch, 4.0 * inch], styles, first_col_bold=True))
    elements.append(PageBreak())


def build_opening(elements, styles):
    section_header(elements, styles, "Opening", "60 seconds")

    add_say_this(elements,
                 "What I'm going to show you is an end-to-end data platform built entirely on Snowflake. "
                 "We start with an operational PostgreSQL database - managed by Snowflake - and in about "
                 "10 minutes we'll have production-ready analytics that business users can query in plain English. "
                 "The key differentiator: the Silver and Gold transformations are driven by AI agents using "
                 "Cortex LLM, not hand-coded ETL.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("Walk Through the Journey", styles['Sub2Title']))
    journey = [
        "<b>Generate Data</b> - Spin up a live PostgreSQL database with realistic business data",
        "<b>Pipeline Status</b> - Watch Openflow CDC replicate changes in real time; Bronze Dynamic Tables wrap everything into schema-flexible VARIANT",
        "<b>Agentic Workflow</b> - The star of the show. An AI agent analyzes raw data, generates Silver transformations, validates quality, and learns from every run",
        "<b>Gold Layer</b> - Build business-ready aggregations, then let the AI discover what's missing and build the rest autonomously",
        "<b>Semantic Views</b> - AI generates business-friendly metadata so anyone can understand the data",
        "<b>AI Chat</b> - The payoff. Business users ask questions in plain English backed by governed, production-quality data",
        "<b>Knowledge Graph</b> - A live, self-updating map of every table, relationship, and lineage path",
    ]
    for j in journey:
        elements.append(Paragraph(f"  {j}", styles['BulletItem']))

    elements.append(Spacer(1, 6))
    add_say_this(elements,
                 "Everything you'll see - the CDC, the agents, the transformations, the chat - runs natively "
                 "inside Snowflake. No external tools, no data movement, no glue code. Let's go.",
                 styles)
    elements.append(PageBreak())


def build_generate_data(elements, styles):
    section_header(elements, styles, "Tab 2: Generate Data", "1 minute")

    add_say_this(elements,
                 "We start with a simulated OLTP application. This is Snowflake Managed PostgreSQL - a "
                 "fully managed Postgres instance running inside Snowflake's ecosystem. We're generating "
                 "realistic business data: customers, orders, products, order items, and support tickets.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The app connects directly to PostgreSQL using External Access Integrations - this Streamlit "
                 "app running in Snowflake can securely reach out to the Postgres instances using stored credentials.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "Behind the scenes, there are two Postgres instances: a SOURCE that simulates the application, "
                 "and a LANDING that receives changes via PostgreSQL logical replication. This is the same pattern "
                 "you'd see in production - source database stays clean, CDC happens from a replica.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("[Click Generate Data]", styles['ActionCue']))
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "PostgreSQL Connection Architecture", [
        "<b>Show</b> the <font face='Courier' size='8'>get_pg_connection()</font> function - "
        "<font face='Courier' size='8'>_snowflake.get_username_password()</font> retrieves credentials securely",
        "<b>Show</b> the CREATE STREAMLIT statement with EXTERNAL_ACCESS_INTEGRATIONS and SECRETS",
        "<b>Data check:</b> Query the SOURCE Postgres directly: <font face='Courier' size='8'>SELECT COUNT(*) FROM customers</font>",
    ], styles)
    elements.append(Spacer(1, 4))


def build_pipeline_status(elements, styles):
    section_header(elements, styles, "Tab 3: Pipeline Status", "1 minute")

    add_say_this(elements,
                 "Now let's see what happened. Openflow - Snowflake's built-in CDC engine - is continuously "
                 "replicating changes from the Landing Postgres into Snowflake. It adds CDC metadata columns: "
                 "_SNOWFLAKE_DELETED, _SNOWFLAKE_UPDATED_AT - so we always know the state of every record.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The data lands in the public schema as raw tables. From there, Dynamic Tables take over. "
                 "Our Bronze layer wraps everything into VARIANT using OBJECT_CONSTRUCT(*) - this gives us "
                 "schema-on-read flexibility. If the source schema changes, Bronze doesn't break.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "All of this is declarative. No Airflow, no scheduler, no orchestration code. Dynamic Tables "
                 "handle the refresh automatically with a 1-minute target lag.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "Bronze VARIANT Pattern", [
        "<b>Show the DDL:</b> <font face='Courier' size='8'>SELECT GET_DDL('TABLE', 'DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT')</font>",
        "<b>Explain:</b> \"Three lines of SQL. OBJECT_CONSTRUCT(*) wraps the entire row into a single VARIANT column. "
        "If the source adds a column tomorrow, this table doesn't break.\"",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT * FROM BRONZE.CUSTOMERS_VARIANT LIMIT 3</font> - show the PAYLOAD variant column",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT PAYLOAD:customer_id, PAYLOAD:first_name FROM BRONZE.CUSTOMERS_VARIANT LIMIT 3</font>",
    ], styles, sql="""CREATE OR REPLACE DYNAMIC TABLE CUSTOMERS_VARIANT(
    PAYLOAD, SOURCE_TABLE, INGESTED_AT
) TARGET_LAG = '1 minute' REFRESH_MODE = AUTO
AS SELECT
    OBJECT_CONSTRUCT(*) AS PAYLOAD,
    'CUSTOMERS' AS SOURCE_TABLE,
    CURRENT_TIMESTAMP() AS INGESTED_AT
FROM DBAONTAP_ANALYTICS."public"."customers";""")


def build_agentic_workflow(elements, styles):
    section_header(elements, styles, "Tab 4: Agentic Workflow", "2 minutes - THE STAR OF THE SHOW")

    add_say_this(elements,
                 "This is where it gets interesting. Traditional medallion architectures require a data engineer "
                 "to hand-code every Silver transformation - column typing, deduplication, null handling, naming "
                 "conventions. That's the bottleneck.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("The Five-Phase AI Pipeline", styles['SubsectionTitle']))
    phases = [
        "<b>Trigger</b> - Detects which Bronze tables need processing",
        "<b>Planner</b> - An LLM agent examines each Bronze table's VARIANT schema, samples data quality, "
        "checks past learnings, and decides the transformation strategy",
        "<b>Executor</b> - The LLM generates the actual CREATE DYNAMIC TABLE DDL for each Silver table, "
        "respecting schema contracts. Includes self-correction if DDL fails",
        "<b>Validator</b> - Runs the DDL, checks row counts, validates data quality",
        "<b>Reflector</b> - Reviews what happened and stores learnings for next time",
    ]
    for i, p in enumerate(phases, 1):
        elements.append(Paragraph(f"{i}. {p}", styles['NumberedItem']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The schema contracts are the guardrails. They enforce column naming standards - for example, "
                 "CDC columns must use _SNOWFLAKE_DELETED, not IS_DELETED. The LLM proposes, the contracts constrain.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("[Click Run Agentic Workflow]", styles['ActionCue']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "Watch the phases execute. The LLM is calling Claude via Cortex - all within Snowflake's "
                 "security perimeter. No data leaves the platform.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "The Agentic Brain (Best for Technical Audiences)", [
        "<b>Show</b> the WORKFLOW_PLANNER procedure - highlight the prompt construction that feeds "
        "schema analysis + data quality + past learnings to the LLM",
        "<b>Show</b> a sample planner decision from METADATA.WORKFLOW_EXECUTIONS",
        "<b>Highlight:</b> The LLM chose flatten_and_type strategy, identified the primary key, decided on "
        "deduplication logic - all from analyzing the raw VARIANT data",
        "<b>Show</b> the Schema Contract: <font face='Courier' size='8'>SELECT * FROM METADATA.SILVER_SCHEMA_CONTRACTS "
        "WHERE SOURCE_TABLE_PATTERN = 'CUSTOMERS'</font>",
    ], styles, sql="""SELECT EXECUTION_ID, PLANNER_OUTPUT
FROM METADATA.WORKFLOW_EXECUTIONS
ORDER BY STARTED_AT DESC LIMIT 1""")

    add_show_tell(elements, "Data Quality Guardrails", [
        "<b>Planner-side (upstream):</b> Before any DDL is generated, the Planner calls "
        "ANALYZE_DATA_QUALITY(table, 500) which samples 500 rows and feeds a quality report to the LLM",
        "<b>Validator-side (downstream):</b> After DDL executes, the Validator compares row counts between "
        "Bronze source and Silver target (passes if variance &lt; 5%)",
        "<b>Show</b> validation results: <font face='Courier' size='8'>SELECT source_table, target_table, passed, variance_pct "
        "FROM METADATA.VALIDATION_RESULTS</font>",
        "<b>Explain:</b> \"Every run is logged to VALIDATION_RESULTS so you have a full audit trail.\"",
    ], styles)

    add_show_tell(elements, "Active Learnings (The Memory)", [
        "<b>Scroll down</b> to the Active Learnings section below the workflow dashboard table",
        "<b>Click each expander</b> to reveal what the Reflector learned - learning type, observation, "
        "recommendation, and confidence score",
        "<b>Key point:</b> \"This is the difference between AI-generated and AI-<i>agentic</i>. "
        "Generated is one-shot. Agentic learns, adapts, and improves.\"",
    ], styles)

    add_show_tell(elements, "Silver Data Quality", [
        "<b>Query:</b> <font face='Courier' size='8'>SELECT * FROM SILVER.CUSTOMERS LIMIT 5</font> - show clean, typed columns",
        "<b>Compare counts:</b> <font face='Courier' size='8'>SELECT COUNT(*) FROM BRONZE.CUSTOMERS_VARIANT</font> vs "
        "<font face='Courier' size='8'>SELECT COUNT(*) FROM SILVER.CUSTOMERS</font>",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'SILVER'</font>",
    ], styles)
    elements.append(PageBreak())


def build_knowledge_graph_checkpoint(elements, styles):
    section_header(elements, styles, "Checkpoint: Knowledge Graph", "30 seconds")

    add_say_this(elements,
                 "Before we build Gold, let me show you something. Flip to the Knowledge Graph tab.",
                 styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("[Click Knowledge Graph tab]", styles['ActionCue']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "This graph is built dynamically from live metadata - it's not a static diagram. Right now "
                 "you can see Bronze-to-Silver lineage for all 5 tables. After we build Gold, new edges will "
                 "appear automatically showing Silver-to-Gold relationships. Watch for that.",
                 styles)
    elements.append(Spacer(1, 4))

    add_callout(elements,
                "<b>Key point:</b> \"This is the system's self-awareness - it knows its own structure.\"",
                styles, accent_color=SF_PURPLE, bg_color=CALLOUT_PURPLE_BG)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("[Click back to Gold Layer tab]", styles['ActionCue']))
    elements.append(Spacer(1, 6))


def build_gold_layer(elements, styles):
    section_header(elements, styles, "Tab 5: Gold Layer", "2 minutes")

    add_say_this(elements,
                 "Silver gives us clean, typed, deduplicated tables. Gold gives us business value. "
                 "We start by building four core Gold Dynamic Tables:",
                 styles)
    elements.append(Spacer(1, 4))

    gold_headers = ["Gold Table", "Business Purpose"]
    gold_rows = [
        ["CUSTOMER_360", "RFM analysis, lifetime value, engagement scoring"],
        ["PRODUCT_PERFORMANCE", "Margins, units sold, revenue by product"],
        ["ORDER_SUMMARY", "Monthly trends by customer segment"],
        ["ML_CUSTOMER_FEATURES", "Encoded features ready for model training"],
    ]
    elements.append(make_table(gold_headers, gold_rows, [2.2 * inch, 4.3 * inch], styles, first_col_bold=True))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "These are all Dynamic Tables with TARGET_LAG = DOWNSTREAM, meaning they refresh only when "
                 "something downstream needs them. Zero wasted compute.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("[Click Build Core Gold Layer]", styles['ActionCue']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "Now here's where it gets interesting. We built four Gold tables - but we have five Silver "
                 "tables. Let's see if the AI can figure out what's missing.",
                 styles)
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("[Click Agentic Gold Build]", styles['ActionCue']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The agentic builder scans every Silver table and checks whether it has dedicated Gold-layer "
                 "coverage. It's not just checking names - it's reasoning about what's covered and what's not.",
                 styles)
    elements.append(Spacer(1, 2))
    elements.append(Paragraph("<i>Wait for the agentic build to complete (~30-60 seconds).</i>", styles['SmallNote']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "Look at what it found - Silver tables that are used in joins inside other Gold tables but "
                 "don't have their own dedicated Gold aggregation. The AI recognized those gaps and built new "
                 "tables autonomously.",
                 styles)
    elements.append(Spacer(1, 4))

    add_callout(elements,
                "<b>Aha moment:</b> A human engineer might not catch that - they'd see the data flowing into joins and "
                "assume it's covered. The AI looked at coverage <i>per source table</i> and said 'these deserve their own "
                "analytical surface.' That's what happens when a new source table appears in production - you don't "
                "hand-code the Gold layer, the AI builds it.",
                styles, accent_color=SF_GREEN, bg_color=CALLOUT_GREEN_BG)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "What the AI Actually Built", [
        "<b>Query:</b> <font face='Courier' size='8'>SHOW DYNAMIC TABLES IN SCHEMA DBAONTAP_ANALYTICS.GOLD</font> - "
        "identify AI-generated tables (any beyond the 4 core tables)",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT GET_DDL('TABLE', 'DBAONTAP_ANALYTICS.GOLD.&lt;AI_TABLE&gt;')</font> - "
        "show the AI-generated aggregation logic",
        "<b>Key point:</b> \"The AI chose the right deleted-row filter, picked meaningful aggregations, and even "
        "joined with related tables - all autonomously.\"",
    ], styles)

    add_show_tell(elements, "Customer 360 Deep Dive", [
        "<b>Query:</b> <font face='Courier' size='8'>SELECT FULL_NAME, SEGMENT, LOYALTY_TIER, TOTAL_ORDERS, "
        "LIFETIME_VALUE FROM GOLD.CUSTOMER_360 ORDER BY LIFETIME_VALUE DESC LIMIT 10</font>",
        "<b>Explain:</b> \"This single table joins Customers, Orders, and Support Tickets. It computes RFM scores, "
        "revenue tiers, and engagement status - all as a Dynamic Table that refreshes automatically.\"",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT PRODUCT_NAME, CATEGORY, MARGIN_PERCENT, TOTAL_REVENUE "
        "FROM GOLD.PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC LIMIT 5</font>",
    ], styles)
    elements.append(PageBreak())


def build_schema_contracts(elements, styles):
    section_header(elements, styles, "Tab 6: Schema Contracts", "30 seconds")

    add_say_this(elements,
                 "Quick look at the contracts. These persist across resets - they're the institutional knowledge "
                 "of your data team encoded as rules. Each table has required columns, data types, and naming "
                 "conventions. The agentic workflow reads these before generating any DDL.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "Contract Enforcement (If Asked)", [
        "<b>Show</b> a contract: <font face='Courier' size='8'>SELECT SOURCE_TABLE_PATTERN, NAMING_RULES:note::VARCHAR "
        "FROM METADATA.SILVER_SCHEMA_CONTRACTS</font>",
        "<b>Explain:</b> \"The PRODUCTS contract says 'Column is NAME not PRODUCT_NAME.' Without this, the LLM might "
        "rename columns inconsistently between runs. Contracts make the output deterministic.\"",
    ], styles)
    elements.append(Spacer(1, 6))


def build_semantic_views(elements, styles):
    section_header(elements, styles, "Tab 7: Semantic Views", "1 minute")

    add_say_this(elements,
                 "Now we bridge the gap between data engineering and business users. Semantic Views are "
                 "Snowflake's way of describing your data in business terms - dimensions, facts, metrics, "
                 "and relationships.",
                 styles)
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("Three Generation Approaches", styles['Sub2Title']))
    sv_headers = ["Approach", "Method", "Recommendation"]
    sv_rows = [
        ["Pure Agentic", "LLM-only generation", "Flexible but less deterministic"],
        ["Knowledge Graph", "Rule-based from metadata", "Deterministic but less descriptive"],
        ["Hybrid (recommended)", "KG structure + LLM enrichment", "Best of both worlds"],
    ]
    elements.append(make_table(sv_headers, sv_rows, [1.4 * inch, 2.1 * inch, 3.0 * inch], styles, first_col_bold=True))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph("[Click Generate Hybrid]", styles['ActionCue']))
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The Hybrid approach first populates a Knowledge Graph from live schema metadata, then the LLM "
                 "enriches each Semantic View with business-friendly descriptions and column synonyms. This gives "
                 "us the best of both worlds - deterministic structure with intelligent context.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "What a Semantic View Looks Like", [
        "<b>Query:</b> <font face='Courier' size='8'>SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD</font>",
        "<b>Query:</b> <font face='Courier' size='8'>SELECT GET_DDL('SEMANTIC VIEW', 'DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360_HYBRID_SV')</font>",
        "<b>Explain:</b> \"The LLM looked at CUSTOMER_360, understood that SEGMENT is a dimension, LIFETIME_VALUE is "
        "a fact, and 'Average Order Value' is a meaningful metric. The synonyms help Cortex Analyst understand "
        "alternate ways users might ask about the same data.\"",
    ], styles)
    elements.append(Spacer(1, 4))


def build_ai_chat(elements, styles):
    section_header(elements, styles, "Tab 8: AI Chat - THE PAYOFF", "1 minute")

    add_say_this(elements,
                 "And here's the payoff. Business users can now ask questions in plain English and get answers "
                 "backed by governed, production-quality data.",
                 styles)
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Suggested Live Questions", styles['SubsectionTitle']))
    q_headers = ["#", "Question", "What It Proves"]
    q_rows = [
        ["1", "Who are our top 5 customers by lifetime value?", "End-to-end pipeline works"],
        ["2", "What's our best-selling product by revenue?", "Cross-table intelligence"],
        ["3", "Show me order trends by customer segment", "Aggregation awareness"],
    ]
    elements.append(make_table(q_headers, q_rows, [0.4 * inch, 3.0 * inch, 3.1 * inch], styles, first_col_bold=True))
    elements.append(Spacer(1, 6))

    add_say_this(elements,
                 "This is Cortex Analyst under the hood, powered by the Semantic Views we just generated. The SQL "
                 "is generated, executed, and results returned - all within Snowflake.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "SQL Behind the Answer", [
        "After asking a question, <b>show the generated SQL</b>",
        "<b>Explain:</b> \"Cortex Analyst translated plain English into this SQL query, using the Semantic View as its guide. "
        "The Semantic View told the AI that 'lifetime value' maps to the LIFETIME_VALUE column in CUSTOMER_360.\"",
    ], styles)
    elements.append(Spacer(1, 4))


def build_knowledge_graph_final(elements, styles):
    section_header(elements, styles, "Tab 9: Knowledge Graph", "30 seconds")

    add_say_this(elements,
                 "The Knowledge Graph tab shows the full data lineage - Bronze to Silver to Gold - rendered "
                 "dynamically based on what actually exists. This is live metadata, not a static diagram.",
                 styles)
    elements.append(Spacer(1, 6))

    add_show_tell(elements, "Dynamic Lineage", [
        "<b>Point out</b> that the graph shows actual tables from INFORMATION_SCHEMA, not hard-coded names",
        "<b>Explain:</b> \"If we added a new source table, the graph would update automatically after the "
        "agentic workflow runs.\"",
    ], styles)
    elements.append(Spacer(1, 8))


def build_closing(elements, styles):
    section_header(elements, styles, "Closing", "30 seconds")

    add_say_this(elements,
                 "So what did we just do? In about 10 minutes, we went from an empty database to a fully "
                 "operational analytics platform. PostgreSQL source, real-time CDC, AI-driven transformations "
                 "with guardrails, business-ready aggregations, and natural language querying.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "Every component is native Snowflake - no external tools, no separate compute, no data "
                 "leaving the platform.",
                 styles)
    elements.append(Spacer(1, 4))

    add_say_this(elements,
                 "The agentic pattern is the key innovation here. When your source schema changes or a new "
                 "table appears, the AI agents adapt. That's the future of data engineering - not replacing "
                 "engineers, but letting them focus on business logic while AI handles the plumbing.",
                 styles)
    elements.append(Spacer(1, 10))


def build_appendix(elements, styles):
    elements.append(Paragraph("Appendix: Talking Points by Audience", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    headers = ["Audience", "Emphasize", "Show & Tell Focus"]
    rows = [
        ["CTO / VP Data", "Speed to value, no external tools, governed AI", "Skip most code, show Chat results"],
        ["Data Architects", "Dynamic Tables, CDC pattern, medallion architecture", "Bronze VARIANT, Silver dedup, Gold aggregations"],
        ["Data Engineers", "Agentic workflow, schema contracts, LLM guardrails", "Planner prompts, contract enforcement, DDL generation"],
        ["Analytics / BI", "Semantic Views, natural language querying", "Semantic View DDL, Chat SQL generation"],
        ["Security / Compliance", "EAI, secrets management, data never leaves Snowflake", "CREATE STREAMLIT with SECRETS, Cortex in-platform"],
    ]
    elements.append(make_table(headers, rows, [1.3 * inch, 2.4 * inch, 2.8 * inch], styles, first_col_bold=True))
    elements.append(Spacer(1, 16))

    elements.append(Paragraph("Quick Reference: Demo Flow", styles['SectionTitle']))
    elements.append(HRFlowable(width="100%", thickness=1, color=SF_BLUE, spaceAfter=8))

    add_code_block(elements, """Generate Data --> Pipeline Status --> Agentic Workflow --> Gold Layer
     |                   |                    |                  |
  [Show:PG]        [Show:Bronze]      [Show:Planner+DDL]  [Show:360 data]
                                                                 |
                                                                 v
Schema Contracts --> Semantic Views --> AI Chat --> Knowledge Graph
     |                    |                |               |
  [Show:Rules]      [Show:SV DDL]   [Show:SQL]       [Show:Lineage]""", styles)


def header_footer(canvas, doc):
    canvas.saveState()
    if doc.page > 1:
        canvas.setFont('Helvetica', 7)
        canvas.setFillColor(SF_GRAY)
        canvas.drawString(0.75 * inch, 0.5 * inch,
                          "Agentic Data Foundry  |  Demo Talk Track  |  March 2026")
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
    build_overview(elements, styles)
    build_opening(elements, styles)
    build_generate_data(elements, styles)
    build_pipeline_status(elements, styles)
    build_agentic_workflow(elements, styles)
    build_knowledge_graph_checkpoint(elements, styles)
    build_gold_layer(elements, styles)
    build_schema_contracts(elements, styles)
    build_semantic_views(elements, styles)
    build_ai_chat(elements, styles)
    build_knowledge_graph_final(elements, styles)
    build_closing(elements, styles)
    build_appendix(elements, styles)

    doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)
    print(f"PDF generated: {OUTPUT_PDF}")


if __name__ == "__main__":
    main()
