"""Vector diagram helpers for the Agentic Data Foundry description PDF.

Uses reportlab.graphics to draw professional box-and-arrow diagrams.
"""

from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon, Group
from reportlab.graphics.widgetbase import Widget
from reportlab.lib.colors import HexColor, white, black, Color
from reportlab.lib.units import inch
from reportlab.platypus import Flowable

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
BRONZE_COLOR = HexColor("#CD7F32")
SILVER_COLOR = HexColor("#708090")
GOLD_COLOR = HexColor("#DAA520")
KG_COLOR = HexColor("#4A90D9")
META_COLOR = HexColor("#7B68EE")
AGENT_COLOR = HexColor("#20B2AA")
SV_COLOR = HexColor("#9B59B6")


def _darken(color, factor=0.7):
    return Color(color.red * factor, color.green * factor, color.blue * factor, color.alpha if hasattr(color, 'alpha') else 1)


def _box(d, x, y, w, h, fill, label_lines, text_color=white, font_size=7, corner=4, stroke_color=None):
    sc = stroke_color if stroke_color else _darken(fill)
    r = Rect(x, y, w, h, rx=corner, ry=corner, fillColor=fill,
             strokeColor=sc, strokeWidth=0.5)
    d.add(r)
    cy = y + h / 2 + (len(label_lines) - 1) * (font_size * 0.6)
    for line in label_lines:
        s = String(x + w / 2, cy, line, fontName='Helvetica-Bold', fontSize=font_size,
                   fillColor=text_color, textAnchor='middle')
        d.add(s)
        cy -= font_size * 1.3


def _label(d, x, y, text, font_size=6.5, color=None, bold=False, anchor='middle'):
    s = String(x, y, text, fontName='Helvetica-Bold' if bold else 'Helvetica',
               fontSize=font_size, fillColor=color or SF_GRAY, textAnchor=anchor)
    d.add(s)


def _arrow_right(d, x1, y, x2, color=SF_GRAY, width=1.5):
    d.add(Line(x1, y, x2 - 5, y, strokeColor=color, strokeWidth=width))
    d.add(Polygon(points=[x2, y, x2 - 6, y + 3, x2 - 6, y - 3],
                  fillColor=color, strokeColor=color, strokeWidth=0.3))


def _arrow_down(d, x, y1, y2, color=SF_GRAY, width=1.5):
    d.add(Line(x, y1, x, y2 + 5, strokeColor=color, strokeWidth=width))
    d.add(Polygon(points=[x, y2, x - 3, y2 + 6, x + 3, y2 + 6],
                  fillColor=color, strokeColor=color, strokeWidth=0.3))


class DrawingFlowable(Flowable):
    def __init__(self, drawing, hAlign='CENTER'):
        Flowable.__init__(self)
        self.drawing = drawing
        self.width = drawing.width
        self.height = drawing.height
        self.hAlign = hAlign

    def wrap(self, availWidth, availHeight):
        return self.width, self.height

    def draw(self):
        self.drawing.drawOn(self.canv, 0, 0)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 1: System Architecture Overview
# ─────────────────────────────────────────────────────────────
def diagram_system_architecture():
    W, H = 460, 310
    d = Drawing(W, H)

    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    bw, bh = 110, 40
    y_top = H - 55
    _box(d, 15, y_top, bw, bh, SF_DARK, ["PostgreSQL", "SOURCE"], font_size=7.5)
    _box(d, 170, y_top, bw, bh, SF_DARK, ["PostgreSQL", "LANDING"], font_size=7.5)
    _box(d, 325, y_top, bw, bh, SF_BLUE, ["Snowflake", '"public"'], font_size=7.5)
    _arrow_right(d, 125, y_top + bh/2, 170, SF_GRAY)
    _arrow_right(d, 280, y_top + bh/2, 325, SF_GRAY)
    _label(d, 147, y_top - 8, "Logical Repl.", font_size=5.5)
    _label(d, 303, y_top - 8, "Openflow CDC", font_size=5.5)

    _arrow_down(d, W/2, y_top - 2, y_top - 30, SF_GRAY)

    db_y = 20
    db_h = H - 90
    d.add(Rect(15, db_y, W - 30, db_h, fillColor=HexColor("#EDF2F7"), strokeColor=SF_ACCENT, strokeWidth=1, rx=6, ry=6))
    _label(d, W/2, db_y + db_h - 14, "DBAONTAP_ANALYTICS", font_size=8, color=SF_ACCENT, bold=True)

    medal_y = db_y + db_h - 80
    medal_w, medal_h = 100, 50
    _box(d, 45, medal_y, medal_w, medal_h, BRONZE_COLOR, ["BRONZE", "VARIANT DTs (5)"], font_size=7)
    _box(d, 180, medal_y, medal_w, medal_h, SILVER_COLOR, ["SILVER", "CDC-Aware DTs (5)"], font_size=7)
    _box(d, 315, medal_y, medal_w, medal_h, GOLD_COLOR, ["GOLD", "Aggregate DTs (5)"], font_size=7)
    _arrow_right(d, 145, medal_y + medal_h/2, 180, SF_DARK, 2)
    _arrow_right(d, 280, medal_y + medal_h/2, 315, SF_DARK, 2)

    _label(d, 95, medal_y - 10, "OBJECT_CONSTRUCT(*)", font_size=5.5, color=BRONZE_COLOR)
    _label(d, 230, medal_y - 10, "LLM + ROW_NUMBER()", font_size=5.5, color=SILVER_COLOR)
    _label(d, 365, medal_y - 10, "LLM Multi-Source", font_size=5.5, color=GOLD_COLOR)

    support_y = db_y + 15
    sw, sh = 92, 55
    gap = 10
    x = 30
    for label, sub, color in [
        ("METADATA", "Contracts\nDirectives\nLearnings", META_COLOR),
        ("AGENTS", "27+ SPs\nWorkflow\nEngine", AGENT_COLOR),
        ("KNOWLEDGE\nGRAPH", "200+ nodes\nLineage", KG_COLOR),
        ("SEMANTIC\nVIEWS", "Cortex\nAnalyst", SV_COLOR),
    ]:
        _box(d, x, support_y, sw, sh, color, label.split('\n'), font_size=6.5)
        lines = sub.split('\n')
        for i, ln in enumerate(lines):
            _label(d, x + sw/2, support_y + sh - 30 - i*8, ln, font_size=5.5, color=white)
        x += sw + gap

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 2: Nine-Stage Pipeline
# ─────────────────────────────────────────────────────────────
def diagram_nine_stage_pipeline():
    W, H = 460, 195
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    stages_top = [
        ("1", "PG Source", SF_DARK),
        ("2", "PG Landing", SF_DARK),
        ("3", "SF Landing", SF_BLUE),
        ("4", "BRONZE", BRONZE_COLOR),
        ("5", "SILVER", SILVER_COLOR),
    ]
    stages_bot = [
        ("6", "GOLD", GOLD_COLOR),
        ("7", "Knowledge\nGraph", KG_COLOR),
        ("8", "Semantic\nViews", SV_COLOR),
        ("9", "AI Chat", SF_GREEN),
    ]

    bw, bh = 76, 42
    y1 = H - 60
    x = 15
    for num, label, color in stages_top:
        _box(d, x, y1, bw, bh, color, [f"Stage {num}", label], font_size=6.5)
        if x > 15:
            pass
        x += bw + 8
    for i in range(len(stages_top) - 1):
        x1 = 15 + (bw + 8) * i + bw
        x2 = 15 + (bw + 8) * (i + 1)
        _arrow_right(d, x1, y1 + bh/2, x2, SF_GRAY)

    y2 = H - 130
    x_start = 15 + (bw + 8) * 1
    x = x_start
    for num, label, color in stages_bot:
        lines = [f"Stage {num}"] + label.split('\n')
        _box(d, x, y2, bw, bh, color, lines, font_size=6.5)
        x += bw + 8
    for i in range(len(stages_bot) - 1):
        x1 = x_start + (bw + 8) * i + bw
        x2 = x_start + (bw + 8) * (i + 1)
        _arrow_right(d, x1, y2 + bh/2, x2, SF_GRAY)

    conn_x = 15 + (bw + 8) * 4 + bw/2
    _arrow_down(d, conn_x, y1, y2 + bh, SF_GRAY)

    _label(d, W/2, 12, "Each stage is autonomous \u2014 once Bronze exists, the workflow generates everything downstream", font_size=6, color=SF_ACCENT, bold=True)

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 3: CDC Replication Architecture
# ─────────────────────────────────────────────────────────────
def diagram_cdc_replication():
    W, H = 460, 155
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    bw, bh = 125, 75
    y = H/2 - bh/2

    _box(d, 15, y, bw, bh, SF_DARK, ["SOURCE_PG", "", "customers", "products", "orders", "order_items", "support_tickets"], font_size=6, text_color=white)
    _box(d, 168, y, bw, bh, HexColor("#2C5282"), ["LANDING_PG", "", "Mirror tables", "dbaontap_sub", "(Subscription)"], font_size=6, text_color=white)
    _box(d, 320, y, bw, bh, SF_BLUE, ['Snowflake "public"', "", "5 CDC tables", "+_SNOWFLAKE_DELETED", "+_SNOWFLAKE_", "  UPDATED_AT"], font_size=6, text_color=white)

    _arrow_right(d, 140, y + bh/2, 168, SF_GRAY, 2)
    _arrow_right(d, 293, y + bh/2, 320, SF_GRAY, 2)
    _label(d, 154, y + bh/2 + 8, "Logical Repl.", font_size=5.5, color=SF_DARK, bold=True)
    _label(d, 307, y + bh/2 + 8, "Openflow CDC", font_size=5.5, color=SF_DARK, bold=True)

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 4: 5-Phase Agentic Workflow
# ─────────────────────────────────────────────────────────────
def diagram_five_phase_workflow():
    W, H = 460, 200
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    phases = [
        ("PHASE 1", "TRIGGER", "Detect events:\nnew table,\nschema change,\nquality breach", HexColor("#3498DB")),
        ("PHASE 2", "PLANNER", "LLM analyzes\nschema + quality\n+ learnings\n+ directives", HexColor("#2ECC71")),
        ("PHASE 3", "EXECUTOR", "LLM generates\nDDL, 3-retry\nself-correction\nloop", HexColor("#E67E22")),
        ("PHASE 4", "VALIDATOR", "Row count\ncomparison\n(\u00b15% tolerance)\nSchema check", HexColor("#E74C3C")),
        ("PHASE 5", "REFLECTOR", "LLM extracts\nlearnings,\nMERGE into\nWORKFLOW_LEARNINGS", HexColor("#9B59B6")),
    ]

    bw, bh = 78, 95
    y = H - 115
    x = 10
    for phase_num, name, desc, color in phases:
        _box(d, x, y, bw, bh, color, [phase_num, name], font_size=7)
        lines = desc.split('\n')
        for i, ln in enumerate(lines):
            _label(d, x + bw/2, y + bh - 32 - i * 8.5, ln, font_size=5.5, color=white)
        if x > 10:
            pass
        x += bw + 5

    for i in range(4):
        x1 = 10 + (bw + 5) * i + bw
        x2 = 10 + (bw + 5) * (i + 1)
        _arrow_right(d, x1, y + bh/2, x2, SF_DARK, 2)

    tables = ["WORKFLOW_\nEXECUTIONS", "PLANNER_\nDECISIONS", "TRANSFORMATION\n_LOG", "VALIDATION_\nRESULTS", "WORKFLOW_\nLEARNINGS"]
    x = 10
    ty = y - 50
    for tbl in tables:
        lines = tbl.split('\n')
        for j, ln in enumerate(lines):
            _label(d, x + bw/2, ty - j*8, ln, font_size=5, color=SF_ACCENT, bold=True)
        _arrow_down(d, x + bw/2, y, ty + 14, HexColor("#AABBCC"), 1)
        x += bw + 5

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 5: Gold Build Algorithm
# ─────────────────────────────────────────────────────────────
def diagram_gold_build():
    W, H = 460, 220
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    _box(d, 130, H - 40, 200, 30, GOLD_COLOR, ["BUILD_GOLD_FOR_NEW_TABLES"], font_size=7.5)
    _arrow_down(d, 180, H - 40, H - 65, SF_GRAY)
    _arrow_down(d, 280, H - 40, H - 65, SF_GRAY)

    _box(d, 30, H - 105, 180, 35, HexColor("#3498DB"), ["Strategy 1: Missing Gold Targets", "from TABLE_LINEAGE_MAP"], font_size=6.5)
    _box(d, 250, H - 105, 180, 35, HexColor("#2ECC71"), ["Strategy 2: Uncovered Silver", "Tables with no mapping"], font_size=6.5)

    _arrow_down(d, 120, H - 105, H - 125, SF_GRAY)
    _arrow_down(d, 340, H - 105, H - 125, SF_GRAY)

    steps_y = 20
    steps = [
        ("Gather Silver\ncolumn info", HexColor("#5DADE2")),
        ("Fetch\nDirectives", META_COLOR),
        ("LLM \u2192 Claude\n3.5 Sonnet", HexColor("#E67E22")),
        ("VALIDATE_\nGOLD_DDL", HexColor("#E74C3C")),
        ("Execute\n(3 retries)", GOLD_COLOR),
        ("Register\nLineage", KG_COLOR),
    ]
    sx = 15
    sw = 65
    for label, color in steps:
        lines = label.split('\n')
        _box(d, sx, steps_y, sw, 35, color, lines, font_size=6)
        if sx > 15:
            _arrow_right(d, sx - 5, steps_y + 17, sx, SF_GRAY, 1)
        sx += sw + 8

    _label(d, W/2, steps_y + 50, "For each work item:", font_size=6.5, color=SF_DARK, bold=True)

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 6: Three-Layer Control Model
# ─────────────────────────────────────────────────────────────
def diagram_three_layer_control():
    W, H = 460, 140
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    bw, bh = 135, 100
    y = H/2 - bh/2
    configs = [
        ("SCHEMA\nCONTRACTS", "(Structure)", '"What must the\noutput look like?"', "Column names,\ntypes, required\nfields", SF_BLUE),
        ("TRANSFORMATION\nDIRECTIVES", "(Intent)", '"Why does this\ndata exist?"', "Business purpose,\ngranularity,\nuse case", SF_GREEN),
        ("LEARNINGS", "(Memory)", '"What has worked\nin the past?"', "Success/failure\npatterns,\nconfidence scores", SF_PURPLE),
    ]
    x = 15
    for title, subtitle, question, details, color in configs:
        _box(d, x, y, bw, bh, color, title.split('\n'), font_size=7.5)
        _label(d, x + bw/2, y + bh - 30, subtitle, font_size=6, color=HexColor("#DDDDDD"))
        q_lines = question.replace('"', '').split('\n')
        for i, ql in enumerate(q_lines):
            _label(d, x + bw/2, y + bh - 42 - i*9, ql, font_size=5.5, color=white)
        d_lines = details.split('\n')
        for i, dl in enumerate(d_lines):
            _label(d, x + bw/2, y + 25 - i*8, dl, font_size=5.5, color=HexColor("#CCCCCC"))
        x += bw + 12

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 7: TABLE_LINEAGE_MAP Dual Population
# ─────────────────────────────────────────────────────────────
def diagram_lineage_map():
    W, H = 460, 185
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    _label(d, W/2, H - 15, "TABLE_LINEAGE_MAP (Dual Population Model)", font_size=8.5, color=SF_DARK, bold=True)

    bw, bh = 185, 55
    y_seed = H - 85
    _box(d, 25, y_seed, bw, bh, SF_BLUE, ["Human-Seeded Entries", "", "Architectural intent", "Bronze\u2192Silver, Silver\u2192Gold"], font_size=6.5)
    _box(d, 250, y_seed, bw, bh, SF_GREEN, ["Agent-Populated Entries", "", "Auto-registered via MERGE", "REGISTER_LINEAGE_FROM_DDL"], font_size=6.5)

    consumer_y = 15
    consumers = [
        ("KG Population", KG_COLOR),
        ("Gold Discovery", GOLD_COLOR),
        ("Drift Detection", SF_ORANGE),
        ("KG Visualization", SV_COLOR),
    ]
    cx = 30
    cw = 92
    for label, color in consumers:
        _box(d, cx, consumer_y, cw, 28, color, [label], font_size=6.5)
        cx += cw + 10

    _label(d, W/2, consumer_y + 38, "Consumers:", font_size=6, color=SF_ACCENT, bold=True)

    return DrawingFlowable(d)


# ─────────────────────────────────────────────────────────────
# DIAGRAM 8: Script Dependency Tree
# ─────────────────────────────────────────────────────────────
def diagram_script_dependency():
    W, H = 460, 250
    d = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=HexColor("#F8FAFC"), strokeColor=HexColor("#D0D8E0"), strokeWidth=0.5, rx=6, ry=6))

    _label(d, W/2, H - 12, "Script Dependency Tree", font_size=8.5, color=SF_DARK, bold=True)

    bw, bh = 82, 22
    def sbox(x, y, label, color):
        _box(d, x, y, bw, bh, color, [label], font_size=5.5)

    y0 = H - 45
    sbox(10, y0, "01_source", SF_DARK)
    sbox(110, y0, "02_landing", SF_DARK)
    sbox(210, y0, "04_openflow", SF_BLUE)
    _arrow_right(d, 92, y0 + bh/2, 110, SF_GRAY, 1)
    _arrow_right(d, 192, y0 + bh/2, 210, SF_GRAY, 1)

    y1 = y0 - 35
    sbox(170, y1, "05_bronze", BRONZE_COLOR)
    sbox(280, y1, "08_agents", AGENT_COLOR)
    _arrow_down(d, 251, y0, y1 + bh, SF_GRAY, 1)

    y2 = y1 - 35
    sbox(130, y2, "06_silver", SILVER_COLOR)
    sbox(280, y2, "07_workflow", HexColor("#E67E22"))
    _arrow_down(d, 211, y1, y2 + bh, SF_GRAY, 1)

    y3 = y2 - 35
    sbox(90, y3, "07_gold", GOLD_COLOR)
    sbox(200, y3, "08_decision", HexColor("#3498DB"))
    sbox(320, y3, "13_directives", META_COLOR)
    _arrow_down(d, 171, y2, y3 + bh, SF_GRAY, 1)

    y4 = y3 - 35
    sbox(50, y4, "09_semantic", SV_COLOR)
    sbox(160, y4, "15_lineage_map", KG_COLOR)
    sbox(280, y4, "14_ddl_valid", HexColor("#E74C3C"))
    _arrow_down(d, 131, y3, y4 + bh, SF_GRAY, 1)

    y5 = y4 - 35
    sbox(10, y5, "10_intelligence", SF_GREEN)
    sbox(120, y5, "09_kg", KG_COLOR)
    sbox(230, y5, "16_register", AGENT_COLOR)
    sbox(350, y5, "17_gold_exec", GOLD_COLOR)
    _arrow_down(d, 91, y4, y5 + bh, SF_GRAY, 1)
    _arrow_down(d, 201, y4, y5 + bh, SF_GRAY, 1)
    _arrow_down(d, 321, y4, y5 + bh, SF_GRAY, 1)

    y6 = y5 - 30
    sbox(310, y6, "18_build_gold", GOLD_COLOR)
    _arrow_down(d, 391, y5, y6 + bh, SF_GRAY, 1)

    return DrawingFlowable(d)
