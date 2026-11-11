"""Build ADPULSE_PRESENTATION_HE.pptx (Hebrew, 20 slides, speaker notes) with python-pptx.

    pip install python-pptx==1.0.2
    python presentation/build_presentation.py

Free and offline: no Microsoft Office or online service is needed.
Numbers on the charts come from demo_data/ (a real run of the pipeline).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Emu, Inches, Pt

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "demo_data"
OUT = ROOT / "ADPULSE_PRESENTATION_HE.pptx"

# Palette: the dashboard's dark theme (navy surfaces, blue/violet accents, orange for warnings)
BG = RGBColor(0x11, 0x12, 0x1F)
CARD = RGBColor(0x1B, 0x1C, 0x30)
CARD2 = RGBColor(0x25, 0x27, 0x45)
TEXT = RGBColor(0xF0, 0xEF, 0xEC)
MUTED = RGBColor(0xC3, 0xC2, 0xB7)
BLUE = RGBColor(0x39, 0x87, 0xE5)
VIOLET = RGBColor(0x90, 0x85, 0xE9)
AQUA = RGBColor(0x19, 0x9E, 0x70)
ORANGE = RGBColor(0xD9, 0x59, 0x26)
YELLOW = RGBColor(0xC9, 0x85, 0x00)
FONT = "Arial"

W, H = Inches(13.333), Inches(7.5)


def load(name: str):
    return json.loads((DEMO / f"{name}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- helpers
_HEB = re.compile("[\u0590-\u05FF]")
RLI, PDI = "\u2067", "\u2069"


def iso(value: str) -> str:
    """Wrap Hebrew text in a right-to-left isolate (RLI ... PDI).

    Viewers that ignore the paragraph's rtl flag would otherwise start a line that
    begins with an English term (e.g. "Kafka ...") left-to-right and scramble it."""
    if not _HEB.search(value):
        return value
    return "\n".join(RLI + line + PDI if _HEB.search(line) else line for line in value.split("\n"))


def _rtl(paragraph, rtl: bool) -> None:
    p_pr = paragraph._p.get_or_add_pPr()
    if rtl:
        p_pr.set("rtl", "1")


def text(slide, x, y, w, h, runs, size=16, color=TEXT, bold=False, align="r", rtl=True,
         anchor=MSO_ANCHOR.TOP, line_spacing=1.1):
    """Add a text box. ``runs`` is a string, or a list of paragraphs; a paragraph is a
    string or a list of (text, {size,color,bold}) tuples."""
    box = slide.shapes.add_textbox(x, y, w, h)
    tf = box.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.05)
    tf.margin_top = tf.margin_bottom = Inches(0.03)
    paragraphs = [runs] if isinstance(runs, str) else runs
    for i, para in enumerate(paragraphs):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = {"r": PP_ALIGN.RIGHT, "l": PP_ALIGN.LEFT, "c": PP_ALIGN.CENTER}[align]
        p.line_spacing = line_spacing
        _rtl(p, rtl)
        parts = [(para, {})] if isinstance(para, str) else list(para)
        if rtl and _HEB.search("".join(t for t, _ in parts)):
            parts[0] = (RLI + parts[0][0], parts[0][1])
            parts[-1] = (parts[-1][0] + PDI, parts[-1][1])
        for part, opts in parts:
            r = p.add_run()
            r.text = part
            f = r.font
            f.name = FONT
            f.size = Pt(opts.get("size", size))
            f.bold = opts.get("bold", bold)
            f.color.rgb = opts.get("color", color)
            r._r.get_or_add_rPr().set("lang", "he-IL")
    return box


def shape(slide, kind, x, y, w, h, fill=CARD, line=None, radius=0.12):
    s = slide.shapes.add_shape(kind, x, y, w, h)
    s.fill.solid()
    s.fill.fore_color.rgb = fill
    if line is None:
        s.line.fill.background()
    else:
        s.line.color.rgb = line
        s.line.width = Pt(1.25)
    s.shadow.inherit = False
    if kind == MSO_SHAPE.ROUNDED_RECTANGLE:
        s.adjustments[0] = radius
    return s


def card(slide, x, y, w, h, title, body, accent=BLUE, title_size=18, body_size=14, badge=None):
    shape(slide, MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h, CARD)
    top = y + Inches(0.2)
    if badge:
        c = shape(slide, MSO_SHAPE.OVAL, x + w - Inches(0.75), top, Inches(0.5), Inches(0.5), accent)
        tf = c.text_frame
        tf.margin_left = tf.margin_right = 0
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = badge
        r.font.name, r.font.size, r.font.bold, r.font.color.rgb = FONT, Pt(16), True, TEXT
        text(slide, x + Inches(0.2), top + Inches(0.02), w - Inches(1.05), Inches(0.5), title,
             size=title_size, bold=True, anchor=MSO_ANCHOR.MIDDLE)
    else:
        text(slide, x + Inches(0.2), top, w - Inches(0.4), Inches(0.5), title, size=title_size, bold=True,
             color=accent)
    text(slide, x + Inches(0.2), top + Inches(0.6), w - Inches(0.4), h - Inches(0.85), body, size=body_size,
         color=MUTED)


def node(slide, x, y, w, h, label, sub=None, fill=CARD2, line=BLUE, size=15):
    """Diagram box with an English component name (left-to-right text)."""
    s = shape(slide, MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h, fill, line)
    tf = s.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf.margin_left = tf.margin_right = Inches(0.06)
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = iso(label)
    _rtl(p, bool(_HEB.search(label)))
    r.font.name, r.font.size, r.font.bold, r.font.color.rgb = FONT, Pt(size), True, TEXT
    if sub:
        p2 = tf.add_paragraph()
        p2.alignment = PP_ALIGN.CENTER
        r2 = p2.add_run()
        r2.text = iso(sub)
        _rtl(p2, bool(_HEB.search(sub)))
        r2.font.name, r2.font.size, r2.font.color.rgb = FONT, Pt(size - 4), MUTED
    return s


def arrow(slide, x1, y1, x2, y2, color=MUTED, width=2.25):
    """Straight connector with an arrowhead at the end."""
    c = slide.shapes.add_connector(1, x1, y1, x2, y2)  # 1 = straight
    c.line.color.rgb = color
    c.line.width = Pt(width)
    ln = c.line._get_or_add_ln()
    tail = ln.makeelement(qn("a:tailEnd"), {"type": "triangle", "w": "med", "len": "med"})
    ln.append(tail)
    return c


def h_arrow(slide, left_shape, right_shape, color=MUTED):
    y = left_shape.top + left_shape.height // 2
    return arrow(slide, left_shape.left + left_shape.width, y, right_shape.left, y, color)


def v_arrow(slide, top_shape, bottom_shape, color=MUTED):
    x = top_shape.left + top_shape.width // 2
    return arrow(slide, x, top_shape.top + top_shape.height, x, bottom_shape.top, color)


def table(slide, x, y, w, rows, col_widths, header_fill=CARD2, size=13, row_h=0.42, rtl=True):
    n_rows, n_cols = len(rows), len(rows[0])
    gt = slide.shapes.add_table(n_rows, n_cols, x, y, w, Inches(row_h * n_rows))
    tbl = gt.table
    tbl_pr = gt._element.graphic.graphicData.tbl.tblPr
    if rtl:
        tbl_pr.set("rtl", "1")
    tbl_pr.set("bandRow", "0")
    tbl_pr.set("firstRow", "1")
    for j, cw in enumerate(col_widths):
        tbl.columns[j].width = Inches(cw)
    for i, row in enumerate(rows):
        tbl.rows[i].height = Inches(row_h)
        for j, value in enumerate(row):
            cell = tbl.cell(i, j)
            cell.fill.solid()
            cell.fill.fore_color.rgb = header_fill if i == 0 else (CARD if i % 2 else BG)
            cell.margin_left = cell.margin_right = Inches(0.08)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            tf = cell.text_frame
            tf.word_wrap = True
            p = tf.paragraphs[0]
            p.alignment = PP_ALIGN.RIGHT if rtl else PP_ALIGN.LEFT
            _rtl(p, rtl)
            r = p.add_run()
            r.text = iso(str(value)) if rtl else str(value)
            r.font.name, r.font.size = FONT, Pt(size)
            r.font.bold = i == 0
            r.font.color.rgb = TEXT if i == 0 else MUTED
    return gt


def stat(slide, x, y, w, value, label, color=BLUE):
    text(slide, x, y, w, Inches(0.9), value, size=44, bold=True, color=color, align="c", rtl=False)
    text(slide, x, y + Inches(0.85), w, Inches(0.5), label, size=14, color=MUTED, align="c")


class Deck:
    def __init__(self) -> None:
        self.prs = Presentation()
        self.prs.slide_width, self.prs.slide_height = W, H
        self.blank = self.prs.slide_layouts[6]
        self.count = 0

    def slide(self, title: str | None, notes: str, kicker: str | None = None):
        s = self.prs.slides.add_slide(self.blank)
        bg = s.background.fill
        bg.solid()
        bg.fore_color.rgb = BG
        self.count += 1
        if title:
            if kicker:
                text(s, Inches(0.6), Inches(0.35), Inches(12.1), Inches(0.4), kicker, size=13, color=VIOLET,
                     bold=True)
            text(s, Inches(0.6), Inches(0.68), Inches(12.1), Inches(0.8), title, size=34, bold=True)
        text(s, Inches(0.4), Inches(7.0), Inches(2.0), Inches(0.35), f"{self.count}", size=11, color=MUTED,
             align="l", rtl=False)
        text(s, Inches(9.9), Inches(7.0), Inches(3.0), Inches(0.35), "AdPulse · פרויקט לימודי אישי", size=11,
             color=MUTED)
        s.notes_slide.notes_text_frame.text = iso(notes)
        for p in s.notes_slide.notes_text_frame.paragraphs:
            _rtl(p, True)
            p.alignment = PP_ALIGN.RIGHT
        return s

    def save(self) -> None:
        self.prs.save(OUT)


# --------------------------------------------------------------------------- slides
def build() -> None:
    summary = load("summary")
    campaigns = load("campaigns")
    suspicious = load("suspicious")
    d = Deck()

    # 1 ── Title / What is AdPulse
    s = d.slide(None, "שלום, אני אציג פרויקט שבניתי לבד כדי ללמוד הנדסת נתונים בזמן אמת. AdPulse מדמה מה שקורה "
                      "בחברת פרסום: מיליוני אירועים של חשיפות, קליקים ורכישות, שצריך לקלוט, לנקות, לספור ולהציג. "
                      "הכול חינמי, רץ אצלי במחשב ב-Docker, והנתונים סינתטיים לגמרי. חשוב לי להגיד: זה פרויקט לימודי – "
                      "את Kafka ו-Spark למדתי דרכו, והניסיון המקצועי שלי הוא ב-SQL, ETL ו-DWH.")
    text(s, Inches(0.8), Inches(1.4), Inches(11.7), Inches(0.5), "מה זה AdPulse?", size=20, color=VIOLET, bold=True)
    text(s, Inches(0.8), Inches(1.9), Inches(11.7), Inches(1.3), "AdPulse", size=80, bold=True, align="r",
         rtl=False)
    text(s, Inches(0.8), Inches(3.25), Inches(11.7), Inches(0.7), "פלטפורמת אנליטיקה לפרסום דיגיטלי – בזמן אמת",
         size=30, bold=True)
    text(s, Inches(0.8), Inches(4.0), Inches(11.7), Inches(0.9),
         "מערכת שמקבלת אירועי פרסום, מנקה אותם, מחשבת מדדים, מזהה הונאה ומציגה הכול בדשבורד", size=18,
         color=MUTED)
    chips = ["Python", "Kafka", "Spark", "Docker", "S3 / Parquet", "PostgreSQL", "FastAPI", "Streamlit"]
    x = Inches(12.5)
    for chip in chips:
        w_chip = Inches(0.25 + 0.115 * len(chip))
        x -= w_chip
        c = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, Inches(5.25), w_chip, Inches(0.5), CARD2, VIOLET, 0.5)
        tf = c.text_frame
        tf.margin_left = tf.margin_right = 0
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = iso(chip)
        r.font.name, r.font.size, r.font.color.rgb, r.font.bold = FONT, Pt(13), TEXT, True
        x -= Inches(0.15)
    text(s, Inches(0.8), Inches(6.1), Inches(11.7), Inches(0.5),
         "100% חינמי · רץ מקומית · נתונים סינתטיים בלבד", size=15, color=AQUA, bold=True)

    # 2 ── The problem
    s = d.slide("איזו בעיה אנחנו פותרים?",
                "חברת פרסום חיה ממספרים. ארבע בעיות: היקף – מיליארדי אירועים ביום, מחשב אחד לא יעמוד בזה. "
                "כפילויות – אם קליק נספר פעמיים, המפרסם משלם פעמיים. הונאה – בוטים שלוחצים כדי לגנוב תקציב. "
                "וזמן אמת – מפרסם רוצה לראות עכשיו שהקמפיין לא עובד, לא מחר בבוקר. AdPulse מטפל בכל הארבע.",
                kicker="הבעיה")
    items = [("1", "היקף עצום", "מיליארדי חשיפות ביום. מחשב אחד לא יכול לעבד את זה לבד.", BLUE),
             ("2", "כפילויות ושגיאות", "רשת לא יציבה שולחת אירועים פעמיים, או שבורים. כל שגיאה = כסף.", VIOLET),
             ("3", "הונאת קליקים", "בוט שלוחץ 20 פעם בדקה גונב את התקציב של המפרסם.", ORANGE),
             ("4", "צריך תשובה עכשיו", "מפרסם רוצה לעצור קמפיין כושל תוך דקות – לא מחר.", AQUA)]
    for i, (badge, title, body, color) in enumerate(items):
        col, row = i % 2, i // 2
        card(s, Inches(6.85 - col * 6.25), Inches(1.75 + row * 2.55), Inches(5.85), Inches(2.25), title, body,
             color, badge=badge, title_size=20, body_size=16)

    # 3 ── How online advertising works
    s = d.slide("איך עובד פרסום באינטרנט?",
                "ארבעה שחקנים. המפרסם – למשל חברת נעליים – משלם לפלטפורמה. הפלטפורמה, כמו Taboola, מחליטה איזו "
                "מודעה להציג ואיפה. האתר – למשל אתר חדשות – מציג את המודעה ומקבל חלק מהכסף. והגולש רואה, לוחץ, "
                "ולפעמים קונה. כל פעולה של הגולש היא אירוע שחוזר לפלטפורמה – וזה בדיוק הנתונים שהמערכת שלי מעבדת.",
                kicker="רקע עסקי")
    labels = [("Advertiser", "המפרסם · משלם", BLUE), ("Ad Platform", "בוחרת מודעה וסופרת", VIOLET),
              ("Publisher", "אתר תוכן · מציג", AQUA), ("User", "גולש · רואה ולוחץ", YELLOW)]
    boxes = []
    for i, (lab, sub, color) in enumerate(labels):
        boxes.append(node(s, Inches(0.7 + i * 3.2), Inches(2.4), Inches(2.5), Inches(1.4), lab, sub, line=color,
                          size=20))
    for a, b in zip(boxes, boxes[1:]):
        h_arrow(s, a, b)
    text(s, Inches(0.7), Inches(4.3), Inches(12.0), Inches(0.5), "חזרה לפלטפורמה: impression · click · conversion",
         size=18, color=VIOLET, bold=True, align="c")
    arrow(s, boxes[3].left + boxes[3].width // 2, Inches(4.0), boxes[1].left + boxes[1].width // 2, Inches(4.0),
          VIOLET)
    card(s, Inches(6.85), Inches(5.0), Inches(5.8), Inches(1.75), "הכסף",
         "המפרסם משלם לפי קליק (CPC). האתר מקבל חלק מההכנסה.", BLUE, body_size=15)
    card(s, Inches(0.7), Inches(5.0), Inches(5.8), Inches(1.75), "הנתונים",
         "כל חשיפה, קליק והמרה נשלחים כאירוע JSON – זה הקלט של AdPulse.", VIOLET, body_size=15)

    # 4 ── Impression
    s = d.slide("מה זו חשיפה (Impression)?",
                "חשיפה היא הרגע שבו המודעה הופיעה על המסך. זה האירוע הכי נפוץ – בפרויקט שלי בערך 40 בשנייה. "
                "כך נראה אירוע אמיתי מהמערכת: מזהה ייחודי, סוג, זמן האירוע, קמפיין, אתר, ומשתמש מומצא. "
                "שימו לב ל-event_id – בזכותו אפשר לזהות כפילויות – ול-event_time, שהוא הזמן שבו זה קרה באמת, "
                "לא הזמן שבו הגיע לשרת.", kicker="המשפך · שלב 1")
    text(s, Inches(6.9), Inches(1.8), Inches(5.8), Inches(2.6),
         ["המודעה הוצגה על המסך של הגולש.", "", "האירוע הנפוץ ביותר – בפרויקט ~40 בשנייה.", "",
          "בכל אירוע יש מזהה ייחודי (event_id) וזמן אמיתי (event_time)."], size=19)
    stat(s, Inches(7.8), Inches(4.7), Inches(4.0), f"{summary['impressions']:,}", "חשיפות ב-snapshot של הדמו")
    code = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.7), Inches(1.8), Inches(5.8), Inches(4.8), CARD, BLUE,
                 0.05)
    sample = ('{\n  "event_id": "753168f4-…",\n  "event_type": "impression",\n  "event_time": "2026-11-10T22:07:01Z",\n'
              '  "campaign_id": "cmp_003",\n  "publisher_id": "pub_02",\n  "user_id": "u_000042",\n'
              '  "device_type": "mobile",\n  "ip_address": "10.25.228.81",\n  "cost_usd": 0.0021\n}')
    text(s, Inches(0.95), Inches(2.0), Inches(5.4), Inches(4.4), sample.split("\n"), size=15, color=TEXT,
         align="l", rtl=False)

    # 5 ── Click
    ctr = summary["ctr_pct"]
    s = d.slide("מה זה קליק (Click)?",
                f"קליק זה כשהגולש לחץ על המודעה. היחס בין קליקים לחשיפות נקרא CTR. אם 1,000 ראו ו-30 לחצו, "
                f"ה-CTR הוא 3%. בדמו שלי ה-CTR הכולל יצא {ctr}%. הקליק מצביע על החשיפה שלו דרך parent_event_id, "
                "כך שאפשר לעקוב אחרי המסלול של הגולש.", kicker="המשפך · שלב 2")
    widths = [(1000, "1,000 חשיפות", BLUE), (30, "30 קליקים", VIOLET)]
    for i, (_, lab, color) in enumerate(widths):
        bw = Inches(11.5) if i == 0 else Inches(11.5 * 0.18)
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(12.6) - bw, Inches(1.9 + i * 1.1), bw, Inches(0.85), color)
        text(s, Inches(0.8), Inches(1.95 + i * 1.1), Inches(11.6), Inches(0.75), lab, size=22, bold=True,
             anchor=MSO_ANCHOR.MIDDLE)
    formula = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(3.4), Inches(4.4), Inches(6.5), Inches(1.4), CARD,
                    VIOLET)
    text(s, Inches(3.5), Inches(4.5), Inches(6.3), Inches(1.2),
         [[("CTR = Clicks ÷ Impressions × 100", {"size": 26, "bold": True})],
          [("30 ÷ 1,000 × 100 = 3%", {"size": 20, "color": MUTED})]], align="c", rtl=False)
    text(s, Inches(0.8), Inches(6.1), Inches(11.8), Inches(0.5),
         "CTR עונה על השאלה: כמה המודעה מושכת?", size=18, color=VIOLET, bold=True, align="c")

    # 6 ── Conversion
    cvr = summary["conversion_rate_pct"]
    s = d.slide("מה זו המרה (Conversion)?",
                f"המרה היא כשמי שלחץ גם עשה את מה שהמפרסם רצה – קנה, נרשם, הוריד אפליקציה. Conversion Rate הוא "
                f"המרות חלקי קליקים. 3 מתוך 30 זה 10%. בדמו יצא {cvr}%. זה המדד שהכי מעניין את המפרסם, כי הוא "
                "מראה אם הקליקים שווים כסף.", kicker="המשפך · שלב 3")
    funnel = [("Impressions", "ראו את המודעה", "1,000", BLUE, 7.5),
              ("Clicks", "לחצו", "30", VIOLET, 5.0), ("Conversions", "קנו / נרשמו", "3", AQUA, 2.6)]
    for i, (en, he, num, color, wd) in enumerate(funnel):
        x0 = Inches(0.7 + (7.5 - wd) / 2)
        t = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x0, Inches(1.8 + i * 1.45), Inches(wd), Inches(1.2), color)
        tf = t.text_frame
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = f"{num}  {en}"
        r.font.name, r.font.size, r.font.bold, r.font.color.rgb = FONT, Pt(22), True, TEXT
        text(s, Inches(8.6), Inches(1.95 + i * 1.45), Inches(4.1), Inches(0.9), he, size=20, color=MUTED,
             anchor=MSO_ANCHOR.MIDDLE)
    text(s, Inches(0.7), Inches(6.2), Inches(12), Inches(0.6),
         [[("Conversion Rate = Conversions ÷ Clicks × 100 = 3 ÷ 30 × 100 = 10%", {"bold": True})]],
         size=20, align="c", rtl=False)

    # 7 ── Architecture
    s = d.slide("ארכיטקטורת המערכת",
                "זו התמונה המלאה, משמאל לימין. מחולל ב-Python מייצר אירועים ושולח ל-Kafka. Kafka שומר אותם ב-3 "
                "partitions. שני צרכנים קוראים במקביל: auditor שבודק תקינות ושולח זבל ל-DLQ, ו-Spark Structured "
                "Streaming שמנקה, מסיר כפילויות, מחשב חלונות של דקה ומזהה הונאה. Spark כותב את כל האירועים "
                "כ-Parquet ל-data lake תואם S3, ואת הסיכומים ל-PostgreSQL. FastAPI מגיש את הסיכומים, והדשבורד "
                "מציג אותם. כל קופסה כאן היא קונטיינר Docker.", kicker="תמונה גדולה")
    y_mid = Inches(3.0)
    g = node(s, Inches(0.4), y_mid, Inches(1.75), Inches(1.15), "Generator", "Python")
    k = node(s, Inches(2.65), y_mid, Inches(1.85), Inches(1.15), "Kafka", "ad-events · 3P", line=VIOLET)
    sp = node(s, Inches(5.0), y_mid, Inches(2.2), Inches(1.15), "Spark", "Structured Streaming", line=ORANGE)
    o = node(s, Inches(7.75), Inches(1.75), Inches(2.0), Inches(1.1), "S3 Data Lake", "Parquet", line=AQUA)
    p_ = node(s, Inches(7.75), Inches(4.25), Inches(2.0), Inches(1.1), "PostgreSQL", "aggregates", line=AQUA)
    f = node(s, Inches(10.25), Inches(4.25), Inches(1.3), Inches(1.1), "FastAPI", None, line=BLUE, size=14)
    dsh = node(s, Inches(11.75), Inches(4.25), Inches(1.25), Inches(1.1), "Streamlit", None, line=BLUE, size=14)
    a = node(s, Inches(2.65), Inches(5.2), Inches(1.85), Inches(1.0), "Auditor", "consumer group", line=MUTED,
             size=14)
    dl = node(s, Inches(0.4), Inches(5.2), Inches(1.75), Inches(1.0), "DLQ topic", "invalid", line=ORANGE, size=14)
    b = node(s, Inches(10.25), Inches(1.75), Inches(2.75), Inches(1.1), "Spark Batch", "RANK() window", line=ORANGE,
             size=14)
    h_arrow(s, g, k)
    h_arrow(s, k, sp)
    arrow(s, sp.left + sp.width, sp.top + Inches(0.3), o.left, o.top + o.height // 2)
    arrow(s, sp.left + sp.width, sp.top + sp.height - Inches(0.3), p_.left, p_.top + p_.height // 2)
    h_arrow(s, o, b)
    arrow(s, b.left + b.width // 2, b.top + b.height, f.left + f.width // 2, f.top, MUTED)
    h_arrow(s, p_, f)
    h_arrow(s, f, dsh)
    v_arrow(s, k, a)
    arrow(s, a.left, a.top + a.height // 2, dl.left + dl.width, dl.top + dl.height // 2)
    text(s, Inches(5.0), Inches(4.5), Inches(2.3), Inches(1.6),
         ["validate", "dedup", "join", "window + watermark", "fraud rules"], size=13, color=MUTED, align="c",
         rtl=False)

    # 8 ── What is Kafka
    s = d.slide("מה זה Kafka?",
                "Kafka הוא כמו סניף דואר ענק. מכתבים נכנסים למגירה עם שם – topic – והמגירה מחולקת לתאים – "
                "partitions. לכל מכתב יש מספר סידורי – offset. המכתבים לא נמחקים אחרי קריאה; הם נשמרים, אצלי יום "
                "שלם, כך שאם Spark נופל, שום דבר לא הולך לאיבוד. המפתח של כל הודעה הוא user_id, ולכן כל האירועים "
                "של אותו משתמש באותו תא ובסדר הנכון.", kicker="הודעות")
    text(s, Inches(0.7), Inches(1.65), Inches(12), Inches(0.5), "Topic: ad-events", size=18, bold=True, color=VIOLET,
         align="l", rtl=False)
    for pi in range(3):
        y0 = Inches(2.2 + pi * 0.95)
        text(s, Inches(0.7), y0, Inches(1.6), Inches(0.7), f"Partition {pi}", size=15, bold=True, align="l",
             rtl=False, anchor=MSO_ANCHOR.MIDDLE)
        for ci in range(9):
            cell = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(2.4 + ci * 0.82), y0, Inches(0.72), Inches(0.7),
                         CARD2 if ci < 8 else CARD, BLUE if ci < 8 else MUTED, 0.15)
            tf = cell.text_frame
            tf.margin_left = tf.margin_right = 0
            pp = tf.paragraphs[0]
            pp.alignment = PP_ALIGN.CENTER
            r = pp.add_run()
            r.text = str(ci) if ci < 8 else "…"
            r.font.name, r.font.size, r.font.color.rgb = FONT, Pt(14), TEXT
    text(s, Inches(2.4), Inches(5.1), Inches(7.3), Inches(0.4), "offset →  (הודעות חדשות נכתבות לסוף)", size=14,
         color=MUTED, align="l")
    text(s, Inches(10.0), Inches(2.2), Inches(2.7), Inches(3.0),
         [[("Topic", {"bold": True, "color": TEXT})], "מגירה עם שם", "",
          [("Partition", {"bold": True, "color": TEXT})], "תא – מאפשר עבודה במקביל", "",
          [("Offset", {"bold": True, "color": TEXT})], "מספר סידורי של הודעה"], size=15, color=MUTED)
    text(s, Inches(0.7), Inches(5.8), Inches(12), Inches(0.9),
         "המפתח (key) הוא user_id → כל האירועים של משתמש באותו partition → הסדר נשמר. ההודעות נשמרות 24 שעות.",
         size=17, color=TEXT)

    # 9 ── Producers & consumers
    s = d.slide("Producers ו-Consumers",
                "ה-Producer כותב. אצלי הוא מוגדר עם acks=all – מחכה לאישור שההודעה נשמרה – ועם idempotence, "
                "כדי ששליחה חוזרת לא תיצור כפילות בתוך Kafka. ה-Consumers מתארגנים בקבוצות. בתוך קבוצה, כל "
                "partition נקרא על ידי צרכן אחד, ולכן 3 partitions מאפשרים עד 3 צרכנים במקביל. קבוצות שונות קוראות "
                "את אותם נתונים בלי להפריע זו לזו – ה-auditor ו-Spark. ה-auditor עושה commit ל-offset רק אחרי "
                "שעיבד הודעה – זה at-least-once.", kicker="הודעות")
    prod = node(s, Inches(0.5), Inches(3.2), Inches(2.2), Inches(1.3), "Producer", "acks=all · idempotent")
    parts = [node(s, Inches(4.0), Inches(2.0 + i * 1.25), Inches(2.0), Inches(0.9), f"P{i}", None, line=VIOLET)
             for i in range(3)]
    for pt in parts:
        arrow(s, prod.left + prod.width, prod.top + prod.height // 2, pt.left, pt.top + pt.height // 2)
    grp = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.3), Inches(1.75), Inches(2.7), Inches(4.0), CARD, AQUA, 0.06)
    text(s, Inches(7.4), Inches(1.8), Inches(2.5), Inches(0.4), "group: adpulse-auditor", size=12, color=AQUA,
         align="c", rtl=False)
    cons = [node(s, Inches(7.6), Inches(2.3 + i * 1.1), Inches(2.1), Inches(0.8), f"Consumer {i + 1}", None,
                 line=AQUA, size=14) for i in range(3)]
    for pt, cn in zip(parts, cons):
        arrow(s, pt.left + pt.width, pt.top + pt.height // 2, cn.left, cn.top + cn.height // 2, AQUA)
    spk = node(s, Inches(10.5), Inches(3.2), Inches(2.3), Inches(1.3), "Spark", "קבוצה נפרדת · checkpoint",
               line=ORANGE)
    # Spark reads ALL partitions too: one line from the producer side bus, routed under the group box
    arrow(s, parts[2].left + parts[2].width // 2, parts[2].top + parts[2].height,
          parts[2].left + parts[2].width // 2, Inches(5.95), ORANGE)
    bus = s.shapes.add_connector(1, parts[2].left + parts[2].width // 2, Inches(5.95), Inches(11.65), Inches(5.95))
    bus.line.color.rgb, bus.line.width = ORANGE, Pt(2.25)
    arrow(s, Inches(11.65), Inches(5.95), Inches(11.65), spk.top + spk.height, ORANGE)
    text(s, Inches(7.3), Inches(6.0), Inches(4.2), Inches(0.4), "Spark קורא את כל ה-partitions", size=12,
         color=ORANGE, align="c")
    text(s, Inches(0.5), Inches(6.4), Inches(12.3), Inches(0.5),
         "בתוך קבוצה: partition אחד ↔ צרכן אחד. קבוצות שונות קוראות את אותם נתונים באופן עצמאי.", size=17,
         color=TEXT)

    # 10 ── What is Spark
    s = d.slide("מה זה Spark?",
                "Spark הוא מנוע חישוב מבוזר. יש Driver – המנהל – שמתכנן את העבודה, ו-Executors – הפועלים – שכל "
                "אחד מקבל חתיכה מהנתונים. במקום מחשב אחד שסופר מיליארד שורות, מאה מחשבים סופרים עשרה מיליון כל "
                "אחד. אצלי הכול רץ בקונטיינר אחד במצב local – thread לכל ליבה – אבל אותו קוד בדיוק היה רץ על "
                "קלאסטר. אני כותבת ב-PySpark, ממשק Python שמדבר עם Spark שרץ ב-Java.", kicker="עיבוד")
    drv = node(s, Inches(5.4), Inches(1.8), Inches(2.6), Inches(1.2), "Driver", "מתכנן את העבודה", line=VIOLET,
               size=18)
    exs = [node(s, Inches(1.2 + i * 2.85), Inches(4.0), Inches(2.4), Inches(1.15), f"Executor {i + 1}",
                f"partition {i + 1}", line=ORANGE) for i in range(4)]
    for ex in exs:
        arrow(s, drv.left + drv.width // 2, drv.top + drv.height, ex.left + ex.width // 2, ex.top)
    text(s, Inches(0.8), Inches(5.6), Inches(11.8), Inches(1.0),
         "100 מחשבים × 10 מיליון שורות במקום מחשב אחד × מיליארד. אצלי: local[*] = thread לכל ליבה – אותו קוד רץ "
         "גם על קלאסטר.", size=17)

    # 11 ── How Spark processes data
    s = d.slide("איך Spark מעבד הרבה נתונים?",
                "שני רעיונות. הראשון: Lazy Evaluation. כשאני כותבת filter, join או groupBy – Spark רק רושם תוכנית. "
                "שום דבר לא רץ עד ה-Action, כמו write או start. ככה הוא רואה את כל התוכנית ומייעל אותה. השני: "
                "Structured Streaming מתייחס לזרם כאל טבלה שגדלה כל הזמן, ומריץ את אותו קוד על כל micro-batch – "
                "אצלי כל 20 שניות. ה-checkpoint זוכר עד איפה הגענו, אז אחרי קריסה ממשיכים בדיוק מאותה נקודה.",
                kicker="עיבוד")
    steps = ["parse JSON", "validate", "join dims", "watermark", "dedup", "window 1m"]
    prev = None
    for i, st_ in enumerate(steps):
        n = node(s, Inches(0.5 + i * 1.83), Inches(2.0), Inches(1.55), Inches(0.95), st_, None, line=VIOLET, size=14)
        if prev:
            h_arrow(s, prev, n)
        prev = n
    act = node(s, Inches(11.48), Inches(2.0), Inches(1.4), Inches(0.95), ".start()", "ACTION", fill=ORANGE,
               line=ORANGE, size=14)
    h_arrow(s, prev, act)
    text(s, Inches(0.5), Inches(3.05), Inches(10.8), Inches(0.45), "Transformations – רק מתכננות (lazy)", size=15,
         color=VIOLET, bold=True, align="c")
    text(s, Inches(0.5), Inches(3.9), Inches(12.3), Inches(0.5), "Micro-batches: אותו קוד רץ על הנתונים החדשים כל 20 שניות",
         size=18, bold=True)
    for i in range(6):
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.5 + i * 2.07), Inches(4.6), Inches(1.85), Inches(0.75),
              CARD2 if i < 5 else CARD, BLUE)
        text(s, Inches(0.5 + i * 2.07), Inches(4.65), Inches(1.85), Inches(0.65), f"batch {i}" if i < 5 else "…",
             size=14, align="c", rtl=False, anchor=MSO_ANCHOR.MIDDLE)
    text(s, Inches(0.5), Inches(5.65), Inches(12.3), Inches(0.9),
         "Checkpoint שומר offsets ו-state → אחרי קריסה ממשיכים בדיוק מאותה נקודה, בלי לאבד ובלי לספור פעמיים.",
         size=16, color=MUTED)

    # 12 ── Docker
    s = d.slide("Docker וקונטיינרים",
                "קונטיינר הוא קופסה שבתוכה התוכנה וכל מה שהיא צריכה. במקום להתקין Kafka, Spark ו-Postgres על "
                "המחשב, יש 11 קונטיינרים שעולים בפקודה אחת: docker compose up. הם ברשת פרטית אחת ומוצאים זה את זה "
                "לפי שם. יש health checks – Spark לא מתחיל עד ש-Kafka באמת מוכן – ו-volumes ששומרים נתונים גם "
                "כשהקונטיינר נמחק. וסיסמאות מגיעות מקובץ .env שלא עולה ל-Git.", kicker="תשתית")
    net = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.5), Inches(1.7), Inches(7.6), Inches(4.9), CARD, VIOLET, 0.04)
    text(s, Inches(0.6), Inches(1.75), Inches(7.4), Inches(0.4), "network: adpulse-net", size=13, color=VIOLET,
         align="l", rtl=False)
    svcs = ["kafka", "kafka-init", "postgres", "seaweedfs", "storage-init", "generator", "auditor",
            "spark-streaming", "spark-batch", "api", "dashboard"]
    for i, sv in enumerate(svcs):
        col, row = i % 3, i // 3
        node(s, Inches(0.8 + col * 2.45), Inches(2.25 + row * 1.05), Inches(2.2), Inches(0.8), sv, None,
             line=BLUE, size=13)
    table(s, Inches(8.5), Inches(1.7), Inches(4.3),
          [["", "Container", "VM"], ["גודל", "MBs", "GBs"], ["עלייה", "שניות", "דקות"],
           ["מערכת הפעלה", "משותפת", "מלאה לכל VM"], ["בידוד", "טוב", "חזק יותר"]], [1.5, 1.4, 1.4], size=13,
          row_h=0.5)
    text(s, Inches(8.5), Inches(4.5), Inches(4.3), Inches(2.0),
         ["health checks · depends_on", "volumes – נתונים שורדים", "סודות מ-.env (לא ב-Git)",
          "פקודה אחת: docker compose up"], size=15, color=MUTED)

    # 13 ── Cloud storage
    s = d.slide("אחסון ענן: S3, Bucket ו-Parquet",
                "Object storage הוא מחסן ענק וזול. כל קובץ הוא object עם שם – key – בתוך bucket. אצלי ה-bucket נקרא "
                "adpulse-lake. במקום MinIO, שהפסיק לפרסם גרסה חינמית מתוחזקת, בחרתי SeaweedFS – קוד פתוח ותואם S3. "
                "הקוד מדבר S3 רגיל, אז אותו קוד עובד מול AWS S3 – רק משנים כתובת והרשאות. הקבצים נשמרים "
                "כ-Parquet – פורמט עמודתי ודחוס – ומחולקים לתיקיות לפי תאריך וסוג אירוע.", kicker="אחסון")
    bkt = shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.5), Inches(1.7), Inches(6.2), Inches(2.9), CARD, AQUA, 0.05)
    text(s, Inches(0.7), Inches(1.8), Inches(5.8), Inches(0.45), "bucket: adpulse-lake", size=16, bold=True,
         color=AQUA, align="l", rtl=False)
    text(s, Inches(0.7), Inches(2.3), Inches(5.9), Inches(2.6),
         ["events/", "  event_date=2026-11-10/", "    event_type=click/part-0000.parquet",
          "    event_type=impression/part-0001.parquet", "invalid_events/", "  ingest_date=2026-11-10/…"],
         size=14, color=TEXT, align="l", rtl=False)
    text(s, Inches(0.5), Inches(4.85), Inches(6.2), Inches(1.5),
         ["Parquet = עמודתי ודחוס: קוראים רק את העמודות הדרושות,", "ומדלגים על תיקיות לא רלוונטיות (partitioning)."],
         size=15, color=MUTED)
    table(s, Inches(7.1), Inches(1.7), Inches(5.7),
          [["אצלי (חינמי, מקומי)", "ב-AWS (תיאורטי)"], ["SeaweedFS", "Amazon S3"], ["Kafka container", "Amazon MSK"],
           ["Spark container", "EMR / Glue"], ["PostgreSQL container", "Amazon RDS"],
           ["מפתחות ב-.env", "IAM Role · least privilege"]], [2.85, 2.85], size=14, row_h=0.55)
    text(s, Inches(7.1), Inches(5.3), Inches(5.7), Inches(1.2),
         "לא פרסתי כלום ל-AWS: Free Tier אינו חינם ללא הגבלה, ו-MSK/EMR עולים כסף.", size=14, color=ORANGE)

    # 14 ── PostgreSQL
    s = d.slide("PostgreSQL – שכבת ההגשה",
                "ב-Postgres שמורים רק סיכומים, כדי שהדשבורד יקבל תשובה במילישניות. זה בעצם star schema קטן – "
                "מוכר לי מעולם ה-DWH: טבלת עובדות metrics_minute לפי דקה, קמפיין ואתר, ושתי טבלאות מימד. Spark "
                "כותב עם UPSERT – INSERT ON CONFLICT DO UPDATE, כמו MERGE. כיוון שהוא כותב את הערך המלא של "
                "החלון, אם batch רץ שוב אחרי קריסה – התוצאה זהה. זה נקרא אידמפוטנטי.", kicker="אחסון")
    fact = node(s, Inches(4.9), Inches(2.9), Inches(3.4), Inches(1.6), "metrics_minute",
                "window · campaign · publisher", line=AQUA, size=18)
    dims = [node(s, Inches(0.6), Inches(1.8), Inches(2.9), Inches(1.0), "campaigns", "dimension", line=BLUE),
            node(s, Inches(0.6), Inches(4.6), Inches(2.9), Inches(1.0), "publishers", "dimension", line=BLUE),
            node(s, Inches(9.7), Inches(1.8), Inches(3.0), Inches(1.0), "suspicious_activity", "fraud", line=ORANGE,
                 size=14),
            node(s, Inches(9.7), Inches(4.6), Inches(3.0), Inches(1.0), "duplicate_events", "quality", line=ORANGE,
                 size=14)]
    arrow(s, dims[0].left + dims[0].width, dims[0].top + dims[0].height // 2, fact.left, fact.top + Inches(0.3))
    arrow(s, dims[1].left + dims[1].width, dims[1].top + dims[1].height // 2, fact.left,
          fact.top + fact.height - Inches(0.3))
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.6), Inches(5.95), Inches(12.1), Inches(0.75), CARD)
    text(s, Inches(0.8), Inches(6.0), Inches(11.7), Inches(0.65),
         "INSERT … ON CONFLICT (window_start, campaign_id, publisher_id) DO UPDATE SET clicks = EXCLUDED.clicks",
         size=15, color=TEXT, align="l", rtl=False, anchor=MSO_ANCHOR.MIDDLE)

    # 15 ── Dashboard
    s = d.slide("הדשבורד",
                "זה הדשבורד ב-Streamlit. למעלה המדדים: חשיפות, קליקים, CTR, המרות, אחוז המרה, ומשתמשים חשודים. "
                "גרפים לפי דקה, CTR לפי קמפיין, ועוד עמודים לאתרים ולהונאה ואיכות נתונים. חשוב: הדשבורד לא ניגש "
                "למסד הנתונים – רק ל-FastAPI, כמו אפליקציה אמיתית. יש גם מצב דמו שקורא snapshot סינתטי, כדי "
                "שאפשר יהיה להציג אותו באינטרנט בחינם בלי Kafka ו-Spark.", kicker="הצגה")
    shot = ROOT / "docs" / "screenshots" / "dashboard_overview.png"
    if shot.exists():
        pic = s.shapes.add_picture(str(shot), Inches(0.6), Inches(1.65), height=Inches(5.15))
        pic.line.color.rgb = CARD2
        x_txt = pic.left + pic.width + Inches(0.3)
    else:
        x_txt = Inches(9.0)
    text(s, x_txt, Inches(1.8), W - x_txt - Inches(0.5), Inches(5.0),
         [[("Streamlit", {"bold": True, "color": TEXT})], "4 עמודים, רענון כל 10 שניות", "",
          [("FastAPI", {"bold": True, "color": TEXT})], "הדשבורד מדבר רק עם ה-API", "",
          [("מצב דמו", {"bold": True, "color": TEXT})], "snapshot סינתטי לאירוח חינמי"], size=15, color=MUTED)

    # 16 ── CTR calculation (native chart from demo data)
    s = d.slide("חישוב CTR לפי קמפיין",
                "הגרף הזה נבנה מהנתונים שהמערכת חישבה בפועל. Spark מקבץ אירועים לחלונות של דקה, לכל קמפיין ואתר, "
                "וסופר חשיפות, קליקים והמרות במעבר אחד – בטריק SUM של CASE WHEN שמוכר מ-SQL. ה-CTR עצמו מחושב "
                "ב-view של Postgres, עם NULLIF כדי שקמפיין חדש עם אפס חשיפות לא יגרום לחלוקה באפס.",
                kicker="מדדים")
    data = sorted(campaigns, key=lambda c: c["ctr_pct"] or 0)
    cd = CategoryChartData()
    cd.categories = [c["campaign_name"] for c in data]
    cd.add_series("CTR %", [round(c["ctr_pct"] or 0, 2) for c in data])
    gf = s.shapes.add_chart(XL_CHART_TYPE.BAR_CLUSTERED, Inches(0.5), Inches(1.6), Inches(7.6), Inches(5.2), cd)
    ch = gf.chart
    ch.has_legend = False
    ch.font.size, ch.font.name = Pt(12), FONT
    ch.font.color.rgb = MUTED
    plot = ch.plots[0]
    plot.gap_width = 60
    plot.has_data_labels = True
    plot.data_labels.number_format = '0.00"%"'
    plot.data_labels.number_format_is_linked = False
    plot.data_labels.position = XL_LABEL_POSITION.OUTSIDE_END
    plot.data_labels.font.color.rgb = TEXT
    plot.series[0].format.fill.solid()
    plot.series[0].format.fill.fore_color.rgb = BLUE
    ch.value_axis.has_major_gridlines = True
    ch.value_axis.major_gridlines.format.line.color.rgb = CARD2
    ch.value_axis.format.line.fill.background()
    ch.category_axis.format.line.color.rgb = CARD2
    ch.category_axis.tick_labels.font.color.rgb = TEXT
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(8.5), Inches(1.7), Inches(4.3), Inches(2.3), CARD, VIOLET)
    text(s, Inches(8.6), Inches(1.85), Inches(4.1), Inches(2.0),
         [[("Spark (PySpark):", {"bold": True, "color": VIOLET, "size": 14})],
          [("sum(when(type=='click',1))", {"size": 14})], [("groupBy(window('1 minute'),", {"size": 14})],
          [("  campaign_id, publisher_id)", {"size": 14})]], align="l", rtl=False)
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(8.5), Inches(4.25), Inches(4.3), Inches(2.5), CARD, AQUA)
    text(s, Inches(8.6), Inches(4.4), Inches(4.1), Inches(2.2),
         [[("PostgreSQL view:", {"bold": True, "color": AQUA, "size": 14})],
          [("100.0 * SUM(clicks)", {"size": 14})], [("/ NULLIF(SUM(impressions), 0)", {"size": 14})], "",
          ], align="l", rtl=False)
    text(s, Inches(8.6), Inches(5.9), Inches(4.1), Inches(0.5), f"CTR כולל בדמו: {ctr}%", size=17, bold=True)

    # 17 ── Suspicious activity
    s = d.slide("זיהוי פעילות חשודה וכפילויות",
                "כלל ההונאה: אותו משתמש ואותה כתובת IP שלחצו על אותו קמפיין 8 פעמים או יותר בתוך דקה – זה לא "
                "בן אדם. המחולל שלי מייצר בוטים שלוחצים 10 עד 25 פעם בכמה שניות, ו-Spark תופס אותם. בנוסף, "
                "כפילויות – אותו event_id שהגיע פעמיים – מוסרות לפני הספירה ונרשמות בטבלה, ואירועים פסולים "
                "נשלחים להסגר ונספרים לפי סיבה. למה זה חשוב? כי מפרסם משלם לפי קליק.", kicker="איכות והונאה")
    card(s, Inches(6.9), Inches(1.7), Inches(5.9), Inches(1.9), "הכלל",
         "אותו user + IP + קמפיין ≥ 8 קליקים בדקה אחת → חשוד", ORANGE, title_size=20, body_size=17)
    stat(s, Inches(6.9), Inches(3.85), Inches(1.95), f"{summary['suspicious_users']}", "משתמשים חשודים", ORANGE)
    stat(s, Inches(8.85), Inches(3.85), Inches(1.95), f"{summary['duplicate_events']}", "כפילויות שהוסרו", VIOLET)
    stat(s, Inches(10.8), Inches(3.85), Inches(1.95), f"{summary['invalid_events']}", "אירועים פסולים", BLUE)
    rows = [["user_id", "campaign", "clicks/min"]]
    for r in suspicious[:6]:
        rows.append([r["user_id"], r["campaign_name"], r["click_count"]])
    table(s, Inches(0.5), Inches(1.7), Inches(6.0), rows, [1.6, 3.2, 1.2], size=13, row_h=0.5, rtl=False)
    text(s, Inches(6.9), Inches(5.5), Inches(5.9), Inches(1.2),
         "מפרסם משלם לפי קליק – קליקים מזויפים הם גניבה. פלטפורמה שלא מסננת מאבדת אמון.", size=15, color=MUTED)

    # 18 ── Journey of one event
    s = d.slide("המסע של קליק אחד במערכת",
                "בואו נלך עם קליק אחד. המחולל יוצר קליק של משתמש 42 על קמפיין 3. הוא נשלח ל-Kafka ונוחת "
                "ב-partition 1 לפי המפתח. תוך עשרים שניות Spark קורא אותו, מוודא שהוא תקין ומצרף את שם הקמפיין. "
                "ה-watermark בודק שהוא לא מאחר מדי, וה-dedup בודק שלא ראינו את ה-event_id הזה. משם הוא נכתב "
                "ל-Parquet, מעדכן את חלון הדקה ב-Postgres, ונבדק מול כלל ההונאה. הדשבורד מושך את המספר החדש "
                "דרך ה-API. כל המסע – פחות משלושים שניות.", kicker="מקצה לקצה")
    journey = [("1", "Generator", "קליק של u_000042"), ("2", "Kafka", "partition 1 · offset 1024"),
               ("3", "Spark", "parse + validate + join"), ("4", "Watermark", "לא מאחר מדי"),
               ("5", "Dedup", "event_id חדש"), ("6", "Parquet + PG", "window 10:00 → clicks+1"),
               ("7", "FastAPI", "GET /summary"), ("8", "Dashboard", "המספר עולה")]
    for i, (num, en, he) in enumerate(journey):
        col, row = i % 4, i // 4
        x0 = Inches(0.5 + col * 3.15)
        y0 = Inches(1.8 + row * 2.45)
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x0, y0, Inches(2.85), Inches(1.95), CARD)
        c = shape(s, MSO_SHAPE.OVAL, x0 + Inches(0.2), y0 + Inches(0.2), Inches(0.55), Inches(0.55),
                  [BLUE, VIOLET, ORANGE, YELLOW, AQUA, AQUA, BLUE, VIOLET][i])
        p = c.text_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = num
        r.font.name, r.font.size, r.font.bold, r.font.color.rgb = FONT, Pt(16), True, TEXT
        text(s, x0 + Inches(0.85), y0 + Inches(0.22), Inches(1.9), Inches(0.5), en, size=17, bold=True, align="l",
             rtl=False)
        text(s, x0 + Inches(0.15), y0 + Inches(0.95), Inches(2.55), Inches(0.9), he, size=15, color=MUTED)
        if col < 3:
            arrow(s, x0 + Inches(2.85), y0 + Inches(0.97), x0 + Inches(3.15), y0 + Inches(0.97))
    text(s, Inches(0.5), Inches(6.55), Inches(12.3), Inches(0.4), "כל המסע: פחות מ-30 שניות", size=16, bold=True,
         color=AQUA, align="c")

    # 19 ── Challenges & solutions
    s = d.slide("אתגרים טכניים ופתרונות",
                "כמה אתגרים אמיתיים. הראשון והכי מעניין: כתיבה ל-S3 נכשלה שוב ושוב, כי ה-committer של Hadoop "
                "עושה rename, ובאחסון אובייקטים אין באמת rename. עברתי ל-file sink של Structured Streaming. שני: "
                "MinIO הפסיק לפרסם גרסה חינמית מתוחזקת, אז בדקתי חלופות ובחרתי SeaweedFS. שלישי: אירועים מאחרים "
                "וכפולים – פתרתי עם watermark ו-dedup, ויש בדיקה שמוכיחה את זה. ורביעי: ריסטרט שמכפיל מספרים – "
                "פתרתי עם checkpoint על volume ו-upsert אידמפוטנטי.", kicker="מה היה קשה")
    table(s, Inches(0.5), Inches(1.7), Inches(12.3),
          [["אתגר", "מה קרה", "הפתרון"],
           ["כתיבה ל-S3 נכשלה", "Hadoop committer עושה rename – אין rename אמיתי ב-object storage",
            "Streaming file sink עם _spark_metadata"],
           ["MinIO נגנז", "אין יותר אימג'ים חינמיים מתוחזקים (2025–2026)", "SeaweedFS (Apache-2.0), אותו S3 API"],
           ["אירועים מאחרים", "טלפון בלי קליטה שולח קליק אחרי 10 דקות", "Event time + watermark 10 דק' + בדיקה"],
           ["כפילויות", "retry ברשת שולח אותו אירוע פעמיים", "dropDuplicatesWithinWatermark(event_id)"],
           ["ריסטרט מכפיל מספרים", "batch רץ שוב אחרי קריסה", "checkpoint על volume + UPSERT אידמפוטנטי"],
           ["import pyspark נכשל", "PYTHONPATH שלנו דרס את של Spark", "PYTHONPATH מלא כולל py4j"]],
          [2.6, 5.2, 4.5], size=14, row_h=0.68)

    # 20 ── What I learned & improvements
    s = d.slide("מה למדתי ומה הייתי משפרת",
                "מה למדתי: לחשוב בזרמים ולא רק ב-batch, ההבדל בין זמן אירוע לזמן עיבוד, איך Kafka שומר על סדר "
                "ועל אמינות, ולמה בדיקות אוטומטיות חוסכות זמן – 74 בדיקות. ומה הייתי משפרת: Kafka עם 3 ברוקרים, "
                "Spark על קלאסטר, exactly-once עם sink טרנזקציוני, schema registry כדי שכללי הוולידציה יוגדרו "
                "פעם אחת, וניטור עם Prometheus ו-Grafana. תודה! אשמח לשאלות.", kicker="סיכום")
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(6.75), Inches(1.6), Inches(6.1), Inches(4.2), CARD)
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.45), Inches(1.6), Inches(6.1), Inches(4.2), CARD)
    text(s, Inches(6.8), Inches(1.7), Inches(6.0), Inches(0.5), "מה למדתי", size=22, bold=True, color=AQUA)
    learned = ["לחשוב בזרמים, לא רק ב-batch", "Event time מול processing time ו-watermark",
               "סדר ואמינות ב-Kafka: keys, partitions, offsets", "פירמידת בדיקות: 59 יחידה, 8 של Spark, 7 מקצה לקצה",
               "Docker Compose לסביבה שלמה בפקודה אחת"]
    text(s, Inches(6.8), Inches(2.3), Inches(6.0), Inches(3.6), learned, size=17, line_spacing=1.5)
    text(s, Inches(0.5), Inches(1.7), Inches(6.0), Inches(0.5), "מה הייתי משפרת", size=22, bold=True, color=VIOLET)
    improve = ["Kafka עם 3 brokers ו-replication 3", "Spark על קלאסטר (Kubernetes / EMR)",
               "Exactly-once עם sink טרנזקציוני", "Schema registry – כללים במקום אחד",
               "ניטור: Prometheus + Grafana על consumer lag"]
    text(s, Inches(0.5), Inches(2.3), Inches(6.0), Inches(3.6), improve, size=17, line_spacing=1.5)
    text(s, Inches(0.5), Inches(6.1), Inches(12.3), Inches(0.6), "תודה! שאלות?", size=26, bold=True, align="c")

    d.save()
    print(f"Saved {OUT} with {d.count} slides")


if __name__ == "__main__":
    build()
