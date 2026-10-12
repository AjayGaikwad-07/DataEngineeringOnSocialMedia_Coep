"""
generate_pdf_report.py

Generates the clean report using the exact layout from the first PDF:
- Natural, multi-page flowing terminal blocks (chunked at 42 lines)
- Beautiful dark terminal boxes (#1E1E1E with Courier monospace)
- Clean Assignment headers (Assignments 2, 3, 4, 5)
- High-res 2x2 grid of all 4 charts (Assignment 5)
- NO page numbers, NO header/footer rules, NO COEP name, NO metadata banner.
"""

import os
import subprocess
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, Preformatted
)

BASE = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = os.path.join(BASE, "Assignments_2_to_5_Report.pdf")


def run_and_capture(cmd):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if "JAVA_HOME" not in env:
        candidates = [
            r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot",
            r"C:\Program Files\Microsoft\jdk-17.0.20.1+1",
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot",
        ]
        for c in candidates:
            if os.path.exists(c):
                env["JAVA_HOME"] = c
                _pk = "PATH" if "PATH" in env else "Path"
                env[_pk] = os.path.join(c, "bin") + ";" + env.get(_pk, "")
                break

    try:
        proc = subprocess.run(
            cmd,
            cwd=BASE,
            env=env,
            shell=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180
        )
        return proc.stdout.strip()
    except Exception as e:
        return f"[ERROR capturing output: {e}]"


def make_terminal_flowable(text, chunk_size=42):
    """
    Formats text like dark terminal windows in chunks of 42 lines
    so ReportLab paginates naturally across pages without LayoutError.
    """
    lines = text.split("\n")

    clean_lines = []
    for l in lines:
        if len(l) > 95:
            clean_lines.append(l[:95] + "...")
        else:
            clean_lines.append(l)

    flowables = []
    for i in range(0, len(clean_lines), chunk_size):
        chunk = clean_lines[i:i + chunk_size]
        formatted_text = "\n".join(chunk)

        pre = Preformatted(
            formatted_text,
            ParagraphStyle(
                name=f"TermText_{i}_{len(chunk)}",
                fontName="Courier",
                fontSize=6.8,
                leading=8.2,
                textColor=colors.HexColor("#D4D4D4"),
            )
        )

        t = Table([[pre]], colWidths=[7.4 * inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#1E1E1E")),
            ("PADDING", (0, 0), (-1, -1), 7),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.HexColor("#333333")),
            ("LINEABOVE", (0, 0), (-1, -1), 0.5, colors.HexColor("#333333")),
            ("LINELEFT", (0, 0), (-1, -1), 0.5, colors.HexColor("#333333")),
            ("LINERIGHT", (0, 0), (-1, -1), 0.5, colors.HexColor("#333333")),
        ]))
        flowables.append(t)
        if i + chunk_size < len(clean_lines):
            flowables.append(Spacer(1, 4))

    return flowables


def build_pdf():
    print("\n[INFO] Starting PDF Compilation (Clean First Version — No Page Numbers)...")
    doc = SimpleDocTemplate(
        PDF_PATH,
        pagesize=letter,
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36
    )

    styles = getSampleStyleSheet()

    h1_style = ParagraphStyle(
        name="Heading1_Custom",
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=18,
        textColor=colors.HexColor("#1565C0"),
        spaceBefore=8,
        spaceAfter=6,
    )
    h2_style = ParagraphStyle(
        name="Heading2_Custom",
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#2E7D32"),
        spaceBefore=8,
        spaceAfter=5,
    )
    body_style = ParagraphStyle(
        name="BodyCustom",
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#333333"),
        spaceAfter=8,
    )
    caption_style = ParagraphStyle(
        name="Caption",
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=11,
        textColor=colors.HexColor("#333333"),
        alignment=1,
        spaceBefore=4,
        spaceAfter=10,
    )

    story = []

    # =========================================================================
    # ASSIGNMENT 2: KAFKA
    # =========================================================================
    print("  [1/4] Gathering Assignment 2 (Kafka) output...")
    story.append(Paragraph("Assignment 2: Real-Time Ingestion with Apache Kafka", h1_style))
    story.append(Paragraph(
        "<b>Objective:</b> Establish a scalable publish-subscribe messaging pipeline to stream raw JSON social "
        "media posts across partitioned Kafka topics with consumer offset tracking.",
        body_style
    ))

    kafka_out = run_and_capture("python kafka/kafka_assignment2_output.py")
    story.append(Paragraph("<b>Terminal Execution Output:</b>", h2_style))
    story.extend(make_terminal_flowable(kafka_out, chunk_size=42))
    story.append(Spacer(1, 14))

    # =========================================================================
    # ASSIGNMENT 3: SPARK
    # =========================================================================
    story.append(PageBreak())
    print("  [2/4] Gathering Assignment 3 (PySpark) output...")
    story.append(Paragraph("Assignment 3: Distributed Data Processing with Apache Spark", h1_style))
    story.append(Paragraph(
        "<b>Objective:</b> Ingest landed streaming data using PySpark DataFrames, apply RDD/SQL cleaning transformations, "
        "extract hashtags, perform keyword-based sentiment scoring, and execute analytical SQL aggregations.",
        body_style
    ))

    spark_out = run_and_capture("python spark/pyspark_analysis.py")
    story.append(Paragraph("<b>PySpark Terminal Execution Output:</b>", h2_style))
    story.extend(make_terminal_flowable(spark_out, chunk_size=42))
    story.append(Spacer(1, 14))

    # =========================================================================
    # ASSIGNMENT 4: AIRFLOW
    # =========================================================================
    story.append(PageBreak())
    print("  [3/4] Gathering Assignment 4 (Airflow) output...")
    story.append(Paragraph("Assignment 4: Workflow Orchestration with Apache Airflow", h1_style))
    story.append(Paragraph(
        "<b>Objective:</b> Design and orchestrate the complete end-to-end data pipeline as an Apache Airflow DAG "
        "(<code>social_media_data_pipeline</code>) scheduling data collection, Kafka streaming, Spark processing, "
        "PostgreSQL loading, and dashboard generation.",
        body_style
    ))

    airflow_out = run_and_capture("python airflow/airflow_assignment4_output.py")
    story.append(Paragraph("<b>Airflow DAG & Task Execution Output:</b>", h2_style))
    story.extend(make_terminal_flowable(airflow_out, chunk_size=42))
    story.append(Spacer(1, 14))

    # =========================================================================
    # ASSIGNMENT 5: POSTGRESQL & DASHBOARD
    # =========================================================================
    story.append(PageBreak())
    print("  [4/4] Gathering Assignment 5 (PostgreSQL & Dashboard) output...")
    story.append(Paragraph("Assignment 5: PostgreSQL Storage & Dashboard Visualisation", h1_style))
    story.append(Paragraph(
        "<b>Objective:</b> Store clean posts and hashtag counts in relational PostgreSQL tables with indexing, "
        "and generate analytical visualizations for key social media metrics.",
        body_style
    ))

    dash_out = run_and_capture("python dashboard/assignment5_output.py")
    story.append(Paragraph("<b>Database Schema & Query Execution Output:</b>", h2_style))
    story.extend(make_terminal_flowable(dash_out, chunk_size=42))
    story.append(Spacer(1, 14))

    # Embed Charts
    story.append(PageBreak())
    story.append(Paragraph("Assignment 5: Generated Dashboard Visualizations", h1_style))
    story.append(Paragraph(
        "The following charts were rendered by <code>matplotlib</code> & <code>seaborn</code> from the processed dataset:",
        body_style
    ))

    c1 = os.path.join(BASE, "dashboard", "output", "top_users.png")
    c2 = os.path.join(BASE, "dashboard", "output", "trending_hashtags.png")
    c3 = os.path.join(BASE, "dashboard", "output", "active_hours.png")
    c4 = os.path.join(BASE, "dashboard", "output", "sentiment_breakdown.png")

    img_w = 3.5 * inch
    img_h = 2.4 * inch

    row1 = []
    if os.path.exists(c1):
        row1.append([Image(c1, width=img_w, height=img_h), Paragraph("Fig 1: Top 10 Users by Post Count", caption_style)])
    if os.path.exists(c2):
        row1.append([Image(c2, width=img_w, height=img_h), Paragraph("Fig 2: Top 15 Trending Hashtags", caption_style)])

    row2 = []
    if os.path.exists(c3):
        row2.append([Image(c3, width=img_w, height=img_h), Paragraph("Fig 3: Most Active Hours (24h UTC)", caption_style)])
    if os.path.exists(c4):
        row2.append([Image(c4, width=img_w, height=img_h), Paragraph("Fig 4: Sentiment Breakdown (%)", caption_style)])

    grid_data = [
        [row1[0][0] if len(row1) > 0 else "", row1[1][0] if len(row1) > 1 else ""],
        [row1[0][1] if len(row1) > 0 else "", row1[1][1] if len(row1) > 1 else ""],
        [row2[0][0] if len(row2) > 0 else "", row2[1][0] if len(row2) > 1 else ""],
        [row2[0][1] if len(row2) > 1 else "", row2[1][1] if len(row2) > 1 else ""],
    ]

    t_charts = Table(grid_data, colWidths=[3.7 * inch, 3.7 * inch])
    t_charts.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(t_charts)

    # Standard clean canvas — NO page numbers, NO footer rules, NO header text!
    doc.build(story)
    print(f"\n[SUCCESS] PDF Generated successfully (No Page Numbers): {PDF_PATH}")
    print(f"File size: {os.path.getsize(PDF_PATH) / 1024:.1f} KB")


if __name__ == "__main__":
    build_pdf()
