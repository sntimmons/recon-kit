"""
audit/reports/generate_pdf.py - Convert audit_report.docx to audit_report.pdf.

Conversion strategy (tried in order):
  1. docx2pdf  - uses Microsoft Word on macOS/Windows (best quality)
  2. LibreOffice headless  - cross-platform fallback (soffice --headless)
  3. python-docx + reportlab  - pure-Python last resort (basic formatting)

If all strategies fail the script exits with code 1 and writes a diagnostic
message to stderr.  The pipeline treats PDF generation as non-fatal (warn).

Usage:
  python audit/reports/generate_pdf.py \\
         --docx <audit_report.docx> \\
         --out  <audit_report.pdf>
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Strategy 1: docx2pdf (Word on macOS/Windows)
# ---------------------------------------------------------------------------

def _try_docx2pdf(docx_path: Path, pdf_path: Path) -> bool:
    try:
        from docx2pdf import convert  # type: ignore
        convert(str(docx_path), str(pdf_path))
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            print(f"[generate_pdf] docx2pdf succeeded -> {pdf_path.name}")
            return True
        print("[generate_pdf] docx2pdf ran but output is empty", file=sys.stderr)
        return False
    except ImportError:
        return False
    except Exception as exc:
        print(f"[generate_pdf] docx2pdf failed: {exc}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Strategy 2: LibreOffice headless
# ---------------------------------------------------------------------------

def _try_libreoffice(docx_path: Path, pdf_path: Path) -> bool:
    import shutil
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        return False

    try:
        result = subprocess.run(
            [soffice,
             "--headless",
             "--convert-to", "pdf",
             "--outdir", str(pdf_path.parent),
             str(docx_path)],
            capture_output=True, text=True, timeout=120,
        )
        # LibreOffice writes <stem>.pdf in --outdir
        candidate = pdf_path.parent / (docx_path.stem + ".pdf")
        if candidate.exists() and candidate.stat().st_size > 0:
            if candidate != pdf_path:
                candidate.replace(pdf_path)  # atomic on POSIX; overwrites on Windows
            print(f"[generate_pdf] LibreOffice succeeded -> {pdf_path.name}")
            return True
        print(
            f"[generate_pdf] LibreOffice ran but output missing. "
            f"stderr: {result.stderr[:200]}",
            file=sys.stderr,
        )
        return False
    except Exception as exc:
        print(f"[generate_pdf] LibreOffice failed: {exc}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Strategy 3: reportlab fallback (plain-text PDF from docx content)
# ---------------------------------------------------------------------------

def _try_reportlab(docx_path: Path, pdf_path: Path) -> bool:
    try:
        from docx import Document as _DocxDoc  # type: ignore
        from reportlab.lib.pagesizes import LETTER  # type: ignore
        from reportlab.lib.styles import getSampleStyleSheet  # type: ignore
        from reportlab.lib.units import inch  # type: ignore
        from reportlab.platypus import (  # type: ignore
            Paragraph, SimpleDocTemplate, Spacer, HRFlowable,
        )
    except ImportError as exc:
        print(f"[generate_pdf] reportlab fallback unavailable: {exc}", file=sys.stderr)
        return False

    try:
        doc_in = _DocxDoc(str(docx_path))
        styles  = getSampleStyleSheet()

        story = []
        for para in doc_in.paragraphs:
            text = para.text.strip()
            if not text:
                story.append(Spacer(1, 0.1 * inch))
                continue
            style_name = para.style.name if para.style else "Normal"
            if style_name.startswith("Heading 1"):
                style = styles["Heading1"]
                story.append(HRFlowable(width="100%", thickness=0.5))
            elif style_name.startswith("Heading 2"):
                style = styles["Heading2"]
            elif style_name.startswith("Heading 3"):
                style = styles["Heading3"]
            else:
                style = styles["Normal"]
            story.append(Paragraph(text, style))

        pdf_doc = SimpleDocTemplate(str(pdf_path), pagesize=LETTER)
        pdf_doc.build(story)

        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            print(f"[generate_pdf] reportlab fallback succeeded -> {pdf_path.name}")
            return True
        return False

    except Exception as exc:
        print(f"[generate_pdf] reportlab fallback failed: {exc}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def convert_to_pdf(docx_path: Path, pdf_path: Path) -> bool:
    """Convert docx_path to pdf_path. Returns True on success."""
    if not docx_path.exists():
        print(f"[generate_pdf] source .docx not found: {docx_path}", file=sys.stderr)
        return False

    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    if _try_docx2pdf(docx_path, pdf_path):
        return True
    if _try_libreoffice(docx_path, pdf_path):
        return True
    if _try_reportlab(docx_path, pdf_path):
        return True

    print(
        "[generate_pdf] all conversion strategies failed. "
        "Install Microsoft Word, LibreOffice, or 'pip install reportlab' to enable PDF output.",
        file=sys.stderr,
    )
    return False


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Convert audit_report.docx to PDF.")
    parser.add_argument("--docx", required=True, help="Path to audit_report.docx")
    parser.add_argument("--out",  required=True, help="Output path for audit_report.pdf")
    args = parser.parse_args(argv)

    ok = convert_to_pdf(Path(args.docx), Path(args.out))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
