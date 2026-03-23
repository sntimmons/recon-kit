"""
pdf_generator - Data Whisperer Reconciliation Audit PDF package.

Public interface:
    build_pdf(run_id, wide_path, out_path, ...)  -> int   (page count)

Lazy import avoids pulling in pandas/reportlab at package-discovery time
(e.g. when only constants or styles are needed).
"""
from __future__ import annotations


def build_pdf(*args, **kwargs) -> int:
    """Lazy wrapper - imports report.py (and pandas) only when called."""
    from .report import build_pdf as _build_pdf
    return _build_pdf(*args, **kwargs)


__all__ = ["build_pdf"]
