"""
Pure helpers for report preview page.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict


def load_report_preview_artifacts(report_dir: Path | str = Path("report")) -> Dict[str, str | bool]:
    base_dir = Path(report_dir)
    md_path = base_dir / "report.md"
    html_path = base_dir / "report.html"

    markdown_text = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
    html_text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""

    return {
        "has_markdown": bool(markdown_text),
        "has_html": bool(html_text),
        "markdown_text": markdown_text,
        "html_text": html_text,
    }

