"""
Pure helpers for report preview page.
"""
from __future__ import annotations

import base64
import json
import mimetypes
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from nodes.stage3.render_html import render_markdown_report_html


_IMG_SRC_PATTERN = re.compile(r'(<img\b[^>]*?\bsrc\s*=\s*["\'])([^"\']+)(["\'])', flags=re.IGNORECASE)


def _is_local_relative_src(src: str) -> bool:
    raw = (src or "").strip().lower()
    if not raw:
        return False
    blocked_prefixes = ("http://", "https://", "data:", "file://", "blob:", "/", "#", "mailto:", "javascript:")
    return not raw.startswith(blocked_prefixes)


def _inline_local_images(html_text: str, report_dir: Path) -> str:
    if not html_text.strip():
        return ""

    def _replace(match: re.Match[str]) -> str:
        prefix, raw_src, suffix = match.group(1), match.group(2), match.group(3)
        src = (raw_src or "").strip()
        if not _is_local_relative_src(src):
            return match.group(0)

        normalized = src.split("?", 1)[0].split("#", 1)[0].strip()
        candidate = (report_dir / normalized).resolve()
        if not candidate.exists() or not candidate.is_file():
            return match.group(0)

        mime_type, _ = mimetypes.guess_type(candidate.name)
        mime_type = mime_type or "application/octet-stream"
        payload = base64.b64encode(candidate.read_bytes()).decode("ascii")
        return f"{prefix}data:{mime_type};base64,{payload}{suffix}"

    return _IMG_SRC_PATTERN.sub(_replace, html_text)


def format_error_message(error: Exception) -> str:
    message = str(error).strip()
    if message:
        return message
    fallback = repr(error).strip()
    return fallback or error.__class__.__name__


def build_pdf_error_payload(error: Exception) -> Dict[str, Any]:
    diagnostics_raw = getattr(error, "diagnostics", {})
    diagnostics = diagnostics_raw if isinstance(diagnostics_raw, dict) else {}
    payload: Dict[str, Any] = {
        "stage": str(getattr(error, "stage", "") or "").strip(),
        "error_type": str(getattr(error, "error_type", error.__class__.__name__) or error.__class__.__name__).strip(),
        "error_message": format_error_message(error),
        "traceback": str(getattr(error, "traceback_text", "") or "").strip(),
        "diagnostics": diagnostics,
    }
    return payload


def write_pdf_error_log(payload: Dict[str, Any], report_dir: Path | str = Path("report")) -> str:
    base_dir = Path(report_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    log_path = base_dir / "pdf_error.log"
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    diagnostics = payload.get("diagnostics", {})
    diagnostics_text = (
        json.dumps(diagnostics, ensure_ascii=False, indent=2, sort_keys=True)
        if isinstance(diagnostics, dict)
        else str(diagnostics)
    )
    lines = [
        f"[{now}] PDF generation failed",
        f"stage: {payload.get('stage', '')}",
        f"error_type: {payload.get('error_type', '')}",
        f"error_message: {payload.get('error_message', '')}",
        "diagnostics:",
        diagnostics_text,
        "traceback:",
        str(payload.get("traceback", "") or ""),
        "",
    ]
    log_path.write_text("\n".join(lines), encoding="utf-8")
    return str(log_path)


def load_report_preview_artifacts(report_dir: Path | str = Path("report")) -> Dict[str, str | bool]:
    base_dir = Path(report_dir)
    md_path = base_dir / "report.md"
    html_path = base_dir / "report.html"

    markdown_text = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
    html_text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    has_html = bool(html_text.strip())
    has_markdown = bool(markdown_text.strip())

    preview_source = ""
    if has_html:
        preview_source = "html"
        preview_html_raw = html_text
    elif has_markdown:
        preview_source = "markdown_fallback"
        preview_html_raw = render_markdown_report_html(markdown_text)
    else:
        preview_html_raw = ""

    preview_html = _inline_local_images(preview_html_raw, base_dir)

    return {
        "has_markdown": has_markdown,
        "has_html": has_html,
        "markdown_text": markdown_text,
        "html_text": html_text,
        "has_preview": bool(preview_html.strip()),
        "preview_source": preview_source,
        "preview_html": preview_html,
        "pdf_html": preview_html_raw,
    }
