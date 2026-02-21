"""
Unified Stage3 HTML rendering node.
"""
import html
import re
from typing import Dict

from nodes.base import MonitoredNode


_INLINE_PATTERN = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)|(?<!!)\[([^\]]+)\]\(([^)]+)\)")


def _slugify_heading(text: str) -> str:
    slug = (text or "").strip().lower().replace(" ", "-")
    slug = re.sub(r"[^\w\-\u4e00-\u9fff]", "", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "section"


def _render_inline_markup(text: str) -> str:
    if not text:
        return ""

    rendered = []
    cursor = 0
    for match in _INLINE_PATTERN.finditer(text):
        rendered.append(html.escape(text[cursor:match.start()]))
        cursor = match.end()

        if match.group(1) is not None:
            alt = html.escape(match.group(1) or "chart")
            src = html.escape(match.group(2) or "")
            rendered.append(
                f'<figure class="chart">'
                f'<img src="{src}" alt="{alt}" onclick="openImageModal(this.src, this.alt)" />'
                f"<figcaption>{alt}</figcaption>"
                f"</figure>"
            )
            continue

        label = html.escape(match.group(3) or "")
        href = html.escape(match.group(4) or "")
        rendered.append(f'<a href="{href}">{label}</a>')

    rendered.append(html.escape(text[cursor:]))
    return "".join(rendered)


def _markdown_to_html(markdown_text: str) -> str:
    lines = (markdown_text or "").splitlines()
    html_lines = []
    in_list = False

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            html_lines.append("</ul>")
            in_list = False

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        if not stripped:
            close_list()
            continue

        if stripped.startswith("<details") or stripped.startswith("</details"):
            close_list()
            html_lines.append(stripped)
            continue
        if stripped.startswith("<summary") or stripped.startswith("</summary"):
            close_list()
            html_lines.append(stripped)
            continue

        if stripped.startswith("#"):
            close_list()
            level = len(stripped) - len(stripped.lstrip("#"))
            level = max(1, min(level, 6))
            title = stripped[level:].strip()
            heading_id = _slugify_heading(title)
            html_lines.append(f'<h{level} id="{html.escape(heading_id)}">{html.escape(title)}</h{level}>')
            continue

        if stripped.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item_text = _render_inline_markup(stripped[2:].strip())
            html_lines.append(f"<li>{item_text}</li>")
            continue

        close_list()
        paragraph = _render_inline_markup(line)
        if "<figure" in paragraph:
            html_lines.append(paragraph)
        else:
            html_lines.append(f"<p>{paragraph}</p>")

    close_list()
    return "\n".join(html_lines)


def render_markdown_report_html(markdown_text: str) -> str:
    body_html = _markdown_to_html(markdown_text)
    return (
        "<!doctype html>\n"
        "<html lang=\"zh-CN\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "  <title>Stage3 Report</title>\n"
        "  <style>\n"
        "    body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.7; }\n"
        "    .chart img { max-width: 100%; cursor: zoom-in; border: 1px solid #ddd; border-radius: 6px; }\n"
        "    .chart figcaption { color: #666; font-size: 0.9em; }\n"
        "    #image-modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.85); }\n"
        "    #image-modal img { max-width: 90vw; max-height: 90vh; margin: 5vh auto; display: block; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"{body_html}\n"
        "<div id=\"image-modal\" class=\"image-modal\" onclick=\"closeImageModal()\">"
        "<img id=\"image-modal-img\" src=\"\" alt=\"preview\" /></div>\n"
        "<script>\n"
        "  function openImageModal(src, alt){ const modal=document.getElementById('image-modal');"
        "const img=document.getElementById('image-modal-img'); img.src=src; img.alt=alt||'preview'; modal.style.display='block'; }\n"
        "  function closeImageModal(){ document.getElementById('image-modal').style.display='none'; }\n"
        "</script>\n"
        "</body>\n"
        "</html>\n"
    )


class RenderHTMLNode(MonitoredNode):
    """Render markdown report to interactive HTML."""

    def prep(self, shared: Dict) -> str:
        stage3_results = shared.get("stage3_results", {})
        return (
            stage3_results.get("final_report_text")
            or stage3_results.get("report_text")
            or ""
        )

    def exec(self, prep_res: str) -> str:
        return render_markdown_report_html(prep_res)

    def post(self, shared: Dict, prep_res: str, exec_res: str) -> str:
        shared.setdefault("stage3_results", {})["final_report_html"] = exec_res
        return "default"
