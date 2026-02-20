"""
Unified Stage3 HTML rendering node.
"""
import html
import re
from typing import Dict

from nodes.base import MonitoredNode


_IMAGE_PATTERN = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")


def _render_inline_images(text: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        alt = html.escape(match.group(1) or "chart")
        src = html.escape(match.group(2) or "")
        return (
            f'<figure class="chart">'
            f'<img src="{src}" alt="{alt}" onclick="openImageModal(this.src, this.alt)" />'
            f"<figcaption>{alt}</figcaption>"
            f"</figure>"
        )

    return _IMAGE_PATTERN.sub(_repl, text)


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
            html_lines.append(f"<h{level}>{html.escape(title)}</h{level}>")
            continue

        if stripped.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item_text = _render_inline_images(html.escape(stripped[2:].strip()))
            html_lines.append(f"<li>{item_text}</li>")
            continue

        close_list()
        paragraph = _render_inline_images(line)
        if "<figure" in paragraph:
            html_lines.append(paragraph)
        else:
            html_lines.append(f"<p>{html.escape(stripped)}</p>")

    close_list()
    return "\n".join(html_lines)


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
        body_html = _markdown_to_html(prep_res)
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

    def post(self, shared: Dict, prep_res: str, exec_res: str) -> str:
        shared.setdefault("stage3_results", {})["final_report_html"] = exec_res
        return "default"
