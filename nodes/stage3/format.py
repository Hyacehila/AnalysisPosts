"""
Stage 3 report formatting node.
"""
from pathlib import Path
from typing import Any, Dict

from nodes.base import MonitoredNode
from nodes._utils import _load_analysis_charts, _remap_report_images


def _build_execution_summary(loop_status: Dict[str, Any]) -> str:
    if not loop_status:
        return ""
    lines = ["## 运行执行摘要", ""]
    for loop_id, status in loop_status.items():
        if isinstance(status, dict):
            current = status.get("current", 0)
            max_rounds = status.get("max", 0)
            reason = status.get("termination_reason", "")
            lines.append(f"- {loop_id}: {current}/{max_rounds} ({reason})")
        else:
            lines.append(f"- {loop_id}: {status}")
    lines.append("")
    return "\n".join(lines)


class FormatReportNode(MonitoredNode):
    """Normalize report markdown and inject execution summary."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.get("stage3_results", {})
        report_text = (
            stage3_results.get("report_text")
            or stage3_results.get("reviewed_report_text")
            or stage3_results.get("final_report_text")
            or shared.get("report", {}).get("full_content", "")
            or stage3_results.get("current_draft", "")
        )
        loop_status = shared.get("trace", {}).get("loop_status", {})
        return {
            "report_text": report_text,
            "loop_status": loop_status,
        }

    def exec(self, prep_res: Any) -> str:
        if isinstance(prep_res, dict):
            report_text = str(prep_res.get("report_text", ""))
            loop_status = prep_res.get("loop_status", {}) or {}
        else:
            report_text = str(prep_res or "")
            loop_status = {}

        if not report_text:
            return ""

        execution_summary = _build_execution_summary(loop_status)
        formatted_content = report_text
        if execution_summary and "运行执行摘要" not in formatted_content:
            formatted_content = execution_summary + "\n" + formatted_content.lstrip()

        path_replacements = [
            ("report\\images\\", "./images/"),
            ("report/images/", "./images/"),
            ("./report/images/", "./images/"),
            ("../report/images/", "./images/"),
        ]
        for old_path, new_path in path_replacements:
            formatted_content = formatted_content.replace(old_path, new_path)

        analysis_charts = _load_analysis_charts()
        if analysis_charts:
            formatted_content = _remap_report_images(formatted_content, analysis_charts)

        image_refs = formatted_content.count("![")
        if image_refs == 0 and analysis_charts:
            appendix_lines = ["## 图表附录", ""]
            appended = 0
            for chart in analysis_charts:
                path = (
                    chart.get("file_path")
                    or chart.get("path")
                    or chart.get("chart_path")
                    or chart.get("image_path")
                    or ""
                )
                if not path:
                    continue
                filename = Path(path).name
                if not filename:
                    continue
                title = chart.get("title") or chart.get("id") or filename
                appendix_lines.append(f"![{title}](./images/{filename})")
                appended += 1
            if appended == 0:
                appendix_lines.append("（未找到可用图表路径，无法插入图片）")
            formatted_content = formatted_content.rstrip() + "\n\n" + "\n".join(appendix_lines) + "\n"

        if "# 目录" not in formatted_content and "## 目录" not in formatted_content:
            import re

            headers = re.findall(r"^(#{1,6})\s+(.+)$", formatted_content, re.MULTILINE)
            if headers:
                toc_lines = ["## 目录\n"]
                for level, title in headers:
                    indent = "  " * (len(level) - 1)
                    anchor = title.strip().lower().replace(" ", "-")
                    toc_lines.append(f"{indent}- [{title}](#{anchor})")
                toc = "\n".join(toc_lines) + "\n\n"
                formatted_content = toc + formatted_content

        if not formatted_content.endswith("\n"):
            formatted_content += "\n"

        return formatted_content

    def post(self, shared: Dict[str, Any], prep_res: Any, exec_res: str) -> str:
        stage3_results = shared.setdefault("stage3_results", {})
        stage3_results["final_report_text"] = exec_res
        stage3_results["generation_mode"] = "unified"
        print(f"[Stage3] 报告最终图片引用数: {exec_res.count('![')}")
        return "default"
