"""
Stage 3 report formatting node.
"""
from pathlib import Path

from nodes.base import MonitoredNode

from nodes._utils import _load_analysis_charts, _remap_report_images


class FormatReportNode(MonitoredNode):
    """
    报告格式化节点
    """

    def prep(self, shared):
        full_content = shared.get("report", {}).get("full_content", "")
        if full_content:
            return full_content
        return shared.get("stage3_results", {}).get("current_draft", "")

    def exec(self, prep_res):
        if not prep_res:
            return ""

        formatted_content = prep_res

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
                    toc_lines.append(f"{indent}- [{title}](#{title.replace(' ', '-').lower()})")
                toc = "\n".join(toc_lines) + "\n\n"
                formatted_content = toc + formatted_content

        if not formatted_content.endswith("\n"):
            formatted_content += "\n"

        return formatted_content

    def post(self, shared, prep_res, exec_res):
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["final_report_text"] = exec_res
        print(f"[Stage3] 报告最终图片引用数: {exec_res.count('![')}")
        return "default"
