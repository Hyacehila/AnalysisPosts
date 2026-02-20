"""
Stage 3 report save node.
"""
import json
import os
from typing import Any, Dict

from nodes.base import MonitoredNode
from utils.path_manager import get_report_dir


class SaveReportNode(MonitoredNode):
    """Persist final Stage3 artifacts (Markdown + HTML + trace)."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.get("stage3_results", {})
        return {
            "markdown": stage3_results.get("final_report_text", ""),
            "html": stage3_results.get("final_report_html", ""),
            "trace": shared.get("trace", {}),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, str]:
        report_dir = get_report_dir()
        os.makedirs(report_dir, exist_ok=True)

        md_path = os.path.join(report_dir, "report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(prep_res.get("markdown", ""))

        html_path = os.path.join(report_dir, "report.html")
        html_text = prep_res.get("html", "")
        if not html_text:
            html_text = "<html><body><pre></pre></body></html>"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_text)

        trace_path = os.path.join(report_dir, "trace.json")
        with open(trace_path, "w", encoding="utf-8") as f:
            json.dump(prep_res.get("trace", {}), f, ensure_ascii=False, indent=2)

        return {
            "report_md": md_path,
            "report_html": html_path,
            "trace_file": trace_path,
        }

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, str]) -> str:
        stage3_results = shared.setdefault("stage3_results", {})
        stage3_results["report_file"] = exec_res["report_md"]
        stage3_results["output_files"] = {
            "report_md": exec_res["report_md"],
            "report_html": exec_res["report_html"],
            "trace_file": exec_res["trace_file"],
        }
        print("\n[Stage3] 报告产物已保存:")
        print(f"  - Markdown: {exec_res['report_md']}")
        print(f"  - HTML: {exec_res['report_html']}")
        print(f"  - Trace: {exec_res['trace_file']}")
        return "default"
