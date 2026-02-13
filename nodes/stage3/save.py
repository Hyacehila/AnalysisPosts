"""
Stage 3 report save node.
"""
import os

from nodes.base import MonitoredNode
from utils.path_manager import get_report_dir


class SaveReportNode(MonitoredNode):
    """
    保存报告节点（仅 Markdown）
    """

    def prep(self, shared):
        return shared.get("stage3_results", {}).get("final_report_text", "")

    def exec(self, prep_res):
        report_dir = get_report_dir()
        md_path = os.path.join(report_dir, "report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(prep_res)
        return md_path

    def post(self, shared, prep_res, exec_res):
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["report_file"] = exec_res
        print(f"\n[Stage3] 报告已保存到: {exec_res}")
        return "default"
