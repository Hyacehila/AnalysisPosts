"""
Stage 3 report output cleanup node.
"""
from __future__ import annotations

import os

from nodes.base import MonitoredNode
from utils.path_manager import PathManager


class ClearStage3OutputsNode(MonitoredNode):
    """
    清理 Stage3 输出文件（仅 report.md 与 status.json）。
    保留 Stage2 产物与 images/ 目录。
    """

    def prep(self, shared):
        return None

    def exec(self, prep_res):
        manager = PathManager()
        report_dir = manager.report_dir()

        targets = [
            report_dir / "report.md",
            report_dir / "status.json",
        ]

        removed = []
        for path in targets:
            if path.exists():
                os.remove(path)
                removed.append(str(path))

        return removed

    def post(self, shared, prep_res, exec_res):
        if exec_res:
            print(f"[Stage3] 已清理输出文件: {exec_res}")
        else:
            print("[Stage3] 未发现需清理的输出文件")
        return "default"
