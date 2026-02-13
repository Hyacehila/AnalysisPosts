"""
Stage 2 report directory cleanup node.
"""
from __future__ import annotations

import shutil

from nodes.base import MonitoredNode
from utils.path_manager import PathManager


class ClearReportDirNode(MonitoredNode):
    """
    清空 report/ 目录，确保每次 Stage2 运行产生干净的输出。
    """

    def prep(self, shared):
        return None

    def exec(self, prep_res):
        manager = PathManager()
        report_dir = manager.report_dir()

        if report_dir.exists():
            shutil.rmtree(report_dir)

        # 重新创建 report/ 与 report/images/，确保后续写入正常
        manager.ensure_dir(report_dir)
        manager.ensure_dir(manager.images_dir())

        return str(report_dir)

    def post(self, shared, prep_res, exec_res):
        print(f"[Stage2] 已清空并重建报告目录: {exec_res}")
        return "default"
