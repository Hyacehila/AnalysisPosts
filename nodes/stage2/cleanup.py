"""
Stage 2 report directory cleanup node.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from nodes.base import MonitoredNode
from utils.path_manager import PathManager


_PRESERVED_FILES = {"status.json"}
_PRESERVED_DIRS = {"acceptance"}


def _should_preserve(path: Path) -> bool:
    if path.is_dir():
        return path.name in _PRESERVED_DIRS
    return path.name in _PRESERVED_FILES


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
            for child in report_dir.iterdir():
                if _should_preserve(child):
                    continue
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=False)
                else:
                    child.unlink()

        # 保留状态文件与验收目录，同时确保 report/images/ 可写。
        manager.ensure_dir(report_dir)
        manager.ensure_dir(manager.images_dir())
        manager.ensure_dir(report_dir / "acceptance")

        return str(report_dir)

    def post(self, shared, prep_res, exec_res):
        print(f"[Stage2] 已清空并重建报告目录: {exec_res}")
        return "default"
