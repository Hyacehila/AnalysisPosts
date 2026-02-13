"""
PathManager:统一报告与图表输出路径。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


class PathManager:
    """Centralized path helper for report outputs."""

    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = Path(base_dir or os.getcwd()).resolve()

    def report_dir(self) -> Path:
        return self.base_dir / "report"

    def images_dir(self) -> Path:
        return self.report_dir() / "images"

    def ensure_dir(self, path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    def report_file(self, filename: str) -> Path:
        return self.report_dir() / filename


def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def get_report_dir(path: Optional[str] = None) -> str:
    if path:
        candidate = Path(path)
        if not candidate.is_absolute():
            project_root = os.environ.get("PROJECT_ROOT")
            if project_root:
                candidate = Path(project_root) / candidate
        target = str(candidate)
    else:
        target = os.environ.get("REPORT_DIR") or str(PathManager().report_dir())
    return ensure_dir(target)


def get_images_dir(path: Optional[str] = None) -> str:
    if path:
        candidate = Path(path)
        if not candidate.is_absolute():
            project_root = os.environ.get("PROJECT_ROOT")
            if project_root:
                candidate = Path(project_root) / candidate
        target = str(candidate)
    else:
        report_dir = os.environ.get("REPORT_DIR")
        if report_dir:
            target = str(Path(report_dir) / "images")
        else:
            target = str(PathManager().images_dir())
    return ensure_dir(target)
