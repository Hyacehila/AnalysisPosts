"""
Shared pipeline running state (lock file).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from utils.path_manager import get_report_dir


def lock_path() -> Path:
    """Return lock file path under report/."""
    return Path(get_report_dir()) / ".pipeline_running.lock"


def is_running() -> bool:
    """Check if pipeline lock file exists."""
    return lock_path().exists()


def set_running(flag: bool) -> None:
    """Create or remove the pipeline lock file."""
    path = lock_path()
    if flag:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(datetime.utcnow().isoformat() + "Z", encoding="utf-8")
    else:
        if path.exists():
            path.unlink()
