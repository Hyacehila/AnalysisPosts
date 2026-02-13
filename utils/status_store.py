"""
Status store helpers for atomic JSON writes with concurrency control.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import msvcrt  # Windows file locking
except ImportError:  # pragma: no cover - non-Windows
    msvcrt = None  # type: ignore

try:
    import fcntl  # type: ignore  # Unix file locking
except ImportError:  # pragma: no cover - Windows
    fcntl = None  # type: ignore


_WRITE_LOCK = threading.Lock()


@contextmanager
def _file_lock(lock_path: Path, timeout: float = 5.0, poll_interval: float = 0.05):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+", encoding="utf-8")
    try:
        if msvcrt is not None:
            deadline = time.time() + timeout
            while True:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.time() >= deadline:
                        raise
                    time.sleep(poll_interval)
        elif fcntl is not None:  # pragma: no cover - Windows
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            if msvcrt is not None:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            elif fcntl is not None:  # pragma: no cover - Windows
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            fh.close()


def _cleanup_tmp_files(directory: Path, base_name: str, max_age_seconds: int) -> None:
    if max_age_seconds <= 0:
        return
    now = time.time()
    prefix = f"{base_name}."
    for entry in directory.glob(f"{base_name}.*.tmp"):
        if not entry.name.startswith(prefix):
            continue
        try:
            if now - entry.stat().st_mtime > max_age_seconds:
                entry.unlink()
        except OSError:
            continue


def atomic_write_json(
    path: str | Path,
    payload: Dict[str, Any],
    *,
    retries: int = 5,
    backoff_seconds: float = 0.05,
    cleanup_age_minutes: int = 10,
) -> None:
    """Write JSON atomically with locks and retry."""
    target = Path(path)
    report_dir = target.parent
    lock_path = report_dir / ".status.lock"

    with _WRITE_LOCK:
        with _file_lock(lock_path):
            _cleanup_tmp_files(
                report_dir,
                target.name,
                max_age_seconds=cleanup_age_minutes * 60,
            )
            for attempt in range(retries):
                token = f"{os.getpid()}_{threading.get_ident()}_{uuid.uuid4().hex}"
                tmp_path = target.with_name(f"{target.name}.{token}.tmp")
                try:
                    report_dir.mkdir(parents=True, exist_ok=True)
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_path, target)
                    return
                except OSError:
                    try:
                        if tmp_path.exists():
                            tmp_path.unlink()
                    except OSError:
                        pass
                    if attempt >= retries - 1:
                        raise
                    time.sleep(backoff_seconds * (attempt + 1))


def read_status(path: str | Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Best-effort JSON reader with fallback."""
    if default is None:
        default = {}
    status_path = Path(path)
    if not status_path.exists():
        return default
    try:
        content = status_path.read_text(encoding="utf-8")
    except OSError:
        return default
    if not content.strip():
        return default
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return default
