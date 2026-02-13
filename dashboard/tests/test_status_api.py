"""
Tests for status API.
"""
import threading
from pathlib import Path

from dashboard.api.status_api import read_status, write_status


def test_read_status_missing(tmp_path):
    status = read_status(str(tmp_path / "missing.json"))
    assert "execution_log" in status


def test_write_and_read_status(tmp_path):
    path = tmp_path / "status.json"
    payload = {"current_stage": "stage1", "execution_log": []}
    write_status(payload, str(path))
    loaded = read_status(str(path))
    assert loaded["current_stage"] == "stage1"


def test_read_status_empty_file(tmp_path):
    path = tmp_path / "status.json"
    path.write_text("", encoding="utf-8")
    loaded = read_status(str(path))
    assert loaded["execution_log"] == []
    assert loaded["error_log"] == []


def test_read_status_invalid_json(tmp_path):
    path = tmp_path / "status.json"
    path.write_text("{bad", encoding="utf-8")
    loaded = read_status(str(path))
    assert loaded["execution_log"] == []
    assert loaded["error_log"] == []


def test_write_status_concurrent(tmp_path):
    path = tmp_path / "status.json"
    errors = []

    def _worker(idx: int) -> None:
        try:
            write_status({"current_stage": f"stage{idx}", "execution_log": []}, str(path))
        except Exception as exc:  # pragma: no cover - should not happen
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    loaded = read_status(str(path))
    assert "execution_log" in loaded
