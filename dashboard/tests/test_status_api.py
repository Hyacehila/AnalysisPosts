"""
Tests for status API.
"""
import threading
from pathlib import Path

from dashboard.api.status_api import read_status, write_status


def test_read_status_missing(tmp_path):
    status = read_status(str(tmp_path / "missing.json"))
    assert status["version"] == 2
    assert "events" in status


def test_write_and_read_status(tmp_path):
    path = tmp_path / "status.json"
    payload = {
        "version": 2,
        "run_id": "run-1",
        "events": [
            {
                "seq": 1,
                "ts": "2026-02-20T00:00:00Z",
                "event": "enter",
                "stage": "stage1",
                "node": "NodeA",
                "branch_id": "main",
                "status": "",
                "error": "",
            }
        ],
    }
    write_status(payload, str(path))
    loaded = read_status(str(path))
    assert loaded["events"][0]["stage"] == "stage1"


def test_read_status_empty_file(tmp_path):
    path = tmp_path / "status.json"
    path.write_text("", encoding="utf-8")
    loaded = read_status(str(path))
    assert loaded["events"] == []


def test_read_status_invalid_json(tmp_path):
    path = tmp_path / "status.json"
    path.write_text("{bad", encoding="utf-8")
    loaded = read_status(str(path))
    assert loaded["events"] == []


def test_write_status_concurrent(tmp_path):
    path = tmp_path / "status.json"
    errors = []

    def _worker(idx: int) -> None:
        try:
            write_status(
                {
                    "version": 2,
                    "run_id": f"run-{idx}",
                    "events": [],
                },
                str(path),
            )
        except Exception as exc:  # pragma: no cover - should not happen
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    loaded = read_status(str(path))
    assert "events" in loaded
