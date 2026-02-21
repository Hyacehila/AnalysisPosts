"""
Tests for shared run state lock.
"""
import json
from datetime import datetime, timedelta, timezone

from utils.run_state import is_running, lock_path, set_running


def test_run_state_lock(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    set_running(False)
    assert not is_running()
    assert not lock_path().exists()


def test_run_state_clears_stale_pid_lock(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    set_running(False)

    stale = {
        "pid": 999999,
        "started_at": "2026-02-20T00:00:00Z",
    }
    lock_path().parent.mkdir(parents=True, exist_ok=True)
    lock_path().write_text(json.dumps(stale), encoding="utf-8")

    assert not is_running()
    assert not lock_path().exists()

    set_running(True)
    assert is_running()
    assert lock_path().exists()

    set_running(False)
    assert not is_running()
    assert not lock_path().exists()


def test_run_state_clears_stale_lock_by_timestamp(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    set_running(False)

    stale = {
        "pid": 0,
        "started_at": "2000-01-01T00:00:00Z",
    }
    lock_path().parent.mkdir(parents=True, exist_ok=True)
    lock_path().write_text(json.dumps(stale), encoding="utf-8")

    assert not is_running()
    assert not lock_path().exists()


def test_run_state_keeps_recent_legacy_timestamp_lock(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    set_running(False)

    recent = (
        datetime.now(timezone.utc) - timedelta(seconds=30)
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    lock_path().parent.mkdir(parents=True, exist_ok=True)
    lock_path().write_text(recent, encoding="utf-8")

    assert is_running()
