"""
Tests for enter/exit event recording in monitored node base classes.
"""
from __future__ import annotations

import asyncio

from nodes.base import MonitoredAsyncNode, MonitoredNode
from utils import status_events


class _SyncOkNode(MonitoredNode):
    def prep(self, shared):
        return None

    def exec(self, prep_res):
        return {"ok": True}

    def post(self, shared, prep_res, exec_res):
        return "default"


class _SyncFailNode(MonitoredNode):
    def prep(self, shared):
        return None

    def exec(self, prep_res):
        raise RuntimeError("sync boom")

    def post(self, shared, prep_res, exec_res):
        return "default"


class _AsyncOkNode(MonitoredAsyncNode):
    async def prep_async(self, shared):
        return None

    async def exec_async(self, prep_res):
        return {"ok": True}

    async def post_async(self, shared, prep_res, exec_res):
        return "default"


def test_monitored_sync_node_records_enter_and_completed_exit(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-sync")

    node = _SyncOkNode()
    node._run({"status_file": str(path)})

    status = status_events.read_status_events(path=path)
    assert len(status["events"]) == 2
    assert status["events"][0]["event"] == "enter"
    assert status["events"][0]["node"] == "_SyncOkNode"
    assert status["events"][1]["event"] == "exit"
    assert status["events"][1]["status"] == "completed"


def test_monitored_sync_node_records_failed_exit(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-sync-fail")

    node = _SyncFailNode()
    try:
        node._run({"status_file": str(path)})
    except RuntimeError as exc:
        assert str(exc) == "sync boom"
    else:  # pragma: no cover
        raise AssertionError("Expected RuntimeError")

    status = status_events.read_status_events(path=path)
    assert len(status["events"]) == 2
    assert status["events"][1]["event"] == "exit"
    assert status["events"][1]["status"] == "failed"
    assert "sync boom" in status["events"][1]["error"]


def test_monitored_async_node_records_enter_and_completed_exit(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-async")

    node = _AsyncOkNode()
    result = asyncio.run(node._run_async({"status_file": str(path)}))

    assert result == "default"
    status = status_events.read_status_events(path=path)
    assert len(status["events"]) == 2
    assert status["events"][0]["event"] == "enter"
    assert status["events"][1]["event"] == "exit"
    assert status["events"][1]["status"] == "completed"

