"""
Unit tests for status.json enter/exit event helpers.
"""
from __future__ import annotations

import threading

from utils import status_events


def test_start_status_run_resets_to_empty_schema(tmp_path):
    path = tmp_path / "status.json"

    status_events.start_status_run(path=path, run_id="run-demo")

    status = status_events.read_status_events(path=path)
    assert status["version"] == 2
    assert status["run_id"] == "run-demo"
    assert status["events"] == []


def test_append_status_event_assigns_monotonic_sequence(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-seq")

    status_events.append_status_event(
        node_name="NodeA",
        stage="stage1",
        event="enter",
        path=path,
    )
    status_events.append_status_event(
        node_name="NodeA",
        stage="stage1",
        event="exit",
        status="completed",
        path=path,
    )

    status = status_events.read_status_events(path=path)
    assert [item["seq"] for item in status["events"]] == [1, 2]
    assert status["events"][0]["event"] == "enter"
    assert status["events"][1]["event"] == "exit"
    assert status["events"][1]["status"] == "completed"


def test_append_status_event_supports_branch_id(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-branch")

    status_events.append_status_event(
        node_name="RunParallelAgentBranchNode",
        stage="stage2",
        event="enter",
        branch_id="data_agent",
        path=path,
    )

    status = status_events.read_status_events(path=path)
    assert status["events"][0]["branch_id"] == "data_agent"


def test_append_status_event_is_thread_safe(tmp_path):
    path = tmp_path / "status.json"
    status_events.start_status_run(path=path, run_id="run-thread")

    errors = []

    def _worker(idx: int) -> None:
        try:
            status_events.append_status_event(
                node_name=f"Node{idx}",
                stage="stage2",
                event="enter",
                path=path,
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    status = status_events.read_status_events(path=path)
    seqs = [item["seq"] for item in status["events"]]
    assert len(seqs) == 10
    assert sorted(seqs) == list(range(1, 11))

