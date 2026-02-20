"""
test_dispatcher.py — DispatcherNode / TerminalNode / StageXCompletionNode unit tests.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    DispatcherNode,
    Stage1CompletionNode,
    Stage2CompletionNode,
    Stage3CompletionNode,
    TerminalNode,
)


class TestDispatcherNodePrep:
    def test_initializes_dispatcher_if_missing(self):
        node = DispatcherNode()
        shared = {"config": {}}
        result = node.prep(shared)

        assert "dispatcher" in shared
        assert result["start_stage"] == 1
        assert result["run_stages"] == [1, 2, 3]
        assert result["current_stage"] == 0
        assert result["completed_stages"] == []

    def test_reads_existing_dispatcher(self, minimal_shared):
        node = DispatcherNode()
        result = node.prep(minimal_shared)

        assert result["start_stage"] == 1
        assert result["enhancement_mode"] == "async"


class TestDispatcherNodeExec:
    def _make_prep_res(self, current_stage=0, completed_stages=None, run_stages=None, start_stage=1):
        return {
            "start_stage": start_stage,
            "run_stages": run_stages or [1, 2, 3],
            "current_stage": current_stage,
            "completed_stages": completed_stages or [],
            "enhancement_mode": "async",
        }

    def test_first_entry_routes_to_stage1(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res())
        assert result["action"] == "stage1_async"
        assert result["next_stage"] == 1

    def test_stage1_done_routes_to_stage2(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(current_stage=1, completed_stages=[1]))
        assert result["action"] == "stage2_agent"
        assert result["next_stage"] == 2

    def test_stage2_done_routes_to_unified_stage3(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(current_stage=2, completed_stages=[1, 2]))
        assert result["action"] == "stage3_report"
        assert result["next_stage"] == 3

    def test_all_stages_done(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(current_stage=3, completed_stages=[1, 2, 3]))
        assert result["action"] == "done"
        assert result["next_stage"] is None

    def test_partial_run_stages(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(current_stage=0, start_stage=2, run_stages=[2, 3]))
        assert result["action"] == "stage2_agent"
        assert result["next_stage"] == 2

    def test_skip_middle_stage(self):
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(current_stage=1, completed_stages=[1], run_stages=[1, 3]))
        assert result["action"] == "stage3_report"
        assert result["next_stage"] == 3


class TestDispatcherNodePost:
    def test_updates_current_stage(self, minimal_shared):
        node = DispatcherNode()
        action = node.post(minimal_shared, {}, {"action": "stage1_async", "next_stage": 1})
        assert action == "stage1_async"
        assert minimal_shared["dispatcher"]["current_stage"] == 1

    def test_done_does_not_update_stage(self, minimal_shared):
        node = DispatcherNode()
        action = node.post(minimal_shared, {}, {"action": "done", "next_stage": None})
        assert action == "done"
        assert minimal_shared["dispatcher"]["current_stage"] == 0

    def test_stores_next_action(self, minimal_shared):
        node = DispatcherNode()
        node.post(minimal_shared, {}, {"action": "stage2_agent", "next_stage": 2})
        assert minimal_shared["dispatcher"]["next_action"] == "stage2_agent"


class TestStageCompletionNodes:
    def test_stage1_completion_adds_stage(self):
        node = Stage1CompletionNode()
        shared = {"dispatcher": {"completed_stages": []}}
        action = node.post(shared, {}, {"stage": 1})
        assert action == "dispatch"
        assert shared["dispatcher"]["completed_stages"] == [1]

    def test_stage2_completion_adds_stage(self):
        node = Stage2CompletionNode()
        shared = {"dispatcher": {"completed_stages": [1]}}
        action = node.post(shared, {}, {"stage": 2})
        assert action == "dispatch"
        assert shared["dispatcher"]["completed_stages"] == [1, 2]

    def test_stage3_completion_adds_stage(self):
        node = Stage3CompletionNode()
        shared = {"dispatcher": {"completed_stages": [1, 2]}}
        prep_res = {"generation_mode": "unified", "iterations": 0, "final_score": 0}
        action = node.post(shared, prep_res, True)
        assert action == "dispatch"
        assert shared["dispatcher"]["completed_stages"] == [1, 2, 3]


class TestTerminalNode:
    def test_exec_generates_summary(self):
        node = TerminalNode()
        result = node.exec(
            {
                "completed_stages": [1, 2],
                "statistics": {"total_blogs": 50},
                "data_save": {"saved": True, "output_path": "data/out.json"},
            }
        )

        assert result["status"] == "completed"
        assert result["completed_stages"] == [1, 2]
        assert result["total_blogs_processed"] == 50
        assert result["data_saved"] is True
        assert result["output_path"] == "data/out.json"

    def test_post_stores_final_summary(self, minimal_shared):
        node = TerminalNode()
        exec_res = {
            "status": "completed",
            "completed_stages": [1],
            "total_blogs_processed": 10,
            "data_saved": False,
            "output_path": "",
        }

        action = node.post(minimal_shared, {}, exec_res)
        assert action == "default"
        assert minimal_shared["final_summary"] == exec_res
