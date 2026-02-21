"""
test_pipeline_state.py — Pipeline state completion and terminal node tests.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    Stage1CompletionNode,
    Stage2CompletionNode,
    Stage3CompletionNode,
    TerminalNode,
)


class TestStageCompletionNodes:
    def test_stage1_completion_updates_pipeline_state(self):
        node = Stage1CompletionNode()
        shared = {"pipeline_state": {"completed_stages": []}}
        action = node.post(shared, {}, {"stage": 1})
        assert action == "default"
        assert shared["pipeline_state"]["completed_stages"] == [1]
        assert shared["pipeline_state"]["current_stage"] == 1

    def test_stage2_completion_updates_pipeline_state(self):
        node = Stage2CompletionNode()
        shared = {"pipeline_state": {"completed_stages": [1], "current_stage": 1}}
        action = node.post(shared, {}, {"stage": 2})
        assert action == "default"
        assert shared["pipeline_state"]["completed_stages"] == [1, 2]
        assert shared["pipeline_state"]["current_stage"] == 2

    def test_stage3_completion_updates_pipeline_state(self):
        node = Stage3CompletionNode()
        shared = {"pipeline_state": {"completed_stages": [1, 2], "current_stage": 2}}
        prep_res = {"generation_mode": "unified", "iterations": 0, "final_score": 0}
        action = node.post(shared, prep_res, True)
        assert action == "default"
        assert shared["pipeline_state"]["completed_stages"] == [1, 2, 3]
        assert shared["pipeline_state"]["current_stage"] == 3

    def test_completion_initializes_pipeline_state_if_missing(self):
        node = Stage1CompletionNode()
        shared = {}
        action = node.post(shared, {}, {"stage": 1})
        assert action == "default"
        assert shared["pipeline_state"]["completed_stages"] == [1]


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
