"""
test_dispatcher.py — DispatcherNode / TerminalNode / StageXCompletionNode 单元测试

验证中央调度器的状态机逻辑：
  - 首次进入时从 start_stage 开始
  - 阶段完成后路由到下一个阶段
  - 全部完成后返回 "done"
  - 不同 mode 组合正确映射 action
  - StageXCompletionNode 正确更新 completed_stages
  - TerminalNode 正确生成 final_summary
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    DispatcherNode,
    TerminalNode,
    Stage1CompletionNode,
    Stage2CompletionNode,
    Stage3CompletionNode,
)


# =============================================================================
# DispatcherNode 测试
# =============================================================================

class TestDispatcherNodePrep:
    """测试 DispatcherNode.prep 方法"""

    def test_initializes_dispatcher_if_missing(self):
        """shared 中无 dispatcher 时自动初始化"""
        node = DispatcherNode()
        shared = {"config": {}}
        result = node.prep(shared)
        assert "dispatcher" in shared
        assert result["start_stage"] == 1
        assert result["run_stages"] == [1, 2, 3]
        assert result["current_stage"] == 0
        assert result["completed_stages"] == []

    def test_reads_existing_dispatcher(self, minimal_shared):
        """读取已有的 dispatcher 配置"""
        node = DispatcherNode()
        result = node.prep(minimal_shared)
        assert result["start_stage"] == 1
        assert result["enhancement_mode"] == "async"
        assert result["report_mode"] == "template"

    def test_reads_mode_from_config(self, minimal_shared):
        """从 config 中读取各阶段模式"""
        minimal_shared["config"]["report_mode"] = "iterative"
        node = DispatcherNode()
        result = node.prep(minimal_shared)
        assert result["enhancement_mode"] == "async"
        assert result["report_mode"] == "iterative"


class TestDispatcherNodeExec:
    """测试 DispatcherNode.exec 决策逻辑"""

    def _make_prep_res(self, current_stage=0, completed_stages=None,
                       run_stages=None, start_stage=1,
                       enhancement_mode="async",
                       report_mode="template"):
        return {
            "start_stage": start_stage,
            "run_stages": run_stages or [1, 2, 3],
            "current_stage": current_stage,
            "completed_stages": completed_stages or [],
            "enhancement_mode": enhancement_mode,
            "report_mode": report_mode,
        }

    def test_first_entry_routes_to_stage1(self):
        """首次进入(current_stage=0) → stage1_async"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res())
        assert result["action"] == "stage1_async"
        assert result["next_stage"] == 1

    def test_stage1_done_routes_to_stage2(self):
        """Stage 1 完成后 → stage2_agent"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=1,
            completed_stages=[1],
        ))
        assert result["action"] == "stage2_agent"
        assert result["next_stage"] == 2

    def test_stage2_done_routes_to_stage3(self):
        """Stage 2 完成后 → stage3_template"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=2,
            completed_stages=[1, 2],
        ))
        assert result["action"] == "stage3_template"
        assert result["next_stage"] == 3

    def test_stage3_iterative_mode(self):
        """report_mode=iterative → stage3_iterative"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=2,
            completed_stages=[1, 2],
            report_mode="iterative",
        ))
        assert result["action"] == "stage3_iterative"

    def test_all_stages_done(self):
        """全部阶段完成 → done"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=3,
            completed_stages=[1, 2, 3],
        ))
        assert result["action"] == "done"
        assert result["next_stage"] is None

    def test_partial_run_stages(self):
        """只运行 Stage 2 和 3"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=0,
            start_stage=2,
            run_stages=[2, 3],
        ))
        assert result["action"] == "stage2_agent"
        assert result["next_stage"] == 2

    def test_single_stage_run(self):
        """只运行 Stage 1"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=1,
            completed_stages=[1],
            run_stages=[1],
        ))
        assert result["action"] == "done"

    def test_skip_middle_stage(self):
        """只运行 Stage 1 和 3（跳过2）"""
        node = DispatcherNode()
        result = node.exec(self._make_prep_res(
            current_stage=1,
            completed_stages=[1],
            run_stages=[1, 3],
        ))
        assert result["action"] == "stage3_template"
        assert result["next_stage"] == 3


class TestDispatcherNodePost:
    """测试 DispatcherNode.post 方法"""

    def test_updates_current_stage(self, minimal_shared):
        """post 更新 shared.dispatcher.current_stage"""
        node = DispatcherNode()
        exec_res = {"action": "stage1_async", "next_stage": 1}
        action = node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["dispatcher"]["current_stage"] == 1
        assert action == "stage1_async"

    def test_done_does_not_update_stage(self, minimal_shared):
        """action=done 时 next_stage=None，不更新 current_stage"""
        node = DispatcherNode()
        exec_res = {"action": "done", "next_stage": None}
        action = node.post(minimal_shared, {}, exec_res)
        assert action == "done"
        assert minimal_shared["dispatcher"]["current_stage"] == 0

    def test_stores_next_action(self, minimal_shared):
        """post 存储 next_action"""
        node = DispatcherNode()
        exec_res = {"action": "stage2_agent", "next_stage": 2}
        node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["dispatcher"]["next_action"] == "stage2_agent"


# =============================================================================
# StageXCompletionNode 测试
# =============================================================================

class TestStage1CompletionNode:
    def test_adds_stage_to_completed(self):
        """Stage1CompletionNode 将 1 加入 completed_stages"""
        node = Stage1CompletionNode()
        shared = {"dispatcher": {"completed_stages": []}}
        exec_res = {"stage": 1}
        action = node.post(shared, {}, exec_res)
        assert 1 in shared["dispatcher"]["completed_stages"]
        assert action == "dispatch"

    def test_no_duplicate_stage(self):
        """重复调用不会添加重复的阶段号"""
        node = Stage1CompletionNode()
        shared = {"dispatcher": {"completed_stages": [1]}}
        exec_res = {"stage": 1}
        node.post(shared, {}, exec_res)
        assert shared["dispatcher"]["completed_stages"].count(1) == 1

    def test_creates_dispatcher_if_missing(self):
        """shared 中无 dispatcher 时自动创建"""
        node = Stage1CompletionNode()
        shared = {}
        exec_res = {"stage": 1}
        action = node.post(shared, {}, exec_res)
        assert "dispatcher" in shared
        assert 1 in shared["dispatcher"]["completed_stages"]
        assert action == "dispatch"


class TestStage2CompletionNode:
    def test_adds_stage2(self):
        node = Stage2CompletionNode()
        shared = {"dispatcher": {"completed_stages": [1]}}
        exec_res = {"stage": 2}
        action = node.post(shared, {}, exec_res)
        assert 2 in shared["dispatcher"]["completed_stages"]
        assert action == "dispatch"


class TestStage3CompletionNode:
    def test_adds_stage3(self):
        node = Stage3CompletionNode()
        shared = {"dispatcher": {"completed_stages": [1, 2]}}
        prep_res = {"generation_mode": "template", "iterations": 0, "final_score": 0}
        exec_res = {"stage": 3}
        action = node.post(shared, prep_res, exec_res)
        assert 3 in shared["dispatcher"]["completed_stages"]
        assert action == "dispatch"


# =============================================================================
# TerminalNode 测试
# =============================================================================

class TestTerminalNode:
    def test_prep_reads_shared(self, minimal_shared):
        """prep 正确提取 completed_stages 和 statistics"""
        minimal_shared["dispatcher"]["completed_stages"] = [1, 2, 3]
        minimal_shared["stage1_results"]["statistics"] = {"total_blogs": 100}
        minimal_shared["stage1_results"]["data_save"] = {
            "saved": True,
            "output_path": "data/enhanced_posts.json",
        }
        node = TerminalNode()
        result = node.prep(minimal_shared)
        assert result["completed_stages"] == [1, 2, 3]
        assert result["statistics"]["total_blogs"] == 100
        assert result["data_save"]["saved"] is True

    def test_exec_generates_summary(self):
        """exec 生成包含 status/completed_stages/total_blogs 的摘要"""
        node = TerminalNode()
        prep_res = {
            "completed_stages": [1, 2],
            "statistics": {"total_blogs": 50},
            "data_save": {"saved": True, "output_path": "data/out.json"},
        }
        result = node.exec(prep_res)
        assert result["status"] == "completed"
        assert result["completed_stages"] == [1, 2]
        assert result["total_blogs_processed"] == 50
        assert result["data_saved"] is True
        assert result["output_path"] == "data/out.json"

    def test_post_stores_final_summary(self, minimal_shared):
        """post 将 final_summary 存入 shared"""
        node = TerminalNode()
        exec_res = {
            "status": "completed",
            "completed_stages": [1],
            "total_blogs_processed": 10,
            "data_saved": False,
            "output_path": "",
        }
        action = node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["final_summary"] == exec_res
        assert action == "default"

    def test_empty_statistics(self):
        """统计为空时返回 0"""
        node = TerminalNode()
        prep_res = {
            "completed_stages": [],
            "statistics": {},
            "data_save": {},
        }
        result = node.exec(prep_res)
        assert result["total_blogs_processed"] == 0
        assert result["data_saved"] is False
