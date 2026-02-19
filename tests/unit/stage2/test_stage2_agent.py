"""
test_stage2_agent.py — Stage 2 Agent 路径节点单元测试

覆盖范围:
  - CollectToolsNode: MCP 工具收集（mocked）
  - DecisionToolsNode: LLM 决策解析 & action 路由
  - Stage2CompletionNode: 阶段完成标记（已在 test_dispatcher.py 测试）
"""
import sys
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    CollectToolsNode,
    DecisionToolsNode,
)


# =============================================================================
# CollectToolsNode
# =============================================================================

class TestCollectToolsNode:

    def test_prep_reads_tool_source(self, minimal_shared):
        minimal_shared["config"]["tool_source"] = "mcp"
        node = CollectToolsNode()
        result = node.prep(minimal_shared)
        assert result["tool_source"] == "mcp"

    def test_prep_default_tool_source(self, minimal_shared):
        node = CollectToolsNode()
        result = node.prep(minimal_shared)
        assert result["tool_source"] == "mcp"

    @patch("nodes.CollectToolsNode.exec")
    def test_exec_returns_tools_structure(self, mock_exec):
        """验证 exec 返回结构正确"""
        mock_exec.return_value = {
            "tools": [
                {"name": "sentiment_distribution_stats", "category": "情感分析",
                 "description": "情感分布统计"},
                {"name": "topic_frequency_stats", "category": "主题分析",
                 "description": "主题频率统计"},
            ],
            "tool_source": "mcp",
            "tool_count": 2,
        }
        node = CollectToolsNode()
        result = node.exec({"tool_source": "mcp"})
        assert result["tool_count"] == 2
        assert len(result["tools"]) == 2

    @patch("utils.mcp_client.mcp_client.list_tools", return_value=[])
    def test_exec_fail_fast_when_no_tools(self, mock_list_tools):
        """工具发现失败 → 直接抛错（Fail Fast）"""
        node = CollectToolsNode()
        with pytest.raises(RuntimeError) as excinfo:
            node.exec({"tool_source": "mcp"})
        msg = str(excinfo.value)
        assert "MCP工具发现失败" in msg
        assert "uv sync" in msg

    def test_post_initializes_agent_state(self, minimal_shared):
        """post 初始化 agent 状态"""
        node = CollectToolsNode()
        tools = [
            {"name": "tool1", "category": "cat1", "description": "desc1"},
        ]
        exec_res = {"tools": tools, "tool_source": "mcp", "tool_count": 1}
        action = node.post(minimal_shared, {}, exec_res)
        agent = minimal_shared["agent"]
        assert agent["available_tools"] == tools
        assert agent["execution_history"] == []
        assert agent["current_iteration"] == 0
        assert agent["is_finished"] is False
        assert action == "default"


# =============================================================================
# DecisionToolsNode
# =============================================================================

class TestDecisionToolsNode:

    def _make_agent_shared(self, minimal_shared):
        """构建含 agent 状态的 shared"""
        minimal_shared["agent"] = {
            "data_summary": "3条博文数据",
            "available_tools": [
                {"name": "sentiment_distribution_stats", "category": "情感分析",
                 "description": "情感分布统计"},
                {"name": "topic_frequency_stats", "category": "主题分析",
                 "description": "主题频率统计"},
            ],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": 10,
            "is_finished": False,
        }
        return minimal_shared

    def test_prep_extracts_agent_state(self, minimal_shared):
        shared = self._make_agent_shared(minimal_shared)
        node = DecisionToolsNode()
        result = node.prep(shared)
        assert result["data_summary"] == "3条博文数据"
        assert len(result["available_tools"]) == 2
        assert result["current_iteration"] == 0

    @patch("nodes.stage2.agent.call_glm46")
    def test_exec_parses_execute_decision(self, mock_llm):
        """LLM 返回 execute 决策"""
        mock_llm.return_value = json.dumps({
            "thinking": "分析中",
            "action": "execute",
            "tool_name": "sentiment_distribution_stats",
            "reason": "先做情感分析",
        })
        node = DecisionToolsNode()
        prep_res = {
            "data_summary": "test",
            "available_tools": [
                {"name": "sentiment_distribution_stats", "category": "情感", "description": "desc"},
            ],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": 10,
        }
        result = node.exec(prep_res)
        assert result["action"] == "execute"
        assert result["tool_name"] == "sentiment_distribution_stats"

    @patch("nodes.stage2.agent.call_glm46")
    def test_exec_parses_finish_decision(self, mock_llm):
        """LLM 返回 finish 决策"""
        mock_llm.return_value = json.dumps({
            "thinking": "已全部完成",
            "action": "finish",
            "tool_name": "",
            "reason": "所有维度已覆盖",
        })
        node = DecisionToolsNode()
        prep_res = {
            "data_summary": "test",
            "available_tools": [],
            "execution_history": [{"tool_name": "a", "summary": "done"}],
            "current_iteration": 5,
            "max_iterations": 10,
        }
        result = node.exec(prep_res)
        assert result["action"] == "finish"

    @patch("nodes.stage2.agent.call_glm46", return_value="invalid json response")
    def test_exec_fallback_on_parse_error(self, mock_llm):
        """JSON 解析失败 → 默认从 sentiment_distribution_stats 开始"""
        node = DecisionToolsNode()
        prep_res = {
            "data_summary": "test",
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": 10,
        }
        result = node.exec(prep_res)
        assert result["action"] == "execute"
        assert result["tool_name"] == "sentiment_distribution_stats"

    @patch("nodes.stage2.agent.call_glm46")
    def test_exec_json_in_code_block(self, mock_llm):
        """LLM 返回被 ```json 包裹的 JSON"""
        mock_llm.return_value = '```json\n{"action": "execute", "tool_name": "topic_frequency_stats", "reason": "test"}\n```'
        node = DecisionToolsNode()
        prep_res = {
            "data_summary": "test",
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": 10,
        }
        result = node.exec(prep_res)
        assert result["action"] == "execute"
        assert result["tool_name"] == "topic_frequency_stats"

    def test_post_execute_action(self, minimal_shared):
        """action=execute → 设置 next_tool"""
        shared = self._make_agent_shared(minimal_shared)
        shared["trace"] = {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        }
        node = DecisionToolsNode()
        exec_res = {
            "action": "execute",
            "tool_name": "sentiment_distribution_stats",
            "reason": "test",
        }
        action = node.post(shared, {}, exec_res)
        assert action == "execute"
        assert shared["agent"]["next_tool"] == "sentiment_distribution_stats"
        assert len(shared["trace"]["decisions"]) == 1
        decision = shared["trace"]["decisions"][0]
        assert decision["action"] == "execute"
        assert decision["tool_name"] == "sentiment_distribution_stats"
        assert decision["reason"] == "test"
        assert shared["agent"]["last_trace_decision_id"] == decision["id"]

    def test_post_finish_action(self, minimal_shared):
        """action=finish → 设置 is_finished"""
        shared = self._make_agent_shared(minimal_shared)
        shared["config"]["stage2_chart"] = {
            "min_per_category": {"sentiment": 0, "topic": 0, "geographic": 0, "interaction": 0, "nlp": 0},
            "tool_allowlist": [],
        }
        shared["stage2_results"] = {"charts": []}
        shared["trace"] = {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        }
        node = DecisionToolsNode()
        exec_res = {"action": "finish", "reason": "done"}
        action = node.post(shared, {}, exec_res)
        assert action == "finish"
        assert shared["agent"]["is_finished"] is True
        assert len(shared["trace"]["decisions"]) == 1
        assert shared["trace"]["decisions"][0]["action"] == "finish"
        assert shared["trace"]["decisions"][0]["tool_name"] == ""

    def test_post_finish_forces_chart_when_missing(self, minimal_shared):
        """action=finish 但图表不足 → 强制 execute"""
        shared = self._make_agent_shared(minimal_shared)
        shared["config"]["stage2_chart"] = {
            "min_per_category": {"sentiment": 1, "topic": 0, "geographic": 0, "interaction": 0, "nlp": 0},
            "tool_allowlist": [],
        }
        shared["stage2_results"] = {"charts": []}
        shared["trace"] = {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        }
        shared["agent"]["available_tools"] = [{
            "name": "sentiment_trend_chart",
            "canonical_name": "sentiment_trend_chart",
            "category": "情感趋势分析",
            "description": "情感趋势图",
            "generates_chart": True,
        }]
        node = DecisionToolsNode()
        exec_res = {"action": "finish", "reason": "done"}
        action = node.post(shared, {}, exec_res)
        assert action == "execute"
        assert shared["agent"]["next_tool"] == "sentiment_trend_chart"
        assert len(shared["trace"]["decisions"]) == 1
        assert shared["trace"]["decisions"][0]["action"] == "execute"
        assert shared["trace"]["decisions"][0]["tool_name"] == "sentiment_trend_chart"


class TestExecuteToolsNodeTrace:

    def test_post_success_appends_trace_execution(self):
        from nodes.stage2.agent import ExecuteToolsNode

        shared = {
            "stage2_results": {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []},
            },
            "agent": {
                "available_tools": [{
                    "name": "sentiment_distribution_stats",
                    "canonical_name": "sentiment_distribution_stats",
                    "category": "情感分析",
                    "generates_chart": False,
                }],
                "current_iteration": 2,
                "last_trace_decision_id": "d_0003",
            },
            "monitor": {},
            "trace": {
                "decisions": [{"id": "d_0003"}],
                "executions": [],
                "reflections": [],
                "insight_provenance": {},
            },
        }

        node = ExecuteToolsNode()
        exec_res = {
            "tool_name": "sentiment_distribution_stats",
            "tool_source": "mcp",
            "result": {
                "charts": [],
                "data": {"value": 1},
                "category": "情感分析",
                "summary": "tool ok",
            },
        }

        node.post(shared, {}, exec_res)

        assert len(shared["trace"]["executions"]) == 1
        execution = shared["trace"]["executions"][0]
        assert execution["decision_ref"] == "d_0003"
        assert execution["status"] == "success"
        assert execution["tool_name"] == "sentiment_distribution_stats"
        assert execution["has_data"] is True
        assert execution["error"] is False

    def test_post_error_appends_trace_execution(self):
        from nodes.stage2.agent import ExecuteToolsNode

        shared = {
            "stage2_results": {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []},
            },
            "agent": {
                "available_tools": [],
                "current_iteration": 1,
                "last_trace_decision_id": "d_0001",
            },
            "monitor": {},
            "trace": {
                "decisions": [{"id": "d_0001"}],
                "executions": [],
                "reflections": [],
                "insight_provenance": {},
            },
        }

        node = ExecuteToolsNode()
        exec_res = {
            "tool_name": "sentiment_distribution_stats",
            "tool_source": "mcp",
            "result": {"error": "mock failed"},
        }

        node.post(shared, {}, exec_res)

        assert len(shared["trace"]["executions"]) == 1
        execution = shared["trace"]["executions"][0]
        assert execution["decision_ref"] == "d_0001"
        assert execution["status"] == "failed"
        assert execution["error"] is True
