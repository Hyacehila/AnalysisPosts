"""
Stage2 dual-source parallel execution flow (B4).
"""
from __future__ import annotations

import asyncio
import copy
from typing import Any, Dict, List

from pocketflow import AsyncFlow, AsyncParallelBatchFlow

from nodes.base import MonitoredAsyncNode
from nodes.stage2.agent import (
    CollectToolsNode,
    DecisionToolsNode,
    EnsureChartsNode,
    ExecuteToolsNode,
    ProcessResultNode,
)
from nodes.stage2.search_agent import SearchAgentNode


def _init_stage2_result_container() -> Dict[str, Any]:
    return {
        "charts": [],
        "tables": [],
        "insights": {},
        "execution_log": {
            "tools_executed": [],
            "total_charts": 0,
            "total_tables": 0,
            "execution_time": 0.0,
            "charts_by_category": {},
        },
    }


def _build_data_agent_shared(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    config = snapshot.get("config", {})
    max_iterations = int(config.get("agent_config", {}).get("max_iterations", 10))
    return {
        "data": {"blog_data": copy.deepcopy(snapshot.get("data", {}).get("blog_data", []))},
        "config": {
            "tool_source": config.get("tool_source", "mcp"),
            "agent_config": {"max_iterations": max_iterations},
            "data_source": copy.deepcopy(config.get("data_source", {})),
            "stage2_chart": copy.deepcopy(config.get("stage2_chart", {})),
        },
        "agent": {
            "data_summary": snapshot.get("agent", {}).get("data_summary", ""),
            "data_statistics": copy.deepcopy(snapshot.get("agent", {}).get("data_statistics", {})),
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": max_iterations,
            "is_finished": False,
        },
        "stage2_results": _init_stage2_result_container(),
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
            "loop_status": {},
        },
    }


def _build_search_agent_shared(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "agent": {
            "data_summary": snapshot.get("agent", {}).get("data_summary", ""),
        },
        "search_results": copy.deepcopy(snapshot.get("search_results", {})),
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
            "loop_status": {},
        },
    }


def _create_data_agent_subflow() -> AsyncFlow:
    collect = CollectToolsNode()
    decision = DecisionToolsNode()
    execute = ExecuteToolsNode()
    process = ProcessResultNode()
    ensure = EnsureChartsNode()

    collect >> decision
    decision - "execute" >> execute
    execute >> process
    process - "continue" >> decision
    decision - "finish" >> ensure
    process - "finish" >> ensure

    return AsyncFlow(start=collect)


def _run_data_agent_branch_sync(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    local_shared = _build_data_agent_shared(snapshot)
    flow = _create_data_agent_subflow()
    asyncio.run(flow.run_async(local_shared))
    return {
        "charts": copy.deepcopy(local_shared.get("stage2_results", {}).get("charts", [])),
        "tables": copy.deepcopy(local_shared.get("stage2_results", {}).get("tables", [])),
        "execution_log": copy.deepcopy(local_shared.get("stage2_results", {}).get("execution_log", {})),
        "trace": copy.deepcopy(local_shared.get("trace", {})),
    }


def _run_search_agent_branch_sync(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    local_shared = _build_search_agent_shared(snapshot)
    node = SearchAgentNode()
    prep_res = node.prep(local_shared)
    exec_res = node.exec(prep_res)
    node.post(local_shared, prep_res, exec_res)
    return copy.deepcopy(local_shared.get("agent_results", {}).get("search_agent", {}))


class RunParallelAgentBranchNode(MonitoredAsyncNode):
    """Run a single branch in worker thread; branch type comes from params."""

    async def prep_async(self, shared):
        return {
            "agent_name": self.params.get("agent_name", "data_agent"),
            "snapshot": {
                "data": shared.get("data", {}),
                "config": shared.get("config", {}),
                "agent": shared.get("agent", {}),
                "search_results": shared.get("search_results", {}),
            },
        }

    async def exec_async(self, prep_res):
        name = prep_res["agent_name"]
        snapshot = prep_res["snapshot"]
        if name == "data_agent":
            result = await asyncio.to_thread(_run_data_agent_branch_sync, snapshot)
        elif name == "search_agent":
            result = await asyncio.to_thread(_run_search_agent_branch_sync, snapshot)
        else:
            raise ValueError(f"Unsupported agent branch: {name}")
        return {"agent_name": name, "result": result}

    async def post_async(self, shared, prep_res, exec_res):
        name = exec_res["agent_name"]
        result = exec_res["result"]

        agent_results = shared.setdefault("agent_results", {})
        if name == "data_agent":
            agent_results["data_agent"] = {
                "charts": copy.deepcopy(result.get("charts", [])),
                "tables": copy.deepcopy(result.get("tables", [])),
                "execution_log": copy.deepcopy(result.get("execution_log", {})),
            }
            trace = shared.setdefault("trace", {})
            data_trace = result.get("trace", {})
            trace["decisions"] = copy.deepcopy(data_trace.get("decisions", []))
            trace["executions"] = copy.deepcopy(data_trace.get("executions", []))
            trace["reflections"] = copy.deepcopy(data_trace.get("reflections", []))
            trace["data_agent_reflections"] = copy.deepcopy(data_trace.get("reflections", []))
            if isinstance(data_trace.get("insight_provenance"), dict):
                trace["insight_provenance"] = copy.deepcopy(data_trace.get("insight_provenance", {}))
            if isinstance(data_trace.get("loop_status"), dict):
                trace.setdefault("loop_status", {}).update(copy.deepcopy(data_trace.get("loop_status", {})))
        else:
            agent_results["search_agent"] = copy.deepcopy(result)
            trace = shared.setdefault("trace", {})
            trace.setdefault("search_agent_analysis", []).append(copy.deepcopy(result))

        return "default"


class ParallelAgentFlow(AsyncParallelBatchFlow):
    """Run DataAgent and SearchAgent branches in parallel."""

    async def prep_async(self, shared):
        return [
            {"agent_name": "data_agent"},
            {"agent_name": "search_agent"},
        ]


def create_parallel_agent_flow() -> AsyncParallelBatchFlow:
    return ParallelAgentFlow(start=RunParallelAgentBranchNode())


__all__ = [
    "RunParallelAgentBranchNode",
    "ParallelAgentFlow",
    "create_parallel_agent_flow",
]
