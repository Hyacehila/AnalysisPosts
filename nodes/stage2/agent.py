"""
Stage 2 agent nodes.
"""
import importlib
import json
import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from nodes.base import MonitoredNode

from utils.call_llm import call_glm46
from utils.trace_manager import append_decision, append_execution, append_reflection


_DIMENSION_KEYWORDS = {
    "sentiment": ["sentiment"],
    "topic": ["topic"],
    "geographic": ["geographic", "geo"],
    "interaction": ["publisher", "interaction", "cross", "influence", "correlation", "participant"],
    "nlp": ["keyword", "entity", "lexicon", "cluster"],
}

_CATEGORY_HINTS = {
    "情感": "sentiment",
    "主题": "topic",
    "地理": "geographic",
    "交互": "interaction",
    "NLP": "nlp",
}

_DEFAULT_DIMENSIONS = ("sentiment", "topic", "geographic", "interaction", "nlp")


def _infer_dimension(tool_name: str, category: Optional[str] = None) -> Optional[str]:
    name = (tool_name or "").lower()
    if category:
        for hint, dim in _CATEGORY_HINTS.items():
            if hint in category:
                return dim
    for dim, keywords in _DIMENSION_KEYWORDS.items():
        if any(k in name for k in keywords):
            return dim
    if "belief" in name:
        return "topic"
    return None


def _build_tool_index(available_tools: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for tool in available_tools or []:
        name = tool.get("name")
        canonical = tool.get("canonical_name") or name
        if name:
            index[name] = tool
        if canonical:
            index[canonical] = tool
    return index


def _normalize_tool_name(tool_name: str, tool_index: Dict[str, Dict[str, Any]]) -> str:
    info = tool_index.get(tool_name)
    if info:
        return info.get("canonical_name") or info.get("name") or tool_name
    return tool_name


def _normalize_tool_category(tool_name: str, tool_index: Dict[str, Dict[str, Any]]) -> str:
    info = tool_index.get(tool_name)
    if info:
        return info.get("category") or ""
    return ""


def _select_chart_tool(
    missing_category: str,
    available_tools: List[Dict[str, Any]],
    executed_tools: List[str],
    allowlist: Optional[List[str]] = None,
) -> Optional[str]:
    allow = set([t for t in (allowlist or []) if t])
    tool_index = _build_tool_index(available_tools)
    executed_norm = {_normalize_tool_name(t, tool_index) for t in executed_tools}

    candidates = []
    for tool in available_tools or []:
        name = tool.get("name") or ""
        canonical = tool.get("canonical_name") or name
        if allow and (canonical not in allow and name not in allow):
            continue
        if not tool.get("generates_chart", False):
            continue
        dim = _infer_dimension(canonical, tool.get("category"))
        if dim != missing_category:
            continue
        candidates.append((name, canonical))

    for name, canonical in candidates:
        if canonical not in executed_norm:
            return name

    return candidates[0][0] if candidates else None


def _count_charts_by_dimension(
    charts: List[Dict[str, Any]],
    available_tools: List[Dict[str, Any]],
) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    tool_index = _build_tool_index(available_tools)
    for chart in charts or []:
        source_tool = chart.get("source_tool") or chart.get("tool_name") or ""
        canonical = _normalize_tool_name(source_tool, tool_index)
        category = _normalize_tool_category(source_tool, tool_index)
        dim = _infer_dimension(canonical, category)
        if dim:
            counts[dim] += 1
    return dict(counts)


def _missing_chart_dimensions(
    charts: List[Dict[str, Any]],
    available_tools: List[Dict[str, Any]],
    min_per_category: Dict[str, int],
) -> List[str]:
    counts = _count_charts_by_dimension(charts, available_tools)
    missing = []
    for dim, minimum in (min_per_category or {}).items():
        try:
            minimum = int(minimum)
        except Exception:
            minimum = 0
        if minimum <= 0:
            continue
        if counts.get(dim, 0) < minimum:
            missing.append(dim)
    return missing


def _summarize_dimension_coverage(
    charts: List[Dict[str, Any]],
    available_tools: List[Dict[str, Any]],
) -> Dict[str, Any]:
    counts = _count_charts_by_dimension(charts, available_tools)
    coverage = {dim: counts.get(dim, 0) > 0 for dim in _DEFAULT_DIMENSIONS}
    gaps = [dim for dim, covered in coverage.items() if not covered]
    covered_count = sum(1 for covered in coverage.values() if covered)
    ratio = covered_count / len(_DEFAULT_DIMENSIONS) if _DEFAULT_DIMENSIONS else 0
    return {
        "coverage": coverage,
        "gaps": gaps,
        "covered_count": covered_count,
        "total_count": len(_DEFAULT_DIMENSIONS),
        "coverage_ratio": round(ratio, 3),
    }


def _normalize_tool_result(tool_name: str, result: Any, tool_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(result, dict) and "error" in result:
        return {"error": result["error"]}
    charts: List[Dict[str, Any]] = []
    data_payload = result
    summary = f"MCP工具 {tool_name} 执行完成"

    if isinstance(result, dict):
        charts = result.get("charts") or []
        summary = result.get("summary", summary)
        data_payload = result if "data" not in result else result.get("data")
        single_path = result.get("chart_path") or result.get("image_path") or result.get("file_path")
        if not charts and single_path:
            charts = [{
                "id": result.get("chart_id", tool_name),
                "title": result.get("title", tool_name),
                "path": single_path,
                "file_path": single_path,
                "type": result.get("type", "unknown"),
                "description": result.get("description", ""),
                "source_tool": tool_name,
            }]

        normalized_charts = []
        for idx, ch in enumerate(charts):
            if not isinstance(ch, dict):
                continue
            path = (
                ch.get("path")
                or ch.get("file_path")
                or ch.get("chart_path")
                or ch.get("image_path")
                or ""
            )
            normalized_charts.append({
                "id": ch.get("id") or f"{tool_name}_{idx}",
                "title": ch.get("title") or tool_name,
                "path": path,
                "file_path": ch.get("file_path") or path,
                "type": ch.get("type") or ch.get("chart_type") or "unknown",
                "description": ch.get("description") or "",
                "source_tool": ch.get("source_tool") or tool_name,
            })
        charts = normalized_charts

    category = _normalize_tool_category(tool_name, tool_index) or _get_tool_category(tool_name)

    return {
        "charts": charts,
        "data": data_payload,
        "category": category,
        "summary": summary,
    }


def _get_tool_category(tool_name: str) -> str:
    name_lower = tool_name.lower()
    if "sentiment" in name_lower:
        return "情感分析"
    if "topic" in name_lower:
        return "主题分析"
    if "geographic" in name_lower or "geo" in name_lower:
        return "地理分析"
    if any(key in name_lower for key in ["publisher", "interaction", "cross", "influence", "correlation", "participant"]):
        return "多维交互分析"
    if "keyword" in name_lower or "entity" in name_lower or "lexicon" in name_lower or "cluster" in name_lower:
        return "NLP增强分析"
    return "其他"


def _diagnose_mcp_tool_failure() -> Dict[str, Any]:
    """
    Diagnose why MCP tools cannot be discovered (returns empty list).
    """
    missing_modules: List[str] = []
    import_errors: List[str] = []

    for module in ("matplotlib", "fastmcp", "mcp"):
        try:
            importlib.import_module(module)
        except Exception as exc:
            missing_modules.append(f"{module}: {exc}")

    for module in ("utils.analysis_tools", "utils.mcp_server"):
        try:
            importlib.import_module(module)
        except Exception as exc:
            import_errors.append(f"{module}: {exc}")

    return {
        "missing_modules": missing_modules,
        "import_errors": import_errors,
    }


class CollectToolsNode(MonitoredNode):
    """
    工具收集节点
    """

    def prep(self, shared):
        config = shared.get("config", {})
        tool_source = config.get("tool_source", "mcp")
        return {"tool_source": tool_source}

    def exec(self, prep_res):
        tool_source = prep_res["tool_source"]
        if tool_source != "mcp":
            raise ValueError(f"Stage2 only supports MCP tool source, got: {tool_source}")

        # MCP 模式：通过 MCP server 收集工具
        from utils.mcp_client.mcp_client import list_tools

        tools = list_tools("utils/mcp_server")
        if not tools:
            diagnostic = _diagnose_mcp_tool_failure()
            lines = [
                "MCP工具发现失败：list_tools 返回 0 个工具。",
            ]
            if diagnostic["missing_modules"]:
                lines.append(
                    "缺失依赖或导入失败: " + "; ".join(diagnostic["missing_modules"])
                )
            if diagnostic["import_errors"]:
                lines.append(
                    "模块导入错误: " + "; ".join(diagnostic["import_errors"])
                )
            lines.append(
                "建议: 在项目根目录运行 `uv sync`，并使用 `uv run analysis` 或 `uv run main.py` 执行。"
            )
            raise RuntimeError("\n".join(lines))
        return {
            "tools": tools,
            "tool_count": len(tools),
            "tool_source": "mcp",
        }

    def post(self, shared, prep_res, exec_res):
        if "agent" not in shared:
            shared["agent"] = {}

        shared["agent"]["available_tools"] = exec_res["tools"]
        shared["agent"]["execution_history"] = []
        shared["agent"]["current_iteration"] = 0
        shared["agent"]["is_finished"] = False
        shared["agent"]["tool_source"] = "mcp"

        config = shared.get("config", {})
        agent_config = config.get("agent_config", {})
        shared["agent"]["max_iterations"] = agent_config.get("max_iterations", 10)

        print(f"\n[CollectTools] [OK] 收集到 {exec_res['tool_count']} 个可用工具 (mcp模式)")

        categories = {}
        for tool in exec_res["tools"]:
            cat = tool.get("category", "其他")
            categories.setdefault(cat, []).append(tool["name"])
        for cat, tool_names in categories.items():
            print(f"  - {cat}: {', '.join(tool_names)}")

        return "default"


class DecisionToolsNode(MonitoredNode):
    """
    工具决策节点
    """

    def prep(self, shared):
        agent = shared.get("agent", {})
        return {
            "data_summary": agent.get("data_summary", ""),
            "available_tools": agent.get("available_tools", []),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10),
        }

    def exec(self, prep_res):
        data_summary = prep_res["data_summary"]
        available_tools = prep_res["available_tools"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]

        tools_description = []
        for tool in available_tools:
            tools_description.append(
                f"- {tool['name']} ({tool['category']}): {tool['description']}"
            )
        tools_text = "\n".join(tools_description)

        if execution_history:
            executed_tools = set()
            history_items = []
            for i, item in enumerate(execution_history, 1):
                tool_name = item["tool_name"]
                summary = item.get("summary", "已执行")
                has_chart = item.get("has_chart", False)
                has_data = item.get("has_data", False)
                error = item.get("error", False)

                status_icon = "✅" if not error else "❌"
                chart_icon = "📊" if has_chart else ""
                data_icon = "📋" if has_data else ""
                history_items.append(f"{i:2d}. {status_icon} **{tool_name}** {chart_icon}{data_icon}")
                executed_tools.add(tool_name)

            history_text = "\n".join(history_items)
            executed_tools_list = sorted(list(executed_tools))
            executed_tools_summary = f"已执行工具清单 ({len(executed_tools_list)}个): {', '.join(executed_tools_list)}"
        else:
            history_text = "尚未执行任何工具"
            executed_tools_summary = "已执行工具清单: 无"

        prompt = f"""你是一个专业的舆情分析智能体，负责决定下一步的分析动作。请运用你的推理能力，基于当前分析状态做出最佳决策。

## 数据概况
{data_summary}

## 可用分析工具
{tools_text}

## 完整执行历史（按时间顺序）
{history_text}

## 工具执行状态总览
{executed_tools_summary}

## 当前状态
- 当前迭代: {current_iteration + 1}/{max_iterations}
- 已执行工具数: {len(execution_history)}
- 已执行工具覆盖率: {len(executed_tools) if execution_history else 0}/{len(available_tools)}

## 推理决策要求
请进行深度推理分析：

### 1. 执行历史分析
注意以下工具已经执行过：
{executed_tools_summary if execution_history else "无"}

### 2. 分析充分性评估
检查四个维度的覆盖情况：
- **情感分析维度**：sentiment_* 系列工具是否已执行？
- **主题分析维度**：topic_* 系列工具是否已执行？
- **地理分析维度**：geographic_* 系列工具是否已执行？
- **多维交互维度**：publisher_*, cross_*, influence_* 工具是否已执行？

### 3. 工具价值评估
- **数据价值优先**：选择能提供新统计数据的工具
- **可视化价值**：选择能生成新图表的工具
- **互补性分析**：选择与已有工具形成互补的工具
- **避免重复**：优先选择未执行过的工具

### 4. 执行策略
- **统计数据先行**：先执行 *_stats 工具获取基础数据
- **可视化工具后续**：再执行 *_chart 工具生成可视化
- **综合工具最后**：comprehensive_analysis 作为总结

## 决策输出
请以JSON格式输出你的推理决策：
```json
{{
    "thinking": "详细推理过程：1)重复检测结果 2)维度覆盖分析 3)工具价值评估 4)最终选择理由",
    "action": "execute或finish",
    "tool_name": "工具名称（必须是未执行的工具）",
    "reason": "选择该工具的具体原因和预期分析价值"
}}
```

**建议**：优先选择未执行过的工具以获得更全面的分析结果。"""

        response = call_glm46(prompt, temperature=0.6, enable_reasoning=True)

        try:
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()
            decision = json.loads(json_str)
        except json.JSONDecodeError:
            decision = {
                "action": "execute",
                "tool_name": "sentiment_distribution_stats",
                "reason": "GLM4.6响应解析失败，默认从情感分析开始",
            }

        return decision

    def post(self, shared, prep_res, exec_res):
        agent = shared.setdefault("agent", {})

        def _record_decision(action_name: str, tool_name: str, reason: str) -> None:
            decision_id = append_decision(
                shared,
                action=action_name,
                tool_name=tool_name or "",
                reason=reason or "",
                iteration=agent.get("current_iteration", 0) + 1,
            )
            agent["last_trace_decision_id"] = decision_id

        action = exec_res.get("action", "execute")

        if action == "finish":
            available_tools = shared.get("agent", {}).get("available_tools", [])
            charts = shared.get("stage2_results", {}).get("charts", [])
            stage2_chart_cfg = shared.get("config", {}).get("stage2_chart", {}) or {}
            min_per_category = stage2_chart_cfg.get("min_per_category", {}) or {}
            allowlist = stage2_chart_cfg.get("tool_allowlist", []) or []
            policy = stage2_chart_cfg.get("tool_policy", "coverage_first")

            missing_dims = _missing_chart_dimensions(charts, available_tools, min_per_category)
            if policy == "coverage_first" and missing_dims:
                executed = [item.get("tool_name", "") for item in shared.get("agent", {}).get("execution_history", [])]
                next_tool = _select_chart_tool(
                    missing_dims[0],
                    available_tools,
                    executed,
                    allowlist=allowlist,
                )
                if next_tool:
                    shared["agent"]["next_tool"] = next_tool
                    shared["agent"]["next_tool_reason"] = (
                        f"图表覆盖不足，缺少维度: {', '.join(missing_dims)}"
                    )
                    _record_decision(
                        "execute",
                        next_tool,
                        shared["agent"]["next_tool_reason"],
                    )
                    print(
                        f"\n[DecisionTools] 覆盖不足，强制补图表: {next_tool} "
                        f"(missing: {', '.join(missing_dims)})"
                    )
                    return "execute"

            shared["agent"]["is_finished"] = True
            _record_decision("finish", "", exec_res.get("reason", ""))
            print(f"\n[DecisionTools] GLM4.6智能体决定: 分析已充分，结束循环")
            print(f"  推理理由: {exec_res.get('reason', '无')}")
            return "finish"

        tool_name = exec_res.get("tool_name", "")
        shared["agent"]["next_tool"] = tool_name
        shared["agent"]["next_tool_reason"] = exec_res.get("reason", "")
        _record_decision("execute", tool_name, shared["agent"]["next_tool_reason"])

        print(f"\n[DecisionTools] GLM4.6智能体决定: 执行工具 {tool_name}")
        print(f"  推理理由: {exec_res.get('reason', '无')}")

        return "execute"


class ExecuteToolsNode(MonitoredNode):
    """
    工具执行节点
    """

    def prep(self, shared):
        agent = shared.get("agent", {})
        blog_data = shared.get("data", {}).get("blog_data", [])
        tool_source = agent.get("tool_source", "mcp")
        available_tools = agent.get("available_tools", [])
        enhanced_data_path = shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", "")
        stage2_chart_cfg = shared.get("config", {}).get("stage2_chart", {}) or {}

        if not enhanced_data_path:
            print(f"[ExecuteTools] 警告: enhanced_data_path 在 prep 中为空")
        else:
            print(f"[ExecuteTools] prep: enhanced_data_path={enhanced_data_path}")

        return {
            "tool_name": agent.get("next_tool", ""),
            "blog_data": blog_data,
            "tool_source": tool_source,
            "available_tools": available_tools,
            "enhanced_data_path": enhanced_data_path,
            "missing_policy": stage2_chart_cfg.get("missing_policy", "warn"),
        }

    def exec(self, prep_res):
        tool_name = prep_res["tool_name"]
        blog_data = prep_res["blog_data"]
        tool_source = prep_res["tool_source"]
        available_tools = prep_res.get("available_tools") or []
        if tool_source != "mcp":
            raise ValueError(f"Stage2 only supports MCP tool source, got: {tool_source}")
        enhanced_data_path = prep_res.get("enhanced_data_path") or ""

        if not tool_name:
            return {"error": "未指定工具名称"}

        print(f"\n[ExecuteTools] 执行工具: {tool_name} ({tool_source}模式)")

        from utils.mcp_client.mcp_client import call_tool

        try:
            if enhanced_data_path:
                abs_path = os.path.abspath(enhanced_data_path)
                os.environ["ENHANCED_DATA_PATH"] = abs_path
                print(f"[ExecuteTools] 设置 ENHANCED_DATA_PATH={abs_path}")
            else:
                env_path = os.environ.get("ENHANCED_DATA_PATH")
                if env_path:
                    print(f"[ExecuteTools] 使用环境变量中的 ENHANCED_DATA_PATH={env_path}")
                else:
                    print(f"[ExecuteTools] 警告: enhanced_data_path 为空，环境变量中也未设置，可能导致数据加载失败")

            result = call_tool("utils/mcp_server", tool_name, {})

            tool_index = _build_tool_index(available_tools)
            final_result = _normalize_tool_result(tool_name, result, tool_index)
        except Exception as e:
            print(f"[ExecuteTools] MCP工具调用失败: {str(e)}")
            final_result = {"error": f"MCP工具调用失败: {str(e)}"}

        return {"tool_name": tool_name, "tool_source": tool_source, "result": final_result}

    def post(self, shared, prep_res, exec_res):
        if "stage2_results" not in shared:
            shared["stage2_results"] = {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []},
            }

        tool_name = exec_res["tool_name"]
        tool_source = exec_res["tool_source"]
        result = exec_res.get("result", {})
        agent_state = shared.setdefault("agent", {})
        trace_iteration = agent_state.get("current_iteration", 0) + 1
        decision_ref = agent_state.get("last_trace_decision_id")
        result_payload = result
        if isinstance(result, dict):
            if isinstance(result.get("result"), dict):
                result_payload = result["result"]
            elif isinstance(result.get("data"), dict) and (
                "charts" in result["data"] or "summary" in result["data"]
            ):
                result_payload = result["data"]

        shared["stage2_results"]["execution_log"]["tools_executed"].append(tool_name)
        exec_log = shared["stage2_results"].setdefault("execution_log", {})
        tool_stats = exec_log.setdefault("tool_stats", {})
        tool_index = _build_tool_index(shared.get("agent", {}).get("available_tools", []))
        tool_info = tool_index.get(tool_name) or tool_index.get(_normalize_tool_name(tool_name, tool_index)) or {}
        generates_chart = bool(tool_info.get("generates_chart", False))

        if "error" in result_payload:
            print(f"  [X] 工具执行失败: {result_payload['error']}")
            shared["agent"]["last_tool_result"] = {
                "tool_name": tool_name,
                "summary": f"工具执行失败: {result_payload['error']}",
                "has_chart": False,
                "has_data": False,
                "error": True,
            }
            error_log = shared.setdefault("monitor", {}).setdefault("error_log", [])
            error_log.append({
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "stage": "stage2",
                "node": "ExecuteToolsNode",
                "status": "failed",
                "extra": {"tool_name": tool_name},
                "error": result_payload["error"],
            })
            tool_stats[tool_name] = {"charts": 0, "data": 0, "error": True}
            execution_id = append_execution(
                shared,
                tool_name=tool_name,
                iteration=trace_iteration,
                status="failed",
                summary=f"工具执行失败: {result_payload['error']}",
                has_chart=False,
                has_data=False,
                error=True,
                decision_ref=decision_ref,
            )
            agent_state["last_trace_execution_id"] = execution_id
            return "default"

        if result_payload.get("charts"):
            shared["stage2_results"]["charts"].extend(result_payload["charts"])
            print(f"  [OK] 生成 {len(result_payload['charts'])} 个图表")

        if result_payload.get("data"):
            shared["stage2_results"]["tables"].append({
                "id": tool_name,
                "title": result_payload.get("category", "") + " - " + tool_name,
                "data": result_payload["data"],
                "source_tool": tool_name,
                "source_type": tool_source,
            })
            print(f"  [OK] 生成数据表格")

        exec_log["total_charts"] = len(shared["stage2_results"].get("charts", []))
        exec_log["total_tables"] = len(shared["stage2_results"].get("tables", []))

        charts_by_category = exec_log.setdefault("charts_by_category", {})
        for chart in result_payload.get("charts") or []:
            source_tool = chart.get("source_tool") or tool_name
            canonical = _normalize_tool_name(source_tool, tool_index)
            category = _normalize_tool_category(source_tool, tool_index)
            dim = _infer_dimension(canonical, category) or "other"
            charts_by_category[dim] = charts_by_category.get(dim, 0) + 1

        chart_count = len(result_payload.get("charts") or [])
        data_count = 1 if result_payload.get("data") not in (None, {}, []) else 0
        tool_stats[tool_name] = {"charts": chart_count, "data": data_count, "error": False, "empty_chart": False}

        if generates_chart and chart_count == 0:
            summary_text = str(result_payload.get("summary", ""))
            no_data_keywords = ["没有", "未找到", "不足"]
            is_no_data = any(k in summary_text for k in no_data_keywords)
            error_msg = f"图表工具 {tool_name} 未生成图表"
            if is_no_data:
                error_msg = f"{error_msg}（无数据）"
            error_log = shared.setdefault("monitor", {}).setdefault("error_log", [])
            error_log.append({
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "stage": "stage2",
                "node": "ExecuteToolsNode",
                "status": "warning",
                "extra": {"tool_name": tool_name},
                "error": error_msg,
            })
            tool_stats[tool_name] = {
                "charts": 0,
                "data": data_count,
                "error": True,
                "empty_chart": True,
            }
            shared["agent"]["last_tool_result"] = {
                "tool_name": tool_name,
                "tool_source": tool_source,
                "summary": error_msg,
                "has_chart": False,
                "has_data": bool(result_payload.get("data")),
                "error": True,
            }
            execution_id = append_execution(
                shared,
                tool_name=tool_name,
                iteration=trace_iteration,
                status="warning",
                summary=error_msg,
                has_chart=False,
                has_data=bool(result_payload.get("data")),
                error=True,
                decision_ref=decision_ref,
            )
            agent_state["last_trace_execution_id"] = execution_id
            return "default"

        shared["agent"]["last_tool_result"] = {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "summary": result_payload.get("summary", "执行完成"),
            "has_chart": bool(result_payload.get("charts")),
            "has_data": bool(result_payload.get("data")),
            "error": False,
        }
        execution_id = append_execution(
            shared,
            tool_name=tool_name,
            iteration=trace_iteration,
            status="success",
            summary=result_payload.get("summary", "执行完成"),
            has_chart=bool(result_payload.get("charts")),
            has_data=bool(result_payload.get("data")),
            error=False,
            decision_ref=decision_ref,
        )
        agent_state["last_trace_execution_id"] = execution_id

        return "default"


class ProcessResultNode(MonitoredNode):
    """
    结果处理节点
    """

    def prep(self, shared):
        agent = shared.get("agent", {})
        return {
            "last_result": agent.get("last_tool_result", {}),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10),
            "is_finished": agent.get("is_finished", False),
        }

    def exec(self, prep_res):
        last_result = prep_res["last_result"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]
        is_finished = prep_res["is_finished"]

        if last_result:
            execution_history.append(last_result)

        new_iteration = current_iteration + 1

        should_continue = (not is_finished and new_iteration < max_iterations)

        return {
            "execution_history": execution_history,
            "new_iteration": new_iteration,
            "should_continue": should_continue,
            "reason": (
                "Agent判断分析已充分" if is_finished else
                f"达到最大迭代次数({max_iterations})" if new_iteration >= max_iterations else
                "继续分析"
            ),
        }

    def post(self, shared, prep_res, exec_res):
        if "agent" not in shared:
            shared["agent"] = {}

        shared["agent"]["execution_history"] = exec_res["execution_history"]
        shared["agent"]["current_iteration"] = exec_res["new_iteration"]
        max_iterations = int(prep_res.get("max_iterations", 10))

        available_tools = shared.get("agent", {}).get("available_tools", [])
        charts = shared.get("stage2_results", {}).get("charts", [])
        coverage = _summarize_dimension_coverage(charts, available_tools)
        history = exec_res.get("execution_history") or []
        last_tool = history[-1] if history else {}
        reflection_result = {
            "should_continue": bool(exec_res.get("should_continue")),
            "reason": exec_res.get("reason", ""),
            "last_tool": {
                "tool_name": last_tool.get("tool_name", ""),
                "has_chart": bool(last_tool.get("has_chart")),
                "has_data": bool(last_tool.get("has_data")),
                "error": bool(last_tool.get("error")),
                "summary": last_tool.get("summary", ""),
            },
            "dimension_coverage": coverage["coverage"],
            "gaps": coverage["gaps"],
            "coverage_ratio": coverage["coverage_ratio"],
            "executed_tool_count": len(history),
        }
        reflection_id = append_reflection(
            shared,
            iteration=exec_res["new_iteration"],
            result=reflection_result,
        )
        shared["agent"]["last_trace_reflection_id"] = reflection_id

        termination_reason = "continue"
        if not exec_res.get("should_continue"):
            if shared.get("agent", {}).get("is_finished", False):
                termination_reason = "agent_sufficient"
            elif int(exec_res.get("new_iteration", 0)) >= max_iterations:
                termination_reason = "max_iterations_reached"
            else:
                termination_reason = "stopped"
        shared.setdefault("trace", {}).setdefault("loop_status", {})["data_agent"] = {
            "current": int(exec_res.get("new_iteration", 0)),
            "max": max_iterations,
            "termination_reason": termination_reason,
        }

        print(f"\n[ProcessResult] 迭代 {exec_res['new_iteration']}: {exec_res['reason']}")

        if exec_res["should_continue"]:
            return "continue"

        print("[ProcessResult] Agent循环结束，准备生成洞察分析")
        return "finish"


class EnsureChartsNode(MonitoredNode):
    """
    Chart coverage fallback node.
    """

    def prep(self, shared):
        stage2_chart_cfg = shared.get("config", {}).get("stage2_chart", {}) or {}
        return {
            "charts": shared.get("stage2_results", {}).get("charts", []),
            "tables": shared.get("stage2_results", {}).get("tables", []),
            "available_tools": shared.get("agent", {}).get("available_tools", []),
            "execution_history": shared.get("agent", {}).get("execution_history", []),
            "min_per_category": stage2_chart_cfg.get("min_per_category", {}) or {},
            "tool_allowlist": stage2_chart_cfg.get("tool_allowlist", []) or [],
            "tool_policy": stage2_chart_cfg.get("tool_policy", "coverage_first"),
            "missing_policy": stage2_chart_cfg.get("missing_policy", "warn"),
            "enhanced_data_path": shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", ""),
        }

    def exec(self, prep_res):
        charts = prep_res["charts"]
        available_tools = prep_res["available_tools"]
        min_per_category = prep_res["min_per_category"]
        allowlist = prep_res["tool_allowlist"]
        enhanced_data_path = prep_res["enhanced_data_path"] or ""

        if prep_res.get("tool_policy") != "coverage_first":
            return {"attempts": [], "missing_dims": [], "errors": []}

        missing_dims = _missing_chart_dimensions(charts, available_tools, min_per_category)
        if not missing_dims:
            return {"attempts": [], "missing_dims": [], "errors": []}

        from utils.mcp_client.mcp_client import call_tool

        tool_index = _build_tool_index(available_tools)
        executed = [item.get("tool_name", "") for item in prep_res.get("execution_history", [])]
        attempts = []
        errors = []

        for dim in missing_dims:
            tool_name = _select_chart_tool(dim, available_tools, executed, allowlist=allowlist)
            if not tool_name:
                errors.append({"dimension": dim, "error": "no_candidate_tool"})
                continue

            try:
                if enhanced_data_path:
                    abs_path = os.path.abspath(enhanced_data_path)
                    os.environ["ENHANCED_DATA_PATH"] = abs_path
                result = call_tool("utils/mcp_server", tool_name, {})
                normalized = _normalize_tool_result(tool_name, result, tool_index)
                attempts.append({
                    "tool_name": tool_name,
                    "dimension": dim,
                    "result": normalized,
                })
                executed.append(tool_name)
            except Exception as exc:
                errors.append({"dimension": dim, "tool_name": tool_name, "error": str(exc)})

        return {"attempts": attempts, "missing_dims": missing_dims, "errors": errors}

    def post(self, shared, prep_res, exec_res):
        if "stage2_results" not in shared:
            shared["stage2_results"] = {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []},
            }

        exec_log = shared["stage2_results"].setdefault("execution_log", {})
        tools_executed = exec_log.setdefault("tools_executed", [])
        charts_by_category = exec_log.setdefault("charts_by_category", {})
        tool_index = _build_tool_index(shared.get("agent", {}).get("available_tools", []))

        for attempt in exec_res.get("attempts", []):
            tool_name = attempt["tool_name"]
            result_payload = attempt["result"]
            tools_executed.append(tool_name)

            if result_payload.get("error"):
                shared.setdefault("agent", {}).setdefault("execution_history", []).append({
                    "tool_name": tool_name,
                    "tool_source": "mcp",
                    "summary": f"图表补全失败: {result_payload['error']}",
                    "has_chart": False,
                    "has_data": False,
                    "error": True,
                })
                error_log = shared.setdefault("monitor", {}).setdefault("error_log", [])
                error_log.append({
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "stage": "stage2",
                    "node": "EnsureChartsNode",
                    "status": "failed",
                    "extra": {"tool_name": tool_name},
                    "error": result_payload["error"],
                })
                continue

            if result_payload.get("charts"):
                shared["stage2_results"]["charts"].extend(result_payload["charts"])
            if result_payload.get("data"):
                shared["stage2_results"]["tables"].append({
                    "id": tool_name,
                    "title": result_payload.get("category", "") + " - " + tool_name,
                    "data": result_payload["data"],
                    "source_tool": tool_name,
                    "source_type": "mcp",
                })

            for chart in result_payload.get("charts") or []:
                source_tool = chart.get("source_tool") or tool_name
                canonical = _normalize_tool_name(source_tool, tool_index)
                category = _normalize_tool_category(source_tool, tool_index)
                dim = _infer_dimension(canonical, category) or "other"
                charts_by_category[dim] = charts_by_category.get(dim, 0) + 1

            shared.setdefault("agent", {}).setdefault("execution_history", []).append({
                "tool_name": tool_name,
                "tool_source": "mcp",
                "summary": result_payload.get("summary", "图表补全执行完成"),
                "has_chart": bool(result_payload.get("charts")),
                "has_data": bool(result_payload.get("data")),
                "error": False,
            })

        exec_log["total_charts"] = len(shared["stage2_results"].get("charts", []))
        exec_log["total_tables"] = len(shared["stage2_results"].get("tables", []))

        if exec_res.get("errors"):
            error_log = shared.setdefault("monitor", {}).setdefault("error_log", [])
            for err in exec_res["errors"]:
                error_log.append({
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "stage": "stage2",
                    "node": "EnsureChartsNode",
                    "status": "failed",
                    "extra": {"dimension": err.get("dimension"), "tool_name": err.get("tool_name", "")},
                    "error": err.get("error", ""),
                })

        missing_policy = prep_res.get("missing_policy", "warn")
        remaining_missing = _missing_chart_dimensions(
            shared["stage2_results"].get("charts", []),
            prep_res.get("available_tools", []),
            prep_res.get("min_per_category", {}),
        )
        if remaining_missing:
            error_log = shared.setdefault("monitor", {}).setdefault("error_log", [])
            error_log.append({
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "stage": "stage2",
                "node": "EnsureChartsNode",
                "status": "failed",
                "extra": {"missing_dimensions": remaining_missing},
                "error": "图表覆盖仍不足",
            })

        missing = exec_res.get("missing_dims", [])
        print(f"[EnsureCharts] 补图完成，缺失维度: {', '.join(missing) if missing else '无'}")
        print(f"[EnsureCharts] 新增图表: {len(shared['stage2_results'].get('charts', []))} 总计")

        if missing_policy == "fail" and (exec_res.get("errors") or remaining_missing):
            raise RuntimeError(
                f"图表覆盖不足或补图失败。missing={remaining_missing}, errors={exec_res.get('errors', [])}"
            )

        return "default"
