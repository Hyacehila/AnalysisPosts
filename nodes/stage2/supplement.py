"""
Stage2 supplement nodes (B6).
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from nodes.stage2.agent import _normalize_tool_result
from utils.call_llm import call_glm46
from utils.mcp_client.mcp_client import call_tool, list_tools
from utils.web_search import batch_search


def _parse_json_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload

    text = str(payload or "").strip()
    if not text:
        return {}
    if "```json" in text:
        text = text.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in text:
        text = text.split("```", 1)[1].split("```", 1)[0].strip()

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _flatten_documents(raw_results: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    docs: List[Dict[str, str]] = []
    for row in raw_results or []:
        query = str(row.get("query", "")).strip()
        for item in row.get("results", []) or []:
            if not isinstance(item, dict):
                continue
            docs.append(
                {
                    "query": query,
                    "title": str(item.get("title", "")).strip(),
                    "url": str(item.get("url", "")).strip(),
                    "snippet": str(item.get("snippet", "")).strip(),
                    "date": str(item.get("date", "")).strip(),
                    "source": str(item.get("source", "")).strip(),
                }
            )
    return docs


def _default_search_agent_update(docs: List[Dict[str, str]]) -> Dict[str, Any]:
    background = ""
    if docs:
        first = docs[0]
        background = f"{first.get('title', '')}：{first.get('snippet', '')}".strip("：")
    return {
        "background_context": background or "已追加搜索，建议结合原结论复核。",
        "consistency_points": [],
        "conflict_points": [],
        "blind_spots": [],
        "recommended_followups": [],
    }


class SupplementDataNode(MonitoredNode):
    """Execute forum-directed MCP tool subset and append to data agent results."""

    def prep(self, shared):
        directive = shared.get("forum", {}).get("current_directive", {}) or {}
        data_source = shared.get("config", {}).get("data_source", {}) or {}
        return {
            "tools": list(directive.get("tools", []) or []),
            "reason": str(directive.get("reason", "") or ""),
            "enhanced_data_path": str(data_source.get("enhanced_data_path", "") or ""),
        }

    def exec(self, prep_res):
        target_tools = [str(t).strip() for t in prep_res.get("tools", []) if str(t).strip()]
        tool_metas = list_tools("utils/mcp_server")
        tool_index = {}
        for tool in tool_metas or []:
            name = str(tool.get("name", "")).strip()
            canonical = str(tool.get("canonical_name", "")).strip()
            if name:
                tool_index[name] = tool
            if canonical:
                tool_index[canonical] = tool

        if not target_tools and tool_metas:
            # Fallback: pick one data-oriented tool to avoid dead loop.
            target_tools = [str(tool_metas[0].get("name", "")).strip()]
        target_tools = [t for t in target_tools if t]

        if prep_res.get("enhanced_data_path"):
            os.environ["ENHANCED_DATA_PATH"] = os.path.abspath(prep_res["enhanced_data_path"])

        charts: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []
        tool_records: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        for tool_name in target_tools[:3]:
            try:
                raw = call_tool("utils/mcp_server", tool_name, {})
                normalized = _normalize_tool_result(tool_name, raw, tool_index)
                charts.extend(normalized.get("charts", []) or [])
                if normalized.get("data") not in (None, {}, []):
                    tables.append(
                        {
                            "id": tool_name,
                            "title": normalized.get("category", "") + " - " + tool_name,
                            "data": normalized.get("data"),
                            "source_tool": tool_name,
                            "source_type": "mcp",
                        }
                    )
                tool_records.append(
                    {
                        "tool_name": tool_name,
                        "summary": str(normalized.get("summary", "")),
                        "has_chart": bool(normalized.get("charts")),
                        "has_data": normalized.get("data") not in (None, {}, []),
                        "error": bool(normalized.get("error")),
                    }
                )
            except Exception as exc:
                errors.append({"tool_name": tool_name, "error": str(exc)})
                tool_records.append(
                    {
                        "tool_name": tool_name,
                        "summary": f"补充执行失败: {exc}",
                        "has_chart": False,
                        "has_data": False,
                        "error": True,
                    }
                )

        return {
            "reason": prep_res.get("reason", ""),
            "tools": target_tools[:3],
            "tool_records": tool_records,
            "charts": charts,
            "tables": tables,
            "errors": errors,
        }

    def post(self, shared, prep_res, exec_res):
        data_agent = shared.setdefault("agent_results", {}).setdefault("data_agent", {})
        data_agent.setdefault("charts", []).extend(exec_res.get("charts", []))
        data_agent.setdefault("tables", []).extend(exec_res.get("tables", []))

        execution_log = data_agent.setdefault("execution_log", {})
        execution_log.setdefault("tools_executed", []).extend(exec_res.get("tools", []))
        execution_log["total_charts"] = len(data_agent.get("charts", []))
        execution_log["total_tables"] = len(data_agent.get("tables", []))

        data_agent.setdefault("supplements", []).append(
            {
                "reason": exec_res.get("reason", ""),
                "tools": list(exec_res.get("tools", []) or []),
                "tool_records": list(exec_res.get("tool_records", []) or []),
            }
        )

        trace = shared.setdefault("trace", {})
        trace.setdefault("data_agent_supplements", []).append(
            {
                "tools": list(exec_res.get("tools", []) or []),
                "errors": list(exec_res.get("errors", []) or []),
            }
        )
        return "default"


class SupplementSearchNode(MonitoredNode):
    """Run follow-up web search and refresh search agent context."""

    def prep(self, shared):
        directive = shared.get("forum", {}).get("current_directive", {}) or {}
        web_search = shared.get("config", {}).get("web_search", {}) or {}
        return {
            "queries": [str(q).strip() for q in list(directive.get("queries", []) or []) if str(q).strip()],
            "reason": str(directive.get("reason", "") or ""),
            "provider": str(web_search.get("provider", "tavily") or "tavily"),
            "max_results": int(web_search.get("max_results", 5)),
            "timeout_seconds": int(web_search.get("timeout_seconds", 20)),
            "api_key": str(web_search.get("api_key", "") or ""),
            "current_search_agent": dict(shared.get("agent_results", {}).get("search_agent", {}) or {}),
        }

    def exec(self, prep_res):
        queries = list(prep_res.get("queries", []) or [])
        if not queries:
            queries = ["事件 官方回应", "事件 最新进展"]

        try:
            search_payload = batch_search(
                queries,
                provider=prep_res.get("provider", "tavily"),
                api_key=prep_res.get("api_key", ""),
                max_results=int(prep_res.get("max_results", 5)),
                timeout_seconds=int(prep_res.get("timeout_seconds", 20)),
            )
        except Exception as exc:
            search_payload = {
                "queries": list(queries),
                "provider": prep_res.get("provider", "tavily"),
                "results_by_query": [
                    {
                        "query": q,
                        "provider": prep_res.get("provider", "tavily"),
                        "results": [],
                        "error": str(exc),
                    }
                    for q in queries
                ],
                "total_results": 0,
            }

        raw_results = list(search_payload.get("results_by_query", []) or [])
        docs = _flatten_documents(raw_results)
        prompt = f"""你是SearchAgent补充模块。请结合已有结论与新增搜索结果，输出更新后的结构化分析JSON。

当前SearchAgent结论:
{json.dumps(prep_res.get("current_search_agent", {}), ensure_ascii=False)}

新增搜索文档:
{json.dumps(docs[:8], ensure_ascii=False)}

输出JSON:
{{
  "background_context": "",
  "consistency_points": [],
  "conflict_points": [],
  "blind_spots": [],
  "recommended_followups": []
}}"""

        parsed: Dict[str, Any] = {}
        try:
            resp = call_glm46(prompt, temperature=0.4, enable_reasoning=True)
            parsed = _parse_json_payload(resp)
        except Exception:
            parsed = {}

        if not parsed:
            parsed = _default_search_agent_update(docs)

        return {
            "queries": list(queries),
            "reason": prep_res.get("reason", ""),
            "raw_results": raw_results,
            "documents": docs,
            "search_agent_update": {
                "background_context": str(parsed.get("background_context", "")),
                "consistency_points": list(parsed.get("consistency_points", []) or []),
                "conflict_points": list(parsed.get("conflict_points", []) or []),
                "blind_spots": list(parsed.get("blind_spots", []) or []),
                "recommended_followups": list(parsed.get("recommended_followups", []) or []),
            },
        }

    def post(self, shared, prep_res, exec_res):
        search_agent = shared.setdefault("agent_results", {}).setdefault("search_agent", {})
        search_agent.update(dict(exec_res.get("search_agent_update", {}) or {}))
        search_agent.setdefault("supplements", []).append(
            {
                "queries": list(exec_res.get("queries", []) or []),
                "reason": exec_res.get("reason", ""),
                "documents_count": len(exec_res.get("documents", []) or []),
            }
        )

        search_state = shared.setdefault("search", {})
        search_state.setdefault("raw_results", []).extend(list(exec_res.get("raw_results", []) or []))
        search_state.setdefault("documents", []).extend(list(exec_res.get("documents", []) or []))
        search_state["total_results"] = int(search_state.get("total_results", 0)) + len(
            exec_res.get("documents", []) or []
        )

        trace = shared.setdefault("trace", {})
        trace.setdefault("search_supplements", []).append(
            {
                "queries": list(exec_res.get("queries", []) or []),
                "documents_count": len(exec_res.get("documents", []) or []),
            }
        )
        return "default"


__all__ = ["SupplementDataNode", "SupplementSearchNode"]
