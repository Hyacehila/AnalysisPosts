"""
Stage2 query-search flow (B1).
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from pocketflow import AsyncFlow

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46
from utils.llm_modes import llm_request_timeout
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


def _normalize_queries(queries: List[str], *, limit: int = 5) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for query in queries or []:
        clean = str(query or "").strip()
        if not clean:
            continue
        if clean in seen:
            continue
        normalized.append(clean)
        seen.add(clean)
        if len(normalized) >= limit:
            break
    return normalized


def _default_summary(raw_docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    actors = []
    timeline = []
    related = []
    for item in raw_docs[:5]:
        title = str(item.get("title", "")).strip()
        snippet = str(item.get("snippet", "")).strip()
        date = str(item.get("date", "")).strip()
        if title:
            actors.append(title[:40])
        if date or title:
            timeline.append(f"{date} {title}".strip())
        if snippet:
            related.append(snippet[:80])

    return {
        "event_timeline": timeline,
        "key_actors": actors,
        "official_responses": [],
        "public_reactions_summary": related[0] if related else "",
        "related_events": related[1:4],
    }


class ExtractQueriesNode(MonitoredNode):
    """Extract search queries from data summary and previous gaps."""

    def prep(self, shared):
        search = shared.setdefault("search", {})
        reflections = search.get("reflections", [])
        last_missing = []
        last_hints = []
        if reflections:
            latest = reflections[-1]
            last_missing = list(latest.get("missing", []) or [])
            last_hints = list(latest.get("query_hints", []) or [])

        return {
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
            "round": int(search.get("round", 0)) + 1,
            "last_missing": last_missing,
            "last_hints": last_hints,
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res):
        prompt = f"""你是舆情检索分析助手。请基于数据概况生成搜索查询词。

## 数据概况
{prep_res.get("data_summary", "")}

## 上一轮缺口
{prep_res.get("last_missing", [])}

## 上一轮查询建议
{prep_res.get("last_hints", [])}

输出严格 JSON：
{{
  "queries": ["查询词1", "查询词2", "查询词3"]
}}
"""
        queries: List[str] = []
        try:
            llm_resp = call_glm46(
                prompt,
                temperature=0.3,
                enable_reasoning=False,
                timeout=int(prep_res.get("request_timeout_seconds", 120)),
            )
            parsed = _parse_json_payload(llm_resp)
            queries = _normalize_queries(parsed.get("queries", []), limit=5)
        except Exception:
            queries = []

        if not queries:
            base = str(prep_res.get("data_summary", "")).strip()[:24]
            fallback = [
                f"{base} 官方回应".strip(),
                f"{base} 事件进展".strip(),
                f"{base} 舆情评论".strip(),
            ]
            queries = _normalize_queries(fallback, limit=5)
        return {"queries": queries}

    def post(self, shared, prep_res, exec_res):
        search = shared.setdefault("search", {})
        search["queries"] = exec_res.get("queries", [])
        search["round"] = prep_res.get("round", 1)
        return "default"


class WebSearchNode(MonitoredNode):
    """Call search provider via utils.web_search wrapper."""

    def prep(self, shared):
        cfg = shared.get("config", {}).get("web_search", {}) or {}
        search = shared.get("search", {})
        return {
            "queries": list(search.get("queries", [])),
            "provider": cfg.get("provider", "tavily"),
            "max_results": int(cfg.get("max_results", 5)),
            "timeout_seconds": int(cfg.get("timeout_seconds", 20)),
            "api_key": cfg.get("api_key", ""),
        }

    def exec(self, prep_res):
        queries = prep_res.get("queries", [])
        if not queries:
            return {
                "queries": [],
                "provider": prep_res.get("provider", "tavily"),
                "results_by_query": [],
                "total_results": 0,
            }

        try:
            return batch_search(
                queries,
                provider=prep_res["provider"],
                api_key=prep_res.get("api_key", ""),
                max_results=prep_res["max_results"],
                timeout_seconds=prep_res["timeout_seconds"],
            )
        except Exception as exc:
            return {
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

    def post(self, shared, prep_res, exec_res):
        search = shared.setdefault("search", {})
        search["raw_results"] = exec_res.get("results_by_query", [])
        search["total_results"] = int(exec_res.get("total_results", 0))
        return "default"


class SearchProcessNode(MonitoredNode):
    """Normalize and deduplicate search documents."""

    def prep(self, shared):
        search = shared.get("search", {})
        return list(search.get("raw_results", []))

    def exec(self, prep_res):
        docs: List[Dict[str, str]] = []
        seen = set()
        for row in prep_res:
            query = str(row.get("query", "")).strip()
            for item in row.get("results", []) or []:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url", "")).strip()
                title = str(item.get("title", "")).strip()
                snippet = str(item.get("snippet", "")).strip()
                key = url or f"{title}|{snippet}"
                if not key or key in seen:
                    continue
                seen.add(key)
                docs.append(
                    {
                        "query": query,
                        "title": title,
                        "url": url,
                        "snippet": snippet,
                        "date": str(item.get("date", "")).strip(),
                        "source": str(item.get("source", "")).strip(),
                    }
                )
        return {"documents": docs, "count": len(docs)}

    def post(self, shared, prep_res, exec_res):
        search = shared.setdefault("search", {})
        search["documents"] = exec_res.get("documents", [])
        search["documents_count"] = int(exec_res.get("count", 0))
        return "default"


class SearchReflectionNode(MonitoredNode):
    """Assess search coverage and decide whether more rounds are required."""

    MAX_ROUNDS = 2

    def prep(self, shared):
        search = shared.get("search", {})
        loops_cfg = shared.get("config", {}).get("stage2_loops", {}) or {}
        return {
            "round": int(search.get("round", 1)),
            "documents": list(search.get("documents", [])),
            "queries": list(search.get("queries", [])),
            "max_rounds": int(loops_cfg.get("search_reflection_max_rounds", self.MAX_ROUNDS)),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res):
        docs = prep_res.get("documents", [])
        round_num = int(prep_res.get("round", 1))
        max_rounds = max(1, int(prep_res.get("max_rounds", self.MAX_ROUNDS)))
        prompt = f"""请评估搜索结果覆盖度。

- 查询词: {prep_res.get("queries", [])}
- 文档数量: {len(docs)}

输出 JSON：
{{
  "is_sufficient": true/false,
  "missing": ["缺失维度"],
  "query_hints": ["下一轮建议查询词"]
}}
"""
        parsed: Dict[str, Any] = {}
        try:
            resp = call_glm46(
                prompt,
                temperature=0.2,
                enable_reasoning=False,
                timeout=int(prep_res.get("request_timeout_seconds", 120)),
            )
            parsed = _parse_json_payload(resp)
        except Exception:
            parsed = {}

        missing = list(parsed.get("missing", []) or [])
        query_hints = _normalize_queries(parsed.get("query_hints", []) or [], limit=5)
        is_sufficient = bool(parsed.get("is_sufficient", False))
        if not parsed:
            # Fallback heuristic for non-LLM or parsing failures.
            is_sufficient = len(docs) >= 4 or round_num >= max_rounds
            if not is_sufficient:
                missing = ["official_responses", "event_timeline"]
                query_hints = _normalize_queries(
                    [
                        "事件 官方回应",
                        "事件 最新进展",
                    ],
                    limit=5,
                )

        if round_num >= max_rounds:
            is_sufficient = True

        return {
            "round": round_num,
            "is_sufficient": is_sufficient,
            "missing": missing,
            "query_hints": query_hints,
            "documents_count": len(docs),
            "max_rounds": max_rounds,
        }

    def post(self, shared, prep_res, exec_res):
        trace = shared.setdefault("trace", {})
        trace.setdefault("search_reflections", []).append(dict(exec_res))

        search = shared.setdefault("search", {})
        search.setdefault("reflections", []).append(dict(exec_res))
        search["last_missing"] = list(exec_res.get("missing", []) or [])
        if exec_res.get("query_hints"):
            search["queries"] = _normalize_queries(exec_res["query_hints"], limit=5)

        max_rounds = int(exec_res.get("max_rounds", self.MAX_ROUNDS))
        termination_reason = "continue"
        if exec_res.get("is_sufficient"):
            if int(exec_res.get("round", 0)) >= max_rounds:
                termination_reason = "max_rounds_reached"
            else:
                termination_reason = "sufficient"

        trace.setdefault("loop_status", {})["search_reflection"] = {
            "current": int(exec_res.get("round", 0)),
            "max": max_rounds,
            "termination_reason": termination_reason,
        }

        if exec_res.get("is_sufficient"):
            return "sufficient"
        return "need_more"


class SearchSummaryNode(MonitoredNode):
    """Generate structured search summary for SearchAgent input."""

    def prep(self, shared):
        search = shared.get("search", {})
        return {
            "documents": list(search.get("documents", [])),
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res):
        docs = prep_res.get("documents", [])
        request_timeout_seconds = int(prep_res.get("request_timeout_seconds", 120))
        prompt = f"""基于搜索文档生成结构化背景摘要。

数据概况:
{prep_res.get("data_summary", "")}

搜索文档（最多10条）:
{json.dumps(docs[:10], ensure_ascii=False)}

输出 JSON:
{{
  "event_timeline": [...],
  "key_actors": [...],
  "official_responses": [...],
  "public_reactions_summary": "...",
  "related_events": [...]
}}
"""
        try:
            llm_resp = call_glm46(
                prompt,
                temperature=0.3,
                enable_reasoning=False,
                timeout=request_timeout_seconds,
            )
            parsed = _parse_json_payload(llm_resp)
        except TimeoutError as exc:
            _ = exc
            parsed = {}
        except Exception as exc:
            _ = exc
            parsed = {}

        if not parsed:
            parsed = _default_summary(docs)

        return {
            "event_timeline": list(parsed.get("event_timeline", []) or []),
            "key_actors": list(parsed.get("key_actors", []) or []),
            "official_responses": list(parsed.get("official_responses", []) or []),
            "public_reactions_summary": str(parsed.get("public_reactions_summary", "") or ""),
            "related_events": list(parsed.get("related_events", []) or []),
        }

    def post(self, shared, prep_res, exec_res):
        shared["search_results"] = dict(exec_res)
        return "default"


def create_query_search_flow() -> AsyncFlow:
    extract = ExtractQueriesNode()
    search = WebSearchNode()
    process = SearchProcessNode()
    reflection = SearchReflectionNode()
    summary = SearchSummaryNode()

    extract >> search
    search >> process
    process >> reflection
    reflection - "need_more" >> extract
    reflection - "sufficient" >> summary

    return AsyncFlow(start=extract)


__all__ = [
    "ExtractQueriesNode",
    "WebSearchNode",
    "SearchProcessNode",
    "SearchReflectionNode",
    "SearchSummaryNode",
    "create_query_search_flow",
]
