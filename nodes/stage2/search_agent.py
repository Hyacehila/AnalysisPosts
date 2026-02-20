"""
Stage2 SearchAgent node (B2).
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46


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


def _fallback_result(search_results: Dict[str, Any]) -> Dict[str, Any]:
    timeline = list(search_results.get("event_timeline", []) or [])
    actors = list(search_results.get("key_actors", []) or [])
    return {
        "background_context": (
            "搜索信息已整理，但结构化提炼受限，建议结合时间线与关键主体做人工复核。"
        ),
        "consistency_points": timeline[:2],
        "conflict_points": [],
        "blind_spots": ["缺少可机读的外部证据链，需追加检索"],
        "recommended_followups": actors[:3],
    }


class SearchAgentNode(MonitoredNode):
    """Analyze search-derived context and produce cross-source findings."""

    def prep(self, shared):
        return {
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
            "search_results": shared.get("search_results", {}),
        }

    def exec(self, prep_res):
        prompt = f"""你是 SearchAgent。请基于搜索结果与数据摘要输出结构化分析。

## 数据摘要
{prep_res.get("data_summary", "")}

## 搜索结果
{json.dumps(prep_res.get("search_results", {}), ensure_ascii=False)}

输出 JSON：
{{
  "background_context": "背景补充",
  "consistency_points": ["与数据集一致的点"],
  "conflict_points": ["潜在矛盾点"],
  "blind_spots": ["信息盲区"],
  "recommended_followups": ["后续行动建议"]
}}
"""
        parsed: Dict[str, Any] = {}
        try:
            llm_resp = call_glm46(prompt, temperature=0.4, enable_reasoning=True)
            parsed = _parse_json_payload(llm_resp)
        except Exception:
            parsed = {}

        if not parsed:
            parsed = _fallback_result(prep_res.get("search_results", {}))

        return {
            "background_context": str(parsed.get("background_context", "")),
            "consistency_points": list(parsed.get("consistency_points", []) or []),
            "conflict_points": list(parsed.get("conflict_points", []) or []),
            "blind_spots": list(parsed.get("blind_spots", []) or []),
            "recommended_followups": list(parsed.get("recommended_followups", []) or []),
        }

    def post(self, shared, prep_res, exec_res):
        shared.setdefault("agent_results", {})["search_agent"] = dict(exec_res)
        trace = shared.setdefault("trace", {})
        trace.setdefault("search_agent_analysis", []).append(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                **dict(exec_res),
            }
        )
        return "default"


__all__ = ["SearchAgentNode"]
