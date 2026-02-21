"""
Stage2 forum host node (B5).
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46
from utils.llm_modes import llm_request_timeout, reasoning_enabled_stage2


_VALID_DECISIONS = {
    "supplement_data",
    "supplement_search",
    "supplement_visual",
    "sufficient",
}


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


def _normalize_decision(raw: str) -> str:
    decision = str(raw or "").strip().lower()
    if decision in _VALID_DECISIONS:
        return decision
    return "sufficient"


def _fallback_result(prep_res: Dict[str, Any]) -> Dict[str, Any]:
    search_agent = prep_res.get("search_agent_results", {}) or {}
    blind_spots = list(search_agent.get("blind_spots", []) or [])
    if blind_spots:
        return {
            "cross_analysis": {"agreement": [], "conflicts": []},
            "gaps": blind_spots,
            "decision": "supplement_search",
            "directive": {
                "queries": [f"{spot} 官方回应" for spot in blind_spots[:2]],
                "reason": "盲区仍存在，继续补充外部信息。",
            },
            "synthesized_conclusions": [],
        }

    return {
        "cross_analysis": {"agreement": [], "conflicts": []},
        "gaps": [],
        "decision": "sufficient",
        "directive": {},
        "synthesized_conclusions": [],
    }


def _normalize_directive(decision: str, directive: Dict[str, Any], gaps: List[str]) -> Dict[str, Any]:
    directive = dict(directive or {})
    if decision == "supplement_data":
        tools = directive.get("tools", [])
        if not isinstance(tools, list):
            tools = []
        directive["tools"] = [str(t).strip() for t in tools if str(t).strip()]
        directive.setdefault("reason", "补齐数据侧分析盲区。")
        return directive

    if decision == "supplement_search":
        queries = directive.get("queries", [])
        if not isinstance(queries, list):
            queries = []
        normalized = [str(q).strip() for q in queries if str(q).strip()]
        if not normalized:
            normalized = [f"{gap} 官方回应" for gap in gaps[:2] if str(gap).strip()]
        if not normalized:
            normalized = ["事件 官方回应", "事件 最新进展"]
        directive["queries"] = normalized[:3]
        directive.setdefault("reason", "补齐外部公开信源。")
        return directive

    if decision == "supplement_visual":
        charts = directive.get("charts", [])
        if not isinstance(charts, list):
            charts = []
        normalized = []
        for item in charts:
            if isinstance(item, dict):
                cid = str(item.get("id", "")).strip()
                if cid:
                    normalized.append(cid)
            else:
                cid = str(item).strip()
                if cid:
                    normalized.append(cid)
        directive["charts"] = normalized[:3]
        directive.setdefault("question", "请提取图表中的关键趋势与异常。")
        directive.setdefault("reason", "补齐视觉证据。")
        return directive

    return {}


class ForumHostNode(MonitoredNode):
    """Forum orchestrator for Stage2 dynamic loop."""

    def prep(self, shared):
        forum = shared.setdefault("forum", {})
        config_loops = shared.get("config", {}).get("stage2_loops", {}) or {}
        return {
            "round": int(forum.get("current_round", 0)),
            "max_rounds": int(config_loops.get("forum_max_rounds", 5)),
            "min_rounds_for_sufficient": int(config_loops.get("forum_min_rounds_for_sufficient", 2)),
            "data_agent_results": shared.get("agent_results", {}).get("data_agent", {}),
            "search_agent_results": shared.get("agent_results", {}).get("search_agent", {}),
            "visual_analyses": list(forum.get("visual_analyses", []) or []),
            "previous_rounds": list(forum.get("rounds", []) or []),
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
            "reasoning_enabled_stage2": reasoning_enabled_stage2(shared),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res):
        round_index = int(prep_res.get("round", 0)) + 1
        prompt = f"""你是舆情分析论坛主持人（第{round_index}轮）。请对双信源结果做交叉评估并给出下一步动作。

数据摘要：
{prep_res.get("data_summary", "")}

DataAgent结果：
{json.dumps(prep_res.get("data_agent_results", {}), ensure_ascii=False)[:2500]}

SearchAgent结果：
{json.dumps(prep_res.get("search_agent_results", {}), ensure_ascii=False)[:2500]}

视觉分析：
{json.dumps(prep_res.get("visual_analyses", []), ensure_ascii=False)[:1200]}

输出严格JSON：
{{
  "cross_analysis": {{"agreement": [], "conflicts": []}},
  "gaps": [],
  "decision": "supplement_data|supplement_search|supplement_visual|sufficient",
  "directive": {{}},
  "synthesized_conclusions": []
}}"""

        try:
            resp = call_glm46(
                prompt,
                temperature=0.4,
                enable_reasoning=bool(prep_res.get("reasoning_enabled_stage2", False)),
                timeout=int(prep_res.get("request_timeout_seconds", 120)),
            )
            parsed = _parse_json_payload(resp)
        except Exception:
            parsed = {}

        if not parsed:
            parsed = _fallback_result(prep_res)

        decision = _normalize_decision(parsed.get("decision"))
        gaps = list(parsed.get("gaps", []) or [])
        directive = _normalize_directive(decision, parsed.get("directive", {}), gaps)

        return {
            "cross_analysis": parsed.get("cross_analysis", {}),
            "gaps": gaps,
            "decision": decision,
            "directive": directive,
            "synthesized_conclusions": list(parsed.get("synthesized_conclusions", []) or []),
            "confidence_assessments": parsed.get("confidence_assessments", {}),
        }

    def post(self, shared, prep_res, exec_res):
        forum = shared.setdefault(
            "forum",
            {
                "current_round": 0,
                "rounds": [],
                "current_directive": {},
                "visual_analyses": [],
            },
        )
        forum["current_round"] = int(forum.get("current_round", 0)) + 1
        forum["rounds"] = list(forum.get("rounds", []) or [])
        forum["rounds"].append(
            {
                "round": forum["current_round"],
                "summary": dict(exec_res),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            }
        )

        action = _normalize_decision(exec_res.get("decision"))
        max_rounds = int(prep_res.get("max_rounds", 5))
        min_rounds = int(prep_res.get("min_rounds_for_sufficient", 2))
        termination_reason = "continue"

        if forum["current_round"] >= max_rounds:
            action = "sufficient"
            termination_reason = "max_rounds_reached"
        elif action == "sufficient" and forum["current_round"] < min_rounds:
            action = "supplement_search"
            termination_reason = "continue"
        elif action == "sufficient":
            termination_reason = "forum_host_sufficient"

        directive = dict(exec_res.get("directive", {}) or {})
        if action == "supplement_search":
            directive = _normalize_directive(
                "supplement_search",
                directive,
                list(exec_res.get("gaps", []) or []),
            )
        elif action == "supplement_data":
            directive = _normalize_directive(
                "supplement_data",
                directive,
                list(exec_res.get("gaps", []) or []),
            )
        elif action == "supplement_visual":
            directive = _normalize_directive(
                "supplement_visual",
                directive,
                list(exec_res.get("gaps", []) or []),
            )
        else:
            directive = {}

        forum["current_directive"] = directive

        trace = shared.setdefault("trace", {})
        trace.setdefault("forum_rounds", []).append(
            {
                "round": forum["current_round"],
                "decision": action,
                "directive": directive,
                "gaps": list(exec_res.get("gaps", []) or []),
                "synthesized_conclusions": list(exec_res.get("synthesized_conclusions", []) or []),
            }
        )
        loop_status = trace.setdefault("loop_status", {})
        loop_status["forum"] = {
            "current": forum["current_round"],
            "max": max_rounds,
            "termination_reason": termination_reason,
        }
        return action


__all__ = ["ForumHostNode"]
