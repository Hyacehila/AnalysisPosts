"""
Stage2 forum host node (B5).
"""
from __future__ import annotations

import json
import re
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
            "host_narrative": "【事件脉络】当前信息不足以构建完整时间线。"
            "【观点综合】主持人判断盲区仍未闭合。"
            "【深层分析】需要补充更多外部证据。",
            "synthesized_conclusions": [],
        }

    return {
        "cross_analysis": {"agreement": [], "conflicts": []},
        "gaps": [],
        "decision": "sufficient",
        "directive": {},
        "host_narrative": "【事件脉络】已获取的信息链基本完整。"
        "【观点综合】各Agent观点趋于一致。"
        "【深层分析】当前证据已形成闭环，可进入收敛阶段。",
        "synthesized_conclusions": [],
    }


def _extract_event_keywords(data_summary: str, *, limit: int = 6) -> List[str]:
    stopwords = {"分析", "数据", "事件", "舆情", "讨论", "结果", "相关", "话题"}
    counts: Dict[str, int] = {}
    for token in re.findall(r"[\u4e00-\u9fff]{2,10}", str(data_summary or "")):
        if token in stopwords:
            continue
        counts[token] = counts.get(token, 0) + 1
    sorted_tokens = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    return [token for token, _ in sorted_tokens[:limit]]


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
        analysis_context = shared.get("analysis_context", {}) or {}
        return {
            "round": int(forum.get("current_round", 0)),
            "max_rounds": int(config_loops.get("forum_max_rounds", 5)),
            "min_rounds_for_sufficient": int(config_loops.get("forum_min_rounds_for_sufficient", 2)),
            "data_agent_results": shared.get("agent_results", {}).get("data_agent", {}),
            "search_agent_results": shared.get("agent_results", {}).get("search_agent", {}),
            "visual_analyses": list(forum.get("visual_analyses", []) or []),
            "previous_rounds": list(forum.get("rounds", []) or []),
            "debate_logs": list(forum.get("debate_logs", []) or []),
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
            "event_keywords": _extract_event_keywords(shared.get("agent", {}).get("data_summary", "")),
            "analysis_time_range_text": str(analysis_context.get("time_range_text", "")).strip(),
            "user_analysis_instruction": str(analysis_context.get("user_analysis_instruction", "")).strip(),
            "reasoning_enabled_stage2": reasoning_enabled_stage2(shared),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res):
        round_index = int(prep_res.get("round", 0)) + 1
        prompt = f"""你是舆情分析论坛主持人（第{round_index}轮）。你的核心角色不仅是总结者，更是“对抗式审查者”。请对多信源结果做严苛的交叉盘问，给出结构化辩论纪要和下一步动作。

数据摘要：
{prep_res.get("data_summary", "")}

事件关键词：
{prep_res.get("event_keywords", [])}

分析时间范围：
{prep_res.get("analysis_time_range_text") or "未知"}

用户分析指令：
{prep_res.get("user_analysis_instruction") or "无"}

DataAgent结果：
{json.dumps(prep_res.get("data_agent_results", {}), ensure_ascii=False)[:2500]}

SearchAgent结果：
{json.dumps(prep_res.get("search_agent_results", {}), ensure_ascii=False)[:2500]}

视觉分析：
{json.dumps(prep_res.get("visual_analyses", []), ensure_ascii=False)[:1200]}

历史主持人纪要：
{json.dumps(prep_res.get("debate_logs", [])[-4:], ensure_ascii=False)}

输出严格JSON：
{{
  "cross_analysis": {{"agreement": ["多源一致的发现"], "conflicts": ["多源矛盾的发现"]}},
  "gaps": ["尚未覆盖的信息盲区"],
  "decision": "supplement_data|supplement_search|supplement_visual|sufficient",
  "directive": {{}},
  "host_narrative": {{
    "timeline_analysis": "从各Agent发言中识别的关键事件时间线和因果关系，100-200字",
    "viewpoint_synthesis": "综合各视角的共识与分歧分析，指出发现的事实错误或逻辑矛盾，100-200字",
    "deep_analysis": "基于已有信息的深层原因、影响因素和趋势预测，100-200字",
    "guided_questions": ["值得深入探讨的、存在逻辑挑战的关键问题"]
  }},
  "synthesized_conclusions": ["本轮形成的确定性结论"]
}}

要求：
1) 若用户分析指令尚未被现有证据充分覆盖，必须优先输出 supplement_search 并在 directive.queries 中给出明确检索指令。
2) directive.reason 必须解释该动作如何弥补时间线或用户诉求缺口。
3) host_narrative 必须客观概述多Agent观点，并主动寻找和指出其结果中的【事实冲突】或【逻辑漏洞】。对于没有实证支撑的空泛结论，必须提出质疑。
4) cross_analysis.agreement 和 conflicts 必须引用具体的数据事实，禁止泛泛而谈。"""

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
        raw_narrative = parsed.get("host_narrative", "")
        if isinstance(raw_narrative, dict):
            narrative_parts = []
            timeline = str(raw_narrative.get("timeline_analysis", "")).strip()
            viewpoint = str(raw_narrative.get("viewpoint_synthesis", "")).strip()
            deep = str(raw_narrative.get("deep_analysis", "")).strip()
            questions = raw_narrative.get("guided_questions", [])
            if timeline:
                narrative_parts.append(f"【事件脉络】{timeline}")
            if viewpoint:
                narrative_parts.append(f"【观点综合】{viewpoint}")
            if deep:
                narrative_parts.append(f"【深层分析】{deep}")
            if questions:
                q_text = "；".join(str(question) for question in questions[:3])
                narrative_parts.append(f"【引导问题】{q_text}")
            host_narrative = " ".join(narrative_parts)
        else:
            host_narrative = str(raw_narrative).strip()

        return {
            "cross_analysis": parsed.get("cross_analysis", {}),
            "gaps": gaps,
            "decision": decision,
            "directive": directive,
            "host_narrative": host_narrative,
            "synthesized_conclusions": list(parsed.get("synthesized_conclusions", []) or []),
            "confidence_assessments": parsed.get("confidence_assessments", {}),
        }

    def post(self, shared, prep_res, exec_res):
        forum = shared.setdefault(
            "forum",
            {
                "current_round": 0,
                "rounds": [],
                "debate_logs": [],
                "current_directive": {},
                "visual_analyses": [],
            },
        )
        forum["debate_logs"] = list(forum.get("debate_logs", []) or [])
        forum["current_round"] = int(forum.get("current_round", 0)) + 1
        forum["rounds"] = list(forum.get("rounds", []) or [])
        host_narrative = str(exec_res.get("host_narrative", "")).strip()
        if host_narrative:
            forum["debate_logs"].append(host_narrative)
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
                "host_narrative": host_narrative,
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
