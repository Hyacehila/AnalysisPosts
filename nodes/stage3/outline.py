"""
Unified Stage3 outline planning node.
"""
import json
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46


def _safe_json_loads(text: str) -> Dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        raise ValueError("empty json payload")
    if "```" in payload:
        payload = payload.replace("```json", "").replace("```", "").strip()
    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        payload = payload[start : end + 1]
    return json.loads(payload)


def _default_outline(charts: List[Dict[str, Any]], insights: Dict[str, Any]) -> Dict[str, Any]:
    chart_ids = [str(c.get("id", "")).strip() for c in charts if str(c.get("id", "")).strip()]
    key_ids = chart_ids[:4]
    return {
        "title": "舆情分析统一报告",
        "chapters": [
            {
                "id": "ch01",
                "title": "执行摘要",
                "target_words": 300,
                "key_data": ["summary", *list((insights or {}).keys())[:2]],
                "relevant_charts": key_ids[:1],
            },
            {
                "id": "ch02",
                "title": "趋势与结构分析",
                "target_words": 600,
                "key_data": ["sentiment", "topic", "trend"],
                "relevant_charts": key_ids[:2],
            },
            {
                "id": "ch03",
                "title": "风险研判与建议",
                "target_words": 400,
                "key_data": ["risk", "recommendation"],
                "relevant_charts": key_ids[2:4],
            },
        ],
    }


class PlanOutlineNode(MonitoredNode):
    """Plan report outline from Stage2 outputs."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_data = shared.get("stage3_data", {})
        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        insights = stage3_data.get("insights", {})
        trace = stage3_data.get("trace") or shared.get("trace", {})
        forum_rounds = trace.get("forum_rounds", [])

        return {
            "charts": charts,
            "insights": insights,
            "forum_rounds": forum_rounds,
            "data_summary": shared.get("agent", {}).get("data_summary", {}),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        charts = prep_res.get("charts", [])
        insights = prep_res.get("insights", {})

        prompt = (
            "你是舆情分析报告专家。请规划统一报告大纲，输出 JSON。\n"
            "要求字段: title, chapters[]。每个 chapter 至少含 id/title/target_words/key_data/relevant_charts。\n"
            f"图表数量: {len(charts)}\n"
            f"洞察键: {list((insights or {}).keys())}\n"
            f"论坛轮次: {len(prep_res.get('forum_rounds', []))}\n"
            "仅输出 JSON 对象。"
        )

        try:
            raw = call_glm46(prompt, temperature=0.3)
            parsed = _safe_json_loads(raw)
            chapters = parsed.get("chapters")
            if not isinstance(chapters, list) or not chapters:
                raise ValueError("invalid chapters")
            return parsed
        except Exception:
            return _default_outline(charts, insights)

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, Any]) -> str:
        shared.setdefault("stage3_results", {})["outline"] = exec_res
        return "default"
