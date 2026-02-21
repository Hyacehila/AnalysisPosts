"""
Unified Stage3 outline planning node.
"""
import json
import re
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46
from utils.llm_modes import llm_request_timeout, reasoning_enabled_stage3


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


def _extract_keywords(*texts: str, limit: int = 8) -> List[str]:
    stopwords = {
        "分析",
        "数据",
        "报告",
        "结果",
        "显示",
        "事件",
        "舆情",
        "摘要",
        "整体",
        "相关",
        "趋势",
    }
    counts: Dict[str, int] = {}
    for text in texts:
        for token in re.findall(r"[\u4e00-\u9fff]{2,10}", str(text or "")):
            if token in stopwords:
                continue
            counts[token] = counts.get(token, 0) + 1
    sorted_tokens = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    return [token for token, _ in sorted_tokens[:limit]]


def _extract_dates(*texts: str, limit: int = 6) -> List[str]:
    seen = set()
    ordered = []
    for text in texts:
        for date in re.findall(r"\d{4}-\d{2}-\d{2}", str(text or "")):
            if date in seen:
                continue
            seen.add(date)
            ordered.append(date)
            if len(ordered) >= limit:
                return ordered
    return ordered


class PlanOutlineNode(MonitoredNode):
    """Plan report outline from Stage2 outputs."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_data = shared.get("stage3_data", {})
        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        insights = stage3_data.get("insights", {})
        search_context = stage3_data.get("analysis_data", {}).get("search_context", {})
        trace = stage3_data.get("trace") or shared.get("trace", {})
        forum_rounds = trace.get("forum_rounds", [])
        data_summary = shared.get("agent", {}).get("data_summary", {})
        keywords = _extract_keywords(
            json.dumps(insights, ensure_ascii=False),
            json.dumps(search_context, ensure_ascii=False),
            json.dumps(data_summary, ensure_ascii=False),
        )
        dates = _extract_dates(
            json.dumps(insights, ensure_ascii=False),
            json.dumps(search_context, ensure_ascii=False),
        )

        return {
            "charts": charts,
            "insights": insights,
            "forum_rounds": forum_rounds,
            "search_context": search_context,
            "data_summary": data_summary,
            "event_keywords": keywords,
            "event_dates": dates,
            "reasoning_enabled_stage3": reasoning_enabled_stage3(shared),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        charts = prep_res.get("charts", [])
        insights = prep_res.get("insights", {})
        insight_preview = []
        if isinstance(insights, dict):
            for key, value in list(insights.items())[:6]:
                text = str(value or "").strip()
                if text:
                    insight_preview.append(f"- {key}: {text[:260]}")
        chart_ids = [str(c.get("id", "")).strip() for c in charts if isinstance(c, dict)]

        prompt = (
            "你是资深舆情分析总编。请基于真实分析数据规划报告大纲，输出 JSON。\n"
            "要求字段: title, chapters[]。每个 chapter 至少含 id/title/target_words/key_data/relevant_charts。\n"
            f"图表数量: {len(charts)}\n"
            f"图表ID候选: {chart_ids[:12]}\n"
            f"事件关键词: {prep_res.get('event_keywords', [])}\n"
            f"时间线线索: {prep_res.get('event_dates', [])}\n"
            f"洞察摘要:\n{chr(10).join(insight_preview) if insight_preview else '无'}\n"
            f"论坛轮次: {len(prep_res.get('forum_rounds', []))}\n"
            "章节要求：\n"
            "1. 标题必须贴合真实事件，不得泛化。\n"
            "2. 禁止使用[议题A]/[争议点]/[媒体A]等占位符。\n"
            "3. 每章 relevant_charts 必须给出可用图表ID。\n"
            "仅输出 JSON 对象。"
        )

        try:
            raw = call_glm46(
                prompt,
                temperature=0.3,
                enable_reasoning=bool(prep_res.get("reasoning_enabled_stage3", False)),
                timeout=int(prep_res.get("request_timeout_seconds", 120)),
            )
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
