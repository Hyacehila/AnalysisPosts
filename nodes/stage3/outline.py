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
        "subtitle": "",
        "hero": {"summary": "", "highlights": [], "actions": []},
        "chapters": [
            {
                "id": "ch01",
                "title": "执行摘要",
                "target_words": 300,
                "key_data": ["summary", *list((insights or {}).keys())[:2]],
                "relevant_charts": key_ids[:1],
                "allowSwot": False,
                "allowPest": False,
                "description": "全局概览",
            },
            {
                "id": "ch02",
                "title": "趋势与结构分析",
                "target_words": 600,
                "key_data": ["sentiment", "topic", "trend"],
                "relevant_charts": key_ids[:2],
                "allowSwot": True,
                "allowPest": False,
                "description": "详细分析",
            },
            {
                "id": "ch03",
                "title": "风险研判与建议",
                "target_words": 400,
                "key_data": ["risk", "recommendation"],
                "relevant_charts": key_ids[2:4],
                "allowSwot": False,
                "allowPest": False,
                "description": "研判与建议",
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


class PlanOutlineNode(MonitoredNode):
    """Plan report outline from Stage2 outputs."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_data = shared.get("stage3_data", {})
        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        insights = stage3_data.get("insights", {})
        search_context = stage3_data.get("analysis_data", {}).get("search_context", {})
        trace = stage3_data.get("trace") or shared.get("trace", {})
        analysis_context = shared.get("analysis_context", {}) or {}
        forum_rounds = trace.get("forum_rounds", [])
        data_summary = shared.get("agent", {}).get("data_summary", {})
        keywords = _extract_keywords(
            json.dumps(insights, ensure_ascii=False),
            json.dumps(search_context, ensure_ascii=False),
            json.dumps(data_summary, ensure_ascii=False),
        )
        time_range_text = str(analysis_context.get("time_range_text", "")).strip()
        user_analysis_instruction = str(analysis_context.get("user_analysis_instruction", "")).strip()

        return {
            "charts": charts,
            "insights": insights,
            "forum_rounds": forum_rounds,
            "search_context": search_context,
            "data_summary": data_summary,
            "event_keywords": keywords,
            "analysis_time_range_text": time_range_text,
            "user_analysis_instruction": user_analysis_instruction,
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
            "你是报告首席设计官，需要结合分析引擎的内容，为整本报告确定最终的标题、导语区、"
            "目录样式与篇幅分配。\n\n"
            "输入包含洞察摘要、图表ID列表、论坛轮次信息以及用户分析指令。\n\n"
            "目标：\n"
            "1. 生成具有中文叙事风格的 title 和 subtitle，确保可直接放在封面中央。\n"
            "2. 给出 hero 对象：包含 summary（100字以内的全局概括）、"
            "highlights（3-5个要点短句数组）、actions（2-3条行动建议数组）。\n"
            "3. 输出 chapters 数组作为 tocPlan，每个元素必须包含：\n"
            "   - id: 章节标识符（如 ch01）\n"
            "   - title: 中文章节标题，必须贴合真实事件\n"
            "   - target_words: 该章节目标字数（整数，所有章节合计应在 1500-3000 之间）\n"
            "   - key_data: 该章节应当引用的数据维度数组\n"
            "   - relevant_charts: 该章节关联的图表 ID 数组\n"
            "   - allowSwot: 布尔值，是否允许该章节使用 SWOT 分析块\n"
            "   - allowPest: 布尔值，是否允许该章节使用 PEST 分析块\n"
            "   - description: 对该章节详略程度的简要说明\n"
            "4. SWOT 块使用规则：\n"
            "   - 全文最多只允许一个章节设置 allowSwot: true\n"
            "   - 其他章节必须设置 allowSwot: false\n"
            "   - 仅在章节主题确实涉及内部优势/劣势/机会/威胁时才允许\n"
            "5. PEST 块使用规则：\n"
            "   - 全文最多只允许一个章节设置 allowPest: true\n"
            "   - SWOT 和 PEST 不应出现在同一章节\n"
            "6. 章节组织必须显式回应用户分析指令。\n"
            "7. 禁止使用[议题A]/[争议点]/[媒体A]等占位符。\n\n"
            f"图表数量: {len(charts)}\n"
            f"图表ID候选: {chart_ids[:12]}\n"
            f"事件关键词: {prep_res.get('event_keywords', [])}\n"
            f"分析时间范围: {prep_res.get('analysis_time_range_text') or '未知'}\n"
            f"用户分析指令: {prep_res.get('user_analysis_instruction') or '无'}\n"
            f"洞察摘要:\n{chr(10).join(insight_preview) if insight_preview else '无'}\n"
            f"论坛轮次: {len(prep_res.get('forum_rounds', []))}\n"
            "输出必须严格满足上述 JSON 结构，只返回 JSON，禁止额外输出。"
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
            for chapter in chapters:
                if not isinstance(chapter, dict):
                    continue
                chapter.setdefault("allowSwot", False)
                chapter.setdefault("allowPest", False)
                chapter.setdefault("target_words", 300)
                chapter.setdefault("description", "")

            swot_count = sum(1 for chapter in chapters if isinstance(chapter, dict) and chapter.get("allowSwot"))
            pest_count = sum(1 for chapter in chapters if isinstance(chapter, dict) and chapter.get("allowPest"))
            if swot_count > 1:
                found = False
                for chapter in chapters:
                    if not isinstance(chapter, dict) or not chapter.get("allowSwot"):
                        continue
                    if found:
                        chapter["allowSwot"] = False
                    found = True
            if pest_count > 1:
                found = False
                for chapter in chapters:
                    if not isinstance(chapter, dict) or not chapter.get("allowPest"):
                        continue
                    if found:
                        chapter["allowPest"] = False
                    found = True
            for chapter in chapters:
                if (
                    isinstance(chapter, dict)
                    and chapter.get("allowSwot")
                    and chapter.get("allowPest")
                ):
                    chapter["allowPest"] = False

            hero = parsed.get("hero", {})
            if not isinstance(hero, dict):
                hero = {}
            hero.setdefault("summary", "")
            hero.setdefault("highlights", [])
            hero.setdefault("actions", [])
            parsed["hero"] = hero
            parsed.setdefault("subtitle", "")
            parsed["chapters"] = chapters
            return parsed
        except Exception:
            return _default_outline(charts, insights)

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, Any]) -> str:
        stage3_results = shared.setdefault("stage3_results", {})
        stage3_results["outline"] = exec_res
        stage3_results["hero"] = exec_res.get("hero", {})
        return "default"
