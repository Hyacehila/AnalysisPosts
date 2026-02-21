"""
Unified Stage3 chapter generation node.
"""
import asyncio
import json
from typing import Any, Dict, List

from nodes.base import AsyncParallelBatchNode
from nodes.stage3.evidence_cards import build_evidence_cards
from utils.call_llm import call_glm46
from utils.llm_modes import llm_request_timeout, reasoning_enabled_stage3


def _coerce_target_words(value: Any, default: int = 300) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        if digits:
            try:
                return int(digits)
            except ValueError:
                return default
        return default


def _build_chart_context(charts: List[Dict[str, Any]], *, limit: int = 4) -> str:
    lines: List[str] = []
    for chart in charts[:limit]:
        if not isinstance(chart, dict):
            continue
        chart_id = str(chart.get("id") or chart.get("chart_id") or "").strip()
        title = str(chart.get("title") or chart.get("chart_title") or chart_id or "图表").strip()
        analysis = str(chart.get("analysis_content") or chart.get("analysis") or "").strip()
        lines.append(f"- {title} ({chart_id or 'n/a'})")
        if analysis:
            lines.append(f"  分析: {analysis[:380]}")
    return "\n".join(lines).strip()


def _build_evidence_catalog(cards: List[Dict[str, str]], *, limit: int = 8) -> str:
    if not cards:
        return "无"
    lines: List[str] = []
    for card in cards[:limit]:
        cid = str(card.get("id", "")).strip() or "E?"
        source = str(card.get("source", "unknown")).strip()
        evidence = str(card.get("evidence", "")).strip()
        confidence = str(card.get("confidence", "中")).strip() or "中"
        reason = str(card.get("reason", "")).strip()
        lines.append(f"- [{cid}] 来源: {source}")
        lines.append(f"  证据: {evidence[:220]}")
        lines.append(f"  置信度: {confidence}；理由: {reason[:180] if reason else '来源与结论方向一致。'}")
    return "\n".join(lines)


class GenerateChaptersBatchNode(AsyncParallelBatchNode):
    """Generate chapter drafts in parallel from outline."""

    async def prep_async(self, shared: Dict[str, Any]) -> List[Dict[str, Any]]:
        stage3_results = shared.setdefault("stage3_results", {})
        outline = stage3_results.get("outline", {})
        chapters = list(outline.get("chapters", []) or [])

        stage3_data = shared.get("stage3_data", {})
        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        chart_index = {str(c.get("id", "")): c for c in charts}
        chart_analyses = stage3_data.get("chart_analyses", {}) or {}
        chart_analysis_index: Dict[str, Dict[str, Any]] = {}
        if isinstance(chart_analyses, dict):
            for key, value in chart_analyses.items():
                if not isinstance(value, dict):
                    continue
                chart_id = str(value.get("chart_id") or key or "").strip()
                if chart_id:
                    chart_analysis_index[chart_id] = value
        feedback_map = stage3_results.get("chapter_feedback", {}) or {}
        use_reasoning = reasoning_enabled_stage3(shared)
        request_timeout_seconds = llm_request_timeout(shared)
        search_context = stage3_data.get("analysis_data", {}).get("search_context", {})
        analysis_context = shared.get("analysis_context", {}) or {}
        analysis_time_range_text = str(analysis_context.get("time_range_text", "")).strip()
        user_analysis_instruction = str(analysis_context.get("user_analysis_instruction", "")).strip()
        evidence_cards = build_evidence_cards(stage3_data.get("trace") or shared.get("trace", {}), limit=12)

        chapter_items: List[Dict[str, Any]] = []
        for chapter in chapters:
            chapter_id = str(chapter.get("id", "")).strip()
            relevant_ids = [str(cid) for cid in chapter.get("relevant_charts", []) if str(cid).strip()]
            relevant_charts = []
            for cid in relevant_ids:
                base_chart = dict(chart_index.get(cid, {}))
                analysis_payload = chart_analysis_index.get(cid, {})
                if analysis_payload:
                    base_chart.setdefault("analysis_content", analysis_payload.get("analysis_content", ""))
                    base_chart.setdefault("analysis", analysis_payload.get("analysis", ""))
                    base_chart.setdefault("chart_title", analysis_payload.get("chart_title", ""))
                if base_chart:
                    relevant_charts.append(base_chart)

            item = dict(chapter)
            item["_feedback"] = str(feedback_map.get(chapter_id, "")).strip()
            item["_relevant_charts"] = relevant_charts
            item["_insights"] = stage3_data.get("insights", {})
            item["_search_context"] = search_context
            item["_analysis_time_range_text"] = analysis_time_range_text
            item["_user_analysis_instruction"] = user_analysis_instruction
            item["_evidence_cards"] = list(evidence_cards)
            item["_reasoning_enabled_stage3"] = use_reasoning
            item["_request_timeout_seconds"] = request_timeout_seconds
            chapter_items.append(item)

        return chapter_items

    async def exec_async(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        chapter_id = str(prep_res.get("id", "")).strip() or "chapter"
        title = str(prep_res.get("title", chapter_id))
        target_words = _coerce_target_words(prep_res.get("target_words", 300), default=300)
        key_data = prep_res.get("key_data", [])
        relevant_charts = prep_res.get("_relevant_charts", [])
        feedback = prep_res.get("_feedback", "")
        insights = prep_res.get("_insights", {}) or {}
        search_context = prep_res.get("_search_context", {}) or {}
        analysis_time_range_text = str(prep_res.get("_analysis_time_range_text", "")).strip()
        user_analysis_instruction = str(prep_res.get("_user_analysis_instruction", "")).strip()
        evidence_cards = list(prep_res.get("_evidence_cards", []) or [])
        use_reasoning = bool(prep_res.get("_reasoning_enabled_stage3", False))
        request_timeout_seconds = int(prep_res.get("_request_timeout_seconds", 120))
        chart_context = _build_chart_context(relevant_charts, limit=5)
        evidence_catalog = _build_evidence_catalog(evidence_cards, limit=8)

        insight_lines = []
        if isinstance(insights, dict):
            for key, value in list(insights.items())[:6]:
                text = str(value or "").strip()
                if text:
                    insight_lines.append(f"- {key}: {text[:260]}")

        prompt = (
            f"请撰写舆情分析报告章节（必须数据可追溯）。\n"
            f"章节ID: {chapter_id}\n"
            f"章节标题: {title}\n"
            f"目标字数: {target_words}\n"
            f"关键数据点: {key_data}\n"
            f"可用图表: {[c.get('id') or c.get('chart_id') for c in relevant_charts]}\n"
            f"图表分析摘要:\n{chart_context if chart_context else '无'}\n"
            f"洞察摘要:\n{chr(10).join(insight_lines) if insight_lines else '无'}\n"
            f"搜索背景:\n{json.dumps(search_context, ensure_ascii=False)[:900] if search_context else '无'}\n"
            f"分析时间范围:\n{analysis_time_range_text or '未知'}\n"
            f"用户分析指令:\n{user_analysis_instruction or '无'}\n"
            f"证据卡片索引:\n{evidence_catalog}\n"
            "要求：\n"
            "1. 内容结构完整，且所有关键判断必须引用上方数据。\n"
            "2. 若引用图表，使用 Markdown 图片格式 ![标题](./images/文件名)。\n"
            "3. 禁止出现[议题A]/[争议点]/[媒体A]等占位符。\n"
            "4. 每个实质性正文段落后，必须紧跟一行“证据说明：...”。\n"
            "5. 证据说明必须包含：证据索引（如 [E1]）、来源、置信度（高/中/低）和置信度理由。\n"
            "6. 证据说明必须是完整可读句子，不能仅列 source。\n"
            "7. 章节内容必须回应用户分析指令，并体现时间范围约束。\n"
            "8. 输出仅为章节正文。\n"
        )
        if feedback:
            prompt += f"\n上轮评审反馈（必须修复）：{feedback}\n"

        try:
            content = await asyncio.to_thread(
                call_glm46,
                prompt,
                0.5,
                enable_reasoning=use_reasoning,
                timeout=request_timeout_seconds,
            )
        except Exception as exc:
            fallback_card = evidence_cards[0] if evidence_cards else {
                "id": "E0",
                "source": "stage3_fallback",
                "confidence": "低",
                "reason": "模型生成失败，使用最小保底文本。",
            }
            content = (
                f"{title}\n\n章节生成失败，已降级输出。错误: {exc}\n\n"
                f"证据说明：该段为降级输出，参考[{fallback_card.get('id', 'E0')}]。"
                f"来源为{fallback_card.get('source', 'stage3_fallback')}。"
                f"置信度：{fallback_card.get('confidence', '低')}。"
                f"理由：{fallback_card.get('reason', '模型生成失败，无法提供更完整证据说明。')}"
            )

        return {
            "id": chapter_id,
            "title": title,
            "content": str(content).strip(),
        }

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(result, dict):
            item["content"] = result.get("content", "")

    async def post_async(
        self,
        shared: Dict[str, Any],
        prep_res: List[Dict[str, Any]],
        exec_res: List[Dict[str, Any]],
    ) -> str:
        ordered: List[Dict[str, Any]] = []
        result_map = {str(item.get("id", "")): item for item in list(exec_res or [])}
        for chapter in prep_res:
            cid = str(chapter.get("id", ""))
            generated = dict(result_map.get(cid, {}))
            if not generated:
                generated = {
                    "id": cid,
                    "title": chapter.get("title", cid),
                    "content": chapter.get("content", ""),
                }
            ordered.append(generated)

        stage3_results = shared.setdefault("stage3_results", {})
        stage3_results["chapters"] = ordered
        stage3_results["generation_mode"] = "unified"
        return "default"
