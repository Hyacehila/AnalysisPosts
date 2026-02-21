"""
Unified Stage3 chapter generation node.
"""
import asyncio
from typing import Any, Dict, List

from nodes.base import AsyncParallelBatchNode
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


class GenerateChaptersBatchNode(AsyncParallelBatchNode):
    """Generate chapter drafts in parallel from outline."""

    async def prep_async(self, shared: Dict[str, Any]) -> List[Dict[str, Any]]:
        stage3_results = shared.setdefault("stage3_results", {})
        outline = stage3_results.get("outline", {})
        chapters = list(outline.get("chapters", []) or [])

        stage3_data = shared.get("stage3_data", {})
        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        chart_index = {str(c.get("id", "")): c for c in charts}
        feedback_map = stage3_results.get("chapter_feedback", {}) or {}
        use_reasoning = reasoning_enabled_stage3(shared)
        request_timeout_seconds = llm_request_timeout(shared)

        chapter_items: List[Dict[str, Any]] = []
        for chapter in chapters:
            chapter_id = str(chapter.get("id", "")).strip()
            relevant_ids = [str(cid) for cid in chapter.get("relevant_charts", []) if str(cid).strip()]
            relevant_charts = [chart_index[cid] for cid in relevant_ids if cid in chart_index]

            item = dict(chapter)
            item["_feedback"] = str(feedback_map.get(chapter_id, "")).strip()
            item["_relevant_charts"] = relevant_charts
            item["_insights"] = stage3_data.get("insights", {})
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
        use_reasoning = bool(prep_res.get("_reasoning_enabled_stage3", False))
        request_timeout_seconds = int(prep_res.get("_request_timeout_seconds", 120))

        prompt = (
            f"请撰写舆情分析报告章节。\n"
            f"章节ID: {chapter_id}\n"
            f"章节标题: {title}\n"
            f"目标字数: {target_words}\n"
            f"关键数据点: {key_data}\n"
            f"可用图表: {[c.get('id') for c in relevant_charts]}\n"
            "要求：\n"
            "1. 内容结构完整，数据驱动。\n"
            "2. 若引用图表，使用 Markdown 图片格式 ![标题](./images/文件名)。\n"
            "3. 输出仅为章节正文。\n"
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
            content = f"{title}\n\n章节生成失败，已降级输出。错误: {exc}"

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
