"""
Unified Stage3 chapter generation node.
"""
import asyncio
import json
import re
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


def _sanitize_bracket_citations(text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        token = str(match.group(1) or "").strip()
        if not token:
            return match.group(0)
        evidence_ids = re.findall(r"E\d+", token)
        if evidence_ids:
            # Normalize forms like [E1, E2] into [E1][E2]
            return "".join(f"[{eid}]" for eid in evidence_ids)
        # Drop bracket wrappers for machine field names, e.g. [topic_distribution].
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{1,63}", token):
            return token
        return match.group(0)

    return re.sub(r"\[([^\]\n]{1,64})\](?!\()", _replace, str(text or ""))


def _normalize_heading_text(text: str) -> str:
    normalized = re.sub(r"\s+", "", str(text or "").strip().lower())
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", normalized)


def _sanitize_blocks(blocks: List[Dict[str, Any]], chapter_title: str) -> List[Dict[str, Any]]:
    sanitized: List[Dict[str, Any]] = []
    chapter_norm = _normalize_heading_text(chapter_title)
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = str(block.get("type", "")).strip()

        if block_type == "heading":
            text = _sanitize_bracket_citations(str(block.get("text", "")).strip())
            if not text:
                continue
            if chapter_norm and _normalize_heading_text(text) == chapter_norm:
                continue
            # 超长 heading 往往是模型误输出正文，降级为段落。
            if len(text) > 48:
                sanitized.append(
                    {"type": "paragraph", "inlines": [{"text": text, "marks": []}]}
                )
                continue
            heading = dict(block)
            heading["text"] = text
            sanitized.append(heading)
            continue

        if block_type == "paragraph":
            paragraph = dict(block)
            inlines = paragraph.get("inlines", [])
            normalized_inlines = []
            if isinstance(inlines, list):
                for run in inlines:
                    if not isinstance(run, dict):
                        continue
                    new_run = dict(run)
                    new_run["text"] = _sanitize_bracket_citations(str(run.get("text", "")))
                    normalized_inlines.append(new_run)
            if not normalized_inlines:
                text = _sanitize_bracket_citations(
                    str(paragraph.get("text", paragraph.get("content", ""))).strip()
                )
                if text:
                    normalized_inlines = [{"text": text, "marks": []}]
            if not normalized_inlines:
                continue
            paragraph["inlines"] = normalized_inlines
            paragraph.pop("text", None)
            paragraph.pop("content", None)
            sanitized.append(paragraph)
            continue

        if block_type == "list":
            block_copy = dict(block)
            items = block_copy.get("items", [])
            if isinstance(items, list):
                block_copy["items"] = [
                    _sanitize_bracket_citations(str(item))
                    for item in items
                    if str(item).strip()
                ]
            sanitized.append(block_copy)
            continue

        if block_type == "table":
            block_copy = dict(block)
            headers = block_copy.get("headers", [])
            rows = block_copy.get("rows", [])
            if isinstance(headers, list):
                block_copy["headers"] = [_sanitize_bracket_citations(str(h)) for h in headers]
            if isinstance(rows, list):
                normalized_rows = []
                for row in rows:
                    if isinstance(row, list):
                        normalized_rows.append(
                            [_sanitize_bracket_citations(str(cell)) for cell in row]
                        )
                block_copy["rows"] = normalized_rows
            sanitized.append(block_copy)
            continue

        if block_type == "engineQuote":
            block_copy = dict(block)
            sub_blocks = block_copy.get("blocks", [])
            if isinstance(sub_blocks, list):
                block_copy["blocks"] = _sanitize_blocks(sub_blocks, "")
            sanitized.append(block_copy)
            continue

        if block_type == "swotTable":
            block_copy = dict(block)
            for dim in ("strengths", "weaknesses", "opportunities", "threats"):
                entries = block_copy.get(dim, [])
                if not isinstance(entries, list):
                    block_copy[dim] = []
                    continue
                normalized_entries = []
                for entry in entries:
                    if isinstance(entry, dict):
                        normalized_entries.append(
                            {
                                "point": _sanitize_bracket_citations(str(entry.get("point", ""))),
                                "detail": _sanitize_bracket_citations(str(entry.get("detail", ""))),
                            }
                        )
                block_copy[dim] = normalized_entries
            sanitized.append(block_copy)
            continue

        if block_type == "pestTable":
            block_copy = dict(block)
            for dim in ("political", "economic", "social", "technological"):
                entries = block_copy.get(dim, [])
                if not isinstance(entries, list):
                    block_copy[dim] = []
                    continue
                normalized_entries = []
                for entry in entries:
                    if isinstance(entry, dict):
                        normalized_entries.append(
                            {
                                "factor": _sanitize_bracket_citations(str(entry.get("factor", ""))),
                                "detail": _sanitize_bracket_citations(str(entry.get("detail", ""))),
                            }
                        )
                block_copy[dim] = normalized_entries
            sanitized.append(block_copy)
            continue

        if block_type == "image":
            block_copy = dict(block)
            block_copy["alt"] = _sanitize_bracket_citations(str(block_copy.get("alt", "")))
            sanitized.append(block_copy)
            continue

        sanitized.append(dict(block))
    return sanitized


class GenerateChaptersBatchNode(AsyncParallelBatchNode):
    """Generate chapter drafts in parallel from outline."""

    def __init__(self, **kwargs):
        kwargs.setdefault("max_concurrent", 3)
        super().__init__(**kwargs)

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
        debate_logs = list(search_context.get("forum_debate_logs", []) or [])

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
            item["_debate_logs"] = list(debate_logs)
            item["_allowSwot"] = bool(chapter.get("allowSwot", False))
            item["_allowPest"] = bool(chapter.get("allowPest", False))
            item["_chapter_description"] = str(chapter.get("description", "")).strip()
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
        debate_logs = [str(item).strip() for item in list(prep_res.get("_debate_logs", []) or []) if str(item).strip()]
        allow_swot = bool(prep_res.get("_allowSwot", False))
        allow_pest = bool(prep_res.get("_allowPest", False))
        chapter_description = str(prep_res.get("_chapter_description", "")).strip()
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
            f"你是 Report Engine 的章节装配工厂。请把以下章节的素材精确装配成"
            f"符合 JSON 契约的章节数据。\n\n"
            f"章节ID: {chapter_id}\n"
            f"章节标题: {title}\n"
            f"章节定位说明: {chapter_description or '按数据内容自行规划'}\n"
            f"目标字数: {target_words}\n"
            f"关键数据点: {key_data}\n"
            f"可用图表: {[c.get('id') or c.get('chart_id') for c in relevant_charts]}\n"
            f"图表分析摘要:\n{chart_context if chart_context else '无'}\n"
            f"洞察摘要:\n{chr(10).join(insight_lines) if insight_lines else '无'}\n"
            f"搜索背景:\n{json.dumps(search_context, ensure_ascii=False)[:900] if search_context else '无'}\n"
            f"分析时间范围:\n{analysis_time_range_text or '未知'}\n"
            f"用户分析指令:\n{user_analysis_instruction or '无'}\n"
            f"论坛讨论纪要:\n{json.dumps(debate_logs[:4], ensure_ascii=False) if debate_logs else '无'}\n"
            f"证据卡片索引:\n{evidence_catalog}\n"
            f"组件权限: allowSwot={allow_swot}, allowPest={allow_pest}\n\n"
            "输出要求（严格 JSON 数组，每个元素是一个 block）：\n"
            "1. 仅使用以下 block 类型:\n"
            "   - paragraph: 正文段落，内含 inlines 数组\n"
            "   - list: 列表，含 items 字符串数组\n"
            "   - table: 表格，含 headers 和 rows\n"
            "   - swotTable: SWOT 分析，含 strengths/weaknesses/opportunities/threats 四个数组，"
            "每个元素含 point 和 detail 字段\n"
            "   - pestTable: PEST 分析，含 political/economic/social/technological 四个数组，"
            "每个元素含 factor 和 detail 字段\n"
            "   - engineQuote: 智能体语录引用块\n"
            "   - heading: 子标题（仅限三级以下，禁止输出与章节标题同名的标题）\n"
            "   - image: 图表引用，含 src 和 alt\n\n"
            "2. paragraph 结构示例:\n"
            '   {"type": "paragraph", "inlines": [\n'
            '     {"text": "根据数据分析，", "marks": []},\n'
            '     {"text": "特定情绪占比达 42%", "marks": [{"type": "bold"}]},\n'
            '     {"text": "，较上月上升 5 个百分点 [E1]。", "marks": []}\n'
            "   ]}\n\n"
            "3. engineQuote 结构示例:\n"
            '   {"type": "engineQuote", "engine": "insight", '
            '"title": "Insight Agent", "blocks": [\n'
            '     {"type": "paragraph", "inlines": [{"text": "原话内容...", "marks": []}]}\n'
            "   ]}\n"
            "   - engine 可选值: insight, media, query\n"
            "   - 仅当论坛讨论纪要或搜索背景中确实有智能体观点值得直接引用时才使用\n"
            "   - 严禁臆造内容或将图表数据改写进 engineQuote\n\n"
            "4. SWOT/PEST 限制:\n"
            f"   - allowSwot={allow_swot}：{'允许使用 swotTable 块' if allow_swot else '禁止使用 swotTable 块'}\n"
            f"   - allowPest={allow_pest}：{'允许使用 pestTable 块' if allow_pest else '禁止使用 pestTable 块'}\n"
            "   - 违反此约束的输出将被自动丢弃重写\n\n"
            "5. 引用规范:\n"
            "   - 事实与数据句末必须在 inlines 中对应 text 里包含角标引用 [E1]、[E2]\n"
            "   - 禁止另起「证据说明」段落\n"
            "   - 多个证据写成 [E1][E2]\n\n"
            "6. 其他禁令:\n"
            "   - 禁止出现 [主题A]/[讨论焦点]/[来源A] 等占位符\n"
            "   - 禁止输出与章节标题同名的 heading\n"
            "   - 不得新增输入中不存在的专有名词\n"
            "   - 引用图表使用 image block: {\"type\": \"image\", \"src\": \"./images/文件名\", \"alt\": \"标题\"}\n\n"
            "7. 内容要求与信息密度考核（实质内容保障）:\n"
            "   - 章节必须优先回答用户指令中的核心问题\n"
            "   - 严禁任何无数据、无引用支撑的空泛过渡句，每段话都必须言之有物\n"
            "   - 保持极高的信息密度：每100字至少包含2-3个具体信息点（如数值、图表引用、代表性用户原话等）\n"
            "   - 必要时引用论坛讨论纪要中的共识与不同视角（以 engineQuote 呈现）\n"
            "   - 内容必须体现时间范围约束\n\n"
            "只返回 JSON 数组，格式为 [{block}, {block}, ...]。禁止返回 Markdown 或额外说明。"
        )
        if feedback:
            prompt += f"\n\n上轮评审反馈（必须修复）：{feedback}\n"

        try:
            raw_content = await asyncio.to_thread(
                call_glm46,
                prompt,
                0.5,
                enable_reasoning=use_reasoning,
                timeout=request_timeout_seconds,
            )
            json_text = str(raw_content or "").strip()
            if "```json" in json_text:
                json_text = json_text.split("```json", 1)[1].split("```", 1)[0].strip()
            elif "```" in json_text:
                json_text = json_text.split("```", 1)[1].split("```", 1)[0].strip()

            if json_text.startswith("["):
                blocks = json.loads(json_text)
            elif json_text.startswith("{"):
                obj = json.loads(json_text)
                blocks = obj.get("blocks", [obj]) if isinstance(obj, dict) else []
            else:
                start = json_text.find("[")
                end = json_text.rfind("]")
                if start >= 0 and end > start:
                    blocks = json.loads(json_text[start : end + 1])
                else:
                    blocks = []

            if not isinstance(blocks, list):
                blocks = []
            blocks = [block for block in blocks if isinstance(block, dict)]
            if not allow_swot:
                blocks = [block for block in blocks if block.get("type") != "swotTable"]
            if not allow_pest:
                blocks = [block for block in blocks if block.get("type") != "pestTable"]
            blocks = _sanitize_blocks(blocks, title)
            content = blocks
        except Exception as exc:
            fallback_card = evidence_cards[0] if evidence_cards else {
                "id": "E0",
                "source": "stage3_fallback",
                "confidence": "低", "reason": "模型生成失败。",
            }
            content = [{
                "type": "paragraph",
                "inlines": [{"text": f"章节生成失败，已降级输出。错误: {exc}。"
                             f"参考[{fallback_card.get('id', 'E0')}]。", "marks": []}],
            }]

        return {
            "id": chapter_id,
            "title": title,
            "blocks": content if isinstance(content, list) else [],
            "content": "",
        }

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(result, dict):
            item["blocks"] = result.get("blocks", [])
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
                    "blocks": [],
                    "content": chapter.get("content", ""),
                }
            ordered.append(generated)

        stage3_results = shared.setdefault("stage3_results", {})
        stage3_results["chapters"] = ordered
        stage3_results["generation_mode"] = "unified_json"
        return "default"
