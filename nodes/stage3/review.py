"""
Unified Stage3 chapter review node.
"""
import json
import re
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46
from utils.llm_modes import llm_request_timeout, reasoning_enabled_stage3


def _safe_json_loads(text: str) -> Dict[str, Any]:
    payload = (text or "").strip()
    if "```" in payload:
        payload = payload.replace("```json", "").replace("```", "").strip()
    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        payload = payload[start : end + 1]
    return json.loads(payload)


def _blocks_to_text(blocks) -> str:
    """将 JSON IR blocks 列表扁平化为纯文本，供评审逻辑使用。"""
    if not isinstance(blocks, list):
        return str(blocks or "")
    lines = []
    for block in blocks:
        if not isinstance(block, dict):
            lines.append(str(block))
            continue
        block_type = block.get("type", "")
        if block_type == "paragraph":
            inlines = block.get("inlines", [])
            text = "".join(str(run.get("text", "")) for run in inlines if isinstance(run, dict))
            lines.append(text)
        elif block_type == "heading":
            lines.append(f"# {block.get('text', '')}")
        elif block_type == "list":
            for item in block.get("items", []):
                lines.append(f"- {item}")
        elif block_type == "engineQuote":
            sub_blocks = block.get("blocks", [])
            lines.append(f"[{block.get('title', 'Agent')}]: {_blocks_to_text(sub_blocks)}")
        elif block_type == "swotTable":
            for dim in ("strengths", "weaknesses", "opportunities", "threats"):
                for entry in block.get(dim, []):
                    if isinstance(entry, dict):
                        lines.append(f"SWOT-{dim}: {entry.get('point', '')} {entry.get('detail', '')}")
        elif block_type == "pestTable":
            for dim in ("political", "economic", "social", "technological"):
                for entry in block.get(dim, []):
                    if isinstance(entry, dict):
                        lines.append(f"PEST-{dim}: {entry.get('factor', '')} {entry.get('detail', '')}")
        elif block_type == "image":
            lines.append(f"![{block.get('alt', '')}]({block.get('src', '')})")
        elif block_type == "table":
            for row in block.get("rows", []):
                lines.append(" | ".join(str(cell) for cell in row))
        else:
            lines.append(str(block))
    return "\n\n".join(lines)


def _assemble_chapters(title: str, chapters: List[Dict[str, Any]]) -> str:
    lines = [f"# {title}", ""]
    for chapter in chapters:
        chapter_title = chapter.get("title") or chapter.get("id") or "未命名章节"
        lines.append(f"## {chapter_title}")
        lines.append("")
        blocks = chapter.get("blocks", [])
        if isinstance(blocks, list) and blocks:
            lines.append(_blocks_to_text(blocks))
        else:
            lines.append(str(chapter.get("content", "")).strip())
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_reference_context(shared: Dict[str, Any]) -> str:
    stage3_data = shared.get("stage3_data", {}) or {}
    insights = stage3_data.get("insights", {}) or {}
    analysis_data = stage3_data.get("analysis_data", {}) or {}
    search_context = analysis_data.get("search_context", {}) or {}
    chart_analyses = stage3_data.get("chart_analyses", {}) or {}

    insight_lines = []
    if isinstance(insights, dict):
        for key, value in list(insights.items())[:6]:
            text = str(value or "").strip()
            if text:
                insight_lines.append(f"- {key}: {text[:260]}")

    chart_lines = []
    if isinstance(chart_analyses, dict):
        for _, item in list(chart_analyses.items())[:4]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("chart_title") or item.get("chart_id") or "chart").strip()
            analysis = str(item.get("analysis_content") or item.get("analysis") or "").strip()
            if analysis:
                chart_lines.append(f"- {title}: {analysis[:220]}")

    search_lines = []
    if isinstance(search_context, dict):
        for key in ("event_timeline", "key_actors", "official_responses"):
            value = search_context.get(key)
            if value:
                search_lines.append(f"- {key}: {str(value)[:220]}")

    sections: List[str] = []
    if insight_lines:
        sections.append("洞察摘要:\n" + "\n".join(insight_lines))
    if chart_lines:
        sections.append("图表分析摘要:\n" + "\n".join(chart_lines))
    if search_lines:
        sections.append("外部搜索摘要:\n" + "\n".join(search_lines))
    return "\n\n".join(sections).strip()


_PLACEHOLDER_KEYWORDS = (
    "议题",
    "争议",
    "媒体",
    "地区",
    "关键事件",
    "人物a",
    "事件a",
)


def _find_placeholder(text: str) -> str:
    if not text:
        return ""
    for match in re.finditer(r"\[([^\]\n]{1,24})\](?!\()", text):
        token = str(match.group(1) or "").strip()
        token_lower = token.lower()
        if any(keyword in token_lower for keyword in _PLACEHOLDER_KEYWORDS):
            return token
    return ""


def _is_narrative_block(block: str) -> bool:
    text = str(block or "").strip()
    if not text:
        return False
    if text.startswith("#"):
        return False
    if text.startswith("- ") or text.startswith("* "):
        return False
    if text.startswith("!["):
        return False
    return True


def _normalize_heading_text(text: str) -> str:
    normalized = re.sub(r"\s+", "", str(text or "").strip().lower())
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", normalized)


def _contains_duplicate_heading(chapter_text: str, chapter_title: str) -> bool:
    title_norm = _normalize_heading_text(chapter_title)
    if not title_norm:
        return False
    for line in str(chapter_text or "").splitlines():
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        heading = re.sub(r"^#+\s*", "", stripped).strip()
        if _normalize_heading_text(heading) == title_norm:
            return True
    return False


def _missing_inline_citations(chapter_text: str) -> List[str]:
    blocks = [chunk.strip() for chunk in re.split(r"\n\s*\n", str(chapter_text or "")) if chunk.strip()]
    issues: List[str] = []
    for block in blocks:
        if not _is_narrative_block(block):
            continue
        if re.search(r"\[E\d+\]", block) is None:
            issues.append("段落缺少证据角标（如 [E1]）")
    return list(dict.fromkeys(issues))


def _find_invalid_bracket_citations(chapter_text: str) -> List[str]:
    """Find bracket tokens that are not valid [E\\d+] evidence citations.

    Exemptions — tokens produced legitimately by _blocks_to_text or sanitize:
    - [E1], [E12] — valid evidence IDs
    - [Insight Agent], [Media Agent] etc — engineQuote title labels ending with "Agent"
    - [SWOT-strengths:], [PEST-political:] — SWOT/PEST dimension prefix labels

    Everything else, including [topic_distribution], [议题A], [争议点], is flagged.
    """
    invalid: List[str] = []
    for match in re.finditer(r"\[([^\]\n]{1,64})\](?!\()", str(chapter_text or "")):
        token = str(match.group(1) or "").strip()
        # Valid evidence citation [E1], [E12]
        if re.fullmatch(r"E\d+", token):
            continue
        # engineQuote flattened labels always end with "Agent"
        if token.endswith("Agent"):
            continue
        # SWOT/PEST dimension labels start with "SWOT-" or "PEST-"
        if re.match(r"(?:SWOT|PEST)-", token):
            continue
        invalid.append(token)
    return list(dict.fromkeys(invalid))


class ReviewChaptersNode(MonitoredNode):
    """Review generated chapters and control revision loop."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.setdefault("stage3_results", {})
        review_cfg = shared.get("config", {}).get("stage3_review", {})
        return {
            "chapters": list(stage3_results.get("chapters", []) or []),
            "review_round": int(stage3_results.get("review_round", 0) or 0),
            "chapter_review_max_rounds": int(review_cfg.get("chapter_review_max_rounds", 2) or 2),
            "outline_title": stage3_results.get("outline", {}).get("title", "舆情分析统一报告"),
            "outline": stage3_results.get("outline", {}),
            "reference_context": _build_reference_context(shared),
            "reasoning_enabled_stage3": reasoning_enabled_stage3(shared),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        use_reasoning = bool(prep_res.get("reasoning_enabled_stage3", False))
        reference_context = str(prep_res.get("reference_context", "") or "")
        reviews: List[Dict[str, Any]] = []
        outline = prep_res.get("outline", {})
        outline_chapters = {str(c.get("id", "")): c for c in outline.get("chapters", []) if isinstance(c, dict)}

        for chapter in prep_res.get("chapters", []):
            chapter_id = chapter.get("id")
            chapter_title = chapter.get("title")
            blocks = chapter.get("blocks", [])
            if isinstance(blocks, list) and blocks:
                chapter_text = _blocks_to_text(blocks)
            else:
                chapter_text = str(chapter.get("content", ""))
            prompt = (
                "请评审以下舆情报告章节，返回 JSON。\n"
                "字段: score(0-100), needs_revision(bool), feedback(str)。\n"
                f"章节标题: {chapter_title}\n"
                f"参考数据:\n{reference_context[:1800] if reference_context else '无'}\n"
                f"章节内容:\n{chapter_text[:2500]}\n"
                "评审要求:\n"
                "1. 发现[议题X]/[争议点]/[媒体A]等占位符必须判定 needs_revision=true。\n"
                "2. 数据、人物、事件名称需与参考数据一致。\n"
                "3. 每个实质性正文段落都应包含证据角标引用（如 [E1]）。\n"
                "4. 禁止在章节正文中重复输出与章节标题同名的标题行。\n"
                "5. 不得引入输入未出现的新专有名词（若无法确认，应判定 needs_revision=true）。\n"
                "6. 反馈需指出具体缺失点（证据、数字、逻辑）。\n"
                "仅输出 JSON。"
            )
            try:
                raw = call_glm46(
                    prompt,
                    temperature=0.3,
                    enable_reasoning=use_reasoning,
                    timeout=int(prep_res.get("request_timeout_seconds", 120)),
                )
                parsed = _safe_json_loads(raw)
                score = int(parsed.get("score", 0) or 0)
                feedback = str(parsed.get("feedback", "")).strip()
                model_flag = bool(parsed.get("needs_revision", False))
            except Exception:
                score = 70
                feedback = "评审输出解析失败，建议补充数据支撑并精简结论。"
                model_flag = True

            needs_revision = bool(model_flag)
            placeholder = _find_placeholder(chapter_text)
            if placeholder:
                needs_revision = True
                score = min(score, 35)
                addition = f"检测到占位符[{placeholder}]，必须替换为真实事件信息。"
                feedback = f"{feedback} {addition}".strip() if feedback else addition

            evidence_issues = _missing_inline_citations(chapter_text)
            if evidence_issues:
                needs_revision = True
                score = min(score, 45)
                addition = "；".join(evidence_issues)
                feedback = (
                    f"{feedback} 段落证据引用不完整：{addition}。".strip()
                    if feedback
                    else f"段落证据引用不完整：{addition}。"
                )

            invalid_citations = _find_invalid_bracket_citations(chapter_text)
            if invalid_citations:
                needs_revision = True
                score = min(score, 40)
                preview = "、".join([f"[{item}]" for item in invalid_citations[:3]])
                addition = f"检测到非法引用标记{preview}，仅允许 [E数字] 格式。"
                feedback = f"{feedback} {addition}".strip() if feedback else addition

            if _contains_duplicate_heading(chapter_text, str(chapter_title or "")):
                needs_revision = True
                score = min(score, 40)
                addition = "检测到与章节标题同名的重复标题行。"
                feedback = f"{feedback} {addition}".strip() if feedback else addition

            ch_outline = outline_chapters.get(str(chapter_id), {})
            if not ch_outline.get("allowSwot", False):
                if isinstance(blocks, list) and any(
                    isinstance(block, dict) and block.get("type") == "swotTable"
                    for block in blocks
                ):
                    needs_revision = True
                    score = min(score, 30)
                    addition = "章节未获授权使用 swotTable 组件，请移除。"
                    feedback = f"{feedback} {addition}".strip() if feedback else addition
            if not ch_outline.get("allowPest", False):
                if isinstance(blocks, list) and any(
                    isinstance(block, dict) and block.get("type") == "pestTable"
                    for block in blocks
                ):
                    needs_revision = True
                    score = min(score, 30)
                    addition = "章节未获授权使用 pestTable 组件，请移除。"
                    feedback = f"{feedback} {addition}".strip() if feedback else addition

            reviews.append(
                {
                    "id": chapter_id,
                    "title": chapter_title,
                    "score": score,
                    "needs_revision": needs_revision,
                    "feedback": feedback,
                }
            )

        return {
            "reviews": reviews,
            "needs_revision": any(r.get("needs_revision") for r in reviews),
        }

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, Any]) -> str:
        stage3_results = shared.setdefault("stage3_results", {})
        reviews = list(exec_res.get("reviews", []) or [])
        stage3_results.setdefault("chapter_review_history", []).append(reviews)

        current_round = int(stage3_results.get("review_round", 0) or 0)
        max_rounds = int(prep_res.get("chapter_review_max_rounds", 2) or 2)
        needs_revision = bool(exec_res.get("needs_revision", False))

        if needs_revision and current_round < max_rounds:
            stage3_results["review_round"] = current_round + 1
            feedback_map = {
                str(item.get("id", "")): str(item.get("feedback", "")).strip()
                for item in reviews
                if item.get("needs_revision")
            }
            stage3_results["chapter_feedback"] = feedback_map
            action = "needs_revision"
            termination_reason = "continue"
            loop_current = stage3_results["review_round"]
        else:
            stage3_results["chapter_feedback"] = {}
            # NOTE: reviewed_report_text will be written by IRRendererNode
            # (which produces properly formatted Markdown from JSON blocks).
            # We only write a fallback here for the pure-Markdown code path
            # (chapters with no blocks field).
            chapters = prep_res.get("chapters", [])
            has_json_blocks = any(
                isinstance(ch.get("blocks"), list) and ch.get("blocks")
                for ch in chapters
            )
            if not has_json_blocks:
                stage3_results["reviewed_report_text"] = _assemble_chapters(
                    prep_res.get("outline_title", "舆情分析统一报告"),
                    chapters,
                )
            action = "satisfied"
            if needs_revision and current_round >= max_rounds:
                termination_reason = "max_iterations_reached"
            else:
                termination_reason = "sufficient"
            loop_current = current_round

        trace = shared.setdefault("trace", {})
        loop_status = trace.setdefault("loop_status", {})
        loop_status["stage3_chapter_review"] = {
            "current": int(loop_current),
            "max": max_rounds,
            "termination_reason": termination_reason,
            "scores": [int(r.get("score", 0) or 0) for r in reviews],
        }

        return action
