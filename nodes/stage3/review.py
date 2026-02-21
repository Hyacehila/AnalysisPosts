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


def _assemble_chapters(title: str, chapters: List[Dict[str, Any]]) -> str:
    lines = [f"# {title}", ""]
    for chapter in chapters:
        chapter_title = chapter.get("title") or chapter.get("id") or "未命名章节"
        lines.append(f"## {chapter_title}")
        lines.append("")
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
    if text.startswith("证据说明："):
        return False
    return True


def _missing_evidence_notes(chapter_text: str) -> List[str]:
    blocks = [chunk.strip() for chunk in re.split(r"\n\s*\n", str(chapter_text or "")) if chunk.strip()]
    issues: List[str] = []
    for idx, block in enumerate(blocks):
        if not _is_narrative_block(block):
            continue
        next_block = blocks[idx + 1] if idx + 1 < len(blocks) else ""
        if not next_block.startswith("证据说明："):
            issues.append("段落后缺少“证据说明：”")
            continue
        if re.search(r"\[E\d+\]", next_block) is None:
            issues.append("证据说明缺少证据索引（如 [E1]）")
        if "置信度：" not in next_block:
            issues.append("证据说明缺少置信度字段")
        if "理由：" not in next_block:
            issues.append("证据说明缺少置信度理由")
    return list(dict.fromkeys(issues))


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
            "reference_context": _build_reference_context(shared),
            "reasoning_enabled_stage3": reasoning_enabled_stage3(shared),
            "request_timeout_seconds": llm_request_timeout(shared),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        use_reasoning = bool(prep_res.get("reasoning_enabled_stage3", False))
        reference_context = str(prep_res.get("reference_context", "") or "")
        reviews: List[Dict[str, Any]] = []

        for chapter in prep_res.get("chapters", []):
            chapter_id = chapter.get("id")
            chapter_title = chapter.get("title")
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
                "3. 每个正文段落后都应有“证据说明：”并包含证据索引、置信度、理由。\n"
                "4. 反馈需指出具体缺失点（证据、数字、逻辑）。\n"
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

            evidence_issues = _missing_evidence_notes(chapter_text)
            if evidence_issues:
                needs_revision = True
                score = min(score, 45)
                addition = "；".join(evidence_issues)
                feedback = (
                    f"{feedback} 段落证据融合不完整：{addition}。".strip()
                    if feedback
                    else f"段落证据融合不完整：{addition}。"
                )

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
            stage3_results["reviewed_report_text"] = _assemble_chapters(
                prep_res.get("outline_title", "舆情分析统一报告"),
                prep_res.get("chapters", []),
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
