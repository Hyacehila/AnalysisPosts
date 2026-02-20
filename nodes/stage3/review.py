"""
Unified Stage3 chapter review node.
"""
import json
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm46


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


class ReviewChaptersNode(MonitoredNode):
    """Review generated chapters and control revision loop."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.setdefault("stage3_results", {})
        review_cfg = shared.get("config", {}).get("stage3_review", {})
        return {
            "chapters": list(stage3_results.get("chapters", []) or []),
            "review_round": int(stage3_results.get("review_round", 0) or 0),
            "chapter_review_max_rounds": int(review_cfg.get("chapter_review_max_rounds", 2) or 2),
            "min_score": int(review_cfg.get("min_score", 80) or 80),
            "outline_title": stage3_results.get("outline", {}).get("title", "舆情分析统一报告"),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        min_score = prep_res["min_score"]
        reviews: List[Dict[str, Any]] = []

        for chapter in prep_res.get("chapters", []):
            chapter_id = chapter.get("id")
            chapter_title = chapter.get("title")
            chapter_text = str(chapter.get("content", ""))
            prompt = (
                "请评审以下报告章节，返回 JSON。\n"
                "字段: score(0-100), needs_revision(bool), feedback(str)。\n"
                f"章节标题: {chapter_title}\n"
                f"章节内容:\n{chapter_text[:2500]}\n"
                "仅输出 JSON。"
            )
            try:
                raw = call_glm46(prompt, temperature=0.3)
                parsed = _safe_json_loads(raw)
                score = int(parsed.get("score", 0) or 0)
                feedback = str(parsed.get("feedback", "")).strip()
                model_flag = bool(parsed.get("needs_revision", False))
            except Exception:
                score = 70
                feedback = "评审输出解析失败，建议补充数据支撑并精简结论。"
                model_flag = True

            needs_revision = model_flag or (score < min_score)
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
