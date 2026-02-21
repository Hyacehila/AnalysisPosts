"""
Unified Stage3 trace injection node.
"""
from __future__ import annotations

from typing import Dict, List

from nodes.base import MonitoredNode
from nodes.stage3.evidence_cards import build_evidence_cards


def _build_reference_index(cards: List[Dict[str, str]]) -> str:
    if not cards:
        return ""
    lines: List[str] = ["## 参考资料与证据索引", ""]
    for card in cards:
        eid = card.get("id", "E?")
        source = card.get("source", "unknown")
        evidence = card.get("evidence", "")
        confidence = card.get("confidence", "中")
        reason = card.get("reason", "来源与结论方向一致，且可追溯。")
        lines.append(
            f"- [{eid}] 该证据来自「{source}」。核心证据：{evidence} 置信度：{confidence}。理由：{reason}"
        )
    lines.append("")
    return "\n".join(lines)


class InjectTraceNode(MonitoredNode):
    """Append concise evidence index for paragraph-level evidence references."""

    def prep(self, shared: Dict[str, object]) -> Dict[str, object]:
        stage3_results = shared.setdefault("stage3_results", {})
        report_text = (
            stage3_results.get("reviewed_report_text")
            or stage3_results.get("report_text")
            or ""
        )
        trace = shared.get("trace", {}) or {}
        return {
            "report_text": str(report_text or ""),
            "evidence_cards": build_evidence_cards(trace, limit=20),
        }

    def exec(self, prep_res: Dict[str, object]) -> str:
        report_text = str(prep_res.get("report_text", "") or "")
        evidence_cards = list(prep_res.get("evidence_cards", []) or [])
        if not evidence_cards:
            return report_text
        if "## 参考资料与证据索引" in report_text:
            return report_text
        appendix = _build_reference_index(evidence_cards)
        if not appendix:
            return report_text
        return report_text.rstrip() + "\n\n" + appendix

    def post(self, shared: Dict[str, object], prep_res: Dict[str, object], exec_res: str) -> str:
        shared.setdefault("stage3_results", {})["report_text"] = exec_res
        return "default"

