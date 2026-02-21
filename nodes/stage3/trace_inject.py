"""
Unified Stage3 trace injection node.
"""
from typing import Any, Dict, List

from nodes.base import MonitoredNode


def _normalize_provenance_items(entry: Any) -> List[Dict[str, Any]]:
    if isinstance(entry, list):
        return [item for item in entry if isinstance(item, dict)]
    if isinstance(entry, dict):
        evidence = entry.get("supporting_evidence", [])
        if isinstance(evidence, list):
            return [item for item in evidence if isinstance(item, dict)]
    return []


def _entry_confidence(entry: Any) -> Any:
    if isinstance(entry, dict):
        return entry.get("confidence", "")
    return ""


class InjectTraceNode(MonitoredNode):
    """Inject provenance details blocks into report markdown."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.setdefault("stage3_results", {})
        report_text = (
            stage3_results.get("reviewed_report_text")
            or stage3_results.get("report_text")
            or ""
        )
        trace = shared.get("trace", {})
        return {
            "report_text": report_text,
            "provenance": trace.get("insight_provenance", {}) or {},
        }

    def exec(self, prep_res: Dict[str, Any]) -> str:
        report_text = str(prep_res.get("report_text", ""))
        provenance = prep_res.get("provenance", {}) or {}
        if not provenance:
            return report_text

        lines: List[str] = [report_text.rstrip(), "", "## 证据追溯", ""]
        for insight_key, evidence_entry in provenance.items():
            safe_items = _normalize_provenance_items(evidence_entry)
            lines.append(f"### {insight_key}")
            lines.append("")
            lines.append("<details>")
            lines.append(f"<summary>{insight_key} 证据（{len(safe_items)}条）</summary>")
            lines.append("")
            if not safe_items:
                lines.append("- 无可用证据")
            for item in safe_items:
                source = (
                    item.get("source")
                    or item.get("tool")
                    or item.get("type")
                    or "unknown"
                )
                evidence = (
                    item.get("evidence")
                    or item.get("summary")
                    or item.get("ref")
                    or ""
                )
                confidence = item.get("confidence", "")
                if confidence == "":
                    confidence = _entry_confidence(evidence_entry)
                lines.append(f"- source: {source}")
                if evidence:
                    lines.append(f"  - evidence: {evidence}")
                if confidence != "":
                    lines.append(f"  - confidence: {confidence}")
            lines.append("")
            lines.append("</details>")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: str) -> str:
        shared.setdefault("stage3_results", {})["report_text"] = exec_res
        return "default"
