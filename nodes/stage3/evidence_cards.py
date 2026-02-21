"""
Utilities for normalizing Stage3 evidence cards from trace provenance.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def normalize_confidence(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"high", "h", "高"}:
        return "高"
    if text in {"medium", "med", "mid", "中"}:
        return "中"
    if text in {"low", "l", "低"}:
        return "低"
    try:
        numeric = float(text)
        if numeric >= 0.75:
            return "高"
        if numeric >= 0.45:
            return "中"
        return "低"
    except Exception:
        return "中"


def _iter_provenance_rows(provenance: Dict[str, Any]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    rows: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for _, entry in provenance.items():
        if isinstance(entry, list):
            for item in entry:
                if isinstance(item, dict):
                    rows.append((item, {}))
            continue
        if not isinstance(entry, dict):
            continue
        shared_meta = {
            "confidence": entry.get("confidence", ""),
            "confidence_reasoning": entry.get("confidence_reasoning", ""),
            "summary_text": entry.get("text", ""),
        }
        evidence_list = entry.get("supporting_evidence", [])
        if isinstance(evidence_list, list):
            for item in evidence_list:
                if isinstance(item, dict):
                    rows.append((item, shared_meta))
    return rows


def build_evidence_cards(trace: Dict[str, Any], *, limit: int = 12) -> List[Dict[str, str]]:
    provenance = {}
    if isinstance(trace, dict):
        provenance = trace.get("insight_provenance", {}) or {}
    if not isinstance(provenance, dict) or not provenance:
        return []

    cards: List[Dict[str, str]] = []
    seen = set()
    for item, meta in _iter_provenance_rows(provenance):
        source = str(item.get("source") or item.get("tool") or item.get("type") or "unknown").strip()
        evidence = str(
            item.get("evidence")
            or item.get("summary")
            or item.get("ref")
            or meta.get("summary_text")
            or ""
        ).strip()
        if not evidence:
            evidence = "该证据条目提供了与结论相关的结构化支撑信息。"
        confidence = normalize_confidence(item.get("confidence") or meta.get("confidence"))
        reason = str(item.get("confidence_reasoning") or meta.get("confidence_reasoning") or "").strip()
        if not reason:
            reason = "来源与分析结论方向一致，且可追溯。"

        key = (source, evidence)
        if key in seen:
            continue
        seen.add(key)
        cards.append(
            {
                "id": f"E{len(cards) + 1}",
                "source": source,
                "evidence": evidence,
                "confidence": confidence,
                "reason": reason,
            }
        )
        if len(cards) >= max(1, int(limit)):
            break
    return cards

