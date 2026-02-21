"""
Helpers for reading LLM reasoning/thinking switches from shared config.
"""
from __future__ import annotations

from typing import Any, Dict


def _profile_defaults(profile: str) -> Dict[str, bool]:
    if str(profile or "").strip().lower() == "quality":
        return {
            "reasoning_enabled_stage2": True,
            "reasoning_enabled_stage3": True,
            "vision_thinking_enabled": True,
        }
    return {
        "reasoning_enabled_stage2": False,
        "reasoning_enabled_stage3": False,
        "vision_thinking_enabled": False,
    }


def _llm_cfg(shared: Dict[str, Any]) -> Dict[str, Any]:
    config = shared.get("config", {}) if isinstance(shared, dict) else {}
    llm = config.get("llm", {}) if isinstance(config, dict) else {}
    return llm if isinstance(llm, dict) else {}


def reasoning_enabled_stage2(shared: Dict[str, Any]) -> bool:
    llm = _llm_cfg(shared)
    defaults = _profile_defaults(llm.get("acceptance_profile", "fast"))
    value = llm.get("reasoning_enabled_stage2")
    if value is None:
        return defaults["reasoning_enabled_stage2"]
    return bool(value)


def reasoning_enabled_stage3(shared: Dict[str, Any]) -> bool:
    llm = _llm_cfg(shared)
    defaults = _profile_defaults(llm.get("acceptance_profile", "fast"))
    value = llm.get("reasoning_enabled_stage3")
    if value is None:
        return defaults["reasoning_enabled_stage3"]
    return bool(value)


def vision_thinking_enabled(shared: Dict[str, Any]) -> bool:
    llm = _llm_cfg(shared)
    defaults = _profile_defaults(llm.get("acceptance_profile", "fast"))
    value = llm.get("vision_thinking_enabled")
    if value is None:
        return defaults["vision_thinking_enabled"]
    return bool(value)


def llm_request_timeout(shared: Dict[str, Any], default: int = 120) -> int:
    llm = _llm_cfg(shared)
    value = llm.get("request_timeout_seconds", default)
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        timeout = int(default)
    return max(1, timeout)
