"""
Unified Stage3 methodology appendix node.
"""
from typing import Any, Dict

from nodes.base import MonitoredNode


class MethodologyAppendixNode(MonitoredNode):
    """Append methodology and loop governance summary."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage3_results = shared.setdefault("stage3_results", {})
        return {
            "report_text": stage3_results.get("report_text", ""),
            "trace": shared.get("trace", {}),
        }

    def exec(self, prep_res: Dict[str, Any]) -> str:
        report_text = str(prep_res.get("report_text", "")).rstrip()
        trace = prep_res.get("trace", {}) or {}
        executions = trace.get("executions", []) or []
        forum_rounds = trace.get("forum_rounds", []) or []
        loop_status = trace.get("loop_status", {}) or {}

        lines = [
            report_text,
            "",
            "---",
            "## 附录：分析方法论",
            "",
            f"- 工具调用次数: {len(executions)}",
            f"- 论坛轮次: {len(forum_rounds)}",
            "- 循环治理状态:",
        ]

        if loop_status:
            for loop_id, status in loop_status.items():
                current = status.get("current", 0) if isinstance(status, dict) else 0
                max_rounds = status.get("max", 0) if isinstance(status, dict) else 0
                reason = status.get("termination_reason", "") if isinstance(status, dict) else ""
                lines.append(f"  - {loop_id}: {current}/{max_rounds} ({reason})")
        else:
            lines.append("  - 无")

        lines.extend(
            [
                "",
                "### 数据局限性声明",
                "- 外部搜索结果可能存在时效性与样本偏差。",
                "- 图表分析与文本结论依赖模型输出，建议结合人工复核。",
                "",
            ]
        )

        return "\n".join(lines).rstrip() + "\n"

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: str) -> str:
        shared.setdefault("stage3_results", {})["report_text"] = exec_res
        return "default"
