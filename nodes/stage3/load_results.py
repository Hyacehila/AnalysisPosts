"""
Stage 3 load analysis results node.
"""
import json
import os
import random
from typing import Any, Dict

from nodes.base import MonitoredNode


class LoadAnalysisResultsNode(MonitoredNode):
    """Load Stage2 outputs (memory-first, file fallback) for unified Stage3."""

    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        stage2_results = shared.get("stage2_results", {})
        completed_stages = shared.get("pipeline_state", {}).get("completed_stages", [])
        insights = stage2_results.get("insights", {})
        if isinstance(insights, dict):
            has_insights = any(bool(value) for value in insights.values())
        else:
            has_insights = bool(insights)

        has_memory_data = (
            2 in completed_stages
            and bool(
                stage2_results.get("charts")
                or stage2_results.get("chart_analyses")
                or stage2_results.get("tables")
                or has_insights
            )
        )

        analysis_data_path = "report/analysis_data.json"
        chart_analyses_path = "report/chart_analyses.json"
        insights_path = "report/insights.json"
        trace_path = "report/trace.json"
        images_dir = "report/images/"

        if not has_memory_data:
            missing_files = []
            for file_path in [analysis_data_path, chart_analyses_path, insights_path, trace_path]:
                if not os.path.exists(file_path):
                    missing_files.append(file_path)
            if missing_files:
                raise FileNotFoundError(f"阶段2输出文件不存在: {missing_files}")

        if not os.path.exists(images_dir):
            raise FileNotFoundError(f"图表目录不存在: {images_dir}")

        return {
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "trace_path": trace_path,
            "images_dir": images_dir,
            "enhanced_data_path": shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", ""),
            "has_memory_data": has_memory_data,
            "stage2_results": stage2_results,
            "memory_trace": shared.get("trace", {}),
        }

    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        if prep_res.get("has_memory_data"):
            stage2_results = prep_res["stage2_results"]
            print("[LoadAnalysisResults] 从内存中加载 stage2 结果")

            analysis_data = {
                "charts": stage2_results.get("charts", []),
                "tables": stage2_results.get("tables", []),
                "execution_log": stage2_results.get("execution_log", {}),
                "search_context": stage2_results.get("search_context", {}),
            }

            chart_analyses = stage2_results.get("chart_analyses", {})
            if isinstance(chart_analyses, list):
                chart_analyses = {f"chart_{i}": item for i, item in enumerate(chart_analyses)}

            insights = stage2_results.get("insights", {})
            trace = prep_res.get("memory_trace", {})
        else:
            print("[LoadAnalysisResults] 从文件加载 stage2 结果")
            with open(prep_res["analysis_data_path"], "r", encoding="utf-8") as f:
                analysis_data = json.load(f)

            with open(prep_res["chart_analyses_path"], "r", encoding="utf-8") as f:
                chart_analyses = json.load(f)

            with open(prep_res["insights_path"], "r", encoding="utf-8") as f:
                insights = json.load(f)

            with open(prep_res["trace_path"], "r", encoding="utf-8") as f:
                trace = json.load(f)

        sample_blogs = []
        enhanced_data_path = prep_res.get("enhanced_data_path", "")
        if enhanced_data_path and os.path.exists(enhanced_data_path):
            with open(enhanced_data_path, "r", encoding="utf-8") as f:
                enhanced_data = json.load(f)
            if isinstance(enhanced_data, list) and enhanced_data:
                sample_blogs = random.sample(enhanced_data, min(10, len(enhanced_data)))

        return {
            "analysis_data": analysis_data,
            "chart_analyses": chart_analyses,
            "insights": insights,
            "trace": trace,
            "sample_blogs": sample_blogs,
            "images_dir": prep_res["images_dir"],
        }

    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, Any]) -> str:
        shared["stage3_data"] = exec_res
        loaded_trace = exec_res.get("trace", {})
        if isinstance(loaded_trace, dict):
            shared_trace = shared.get("trace", {})
            if not isinstance(shared_trace, dict) or not shared_trace:
                shared["trace"] = dict(loaded_trace)
            else:
                for key, value in loaded_trace.items():
                    if key not in shared_trace or shared_trace.get(key) in (None, "", [], {}):
                        shared_trace[key] = value
                    elif isinstance(shared_trace.get(key), dict) and isinstance(value, dict):
                        merged = dict(value)
                        merged.update(shared_trace[key])
                        shared_trace[key] = merged
                shared["trace"] = shared_trace
        return "default"
