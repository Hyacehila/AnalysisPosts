"""
Stage 2 save results node.
"""
import json
import os

from nodes.base import MonitoredNode


class SaveAnalysisResultsNode(MonitoredNode):
    """
    保存分析结果节点
    """

    def prep(self, shared):
        stage2_results = shared.get("stage2_results", {})
        return {
            "charts": stage2_results.get("charts", []),
            "tables": stage2_results.get("tables", []),
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "insights": stage2_results.get("insights", {}),
            "execution_log": stage2_results.get("execution_log", {}),
        }

    def exec(self, prep_res):
        output_dir = "report"
        os.makedirs(output_dir, exist_ok=True)

        analysis_data = {
            "charts": prep_res["charts"],
            "tables": prep_res["tables"],
            "execution_log": prep_res["execution_log"],
        }

        analysis_data_path = os.path.join(output_dir, "analysis_data.json")
        with open(analysis_data_path, "w", encoding="utf-8") as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        chart_analyses_path = os.path.join(output_dir, "chart_analyses.json")
        with open(chart_analyses_path, "w", encoding="utf-8") as f:
            json.dump(prep_res["chart_analyses"], f, ensure_ascii=False, indent=2)

        insights_path = os.path.join(output_dir, "insights.json")
        with open(insights_path, "w", encoding="utf-8") as f:
            json.dump(prep_res["insights"], f, ensure_ascii=False, indent=2)

        return {
            "success": True,
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "charts_count": len(prep_res["charts"]),
            "tables_count": len(prep_res["tables"]),
            "chart_analyses_count": len(prep_res["chart_analyses"]),
        }

    def post(self, shared, prep_res, exec_res):
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        shared["stage2_results"]["output_files"] = {
            "charts_dir": "report/images/",
            "analysis_data": exec_res["analysis_data_path"],
            "chart_analyses_file": exec_res["chart_analyses_path"],
            "insights_file": exec_res["insights_path"],
        }

        print(f"\n[SaveAnalysisResults] [OK] 分析结果已保存")
        print(f"  - 分析数据: {exec_res['analysis_data_path']}")
        print(f"  - 图表分析: {exec_res['chart_analyses_path']}")
        print(f"  - 洞察描述: {exec_res['insights_path']}")
        print(f"  - 生成图表: {exec_res['charts_count']} 个")
        print(f"  - 分析图表: {exec_res['chart_analyses_count']} 个")
        print(f"  - 生成表格: {exec_res['tables_count']} 个")

        return "default"
