"""
Stage 2 chart analysis node.
"""
import os
import time

from nodes.base import MonitoredNode

from utils.call_llm import call_glm45v_thinking


class ChartAnalysisNode(MonitoredNode):
    """
    图表分析节点 - 使用GLM4.5V+思考模式分析图表
    """

    def __init__(self, max_retries: int = 3, wait: int = 2):
        super().__init__(max_retries=max_retries, wait=wait)

    def prep(self, shared):
        charts = shared.get("stage2_results", {}).get("charts", [])
        limit_raw = os.getenv("CHART_ANALYSIS_LIMIT")
        if limit_raw is not None:
            try:
                limit = int(limit_raw)
                if limit < 0:
                    raise ValueError("limit must be non-negative")
                charts = charts[:limit]
                print(f"[ChartAnalysis] CHART_ANALYSIS_LIMIT={limit} applied")
            except ValueError:
                print(f"[ChartAnalysis] 无效的 CHART_ANALYSIS_LIMIT: {limit_raw}")
        print(f"\n[ChartAnalysis] 准备分析 {len(charts)} 张图表")
        return charts

    def exec(self, prep_res):
        charts = prep_res
        chart_analyses = {}
        success_count = 0

        print(f"[ChartAnalysis] 开始逐个分析图表...")
        start_time = time.time()

        for i, chart in enumerate(charts, 1):
            chart_id = chart.get("id", f"chart_{i}")
            chart_title = chart.get("title", "")
            chart_path = (
                chart.get("path")
                or chart.get("file_path")
                or chart.get("chart_path")
                or chart.get("image_path")
                or ""
            )

            print(f"[ChartAnalysis] [{i}/{len(charts)}] 分析图表: {chart_title}")

            analysis_prompt = f"""你是专业的舆情数据分析师，请对这张舆情分析图表进行分析说明。

## 图表信息
- 图表ID: {chart_id}
- 图表标题: {chart_title}
- 图表类型: {chart.get('type', 'unknown')}

## 分析要求
请基于图表视觉信息提供详细分析，包括：

### 图表基础描述
- 图表类型和结构特征
- 坐标轴标签和刻度
- 数据系列的标识和图例
- 整体布局和视觉设计

### 数据细节
- 每个数据项的具体数值
- 最高值、最低值及其标识
- 数据分布特征和趋势
- 重要的数据关系

### 宏观洞察
- 数据反映的主要模式
- 趋势变化和转折点
- 关键的业务发现
- 数据质量和可读性评估

请用自然语言描述，不要使用JSON格式。直接返回分析结果。
"""

            try:
                response = call_glm45v_thinking(
                    prompt=analysis_prompt,
                    image_paths=[chart_path] if chart_path and os.path.exists(chart_path) else None,
                    temperature=0.7,
                    max_tokens=2000,
                    enable_thinking=True,
                )

                analysis_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": response.strip(),
                    "analysis_timestamp": time.time(),
                    "analysis_status": "success",
                }

                chart_analyses[chart_id] = analysis_result
                success_count += 1
                print(f"[ChartAnalysis] [√] 图表 {chart_id} 分析完成")
                print(f"[ChartAnalysis] [√] 分析长度: {len(response)} 字符")
            except Exception as e:
                print(f"[ChartAnalysis] [!] 图表 {chart_id} 分析失败: {str(e)}")
                chart_analyses[chart_id] = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": f"图表分析失败: {str(e)}",
                    "analysis_timestamp": time.time(),
                    "analysis_status": "failed",
                }

        execution_time = time.time() - start_time
        return {
            "chart_analyses": chart_analyses,
            "success_count": success_count,
            "total_charts": len(charts),
            "success_rate": success_count / len(charts) if charts else 0,
            "execution_time": execution_time,
        }

    def post(self, shared, prep_res, exec_res):
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        shared["stage2_results"]["chart_analyses"] = exec_res["chart_analyses"]

        print(f"\n[ChartAnalysis] 图表分析完成:")
        print(f"  ├─ 总图表数: {exec_res['total_charts']}")
        print(f"  ├─ 成功分析: {exec_res['success_count']}")
        print(f"  ├─ 失败数量: {exec_res['total_charts'] - exec_res['success_count']}")
        print(f"  └─ 成功率: {exec_res['success_rate']*100:.1f}%")
        print(f"  └─ 耗时: {exec_res['execution_time']:.2f}秒")

        if "execution_log" not in shared["stage2_results"]:
            shared["stage2_results"]["execution_log"] = {}

        shared["stage2_results"]["execution_log"]["chart_analysis"] = {
            "total_charts": exec_res["total_charts"],
            "success_count": exec_res["success_count"],
            "success_rate": exec_res["success_rate"],
            "analysis_timestamp": exec_res["execution_time"],
        }

        return "default"
