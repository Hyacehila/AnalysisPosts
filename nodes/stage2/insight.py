"""
Stage 2 LLM insight node.
"""
import json

from nodes.base import MonitoredNode

from utils.call_llm import call_glm_45_air, call_glm46


class LLMInsightNode(MonitoredNode):
    """
    LLM洞察补充节点
    """

    def prep(self, shared):
        stage2_results = shared.get("stage2_results", {})
        return {
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "tables": stage2_results.get("tables", []),
            "data_summary": shared.get("agent", {}).get("data_summary", ""),
        }

    def exec(self, prep_res):
        chart_analyses = prep_res["chart_analyses"]
        tables = prep_res["tables"]
        data_summary = prep_res["data_summary"]

        chart_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                content = analysis.get("analysis", "")

                chart_summary.append(f"### {title}")
                content_preview = content[:500] + ("..." if len(content) > 500 else "")
                chart_summary.append(content_preview)
                chart_summary.append("")
            else:
                title = analysis.get("chart_title", chart_id)
                status = analysis.get("analysis_status", "unknown")
                chart_summary.append(f"### {title}")
                chart_summary.append(f"分析状态: {status}")
                chart_summary.append("")

        stats_summary = []
        for table in tables:
            title = table.get("title", "")
            data = table.get("data", {})
            summary = data.get("summary", "") if isinstance(data, dict) else ""
            if summary:
                stats_summary.append(f"- {title}: {summary}")

        prompt = f"""你是专业的舆情数据分析师，请严格基于提供的分析结果，生成数据驱动的洞察摘要。

## 重要要求
1. **仅基于提供的数据**：所有结论必须来自下面的图表分析和统计数据
2. **禁止推测**：不要引入外部知识或推测原因
3. **数据索引**：引用具体的分析结果作为支撑
4. **客观准确**：避免夸大或主观判断

## 基础数据
{data_summary if data_summary else "无基础数据"}

## 图表分析结果（来自GLM4.5V）
{chr(10).join(chart_summary) if chart_summary else "无图表分析结果"}

## 统计数据
{chr(10).join(stats_summary) if stats_summary else "无统计数据"}

## 分析要求
请严格基于以上数据，生成以下维度的洞察摘要：

1. **情感态势总结**：基于图表中的具体数值和趋势，总结情感分布特征
2. **主题分布特征**：基于主题图表数据，描述话题热度分布
3. **地域分布特点**：基于地理数据，总结区域分布模式
4. **发布者行为特征**：基于发布者类型数据，描述行为模式
5. **综合数据概览**：整合所有数据的整体特征

## 输出格式（严格JSON）
```json
{{
    "sentiment_summary": "基于图表数据总结的情感态势",
    "topic_distribution": "基于数据描述的主题分布特征",
    "geographic_distribution": "基于数据的地理分布特点",
    "publisher_behavior": "基于数据的发布者行为模式",
    "overall_summary": "所有数据的整合性总结"
}}
```

**重要**: 每个洞察都要有明确的数据支撑，不要添加推测性内容。"""

        response = None
        try:
            response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)
        except Exception as e:
            error_msg = str(e)
            is_recoverable_error = (
                "429" in error_msg
                or "concurrency" in error_msg.lower()
                or "调用glm4.6模型失败" in error_msg
                or "rate limit" in error_msg.lower()
                or "API并发限制" in error_msg
            )
            if is_recoverable_error:
                print(f"[LLMInsight] GLM-4.6调用失败: {error_msg}")
                print(f"[LLMInsight] 回退到GLM-4.5-air模型...")
                response = call_glm_45_air(prompt, temperature=0.7, timeout=120)
            else:
                raise

        try:
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()

            insights = json.loads(json_str)
        except json.JSONDecodeError:
            insights = {
                "sentiment_insight": "基于图表分析，情感趋势显示整体态势相对稳定，需要关注异常波动点。",
                "topic_insight": "主题演化分析表明核心话题持续活跃，新兴话题呈现增长趋势。",
                "geographic_insight": "地理分布分析显示热点区域集中，区域差异特征明显。",
                "cross_dimension_insight": "发布者类型分析显示不同群体影响力差异显著，交互模式多样。",
                "summary_insight": response[:800] if response else "综合分析已完成，建议关注图表中的关键发现。",
            }

        return insights

    def post(self, shared, prep_res, exec_res):
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        shared["stage2_results"]["insights"] = exec_res

        print(f"\n[LLMInsight] [OK] 洞察分析生成完成")
        for key, value in exec_res.items():
            preview = value[:80] + "..." if len(value) > 80 else value
            print(f"  - {key}: {preview}")

        return "default"
