"""
stage2.py - 阶段2节点：分析执行

包含数据加载/摘要/保存节点 + Workflow路径节点 + Agent循环节点。
"""

import json
import os
import asyncio
import subprocess
import time
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import Counter, defaultdict
from datetime import datetime

from pocketflow import Node, AsyncNode

from nodes._utils import normalize_path, get_project_relative_path, ensure_dir_exists
from utils.call_llm import call_glm_45_air, call_glm4v_plus, call_glm45v_thinking, call_glm46
from utils.data_loader import load_enhanced_blog_data





# =============================================================================
# 4. 阶段2节点: 分析执行
# =============================================================================

# -----------------------------------------------------------------------------
# 4.1 通用节点
# -----------------------------------------------------------------------------

class LoadEnhancedDataNode(Node):
    """
    加载增强数据节点
    
    功能：加载已完成增强处理的博文数据
    类型：Regular Node
    前置检查：验证阶段1输出文件是否存在
    """
    
    def prep(self, shared):
        """读取增强数据文件路径，检查前置条件"""
        config = shared.get("config", {})
        enhanced_data_path = config.get("data_source", {}).get(
            "enhanced_data_path", "data/enhanced_blogs.json"
        )
        
        # 检查文件是否存在
        if not os.path.exists(enhanced_data_path):
            raise FileNotFoundError(
                f"阶段1输出文件不存在: {enhanced_data_path}\n"
                f"请先运行阶段1（增强处理）或确保文件路径正确"
            )
        
        return {"data_path": enhanced_data_path}
    
    def exec(self, prep_res):
        """加载JSON数据，验证增强字段完整性"""
        data_path = prep_res["data_path"]
        
        print(f"\n[LoadEnhancedData] 加载增强数据: {data_path}")
        blog_data = load_enhanced_blog_data(data_path)
        
        # 验证增强字段
        enhanced_fields = ["sentiment_polarity", "sentiment_attribute", "topics", "publisher"]
        valid_count = 0
        for post in blog_data:
            has_all_fields = all(post.get(field) is not None for field in enhanced_fields)
            if has_all_fields:
                valid_count += 1
        
        return {
            "blog_data": blog_data,
            "total_count": len(blog_data),
            "valid_count": valid_count,
            "enhancement_rate": round(valid_count / len(blog_data) * 100, 2) if blog_data else 0
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储数据到shared"""
        if "data" not in shared:
            shared["data"] = {}
        
        shared["data"]["blog_data"] = exec_res["blog_data"]
        
        print(f"[LoadEnhancedData] [√] 加载 {exec_res['total_count']} 条博文")
        print(f"[LoadEnhancedData] [√] 完整增强率: {exec_res['enhancement_rate']}%")
        
        return "default"


class DataSummaryNode(Node):
    """
    数据概况生成节点
    
    功能：生成增强数据的统计概况（供Agent决策参考）
    类型：Regular Node
    """
    
    def prep(self, shared):
        """读取增强数据"""
        return shared.get("data", {}).get("blog_data", [])
    
    def exec(self, prep_res):
        """计算各维度分布、时间跨度、总量等统计信息"""
        blog_data = prep_res
        
        if not blog_data:
            return {"summary": "无数据", "statistics": {}}
        
        from collections import Counter
        from datetime import datetime
        
        # 基础统计
        total = len(blog_data)
        
        # 情感分布
        sentiment_dist = Counter(p.get("sentiment_polarity") for p in blog_data if p.get("sentiment_polarity"))
        
        # 发布者分布
        publisher_dist = Counter(p.get("publisher") for p in blog_data if p.get("publisher"))
        
        # 主题分布
        parent_topics = Counter()
        for p in blog_data:
            topics = p.get("topics") or []
            if not isinstance(topics, list):
                continue
            for t in topics:
                if isinstance(t, dict) and t.get("parent_topic"):
                    parent_topics[t["parent_topic"]] += 1
        
        # 地理分布
        location_dist = Counter(p.get("location") for p in blog_data if p.get("location"))
        
        # 时间范围
        publish_times = []
        for p in blog_data:
            pt = p.get("publish_time")
            if pt:
                try:
                    publish_times.append(datetime.strptime(pt, "%Y-%m-%d %H:%M:%S"))
                except:
                    pass
        
        time_range = None
        if publish_times:
            time_range = {
                "start": min(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "end": max(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "span_hours": round((max(publish_times) - min(publish_times)).total_seconds() / 3600, 1)
            }
        
        # 互动统计
        total_reposts = sum(p.get("repost_count", 0) for p in blog_data)
        total_comments = sum(p.get("comment_count", 0) for p in blog_data)
        total_likes = sum(p.get("like_count", 0) for p in blog_data)
        
        summary_text = f"""数据概况:
- 总博文数: {total}
- 时间范围: {time_range['start'] if time_range else '未知'} 至 {time_range['end'] if time_range else '未知'}
- 情感分布: {dict(sentiment_dist.most_common(5))}
- 热门主题Top3: {[t[0] for t in parent_topics.most_common(3)]}
- 主要地区Top3: {[l[0] for l in location_dist.most_common(3)]}
- 发布者类型: {list(publisher_dist.keys())}
- 总互动量: 转发{total_reposts}, 评论{total_comments}, 点赞{total_likes}"""
        
        return {
            "summary": summary_text,
            "statistics": {
                "total_posts": total,
                "time_range": time_range,
                "sentiment_distribution": dict(sentiment_dist),
                "publisher_distribution": dict(publisher_dist),
                "topic_distribution": dict(parent_topics.most_common(10)),
                "location_distribution": dict(location_dist.most_common(10)),
                "engagement": {
                    "total_reposts": total_reposts,
                    "total_comments": total_comments,
                    "total_likes": total_likes
                }
            }
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储统计信息"""
        if "agent" not in shared:
            shared["agent"] = {}
        
        shared["agent"]["data_summary"] = exec_res["summary"]
        shared["agent"]["data_statistics"] = exec_res["statistics"]
        
        print(f"\n[DataSummary] 数据概况已生成")
        print(exec_res["summary"])
        
        return "default"


class SaveAnalysisResultsNode(Node):
    """
    保存分析结果节点

    功能：将分析结果持久化，供阶段3使用
    类型：Regular Node
    输出位置：
    - 统计数据：report/analysis_data.json
    - 图表分析：report/chart_analyses.json
    - 洞察描述：report/insights.json
    - 图表文件：report/images/
    """

    def prep(self, shared):
        """读取分析输出、图表列表和图表分析结果"""
        stage2_results = shared.get("stage2_results", {})

        return {
            "charts": stage2_results.get("charts", []),
            "tables": stage2_results.get("tables", []),
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "insights": stage2_results.get("insights", {}),
            "execution_log": stage2_results.get("execution_log", {})
        }
    
    def exec(self, prep_res):
        """保存JSON结果文件"""
        output_dir = "report"
        os.makedirs(output_dir, exist_ok=True)

        # 保存分析数据
        analysis_data = {
            "charts": prep_res["charts"],
            "tables": prep_res["tables"],
            "execution_log": prep_res["execution_log"]
        }

        analysis_data_path = os.path.join(output_dir, "analysis_data.json")
        with open(analysis_data_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        # 保存图表分析结果（新增）
        chart_analyses_path = os.path.join(output_dir, "chart_analyses.json")
        with open(chart_analyses_path, 'w', encoding='utf-8') as f:
            json.dump(prep_res["chart_analyses"], f, ensure_ascii=False, indent=2)

        # 保存洞察描述
        insights_path = os.path.join(output_dir, "insights.json")
        with open(insights_path, 'w', encoding='utf-8') as f:
            json.dump(prep_res["insights"], f, ensure_ascii=False, indent=2)

        return {
            "success": True,
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "charts_count": len(prep_res["charts"]),
            "tables_count": len(prep_res["tables"]),
            "chart_analyses_count": len(prep_res["chart_analyses"])
        }
    
    def post(self, shared, prep_res, exec_res):
        """记录保存状态"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        shared["stage2_results"]["output_files"] = {
            "charts_dir": "report/images/",
            "analysis_data": exec_res["analysis_data_path"],
            "chart_analyses_file": exec_res["chart_analyses_path"],
            "insights_file": exec_res["insights_path"]
        }

        print(f"\n[SaveAnalysisResults] [OK] 分析结果已保存")
        print(f"  - 分析数据: {exec_res['analysis_data_path']}")
        print(f"  - 图表分析: {exec_res['chart_analyses_path']}")
        print(f"  - 洞察描述: {exec_res['insights_path']}")
        print(f"  - 生成图表: {exec_res['charts_count']} 个")
        print(f"  - 分析图表: {exec_res['chart_analyses_count']} 个")
        print(f"  - 生成表格: {exec_res['tables_count']} 个")

        return "default"




# -----------------------------------------------------------------------------
# 4.2 预定义Workflow路径节点 (analysis_mode="workflow")
# -----------------------------------------------------------------------------

class ExecuteAnalysisScriptNode(Node):
    """
    执行分析脚本节点
    
    功能：执行固定的分析脚本，生成全部所需图形
    类型：Regular Node
    
    执行四类工具集的全部工具函数：
    - 情感趋势分析工具集
    - 主题演化分析工具集
    - 地理分布分析工具集
    - 多维交互分析工具集
    """
    
    def prep(self, shared):
        """读取增强数据"""
        return shared.get("data", {}).get("blog_data", [])
    
    def exec(self, prep_res):
        """执行预定义的分析脚本"""
        from utils.analysis_tools import (
            # 情感工具
            sentiment_distribution_stats,
            sentiment_time_series,
            sentiment_anomaly_detection,
            sentiment_trend_chart,
            sentiment_pie_chart,
            sentiment_bucket_trend_chart,
            sentiment_attribute_trend_chart,
            sentiment_focus_window_chart,
            sentiment_focus_publisher_chart,
            # 主题工具
            topic_frequency_stats,
            topic_time_evolution,
            topic_cooccurrence_analysis,
            topic_ranking_chart,
            topic_evolution_chart,
            topic_network_chart,
            topic_focus_evolution_chart,
            topic_keyword_trend_chart,
            topic_focus_distribution_chart,
            # 地理工具
            geographic_distribution_stats,
            geographic_hotspot_detection,
            geographic_sentiment_analysis,
            geographic_heatmap,
            geographic_bar_chart,
            geographic_sentiment_bar_chart,
            geographic_topic_heatmap,
            geographic_temporal_heatmap,
            # 交互工具
            publisher_distribution_stats,
            cross_dimension_matrix,
            influence_analysis,
            correlation_analysis,
            interaction_heatmap,
            publisher_bar_chart,
            publisher_sentiment_bucket_chart,
            publisher_topic_distribution_chart,
            participant_trend_chart,
            publisher_focus_distribution_chart,
            belief_network_chart,
        )
        import time
        
        blog_data = prep_res
        start_time = time.time()
        
        charts = []
        tables = []
        tools_executed = []
        
        print("\n[ExecuteAnalysisScript] 开始执行预定义分析脚本...")
        
        # === 1. 情感趋势分析 ===
        print("\n  [CHART] 执行情感趋势分析...")
        
        # 情感分布统计
        result = sentiment_distribution_stats(blog_data)
        tables.append({
            "id": "sentiment_distribution",
            "title": "情感极性分布统计",
            "data": result["data"],
            "source_tool": "sentiment_distribution_stats"
        })
        tools_executed.append("sentiment_distribution_stats")
        
        # 情感时序分析
        sentiment_ts_result = sentiment_time_series(blog_data, granularity="hour")
        tables.append({
            "id": "sentiment_time_series",
            "title": "情感时序趋势数据",
            "data": sentiment_ts_result["data"],
            "source_tool": "sentiment_time_series"
        })
        tools_executed.append("sentiment_time_series")

        tables.append({
            "id": "sentiment_peaks",
            "title": "情感峰值与拐点",
            "data": {
                "peak_periods": sentiment_ts_result["data"].get("peak_periods", []),
                "peak_hours": sentiment_ts_result["data"].get("peak_hours", []),
                "turning_points": sentiment_ts_result["data"].get("turning_points", []),
                "volume_spikes": sentiment_ts_result["data"].get("volume_spikes", [])
            },
            "source_tool": "sentiment_time_series"
        })
        
        # 情感异常检测
        result = sentiment_anomaly_detection(blog_data)
        tables.append({
            "id": "sentiment_anomaly",
            "title": "情感异常点",
            "data": result["data"],
            "source_tool": "sentiment_anomaly_detection"
        })
        tools_executed.append("sentiment_anomaly_detection")
        
        # 情感趋势图
        result = sentiment_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_trend_chart")

        # 情感桶趋势
        result = sentiment_bucket_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_bucket_trend_chart")

        # 情感属性趋势
        result = sentiment_attribute_trend_chart(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_attribute_trend_chart")

        # 焦点窗口情感趋势（窗口内极性均值 + 三分类）
        result = sentiment_focus_window_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "sentiment_focus_window_data",
                "title": "焦点窗口情感数据",
                "data": result.get("data", {}),
                "source_tool": "sentiment_focus_window_chart"
            })
        tools_executed.append("sentiment_focus_window_chart")

        # 焦点窗口发布者情感趋势
        result = sentiment_focus_publisher_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "sentiment_focus_publisher_data",
                "title": "焦点窗口发布者情感均值",
                "data": result.get("data", {}),
                "source_tool": "sentiment_focus_publisher_chart"
            })
        tools_executed.append("sentiment_focus_publisher_chart")
        
        # 情感饼图
        result = sentiment_pie_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_pie_chart")
        
        # === 2. 主题演化分析 ===
        print("  [CHART] 执行主题演化分析...")
        
        # 主题频次统计
        result = topic_frequency_stats(blog_data)
        tables.append({
            "id": "topic_frequency",
            "title": "主题频次统计",
            "data": result["data"],
            "source_tool": "topic_frequency_stats"
        })
        tools_executed.append("topic_frequency_stats")
        
        # 主题演化分析
        result = topic_time_evolution(blog_data, granularity="day", top_n=5)
        tables.append({
            "id": "topic_evolution",
            "title": "主题演化数据",
            "data": result["data"],
            "source_tool": "topic_time_evolution"
        })
        tools_executed.append("topic_time_evolution")
        
        # 主题共现分析
        result = topic_cooccurrence_analysis(blog_data)
        tables.append({
            "id": "topic_cooccurrence",
            "title": "主题共现关系",
            "data": result["data"],
            "source_tool": "topic_cooccurrence_analysis"
        })
        tools_executed.append("topic_cooccurrence_analysis")
        
        # 主题排行图
        result = topic_ranking_chart(blog_data, top_n=10)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_ranking_chart")
        
        # 主题演化图
        result = topic_evolution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_evolution_chart")

        # 主题焦点演化
        result = topic_focus_evolution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_focus_evolution_chart")

        # 焦点窗口主题发布趋势（独立窗口数据）
        result = topic_focus_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "topic_focus_distribution_data",
                "title": "焦点窗口主题发布趋势数据",
                "data": result.get("data", {}),
                "source_tool": "topic_focus_distribution_chart"
            })
        tools_executed.append("topic_focus_distribution_chart")

        # 焦点关键词趋势
        result = topic_keyword_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_keyword_trend_chart")
        
        # 主题网络图
        result = topic_network_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_network_chart")
        
        # === 3. 地理分布分析 ===
        print("  [CHART] 执行地理分布分析...")
        
        # 地理分布统计
        result = geographic_distribution_stats(blog_data)
        tables.append({
            "id": "geographic_distribution",
            "title": "地理分布统计",
            "data": result["data"],
            "source_tool": "geographic_distribution_stats"
        })
        tools_executed.append("geographic_distribution_stats")
        
        # 热点区域识别
        result = geographic_hotspot_detection(blog_data)
        tables.append({
            "id": "geographic_hotspot",
            "title": "热点区域",
            "data": result["data"],
            "source_tool": "geographic_hotspot_detection"
        })
        tools_executed.append("geographic_hotspot_detection")
        
        # 地区情感分析
        result = geographic_sentiment_analysis(blog_data)
        tables.append({
            "id": "geographic_sentiment",
            "title": "地区情感分析",
            "data": result["data"],
            "source_tool": "geographic_sentiment_analysis"
        })
        tools_executed.append("geographic_sentiment_analysis")
        
        # 地理热力图
        result = geographic_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_heatmap")
        
        # 地区分布图
        result = geographic_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_bar_chart")

        # 地区正负面对比
        result = geographic_sentiment_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_sentiment_bar_chart")

        # 地区 × 主题热力图
        result = geographic_topic_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_topic_heatmap")

        # 地区 × 时间热力图（天粒度）
        result = geographic_temporal_heatmap(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_temporal_heatmap")
        
        # === 4. 多维交互分析 ===
        print("  [CHART] 执行多维交互分析...")
        
        # 发布者分布统计
        result = publisher_distribution_stats(blog_data)
        tables.append({
            "id": "publisher_distribution",
            "title": "发布者分布统计",
            "data": result["data"],
            "source_tool": "publisher_distribution_stats"
        })
        tools_executed.append("publisher_distribution_stats")
        
        # 交叉矩阵分析
        result = cross_dimension_matrix(blog_data, dim1="publisher", dim2="sentiment_polarity")
        tables.append({
            "id": "cross_dimension_matrix",
            "title": "发布者×情感交叉矩阵",
            "data": result["data"],
            "source_tool": "cross_dimension_matrix"
        })
        tools_executed.append("cross_dimension_matrix")
        
        # 影响力分析
        result = influence_analysis(blog_data, top_n=20)
        tables.append({
            "id": "influence_analysis",
            "title": "影响力分析",
            "data": result["data"],
            "source_tool": "influence_analysis"
        })
        tools_executed.append("influence_analysis")
        
        # 相关性分析
        result = correlation_analysis(blog_data)
        tables.append({
            "id": "correlation_analysis",
            "title": "维度相关性分析",
            "data": result["data"],
            "source_tool": "correlation_analysis"
        })
        tools_executed.append("correlation_analysis")
        
        # 交互热力图
        result = interaction_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("interaction_heatmap")
        
        # 发布者分布图
        result = publisher_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_bar_chart")

        # 发布者情绪桶对比
        result = publisher_sentiment_bucket_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_sentiment_bucket_chart")

        # 发布者话题偏好
        result = publisher_topic_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_topic_distribution_chart")

        # 参与人数趋势
        result = participant_trend_chart(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("participant_trend_chart")

        # 焦点窗口发布者类型发布趋势（独立窗口数据）
        result = publisher_focus_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "publisher_focus_distribution_data",
                "title": "焦点窗口发布者类型发布趋势数据",
                "data": result.get("data", {}),
                "source_tool": "publisher_focus_distribution_chart"
            })
        tools_executed.append("publisher_focus_distribution_chart")

        # 信念系统网络
        result = belief_network_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "belief_network_data",
                "title": "信念系统共现网络数据",
                "data": result.get("data", {}),
                "source_tool": "belief_network_chart"
            })
        tools_executed.append("belief_network_chart")

        # 确保已注册工具都被调用（避免遗漏新工具）
        try:
            from utils.analysis_tools.tool_registry import TOOL_REGISTRY
            executed_set = set(tools_executed)
            for tool_name, tool_def in TOOL_REGISTRY.items():
                if tool_name in executed_set:
                    continue
                params = {}
                for param_name, spec in (tool_def.get("parameters") or {}).items():
                    if param_name == "blog_data":
                        params[param_name] = blog_data
                    elif "default" in spec:
                        params[param_name] = spec["default"]
                result = tool_def["function"](**params)
                tools_executed.append(tool_name)
                executed_set.add(tool_name)
                if isinstance(result, dict) and result.get("charts"):
                    charts.extend(result["charts"])
                elif isinstance(result, dict) and "data" in result:
                    tables.append({
                        "id": tool_name,
                        "title": tool_def.get("description", tool_name),
                        "data": result["data"],
                        "source_tool": tool_name
                    })
        except Exception as e:
            print(f"[ExecuteAnalysisScript] [!] 自动补齐工具失败: {e}")

        execution_time = time.time() - start_time
        
        print(f"\n[ExecuteAnalysisScript] [OK] 分析脚本执行完成")
        print(f"  - 执行工具: {len(tools_executed)} 个")
        print(f"  - 生成图表: {len(charts)} 个")
        print(f"  - 生成表格: {len(tables)} 个")
        print(f"  - 耗时: {execution_time:.2f} 秒")
        
        return {
            "charts": charts,
            "tables": tables,
            "tools_executed": tools_executed,
            "execution_time": execution_time
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储图形和表格到shared"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}
        
        shared["stage2_results"]["charts"] = exec_res["charts"]
        shared["stage2_results"]["tables"] = exec_res["tables"]
        shared["stage2_results"]["execution_log"] = {
            "tools_executed": exec_res["tools_executed"],
            "total_charts": len(exec_res["charts"]),
            "total_tables": len(exec_res["tables"]),
            "execution_time": exec_res["execution_time"]
        }

        return "default"


class ChartAnalysisNode(Node):
    """
    图表分析节点 - 使用GLM4.5V+思考模式分析图表

    功能：对每个生成的图表进行深度视觉分析
    类型：Regular Node（兼容现有Workflow）

    设计特点：
    - GLM4.5V + 思考模式：既支持视觉理解，又支持深度推理
    - 顺序处理：为确保与现有Flow兼容，采用同步处理
    - 结构化输出：提供一致性的分析结果格式
    """

    def __init__(self, max_retries: int = 3, wait: int = 2):
        """
        初始化图表分析节点

        Args:
            max_retries: API调用失败重试次数
            wait: 重试等待时间(秒)
        """
        super().__init__(max_retries=max_retries, wait=wait)

    def prep(self, shared):
        """读取图表列表"""
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
        """顺序分析所有图表"""
        import time
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

            # 构建简化的分析提示词
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
                # 调用GLM4.5V分析图表
                response = call_glm45v_thinking(
                    prompt=analysis_prompt,
                    image_paths=[chart_path] if chart_path and os.path.exists(chart_path) else None,
                    temperature=0.7,
                    max_tokens=2000,
                    enable_thinking=True
                )

                # 直接使用LLM的自然语言输出，无需JSON解析
                analysis_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": response.strip(),
                    "analysis_timestamp": time.time(),
                    "analysis_status": "success"
                }

                chart_analyses[chart_id] = analysis_result
                success_count += 1
                print(f"[ChartAnalysis] [√] 图表 {chart_id} 分析完成")
                print(f"[ChartAnalysis] [√] 分析长度: {len(response)} 字符")

            except Exception as e:
                # 简化错误处理
                print(f"[ChartAnalysis] [!] 图表 {chart_id} 分析失败: {str(e)}")

                # 创建简单的fallback结果
                fallback_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": f"图表分析失败: {str(e)}",
                    "analysis_timestamp": time.time(),
                    "analysis_status": "failed"
                }
                chart_analyses[chart_id] = fallback_result

        execution_time = time.time() - start_time

        return {
            "chart_analyses": chart_analyses,
            "success_count": success_count,
            "total_charts": len(charts),
            "success_rate": success_count/len(charts) if charts else 0,
            "execution_time": execution_time
        }

    def post(self, shared, prep_res, exec_res):
        """存储分析结果到shared"""
        # 初始化图表分析结果
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        # 存储到shared字典
        shared["stage2_results"]["chart_analyses"] = exec_res["chart_analyses"]

        # 输出执行摘要
        print(f"\n[ChartAnalysis] 图表分析完成:")
        print(f"  ├─ 总图表数: {exec_res['total_charts']}")
        print(f"  ├─ 成功分析: {exec_res['success_count']}")
        print(f"  ├─ 失败数量: {exec_res['total_charts'] - exec_res['success_count']}")
        print(f"  └─ 成功率: {exec_res['success_rate']*100:.1f}%")
        print(f"  └─ 耗时: {exec_res['execution_time']:.2f}秒")

        # 存储执行日志
        if "execution_log" not in shared["stage2_results"]:
            shared["stage2_results"]["execution_log"] = {}

        shared["stage2_results"]["execution_log"]["chart_analysis"] = {
            "total_charts": exec_res["total_charts"],
            "success_count": exec_res["success_count"],
            "success_rate": exec_res["success_rate"],
            "analysis_timestamp": exec_res["execution_time"]
        }

        return "default"

    

class LLMInsightNode(Node):
    """
    LLM洞察补充节点

    功能：基于GLM4.5V图表分析结果，调用LLM生成综合洞察
    类型：Regular Node (LLM Call)

    基于图表分析结果和统计数据，利用LLM生成各维度的深度洞察描述
    """

    def prep(self, shared):
        """读取图表分析结果和统计数据"""
        stage2_results = shared.get("stage2_results", {})

        return {
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "tables": stage2_results.get("tables", []),
            "data_summary": shared.get("agent", {}).get("data_summary", "")
        }
    
    def exec(self, prep_res):
        """基于图表分析结果构建Prompt调用LLM，生成深度洞察"""
        chart_analyses = prep_res["chart_analyses"]
        tables = prep_res["tables"]
        data_summary = prep_res["data_summary"]

        # 构建简化图表分析摘要
        chart_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                content = analysis.get("analysis", "")

                chart_summary.append(f"### {title}")

                # 截取前500字符作为摘要，避免过长
                content_preview = content[:500] + ("..." if len(content) > 500 else "")
                chart_summary.append(content_preview)
                chart_summary.append("")
            else:
                # 处理分析失败的情况
                title = analysis.get("chart_title", chart_id)
                status = analysis.get("analysis_status", "unknown")
                chart_summary.append(f"### {title}")
                chart_summary.append(f"分析状态: {status}")
                chart_summary.append("")

        # 构建统计数据摘要
        stats_summary = []
        for table in tables:
            title = table.get("title", "")
            data = table.get("data", {})
            summary = data.get("summary", "") if isinstance(data, dict) else ""
            if summary:
                stats_summary.append(f"- {title}: {summary}")

        # 构建完整提示词
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

        # 优先使用GLM-4.6推理模型进行综合分析，开启推理模式以获得更好的分析质量
        # 如果GLM-4.6失败（如并发限制），自动回退到GLM-4.5-air
        response = None
        use_fallback = False
        
        try:
            response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)
        except Exception as e:
            error_msg = str(e)
            # 检测是否是并发限制或其他可恢复的错误，回退到glm-4.5-air
            # 429: 并发限制；concurrency: 并发相关错误；调用glm4.6模型失败: 通用失败
            is_recoverable_error = (
                "429" in error_msg or 
                "concurrency" in error_msg.lower() or 
                "调用glm4.6模型失败" in error_msg or
                "rate limit" in error_msg.lower() or
                "API并发限制" in error_msg
            )
            
            if is_recoverable_error:
                print(f"[LLMInsight] GLM-4.6调用失败: {error_msg}")
                print(f"[LLMInsight] 回退到GLM-4.5-air模型...")
                try:
                    # 使用glm-4.5-air，增加超时时间以适应长prompt
                    response = call_glm_45_air(prompt, temperature=0.7, timeout=120)
                    use_fallback = True
                    print(f"[LLMInsight] ✓ 已成功使用GLM-4.5-air生成洞察")
                except Exception as fallback_error:
                    # 如果回退也失败，抛出详细的错误信息
                    raise Exception(
                        f"GLM-4.6和GLM-4.5-air都调用失败。\n"
                        f"GLM-4.6错误: {error_msg}\n"
                        f"GLM-4.5-air错误: {str(fallback_error)}"
                    )
            else:
                # 其他类型的错误直接抛出，不进行回退
                raise

        # 解析JSON响应
        try:
            # 尝试提取JSON部分
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()

            insights = json.loads(json_str)
        except json.JSONDecodeError:
            # 如果解析失败，基于响应创建结构化洞察
            insights = {
                "sentiment_insight": "基于图表分析，情感趋势显示整体态势相对稳定，需要关注异常波动点。",
                "topic_insight": "主题演化分析表明核心话题持续活跃，新兴话题呈现增长趋势。",
                "geographic_insight": "地理分布分析显示热点区域集中，区域差异特征明显。",
                "cross_dimension_insight": "发布者类型分析显示不同群体影响力差异显著，交互模式多样。",
                "summary_insight": response[:800] if response else "综合分析已完成，建议关注图表中的关键发现。"
            }

        return insights
    
    def post(self, shared, prep_res, exec_res):
        """填充insights到shared"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}
        
        shared["stage2_results"]["insights"] = exec_res
        
        print(f"\n[LLMInsight] [OK] 洞察分析生成完成")
        for key, value in exec_res.items():
            preview = value[:80] + "..." if len(value) > 80 else value
            print(f"  - {key}: {preview}")
        
        return "default"


# -----------------------------------------------------------------------------
# 4.3 Agent自主调度路径节点 (analysis_mode="agent")
# -----------------------------------------------------------------------------

class CollectToolsNode(Node):
    """
    工具收集节点

    功能：通过MCP服务器收集所有可用的分析工具列表
    类型：Regular Node
    控制参数：shared["config"]["tool_source"]

    MCP协议特点：
    - 通过MCP协议动态发现和调用分析工具
    - 支持工具的动态扩展和版本管理
    - 标准化的工具调用接口
    """

    def prep(self, shared):
        """读取tool_source配置"""
        config = shared.get("config", {})
        tool_source = config.get("tool_source", "mcp")
        return {"tool_source": tool_source}

    def exec(self, prep_res):
        """通过MCP服务器收集所有可用的分析工具列表"""
        tool_source = prep_res["tool_source"]

        # 启用MCP模式
        from utils.mcp_client.mcp_client import set_mcp_mode, get_tools

        if tool_source == "mcp":
            set_mcp_mode(True)
            print(f"[CollectTools] 使用MCP模式获取工具")
            tools = get_tools('utils/mcp_server')
        else:
            set_mcp_mode(False)
            print(f"[CollectTools] 不支持的工具源: {tool_source}")
            tools = []

        return {
            "tools": tools,
            "tool_source": tool_source,
            "tool_count": len(tools)
        }

    def post(self, shared, prep_res, exec_res):
        """将工具定义存储到shared"""
        if "agent" not in shared:
            shared["agent"] = {}

        shared["agent"]["available_tools"] = exec_res["tools"]
        shared["agent"]["execution_history"] = []
        shared["agent"]["current_iteration"] = 0
        shared["agent"]["is_finished"] = False
        shared["agent"]["tool_source"] = exec_res["tool_source"]  # 记录使用的工具来源

        config = shared.get("config", {})
        agent_config = config.get("agent_config", {})
        shared["agent"]["max_iterations"] = agent_config.get("max_iterations", 10)

        print(f"\n[CollectTools] [OK] 收集到 {exec_res['tool_count']} 个可用工具 ({exec_res['tool_source']}模式)")

        # 按类别显示工具
        categories = {}
        for tool in exec_res["tools"]:
            cat = tool.get("category", "其他")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(tool["name"])

        for cat, tool_names in categories.items():
            print(f"  - {cat}: {', '.join(tool_names)}")

        return "default"


class DecisionToolsNode(Node):
    """
    工具决策节点

    功能：GLM4.6智能体推理决定下一步执行哪个分析工具，或判断分析已充分
    类型：Regular Node (LLM Call)
    模型配置：GLM4.6 + 推理模式（智能体推理）
    循环入口：Agent Loop的决策起点
    """
    
    def prep(self, shared):
        """读取数据概况、可用工具、执行历史、当前迭代次数"""
        agent = shared.get("agent", {})
        
        return {
            "data_summary": agent.get("data_summary", ""),
            "available_tools": agent.get("available_tools", []),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10)
        }
    
    def exec(self, prep_res):
        """构建Prompt调用GLM4.6，获取决策结果"""
        data_summary = prep_res["data_summary"]
        available_tools = prep_res["available_tools"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]

        # 构建工具列表描述
        tools_description = []
        for tool in available_tools:
            tools_description.append(
                f"- {tool['name']} ({tool['category']}): {tool['description']}"
            )
        tools_text = "\n".join(tools_description)

        # 构建完整执行历史描述
        if execution_history:
            # 创建已执行工具的集合，便于检测重复
            executed_tools = set()
            history_items = []

            # 按时间顺序整理所有执行过的工具
            for i, item in enumerate(execution_history, 1):
                tool_name = item['tool_name']
                summary = item.get('summary', '已执行')
                has_chart = item.get('has_chart', False)
                has_data = item.get('has_data', False)
                error = item.get('error', False)

                # 标记状态图标
                status_icon = "✅" if not error else "❌"
                chart_icon = "📊" if has_chart else ""
                data_icon = "📋" if has_data else ""

                history_items.append(
                    f"{i:2d}. {status_icon} **{tool_name}** {chart_icon}{data_icon}"
                )

                # 记录已执行的工具
                executed_tools.add(tool_name)

            # 生成历史文本
            history_text = "\n".join(history_items)

            # 创建已执行工具清单，避免重复
            executed_tools_list = sorted(list(executed_tools))
            executed_tools_summary = f"已执行工具清单 ({len(executed_tools_list)}个): {', '.join(executed_tools_list)}"

        else:
            history_text = "尚未执行任何工具"
            executed_tools_summary = "已执行工具清单: 无"

        prompt = f"""你是一个专业的舆情分析智能体，负责决定下一步的分析动作。请运用你的推理能力，基于当前分析状态做出最佳决策。

## 数据概况
{data_summary}

## 可用分析工具
{tools_text}

## 完整执行历史（按时间顺序）
{history_text}

## 工具执行状态总览
{executed_tools_summary}

## 当前状态
- 当前迭代: {current_iteration + 1}/{max_iterations}
- 已执行工具数: {len(execution_history)}
- 已执行工具覆盖率: {len(executed_tools) if execution_history else 0}/{len(available_tools)}

## 推理决策要求
请进行深度推理分析：

### 1. 执行历史分析
注意以下工具已经执行过：
{executed_tools_summary if execution_history else "无"}

### 2. 分析充分性评估
检查四个维度的覆盖情况：
- **情感分析维度**：sentiment_* 系列工具是否已执行？
- **主题分析维度**：topic_* 系列工具是否已执行？
- **地理分析维度**：geographic_* 系列工具是否已执行？
- **多维交互维度**：publisher_*, cross_*, influence_* 工具是否已执行？

### 3. 工具价值评估
- **数据价值优先**：选择能提供新统计数据的工具
- **可视化价值**：选择能生成新图表的工具
- **互补性分析**：选择与已有工具形成互补的工具
- **避免重复**：优先选择未执行过的工具

### 4. 执行策略
- **统计数据先行**：先执行 *_stats 工具获取基础数据
- **可视化工具后续**：再执行 *_chart 工具生成可视化
- **综合工具最后**：comprehensive_analysis 作为总结

## 决策输出
请以JSON格式输出你的推理决策：
```json
{{
    "thinking": "详细推理过程：1)重复检测结果 2)维度覆盖分析 3)工具价值评估 4)最终选择理由",
    "action": "execute或finish",
    "tool_name": "工具名称（必须是未执行的工具）",
    "reason": "选择该工具的具体原因和预期分析价值"
}}
```

**建议**：优先选择未执行过的工具以获得更全面的分析结果。"""

        # 使用GLM4.6模型，开启推理模式
        response = call_glm46(prompt, temperature=0.6, enable_reasoning=True)

        # 解析JSON响应
        try:
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()

            decision = json.loads(json_str)
        except json.JSONDecodeError:
            # 解析失败，默认继续执行
            decision = {
                "action": "execute",
                "tool_name": "sentiment_distribution_stats",
                "reason": "GLM4.6响应解析失败，默认从情感分析开始"
            }

        return decision
    
    def post(self, shared, prep_res, exec_res):
        """解析决策，返回Action"""
        action = exec_res.get("action", "execute")

        if action == "finish":
            shared["agent"]["is_finished"] = True
            print(f"\n[DecisionTools] GLM4.6智能体决定: 分析已充分，结束循环")
            print(f"  推理理由: {exec_res.get('reason', '无')}")
            return "finish"
        else:
            tool_name = exec_res.get("tool_name", "")
            shared["agent"]["next_tool"] = tool_name
            shared["agent"]["next_tool_reason"] = exec_res.get("reason", "")

            print(f"\n[DecisionTools] GLM4.6智能体决定: 执行工具 {tool_name}")
            print(f"  推理理由: {exec_res.get('reason', '无')}")

            return "execute"


class ExecuteToolsNode(Node):
    """
    工具执行节点

    功能：通过MCP协议执行决策节点选定的分析工具
    类型：Regular Node

    MCP协议特点：
    - 通过MCP协议调用远程分析工具
    - 标准化的工具调用接口
    - 支持工具的动态发现和版本管理
    """

    def prep(self, shared):
        """读取决策结果中的工具名称和数据"""
        agent = shared.get("agent", {})
        blog_data = shared.get("data", {}).get("blog_data", [])
        tool_source = agent.get("tool_source", "mcp")
        enhanced_data_path = shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", "")
        
        if not enhanced_data_path:
            print(f"[ExecuteTools] 警告: enhanced_data_path 在 prep 中为空")
        else:
            print(f"[ExecuteTools] prep: enhanced_data_path={enhanced_data_path}")

        return {
            "tool_name": agent.get("next_tool", ""),
            "blog_data": blog_data,
            "tool_source": tool_source,
            "enhanced_data_path": enhanced_data_path
        }

    def exec(self, prep_res):
        """通过MCP协议调用对应的分析工具函数"""
        tool_name = prep_res["tool_name"]
        blog_data = prep_res["blog_data"]
        tool_source = prep_res["tool_source"]
        enhanced_data_path = prep_res.get("enhanced_data_path") or ""

        if not tool_name:
            return {"error": "未指定工具名称"}

        print(f"\n[ExecuteTools] 执行工具: {tool_name} ({tool_source}模式)")

        # 使用MCP客户端调用工具
        from utils.mcp_client.mcp_client import call_tool

        try:
            # MCP server 是独立子进程：通过环境变量把增强数据路径传给它
            # 否则 mcp_server.get_blog_data() 会返回空列表，导致"没有可绘制的数据/没有地区数据"等
            # 优先使用 prep_res 中的路径，如果为空则使用环境变量中的路径
            if enhanced_data_path:
                abs_path = os.path.abspath(enhanced_data_path)
                os.environ["ENHANCED_DATA_PATH"] = abs_path
                print(f"[ExecuteTools] 设置 ENHANCED_DATA_PATH={abs_path}")
            else:
                # 如果没有从 prep_res 获取到路径，尝试从环境变量获取
                env_path = os.environ.get("ENHANCED_DATA_PATH")
                if env_path:
                    print(f"[ExecuteTools] 使用环境变量中的 ENHANCED_DATA_PATH={env_path}")
                else:
                    print(f"[ExecuteTools] 警告: enhanced_data_path 为空，环境变量中也未设置，可能导致数据加载失败")

            # 对于MCP工具，传递正确的服务器路径，不需要传递blog_data，服务器会自动加载
            result = call_tool('utils/mcp_server', tool_name, {})

            # 转换MCP结果为统一格式，保证charts存在且含id/title/path
            charts = []
            if isinstance(result, dict):
                charts = result.get("charts") or []

                # 兼容只有单个路径字段的返回
                single_path = result.get("chart_path") or result.get("image_path") or result.get("file_path")
                if not charts and single_path:
                    charts = [{
                        "id": result.get("chart_id", tool_name),
                        "title": result.get("title", tool_name),
                        "path": single_path,
                        "file_path": single_path,
                        "type": result.get("type", "unknown"),
                        "description": result.get("description", ""),
                        "source_tool": tool_name
                    }]

                # 规范化每个chart的字段
                normalized_charts = []
                for idx, ch in enumerate(charts):
                    if not isinstance(ch, dict):
                        continue
                    path = (
                        ch.get("path")
                        or ch.get("file_path")
                        or ch.get("chart_path")
                        or ch.get("image_path")
                        or ""
                    )
                    normalized_charts.append({
                        "id": ch.get("id") or f"{tool_name}_{idx}",
                        "title": ch.get("title") or tool_name,
                        "path": path,
                        "file_path": ch.get("file_path") or path,
                        "type": ch.get("type") or ch.get("chart_type") or "unknown",
                        "description": ch.get("description") or "",
                        "source_tool": ch.get("source_tool") or tool_name
                    })
                charts = normalized_charts

                final_result = {
                    "charts": charts,
                    "data": result if "data" not in result else result["data"],
                    "category": result.get("category") or self._get_tool_category(tool_name),
                    "summary": result.get("summary", f"MCP工具 {tool_name} 执行完成")
                }
            else:
                # 非字典结果兜底
                final_result = {
                    "charts": [],
                    "data": result,
                    "category": self._get_tool_category(tool_name),
                    "summary": f"MCP工具 {tool_name} 执行完成"
                }
        except Exception as e:
            print(f"[ExecuteTools] MCP工具调用失败: {str(e)}")
            final_result = {"error": f"MCP工具调用失败: {str(e)}"}

        return {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "result": final_result
        }

    def _get_tool_category(self, tool_name: str) -> str:
        """根据工具名称推断类别"""
        name_lower = tool_name.lower()
        if "sentiment" in name_lower:
            return "情感分析"
        elif "topic" in name_lower:
            return "主题分析"
        elif "geographic" in name_lower or "geo" in name_lower:
            return "地理分析"
        elif "publisher" in name_lower or "interaction" in name_lower:
            return "多维交互分析"
        else:
            return "其他"

    def post(self, shared, prep_res, exec_res):
        """存储结果，注册图表"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []}
            }

        tool_name = exec_res["tool_name"]
        tool_source = exec_res["tool_source"]
        result = exec_res.get("result", {})
        result_payload = result
        if isinstance(result, dict):
            if isinstance(result.get("result"), dict):
                result_payload = result["result"]
            elif isinstance(result.get("data"), dict) and (
                "charts" in result["data"] or "summary" in result["data"]
            ):
                result_payload = result["data"]

        # 记录执行的工具
        shared["stage2_results"]["execution_log"]["tools_executed"].append(tool_name)

        # 处理错误情况
        if "error" in result_payload:
            print(f"  [X] 工具执行失败: {result_payload['error']}")
            # 存储失败结果
            shared["agent"]["last_tool_result"] = {
                "tool_name": tool_name,
                "summary": f"工具执行失败: {result_payload['error']}",
                "has_chart": False,
                "has_data": False,
                "error": True
            }
            return "default"

        # 处理图表
        if result_payload.get("charts"):
            shared["stage2_results"]["charts"].extend(result_payload["charts"])
            print(f"  [OK] 生成 {len(result_payload['charts'])} 个图表")

        # 处理数据表格
        if result_payload.get("data"):
            shared["stage2_results"]["tables"].append({
                "id": tool_name,
                "title": result_payload.get("category", "") + " - " + tool_name,
                "data": result_payload["data"],
                "source_tool": tool_name,
                "source_type": tool_source  # 记录数据来源
            })
            print(f"  [OK] 生成数据表格")

        # 存储执行结果供ProcessResultNode使用
        shared["agent"]["last_tool_result"] = {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "summary": result_payload.get("summary", "执行完成"),
            "has_chart": bool(result_payload.get("charts")),
            "has_data": bool(result_payload.get("data")),
            "error": False
        }

        return "default"


class ProcessResultNode(Node):
    """
    结果处理节点
    
    功能：简单分析工具执行结果，更新执行历史，判断是否继续循环
    类型：Regular Node
    循环控制：根据分析结果和迭代次数决定是否返回决策节点
    """
    
    def prep(self, shared):
        """读取工具执行结果和当前迭代次数"""
        agent = shared.get("agent", {})
        
        return {
            "last_result": agent.get("last_tool_result", {}),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10),
            "is_finished": agent.get("is_finished", False)
        }
    
    def exec(self, prep_res):
        """格式化结果、更新迭代计数"""
        last_result = prep_res["last_result"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]
        is_finished = prep_res["is_finished"]
        
        # 添加到执行历史
        if last_result:
            execution_history.append(last_result)
        
        # 更新迭代计数
        new_iteration = current_iteration + 1
        
        # 判断是否继续
        should_continue = (
            not is_finished and 
            new_iteration < max_iterations
        )
        
        return {
            "execution_history": execution_history,
            "new_iteration": new_iteration,
            "should_continue": should_continue,
            "reason": (
                "Agent判断分析已充分" if is_finished else
                f"达到最大迭代次数({max_iterations})" if new_iteration >= max_iterations else
                "继续分析"
            )
        }
    
    def post(self, shared, prep_res, exec_res):
        """更新状态，返回Action"""
        if "agent" not in shared:
            shared["agent"] = {}
        
        shared["agent"]["execution_history"] = exec_res["execution_history"]
        shared["agent"]["current_iteration"] = exec_res["new_iteration"]
        
        print(f"\n[ProcessResult] 迭代 {exec_res['new_iteration']}: {exec_res['reason']}")
        
        if exec_res["should_continue"]:
            return "continue"
        else:
            # 结束循环前，生成洞察
            print("[ProcessResult] Agent循环结束，准备生成洞察分析")
            return "finish"


# =============================================================================
# 5. 阶段3节点: 报告生成
# =============================================================================

