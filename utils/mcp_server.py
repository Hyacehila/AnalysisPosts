"""
舆情分析智能体 - MCP服务器
提供所有分析工具的MCP接口
"""

from fastmcp import FastMCP
import sys
import os
from typing import Dict, Any, List

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.analysis_tools import (
    # 情感工具
    sentiment_distribution_stats,
    sentiment_time_series,
    sentiment_anomaly_detection,
    sentiment_trend_chart,
    sentiment_pie_chart,
    # 主题工具
    topic_frequency_stats,
    topic_time_evolution,
    topic_cooccurrence_analysis,
    topic_ranking_chart,
    topic_evolution_chart,
    topic_network_chart,
    # 地理工具
    geographic_distribution_stats,
    geographic_hotspot_detection,
    geographic_sentiment_analysis,
    geographic_heatmap,
    geographic_bar_chart,
    # 交互工具
    publisher_distribution_stats,
    cross_dimension_matrix,
    influence_analysis,
    correlation_analysis,
    interaction_heatmap,
    publisher_bar_chart,
)
from utils.data_loader import load_enhanced_blog_data

# 创建MCP服务器
mcp = FastMCP("Opinion Analysis Server")

# 全局数据存储
blog_data = None

def get_blog_data():
    """获取博文数据，如果未加载则自动加载"""
    global blog_data
    if blog_data is None:
        try:
            blog_data = load_enhanced_blog_data("data/test_enhanced_blogs.json")
        except Exception as e:
            # 尝试默认路径
            blog_data = load_enhanced_blog_data("data/enhanced_blogs.json")
    return blog_data

# =============================================================================
# 情感分析工具
# =============================================================================

@mcp.tool()
def sentiment_distribution() -> Dict[str, Any]:
    """获取情感分布统计数据"""
    blog_data = get_blog_data()
    result = sentiment_distribution_stats(blog_data)
    return result

@mcp.tool()
def sentiment_timeseries() -> Dict[str, Any]:
    """获取情感时序趋势数据"""
    blog_data = get_blog_data()
    result = sentiment_time_series(blog_data, granularity="hour")
    return result

@mcp.tool()
def sentiment_anomaly() -> Dict[str, Any]:
    """获取情感异常点检测数据"""
    blog_data = get_blog_data()
    result = sentiment_anomaly_detection(blog_data)
    return result

@mcp.tool()
def generate_sentiment_chart() -> Dict[str, Any]:
    """生成情感分析图表"""
    blog_data = get_blog_data()
    # 生成趋势图和饼图
    trend_result = sentiment_trend_chart(blog_data)
    pie_result = sentiment_pie_chart(blog_data)

    charts = []
    if trend_result.get("charts"):
        charts.extend(trend_result["charts"])
    if pie_result.get("charts"):
        charts.extend(pie_result["charts"])

    return {
        "charts": charts,
        "chart_count": len(charts),
        "type": "sentiment_visualization"
    }

# =============================================================================
# 主题分析工具
# =============================================================================

@mcp.tool()
def topic_frequency() -> Dict[str, Any]:
    """获取主题频次统计数据"""
    blog_data = get_blog_data()
    result = topic_frequency_stats(blog_data)
    return result

@mcp.tool()
def topic_evolution() -> Dict[str, Any]:
    """获取主题演化数据"""
    blog_data = get_blog_data()
    result = topic_time_evolution(blog_data, granularity="day", top_n=5)
    return result

@mcp.tool()
def topic_cooccurrence() -> Dict[str, Any]:
    """获取主题共现关系数据"""
    blog_data = get_blog_data()
    result = topic_cooccurrence_analysis(blog_data)
    return result

@mcp.tool()
def generate_topic_charts() -> Dict[str, Any]:
    """生成主题分析图表"""
    blog_data = get_blog_data()
    # 生成排行图、演化图和网络图
    ranking_result = topic_ranking_chart(blog_data, top_n=10)
    evolution_result = topic_evolution_chart(blog_data)
    network_result = topic_network_chart(blog_data)

    charts = []
    for result in [ranking_result, evolution_result, network_result]:
        if result.get("charts"):
            charts.extend(result["charts"])

    return {
        "charts": charts,
        "chart_count": len(charts),
        "type": "topic_visualization"
    }

# =============================================================================
# 地理分析工具
# =============================================================================

@mcp.tool()
def geographic_distribution() -> Dict[str, Any]:
    """获取地理分布统计数据"""
    blog_data = get_blog_data()
    result = geographic_distribution_stats(blog_data)
    return result

@mcp.tool()
def geographic_hotspot() -> Dict[str, Any]:
    """获取地理热点区域数据"""
    blog_data = get_blog_data()
    result = geographic_hotspot_detection(blog_data)
    return result

@mcp.tool()
def geographic_sentiment() -> Dict[str, Any]:
    """获取地区情感分析数据"""
    blog_data = get_blog_data()
    result = geographic_sentiment_analysis(blog_data)
    return result

@mcp.tool()
def generate_geographic_charts() -> Dict[str, Any]:
    """生成地理分析图表"""
    blog_data = get_blog_data()
    # 生成热力图和柱状图
    heatmap_result = geographic_heatmap(blog_data)
    bar_result = geographic_bar_chart(blog_data)

    charts = []
    for result in [heatmap_result, bar_result]:
        if result.get("charts"):
            charts.extend(result["charts"])

    return {
        "charts": charts,
        "chart_count": len(charts),
        "type": "geographic_visualization"
    }

# =============================================================================
# 多维交互分析工具
# =============================================================================

@mcp.tool()
def publisher_distribution() -> Dict[str, Any]:
    """获取发布者分布统计数据"""
    blog_data = get_blog_data()
    result = publisher_distribution_stats(blog_data)
    return result

@mcp.tool()
def cross_matrix(dim1: str = "publisher", dim2: str = "sentiment_polarity") -> Dict[str, Any]:
    """获取交叉维度分析数据"""
    blog_data = get_blog_data()
    result = cross_dimension_matrix(blog_data, dim1=dim1, dim2=dim2)
    return result

@mcp.tool()
def influence_analysis(top_n: int = 20) -> Dict[str, Any]:
    """获取影响力分析数据"""
    blog_data = get_blog_data()
    result = influence_analysis(blog_data, top_n=top_n)
    return result

@mcp.tool()
def correlation_analysis() -> Dict[str, Any]:
    """获取维度相关性分析数据"""
    blog_data = get_blog_data()
    result = correlation_analysis(blog_data)
    return result

@mcp.tool()
def generate_interaction_charts() -> Dict[str, Any]:
    """生成多维交互分析图表"""
    blog_data = get_blog_data()
    # 生成热力图和柱状图
    heatmap_result = interaction_heatmap(blog_data)
    bar_result = publisher_bar_chart(blog_data)

    charts = []
    for result in [heatmap_result, bar_result]:
        if result.get("charts"):
            charts.extend(result["charts"])

    return {
        "charts": charts,
        "chart_count": len(charts),
        "type": "interaction_visualization"
    }

# =============================================================================
# 综合分析工具
# =============================================================================

@mcp.tool()
def comprehensive_analysis() -> Dict[str, Any]:
    """执行综合分析，生成所有图表和统计数据"""
    blog_data = get_blog_data()

    all_charts = []
    all_tables = []

    # 情感分析
    sentiment_trend = sentiment_trend_chart(blog_data)
    sentiment_pie = sentiment_pie_chart(blog_data)
    all_tables.extend([
        {"id": "sentiment_distribution", "data": sentiment_distribution_stats(blog_data)["data"]},
        {"id": "sentiment_time_series", "data": sentiment_time_series(blog_data)["data"]},
        {"id": "sentiment_anomaly", "data": sentiment_anomaly_detection(blog_data)["data"]},
    ])

    # 主题分析
    topic_ranking = topic_ranking_chart(blog_data, top_n=10)
    topic_evolution = topic_evolution_chart(blog_data)
    topic_network = topic_network_chart(blog_data)
    all_tables.extend([
        {"id": "topic_frequency", "data": topic_frequency_stats(blog_data)["data"]},
        {"id": "topic_evolution", "data": topic_time_evolution(blog_data)["data"]},
        {"id": "topic_cooccurrence", "data": topic_cooccurrence_analysis(blog_data)["data"]},
    ])

    # 地理分析
    geo_heatmap = geographic_heatmap(blog_data)
    geo_bar = geographic_bar_chart(blog_data)
    all_tables.extend([
        {"id": "geographic_distribution", "data": geographic_distribution_stats(blog_data)["data"]},
        {"id": "geographic_hotspot", "data": geographic_hotspot_detection(blog_data)["data"]},
        {"id": "geographic_sentiment", "data": geographic_sentiment_analysis(blog_data)["data"]},
    ])

    # 多维交互分析
    interaction_heatmap_chart = interaction_heatmap(blog_data)
    publisher_bar = publisher_bar_chart(blog_data)
    all_tables.extend([
        {"id": "publisher_distribution", "data": publisher_distribution_stats(blog_data)["data"]},
        {"id": "cross_dimension_matrix", "data": cross_dimension_matrix(blog_data)["data"]},
        {"id": "influence_analysis", "data": influence_analysis(blog_data)["data"]},
        {"id": "correlation_analysis", "data": correlation_analysis(blog_data)["data"]},
    ])

    # 收集所有图表
    for result in [
        sentiment_trend, sentiment_pie,
        topic_ranking, topic_evolution, topic_network,
        geo_heatmap, geo_bar,
        interaction_heatmap_chart, publisher_bar
    ]:
        if result.get("charts"):
            all_charts.extend(result["charts"])

    return {
        "charts": all_charts,
        "tables": all_tables,
        "summary": {
            "total_charts": len(all_charts),
            "total_tables": len(all_tables),
            "analysis_types": ["sentiment", "topic", "geographic", "interaction"],
            "data_points": len(blog_data)
        },
        "type": "comprehensive_analysis"
    }

# 启动服务器
if __name__ == "__main__":
    mcp.run()