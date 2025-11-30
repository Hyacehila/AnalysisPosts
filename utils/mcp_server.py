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


# =============================================================================
# 图表生成工具
# =============================================================================

@mcp.tool()
def generate_sentiment_trend_chart() -> Dict[str, Any]:
    """生成情感趋势折线图/面积图"""
    blog_data = get_blog_data()
    result = sentiment_trend_chart(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def generate_sentiment_pie_chart() -> Dict[str, Any]:
    """生成情感分布饼图"""
    blog_data = get_blog_data()
    result = sentiment_pie_chart(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def generate_topic_ranking_chart(top_n: int = 10) -> Dict[str, Any]:
    """生成主题排行柱状图"""
    blog_data = get_blog_data()
    result = topic_ranking_chart(blog_data, top_n=top_n)
    return result

@mcp.tool()
def generate_topic_evolution_chart() -> Dict[str, Any]:
    """生成主题演化图"""
    blog_data = get_blog_data()
    result = topic_evolution_chart(blog_data)
    return result

@mcp.tool()
def generate_topic_network_chart() -> Dict[str, Any]:
    """生成主题关系网络图"""
    blog_data = get_blog_data()
    result = topic_network_chart(blog_data)
    return result

@mcp.tool()
def generate_geographic_heatmap() -> Dict[str, Any]:
    """生成地理分布热力图"""
    blog_data = get_blog_data()
    result = geographic_heatmap(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def generate_geographic_bar_chart() -> Dict[str, Any]:
    """生成地区分布柱状图"""
    blog_data = get_blog_data()
    result = geographic_bar_chart(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def generate_interaction_heatmap() -> Dict[str, Any]:
    """生成多维交互热力图"""
    blog_data = get_blog_data()
    result = interaction_heatmap(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def generate_publisher_bar_chart() -> Dict[str, Any]:
    """生成发布者分布柱状图"""
    blog_data = get_blog_data()
    result = publisher_bar_chart(blog_data, output_dir="report/images")
    return result


# 启动服务器
if __name__ == "__main__":
    mcp.run()