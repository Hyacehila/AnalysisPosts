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
    influence_analysis as influence_analysis_tool,
    correlation_analysis as correlation_analysis_tool,
    interaction_heatmap,
    publisher_bar_chart,
    publisher_sentiment_bucket_chart,
    publisher_topic_distribution_chart,
    participant_trend_chart,
    publisher_focus_distribution_chart,
    belief_network_chart as belief_network_chart_tool,
)
from utils.data_loader import load_enhanced_blog_data

# 创建MCP服务器
mcp = FastMCP("Opinion Analysis Server")

# 全局数据存储
blog_data = None

def get_blog_data():
    """获取博文数据，如果未加载则自动加载"""
    global blog_data
    if blog_data is not None:
        return blog_data

    candidates: List[str] = []
    env_path = os.getenv("ENHANCED_DATA_PATH")
    if env_path:
        candidates.append(env_path)

    # 兜底：常见默认路径（避免未设置 ENHANCED_DATA_PATH 时 MCP 工具全部“无数据”）
    candidates.extend([
        "data/enhanced_blogs.json",
        "data/test_posts_enhenced.json",
        "data/test_posts_enhanced.json",
    ])

    # 去重但保持顺序
    seen = set()
    candidates = [p for p in candidates if p and not (p in seen or seen.add(p))]

    
    last_err = None
    for path in candidates:
        if not path or not os.path.exists(path):
            continue
        try:
            blog_data = load_enhanced_blog_data(path)
            print(f"[MCP] Loaded blog data from {path}, count={len(blog_data)}")
            return blog_data
        except Exception as e:
            last_err = e
            print(f"[MCP] Failed to load {path}: {e}")

    print(f"[MCP] 无法加载博文数据，返回空列表。last_err={last_err}")
    blog_data = []
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

@mcp.tool()
def topic_focus_evolution() -> Dict[str, Any]:
    """获取带焦点窗口高亮的主题演化数据"""
    blog_data = get_blog_data()
    result = topic_focus_evolution_chart(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def topic_keyword_trend() -> Dict[str, Any]:
    """获取焦点关键词热度趋势数据"""
    blog_data = get_blog_data()
    result = topic_keyword_trend_chart(blog_data, output_dir="report/images")
    return result

@mcp.tool()
def topic_focus_distribution(window_days: int = 14, top_n: int = 5) -> Dict[str, Any]:
    """获取焦点窗口主题分布趋势（仅窗口数据）"""
    blog_data = get_blog_data()
    result = topic_focus_distribution_chart(
        blog_data, output_dir="report/images", window_days=window_days, top_n=top_n
    )
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

@mcp.tool()
def geographic_sentiment_bar(top_n: int = 12) -> Dict[str, Any]:
    """生成地区情感对比条形图"""
    blog_data = get_blog_data()
    result = geographic_sentiment_bar_chart(blog_data, output_dir="report/images", top_n=top_n)
    return result

@mcp.tool()
def geographic_topic_heatmap_tool(top_regions: int = 10, top_topics: int = 8) -> Dict[str, Any]:
    """生成地区×主题热力图"""
    blog_data = get_blog_data()
    result = geographic_topic_heatmap(
        blog_data, output_dir="report/images", top_regions=top_regions, top_topics=top_topics
    )
    return result

@mcp.tool()
def geographic_temporal_heatmap_tool(granularity: str = "day", top_regions: int = 8) -> Dict[str, Any]:
    """生成地区×时间热力图"""
    blog_data = get_blog_data()
    result = geographic_temporal_heatmap(
        blog_data, output_dir="report/images", granularity=granularity, top_regions=top_regions
    )
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
    result = influence_analysis_tool(blog_data, top_n=top_n)
    return result

@mcp.tool()
def correlation_analysis() -> Dict[str, Any]:
    """获取维度相关性分析数据"""
    blog_data = get_blog_data()
    result = correlation_analysis_tool(blog_data)
    return result

@mcp.tool()
def publisher_sentiment_bucket(top_n: int = 10) -> Dict[str, Any]:
    """生成发布者维度正/中/负情绪桶堆叠图"""
    blog_data = get_blog_data()
    result = publisher_sentiment_bucket_chart(blog_data, output_dir="report/images", top_n=top_n)
    return result

@mcp.tool()
def publisher_topic_distribution(top_publishers: int = 8, top_topics: int = 8) -> Dict[str, Any]:
    """生成发布者×主题分布堆叠图"""
    blog_data = get_blog_data()
    result = publisher_topic_distribution_chart(
        blog_data, output_dir="report/images", top_publishers=top_publishers, top_topics=top_topics
    )
    return result

@mcp.tool()
def participant_trend(granularity: str = "day") -> Dict[str, Any]:
    """生成新增/累计参与用户趋势"""
    blog_data = get_blog_data()
    result = participant_trend_chart(blog_data, output_dir="report/images", granularity=granularity)
    return result

@mcp.tool()
def publisher_focus_distribution(window_days: int = 14, top_n: int = 5) -> Dict[str, Any]:
    """生成焦点窗口发布者类型发布趋势"""
    blog_data = get_blog_data()
    result = publisher_focus_distribution_chart(
        blog_data, output_dir="report/images", window_days=window_days, top_n=top_n
    )
    return result


# =============================================================================
# 情感图表扩展
# =============================================================================

@mcp.tool()
def generate_sentiment_bucket_trend_chart(granularity: str = "day") -> Dict[str, Any]:
    """生成正中负情绪桶堆叠趋势图"""
    blog_data = get_blog_data()
    result = sentiment_bucket_trend_chart(blog_data, output_dir="report/images", granularity=granularity)
    return result

@mcp.tool()
def generate_sentiment_attribute_trend_chart(granularity: str = "day", top_n: int = 6) -> Dict[str, Any]:
    """生成情感属性热度趋势图"""
    blog_data = get_blog_data()
    result = sentiment_attribute_trend_chart(
        blog_data, output_dir="report/images", granularity=granularity, top_n=top_n
    )
    return result

@mcp.tool()
def generate_sentiment_focus_window_chart(window_days: int = 14) -> Dict[str, Any]:
    """生成焦点窗口情感趋势图（独立窗口数据）"""
    blog_data = get_blog_data()
    result = sentiment_focus_window_chart(blog_data, output_dir="report/images", window_days=window_days)
    return result

@mcp.tool()
def generate_sentiment_focus_publisher_chart(window_days: int = 14, top_n: int = 5) -> Dict[str, Any]:
    """生成焦点窗口 TopN 发布者情感趋势图"""
    blog_data = get_blog_data()
    result = sentiment_focus_publisher_chart(
        blog_data, output_dir="report/images", window_days=window_days, top_n=top_n
    )
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

@mcp.tool()
def belief_network_chart(event_name: str = "belief_network") -> Dict[str, Any]:
    """生成信念系统共现网络图"""
    blog_data = get_blog_data()
    result = belief_network_chart_tool(
        blog_data, output_dir="report/images", data_dir="report", event_name=event_name
    )
    return result


# 启动服务器
if __name__ == "__main__":
    mcp.run()
