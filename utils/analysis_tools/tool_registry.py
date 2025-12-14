"""
分析工具注册表

统一管理所有分析工具函数，支持：
1. 工具列表获取（用于Agent决策）
2. 按名称获取工具定义
3. 工具执行
"""

from typing import List, Dict, Any, Callable, Optional

# 导入所有工具函数
from .sentiment_tools import (
    sentiment_distribution_stats,
    sentiment_time_series,
    sentiment_anomaly_detection,
    sentiment_trend_chart,
    sentiment_pie_chart,
    sentiment_bucket_trend_chart,
    sentiment_attribute_trend_chart,
)

from .topic_tools import (
    topic_frequency_stats,
    topic_time_evolution,
    topic_cooccurrence_analysis,
    topic_ranking_chart,
    topic_evolution_chart,
    topic_network_chart,
    topic_focus_evolution_chart,
    topic_keyword_trend_chart,
)

from .geographic_tools import (
    geographic_distribution_stats,
    geographic_hotspot_detection,
    geographic_sentiment_analysis,
    geographic_heatmap,
    geographic_bar_chart,
    geographic_sentiment_bar_chart,
    geographic_topic_heatmap,
    geographic_temporal_heatmap,
)

from .interaction_tools import (
    publisher_distribution_stats,
    cross_dimension_matrix,
    influence_analysis,
    correlation_analysis,
    interaction_heatmap,
    publisher_bar_chart,
    publisher_sentiment_bucket_chart,
    publisher_topic_distribution_chart,
    participant_trend_chart,
)


# 工具注册表
TOOL_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ===== 情感趋势分析工具集 =====
    "sentiment_distribution_stats": {
        "name": "sentiment_distribution_stats",
        "category": "情感趋势分析",
        "description": "情感极性分布统计，统计各档位（极度悲观到极度乐观）的数量和占比",
        "function": sentiment_distribution_stats,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "sentiment_time_series": {
        "name": "sentiment_time_series",
        "category": "情感趋势分析",
        "description": "情感时序趋势分析，按小时或天聚合分析情感极性变化",
        "function": sentiment_time_series,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "hour"}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "sentiment_anomaly_detection": {
        "name": "sentiment_anomaly_detection",
        "category": "情感趋势分析",
        "description": "情感异常点检测，识别情感极性突变和峰值时刻",
        "function": sentiment_anomaly_detection,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "threshold": {"type": "float", "description": "异常阈值（标准差倍数）", "required": False, "default": 2.0}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "sentiment_trend_chart": {
        "name": "sentiment_trend_chart",
        "category": "情感趋势分析",
        "description": "生成情感趋势折线图和正负面占比面积图",
        "function": sentiment_trend_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "hour"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "sentiment_pie_chart": {
        "name": "sentiment_pie_chart",
        "category": "情感趋势分析",
        "description": "生成情感分布饼图，展示各情感极性的占比",
        "function": sentiment_pie_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "sentiment_bucket_trend_chart": {
        "name": "sentiment_bucket_trend_chart",
        "category": "情感趋势分析",
        "description": "正/负/中性情绪桶时序堆叠图，定位阶段差异和峰值",
        "function": sentiment_bucket_trend_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "hour"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "sentiment_attribute_trend_chart": {
        "name": "sentiment_attribute_trend_chart",
        "category": "情感趋势分析",
        "description": "Top情绪属性热度随时间的折线图，支持焦点窗口标注",
        "function": sentiment_attribute_trend_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_n": {"type": "int", "description": "展示的属性数量", "required": False, "default": 6}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    
    # ===== 主题演化分析工具集 =====
    "topic_frequency_stats": {
        "name": "topic_frequency_stats",
        "category": "主题演化分析",
        "description": "主题频次统计，统计父主题和子主题的出现频次和占比",
        "function": topic_frequency_stats,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "topic_time_evolution": {
        "name": "topic_time_evolution",
        "category": "主题演化分析",
        "description": "主题时序演化分析，分析主题热度随时间的变化趋势",
        "function": topic_time_evolution,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_n": {"type": "int", "description": "显示的热门主题数量", "required": False, "default": 5}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "topic_cooccurrence_analysis": {
        "name": "topic_cooccurrence_analysis",
        "category": "主题演化分析",
        "description": "主题共现关联分析，分析主题之间的共现关系",
        "function": topic_cooccurrence_analysis,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "min_support": {"type": "int", "description": "最小支持度阈值", "required": False, "default": 2}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "topic_ranking_chart": {
        "name": "topic_ranking_chart",
        "category": "主题演化分析",
        "description": "生成主题热度排行柱状图",
        "function": topic_ranking_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_n": {"type": "int", "description": "显示的主题数量", "required": False, "default": 10}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "topic_evolution_chart": {
        "name": "topic_evolution_chart",
        "category": "主题演化分析",
        "description": "生成主题演化时序图，展示主题热度变化趋势",
        "function": topic_evolution_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_n": {"type": "int", "description": "显示的主题数量", "required": False, "default": 5}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "topic_focus_evolution_chart": {
        "name": "topic_focus_evolution_chart",
        "category": "主题演化分析",
        "description": "带焦点窗口高亮的主题演化趋势图，定位峰值阶段",
        "function": topic_focus_evolution_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_n": {"type": "int", "description": "展示的主题数量", "required": False, "default": 5}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "topic_keyword_trend_chart": {
        "name": "topic_keyword_trend_chart",
        "category": "主题演化分析",
        "description": "焦点关键词热度趋势，用于话题词演化分析",
        "function": topic_keyword_trend_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_n": {"type": "int", "description": "展示的关键词数量", "required": False, "default": 8}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "topic_network_chart": {
        "name": "topic_network_chart",
        "category": "主题演化分析",
        "description": "生成主题关联网络图，展示主题间共现关系",
        "function": topic_network_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "min_support": {"type": "int", "description": "最小支持度阈值", "required": False, "default": 3}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    
    # ===== 地理分布分析工具集 =====
    "geographic_distribution_stats": {
        "name": "geographic_distribution_stats",
        "category": "地理分布分析",
        "description": "地理分布统计，统计博文的地理位置分布情况",
        "function": geographic_distribution_stats,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "geographic_hotspot_detection": {
        "name": "geographic_hotspot_detection",
        "category": "地理分布分析",
        "description": "热点区域识别，识别高密度的热点区域",
        "function": geographic_hotspot_detection,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "threshold_percentile": {"type": "float", "description": "热点阈值百分位数", "required": False, "default": 90}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "geographic_sentiment_analysis": {
        "name": "geographic_sentiment_analysis",
        "category": "地理分布分析",
        "description": "地区情感差异分析，分析不同地区的情感倾向差异",
        "function": geographic_sentiment_analysis,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "min_posts": {"type": "int", "description": "最小博文数阈值", "required": False, "default": 5}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "geographic_heatmap": {
        "name": "geographic_heatmap",
        "category": "地理分布分析",
        "description": "生成地区舆情热力图，展示各地区舆情指标差异",
        "function": geographic_heatmap,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "geographic_bar_chart": {
        "name": "geographic_bar_chart",
        "category": "地理分布分析",
        "description": "生成地区分布柱状图，展示博文数量及情感倾向",
        "function": geographic_bar_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_n": {"type": "int", "description": "显示的地区数量", "required": False, "default": 15}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "geographic_sentiment_bar_chart": {
        "name": "geographic_sentiment_bar_chart",
        "category": "地理分布分析",
        "description": "地区正/负面占比与平均极性的对比条形图",
        "function": geographic_sentiment_bar_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_n": {"type": "int", "description": "展示的地区数", "required": False, "default": 12}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "geographic_topic_heatmap": {
        "name": "geographic_topic_heatmap",
        "category": "地理分布分析",
        "description": "地区 × 主题热力图，呈现各地话题差异",
        "function": geographic_topic_heatmap,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_regions": {"type": "int", "description": "展示的地区数量", "required": False, "default": 10},
            "top_topics": {"type": "int", "description": "展示的主题数量", "required": False, "default": 8}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "geographic_temporal_heatmap": {
        "name": "geographic_temporal_heatmap",
        "category": "地理分布分析",
        "description": "地区 × 时间热力图，定位地区高峰与差异",
        "function": geographic_temporal_heatmap,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"},
            "top_regions": {"type": "int", "description": "展示的地区数", "required": False, "default": 8}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    
    # ===== 多维交互分析工具集 =====
    "publisher_distribution_stats": {
        "name": "publisher_distribution_stats",
        "category": "多维交互分析",
        "description": "发布者类型分布统计，统计不同发布者类型的博文数量和特征",
        "function": publisher_distribution_stats,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "cross_dimension_matrix": {
        "name": "cross_dimension_matrix",
        "category": "多维交互分析",
        "description": "多维交叉矩阵分析，生成两个维度的交叉分析矩阵",
        "function": cross_dimension_matrix,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "dim1": {"type": "string", "description": "第一个维度: publisher/location/topic", "required": False, "default": "publisher"},
            "dim2": {"type": "string", "description": "第二个维度: sentiment_polarity/topic/publisher", "required": False, "default": "sentiment_polarity"}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "influence_analysis": {
        "name": "influence_analysis",
        "category": "多维交互分析",
        "description": "影响力分析，分析博文的互动量和传播力",
        "function": influence_analysis,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "top_n": {"type": "int", "description": "返回的高影响力博文数量", "required": False, "default": 20}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "correlation_analysis": {
        "name": "correlation_analysis",
        "category": "多维交互分析",
        "description": "维度相关性分析，分析各特征维度之间的相关性",
        "function": correlation_analysis,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True}
        },
        "output_type": "data",
        "generates_chart": False
    },
    "interaction_heatmap": {
        "name": "interaction_heatmap",
        "category": "多维交互分析",
        "description": "生成交互热力图，展示两个维度的交叉分布",
        "function": interaction_heatmap,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "dim1": {"type": "string", "description": "第一个维度", "required": False, "default": "publisher"},
            "dim2": {"type": "string", "description": "第二个维度", "required": False, "default": "sentiment_polarity"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "publisher_bar_chart": {
        "name": "publisher_bar_chart",
        "category": "多维交互分析",
        "description": "生成发布者分布柱状图，展示不同发布者类型的博文数量及情感倾向",
        "function": publisher_bar_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "publisher_sentiment_bucket_chart": {
        "name": "publisher_sentiment_bucket_chart",
        "category": "多维交互分析",
        "description": "发布者维度的正/中/负面情绪桶堆叠对比",
        "function": publisher_sentiment_bucket_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_n": {"type": "int", "description": "展示的发布者数量", "required": False, "default": 10}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "publisher_topic_distribution_chart": {
        "name": "publisher_topic_distribution_chart",
        "category": "多维交互分析",
        "description": "发布者 × 主题堆叠图，呈现发布者话题偏好差异",
        "function": publisher_topic_distribution_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "top_publishers": {"type": "int", "description": "展示的发布者数", "required": False, "default": 8},
            "top_topics": {"type": "int", "description": "展示的主题数", "required": False, "default": 8}
        },
        "output_type": "chart",
        "generates_chart": True
    },
    "participant_trend_chart": {
        "name": "participant_trend_chart",
        "category": "多维交互分析",
        "description": "按时间粒度统计新增/累计参与用户趋势",
        "function": participant_trend_chart,
        "parameters": {
            "blog_data": {"type": "list", "description": "增强后的博文数据列表", "required": True},
            "output_dir": {"type": "string", "description": "图表输出目录", "required": False, "default": "report/images"},
            "granularity": {"type": "string", "description": "时间粒度: hour/day", "required": False, "default": "day"}
        },
        "output_type": "chart",
        "generates_chart": True
    }
}


def get_all_tools() -> List[Dict[str, Any]]:
    """
    获取所有可用工具的列表（供Agent决策使用）
    
    Returns:
        工具定义列表，每个工具包含name, category, description, parameters
    """
    tools = []
    for name, tool_info in TOOL_REGISTRY.items():
        tools.append({
            "name": tool_info["name"],
            "category": tool_info["category"],
            "description": tool_info["description"],
            "parameters": tool_info["parameters"],
            "output_type": tool_info["output_type"],
            "generates_chart": tool_info["generates_chart"]
        })
    return tools


def get_tool_by_name(tool_name: str) -> Optional[Dict[str, Any]]:
    """
    按名称获取工具定义
    
    Args:
        tool_name: 工具名称
        
    Returns:
        工具定义字典，如果不存在返回None
    """
    return TOOL_REGISTRY.get(tool_name)


def execute_tool(tool_name: str, blog_data: List[Dict[str, Any]], 
                 **kwargs) -> Dict[str, Any]:
    """
    执行指定的分析工具
    
    Args:
        tool_name: 工具名称
        blog_data: 博文数据列表
        **kwargs: 工具的额外参数
        
    Returns:
        工具执行结果
    """
    tool_info = TOOL_REGISTRY.get(tool_name)
    if not tool_info:
        return {
            "error": f"未找到工具: {tool_name}",
            "available_tools": list(TOOL_REGISTRY.keys())
        }
    
    func = tool_info["function"]
    
    # 构建参数
    params = {"blog_data": blog_data}
    
    # 添加可选参数
    for param_name, param_info in tool_info["parameters"].items():
        if param_name == "blog_data":
            continue
        if param_name in kwargs:
            params[param_name] = kwargs[param_name]
        elif "default" in param_info:
            params[param_name] = param_info["default"]
    
    try:
        result = func(**params)
        result["tool_name"] = tool_name
        result["category"] = tool_info["category"]
        return result
    except Exception as e:
        return {
            "error": str(e),
            "tool_name": tool_name
        }


def get_tools_by_category(category: str) -> List[Dict[str, Any]]:
    """
    按分类获取工具列表
    
    Args:
        category: 工具分类名称
        
    Returns:
        该分类下的工具列表
    """
    tools = []
    for name, tool_info in TOOL_REGISTRY.items():
        if tool_info["category"] == category:
            tools.append({
                "name": tool_info["name"],
                "description": tool_info["description"],
                "generates_chart": tool_info["generates_chart"]
            })
    return tools


def get_chart_tools() -> List[str]:
    """
    获取所有能生成图表的工具名称列表
    
    Returns:
        工具名称列表
    """
    return [name for name, info in TOOL_REGISTRY.items() if info["generates_chart"]]


def get_data_tools() -> List[str]:
    """
    获取所有仅产生数据的工具名称列表
    
    Returns:
        工具名称列表
    """
    return [name for name, info in TOOL_REGISTRY.items() if not info["generates_chart"]]
