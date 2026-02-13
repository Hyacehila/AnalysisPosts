"""
舆情分析工具集

提供四类核心分析工具集：
1. sentiment_tools: 情感趋势分析工具集
2. topic_tools: 主题演化分析工具集
3. geographic_tools: 地理分布分析工具集
4. interaction_tools: 多维交互分析工具集

每类工具集包含数据处理函数和可视化函数。
"""

from .sentiment_tools import (
    sentiment_distribution_stats,
    sentiment_time_series,
    sentiment_anomaly_detection,
    sentiment_trend_chart,
    sentiment_pie_chart,
    sentiment_bucket_trend_chart,
    sentiment_attribute_trend_chart,
    sentiment_focus_window_chart,
    sentiment_focus_publisher_chart,
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
    topic_focus_distribution_chart,
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
    publisher_focus_distribution_chart,
)

from .belief_tools import (
    belief_network_chart,
)

from .nlp_tools import (
    keyword_wordcloud,
    entity_cooccurrence_network,
    text_cluster_analysis,
    sentiment_lexicon_comparison,
    temporal_keyword_heatmap,
)
from .tool_registry import (
    TOOL_REGISTRY,
    get_all_tools,
    get_tool_by_name,
    execute_tool,
)

__all__ = [
    # 情感工具
    "sentiment_distribution_stats",
    "sentiment_time_series",
    "sentiment_anomaly_detection",
    "sentiment_trend_chart",
    "sentiment_pie_chart",
    "sentiment_bucket_trend_chart",
    "sentiment_attribute_trend_chart",
    "sentiment_focus_window_chart",
    "sentiment_focus_publisher_chart",
    # 主题工具
    "topic_frequency_stats",
    "topic_time_evolution",
    "topic_cooccurrence_analysis",
    "topic_ranking_chart",
    "topic_evolution_chart",
    "topic_network_chart",
    "topic_focus_evolution_chart",
    "topic_keyword_trend_chart",
    "topic_focus_distribution_chart",
    # 地理工具
    "geographic_distribution_stats",
    "geographic_hotspot_detection",
    "geographic_sentiment_analysis",
    "geographic_heatmap",
    "geographic_bar_chart",
    "geographic_sentiment_bar_chart",
    "geographic_topic_heatmap",
    "geographic_temporal_heatmap",
    # 交互工具
    "publisher_distribution_stats",
    "cross_dimension_matrix",
    "influence_analysis",
    "correlation_analysis",
    "interaction_heatmap",
    "publisher_bar_chart",
    "publisher_sentiment_bucket_chart",
    "publisher_topic_distribution_chart",
    "participant_trend_chart",
    "publisher_focus_distribution_chart",
    # 信念系统工具
    "belief_network_chart",
    # NLP tools
    "keyword_wordcloud",
    "entity_cooccurrence_network",
    "text_cluster_analysis",
    "sentiment_lexicon_comparison",
    "temporal_keyword_heatmap",
    # 工具注册
    "TOOL_REGISTRY",
    "get_all_tools",
    "get_tool_by_name",
    "execute_tool",
]
