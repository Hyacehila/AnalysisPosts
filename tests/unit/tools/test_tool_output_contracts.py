"""
Output contract tests for chart-oriented tools.
"""
from __future__ import annotations

from utils.analysis_tools import TOOL_REGISTRY
from utils.analysis_tools.interaction_tools import influence_analysis


def _run_tool(tool_name: str, blog_data, tmp_path):
    tool_info = TOOL_REGISTRY[tool_name]
    func = tool_info["function"]
    params = {"blog_data": blog_data}
    for name, meta in tool_info.get("parameters", {}).items():
        if name == "blog_data":
            continue
        if name in {"output_dir", "data_dir"}:
            params[name] = str(tmp_path)
        elif "default" in meta:
            params[name] = meta["default"]
    return func(**params)


CHART_ONLY_TOOLS = [
    # sentiment
    "sentiment_distribution_stats",
    "sentiment_time_series",
    "sentiment_bucket_trend_chart",
    "sentiment_attribute_trend_chart",
    "sentiment_trend_chart",
    "sentiment_focus_window_chart",
    "sentiment_focus_publisher_chart",
    "sentiment_pie_chart",
    # topic
    "topic_frequency_stats",
    "topic_time_evolution",
    "topic_cooccurrence_analysis",
    "topic_ranking_chart",
    "topic_evolution_chart",
    "topic_focus_distribution_chart",
    "topic_network_chart",
    "topic_focus_evolution_chart",
    # geographic
    "geographic_distribution_stats",
    "geographic_hotspot_detection",
    "geographic_sentiment_analysis",
    "geographic_heatmap",
    "geographic_bar_chart",
    "geographic_sentiment_bar_chart",
    # interaction
    "publisher_distribution_stats",
    "cross_dimension_matrix",
    "interaction_heatmap",
    "publisher_bar_chart",
    "publisher_focus_distribution_chart",
    # nlp
    "keyword_wordcloud",
    "entity_cooccurrence_network",
    "text_cluster_analysis",
    "sentiment_lexicon_comparison",
    "temporal_keyword_heatmap",
]


def test_chart_oriented_tools_do_not_return_data(sample_enhanced_data, tmp_path):
    for tool_name in CHART_ONLY_TOOLS:
        result = _run_tool(tool_name, sample_enhanced_data, tmp_path)
        assert "charts" in result, tool_name
        assert "data" not in result, tool_name


def test_influence_analysis_caps_top_posts_to_five():
    blog_data = []
    for idx in range(8):
        blog_data.append(
            {
                "content": f"post-{idx}",
                "username": f"user-{idx}",
                "publisher": "媒体",
                "publish_time": f"2024-08-{idx + 1:02d} 08:00:00",
                "repost_count": idx + 1,
                "comment_count": idx + 2,
                "like_count": idx + 3,
                "sentiment_polarity": 3,
            }
        )

    result = influence_analysis(blog_data, top_n=20)
    top_posts = result["data"]["top_influential_posts"]

    assert len(top_posts) == 5
    assert "Top5" in result["summary"]
