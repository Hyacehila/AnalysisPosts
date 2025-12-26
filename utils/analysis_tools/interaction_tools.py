"""
多维交互分析工具集

提供情感、主题、地理、发布者多维度交叉分析的工具函数，包括：
- publisher_distribution_stats: 发布者类型分布统计
- cross_dimension_matrix: 多维交叉矩阵分析
- influence_analysis: 影响力分析
- correlation_analysis: 维度相关性分析
- interaction_heatmap: 交互热力图生成
- publisher_bar_chart: 发布者分布柱状图生成
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import Counter, defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 情感极性标签
POLARITY_LABELS = {
    1: "极度悲观", 2: "悲观", 3: "中性", 4: "乐观", 5: "极度乐观"
}


def publisher_distribution_stats(blog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    发布者类型分布统计
    
    统计不同发布者类型的博文数量和特征。
    
    Args:
        blog_data: 增强后的博文数据列表
        
    Returns:
        包含data、summary的标准字典结构
    """
    total = len(blog_data)
    if total == 0:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    publisher_stats = defaultdict(lambda: {
        "count": 0,
        "total_reposts": 0,
        "total_comments": 0,
        "total_likes": 0,
        "sentiment_sum": 0,
        "sentiment_count": 0
    })
    
    for post in blog_data:
        publisher = post.get("publisher", "未知")
        if not publisher:
            publisher = "未知"
        
        stats = publisher_stats[publisher]
        stats["count"] += 1
        stats["total_reposts"] += post.get("repost_count", 0)
        stats["total_comments"] += post.get("comment_count", 0)
        stats["total_likes"] += post.get("like_count", 0)
        
        polarity = post.get("sentiment_polarity")
        if polarity is not None:
            stats["sentiment_sum"] += polarity
            stats["sentiment_count"] += 1
    
    # 构建分布数据
    distribution = {}
    for publisher, stats in publisher_stats.items():
        count = stats["count"]
        distribution[publisher] = {
            "count": count,
            "percentage": round(count / total * 100, 2),
            "avg_reposts": round(stats["total_reposts"] / count, 2) if count > 0 else 0,
            "avg_comments": round(stats["total_comments"] / count, 2) if count > 0 else 0,
            "avg_likes": round(stats["total_likes"] / count, 2) if count > 0 else 0,
            "avg_sentiment": round(stats["sentiment_sum"] / stats["sentiment_count"], 2) if stats["sentiment_count"] > 0 else None,
            "total_engagement": stats["total_reposts"] + stats["total_comments"] + stats["total_likes"]
        }

    # 高互动账号与代表性帖子（融合新工具能力，保持旧接口向后兼容）
    def _engagement(post):
        return post.get("repost_count", 0) + post.get("comment_count", 0) + post.get("like_count", 0)

    sorted_posts = sorted(blog_data, key=_engagement, reverse=True)
    top_accounts = []
    seen = set()
    for post in sorted_posts:
        name = post.get("username") or post.get("publisher") or "未知"
        if name in seen:
            continue
        seen.add(name)
        top_accounts.append({
            "username": name,
            "publisher": post.get("publisher", "未知"),
            "engagement": _engagement(post),
            "followers_count": post.get("followers_count")
        })
        if len(top_accounts) >= 20:
            break

    representative_posts = [
        {
            "username": p.get("username"),
            "publisher": p.get("publisher", "未知"),
            "publish_time": p.get("publish_time"),
            "engagement": _engagement(p),
            "content": p.get("content")
        }
        for p in sorted_posts[:50]
    ]
    
    # 排序
    sorted_publishers = sorted(distribution.items(), key=lambda x: x[1]["count"], reverse=True)
    top_publisher = sorted_publishers[0][0] if sorted_publishers else "无"
    
    summary = f"共{len(distribution)}种发布者类型，「{top_publisher}」占比最高"
    
    return {
        "data": {
            "distribution": dict(sorted_publishers),
            "total_posts": total,
            "publisher_types": len(distribution),
            "top_publishers": [
                {"publisher": p, **stats} for p, stats in sorted_publishers[:5]
            ],
            "top_accounts": top_accounts,
            "representative_posts": representative_posts
        },
        "summary": summary
    }


def cross_dimension_matrix(blog_data: List[Dict[str, Any]],
                           dim1: str = "publisher",
                           dim2: str = "sentiment_polarity") -> Dict[str, Any]:
    """
    多维交叉矩阵分析
    
    生成两个维度的交叉分析矩阵。
    
    Args:
        blog_data: 增强后的博文数据列表
        dim1: 第一个维度（行），可选: publisher, location, topic
        dim2: 第二个维度（列），可选: sentiment_polarity, topic, publisher
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 提取维度值
    def get_dim_value(post, dim):
        if dim == "publisher":
            return post.get("publisher") or "未知"
        elif dim == "location":
            return post.get("location") or "未知"
        elif dim == "sentiment_polarity":
            polarity = post.get("sentiment_polarity")
            return POLARITY_LABELS.get(polarity, "未知") if polarity else "未知"
        elif dim == "topic":
            topics = post.get("topics", [])
            if topics:
                return topics[0].get("parent_topic", "未知")
            return "未知"
        return "未知"
    
    # 统计交叉计数
    cross_counts = defaultdict(lambda: defaultdict(int))
    dim1_counts = Counter()
    dim2_counts = Counter()
    
    for post in blog_data:
        val1 = get_dim_value(post, dim1)
        val2 = get_dim_value(post, dim2)
        
        cross_counts[val1][val2] += 1
        dim1_counts[val1] += 1
        dim2_counts[val2] += 1
    
    # 取Top值（避免矩阵过大）
    top_dim1 = [v[0] for v in dim1_counts.most_common(10)]
    top_dim2 = [v[0] for v in dim2_counts.most_common(10)]
    
    # 构建矩阵
    matrix = {}
    for v1 in top_dim1:
        matrix[v1] = {}
        for v2 in top_dim2:
            count = cross_counts[v1][v2]
            matrix[v1][v2] = {
                "count": count,
                "row_percentage": round(count / dim1_counts[v1] * 100, 2) if dim1_counts[v1] > 0 else 0,
                "col_percentage": round(count / dim2_counts[v2] * 100, 2) if dim2_counts[v2] > 0 else 0
            }
    
    summary = f"{dim1} × {dim2} 交叉分析矩阵，{len(top_dim1)}行×{len(top_dim2)}列"
    
    return {
        "data": {
            "matrix": matrix,
            "dimensions": {"dim1": dim1, "dim2": dim2},
            "dim1_values": top_dim1,
            "dim2_values": top_dim2,
            "dim1_totals": {v: dim1_counts[v] for v in top_dim1},
            "dim2_totals": {v: dim2_counts[v] for v in top_dim2}
        },
        "summary": summary
    }


def influence_analysis(blog_data: List[Dict[str, Any]],
                       top_n: int = 20) -> Dict[str, Any]:
    """
    影响力分析
    
    分析博文的互动量和传播力。
    
    Args:
        blog_data: 增强后的博文数据列表
        top_n: 返回的高影响力博文数量
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 计算每篇博文的影响力得分
    posts_with_score = []
    for i, post in enumerate(blog_data):
        reposts = post.get("repost_count", 0)
        comments = post.get("comment_count", 0)
        likes = post.get("like_count", 0)
        
        # 影响力得分：转发权重最高，评论次之，点赞最低
        influence_score = reposts * 3 + comments * 2 + likes * 1
        
        posts_with_score.append({
            "index": i,
            "username": post.get("username", ""),
            "content_preview": post.get("content", "")[:100] + "..." if len(post.get("content", "")) > 100 else post.get("content", ""),
            "publish_time": post.get("publish_time", ""),
            "repost_count": reposts,
            "comment_count": comments,
            "like_count": likes,
            "influence_score": influence_score,
            "sentiment_polarity": post.get("sentiment_polarity"),
            "publisher": post.get("publisher", "未知")
        })
    
    # 排序获取Top N
    sorted_posts = sorted(posts_with_score, key=lambda x: x["influence_score"], reverse=True)
    top_posts = sorted_posts[:top_n]
    
    # 统计指标
    total_engagement = sum(p["influence_score"] for p in posts_with_score)
    top_engagement = sum(p["influence_score"] for p in top_posts)
    concentration = round(top_engagement / total_engagement * 100, 2) if total_engagement > 0 else 0
    
    # 按发布者类型统计影响力
    publisher_influence = defaultdict(int)
    for p in posts_with_score:
        publisher_influence[p["publisher"]] += p["influence_score"]
    
    sorted_publisher_influence = sorted(publisher_influence.items(), key=lambda x: x[1], reverse=True)
    
    summary = f"Top{top_n}博文贡献了{concentration}%的总影响力"
    
    return {
        "data": {
            "top_influential_posts": top_posts,
            "influence_concentration": concentration,
            "total_posts": len(blog_data),
            "total_engagement": total_engagement,
            "avg_influence_score": round(total_engagement / len(blog_data), 2) if blog_data else 0,
            "publisher_influence": [
                {"publisher": p, "total_influence": score}
                for p, score in sorted_publisher_influence[:10]
            ]
        },
        "summary": summary
    }


def correlation_analysis(blog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    维度相关性分析
    
    分析各维度之间的相关性。
    
    Args:
        blog_data: 增强后的博文数据列表
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 提取数值特征
    features = []
    for post in blog_data:
        polarity = post.get("sentiment_polarity")
        if polarity is None:
            continue
        
        topics = post.get("topics") or []
        if not isinstance(topics, list):
            topics = []

        features.append({
            "sentiment_polarity": polarity,
            "repost_count": post.get("repost_count", 0),
            "comment_count": post.get("comment_count", 0),
            "like_count": post.get("like_count", 0),
            "content_length": len(post.get("content", "")),
            "image_count": len(post.get("image_urls", [])),
            "topic_count": len(topics)
        })
    
    if len(features) < 10:
        return {
            "data": {},
            "summary": "数据量不足，无法进行相关性分析"
        }
    
    # 计算相关系数
    feature_names = list(features[0].keys())
    correlations = {}
    
    for i, name1 in enumerate(feature_names):
        correlations[name1] = {}
        vals1 = [f[name1] for f in features]
        
        for name2 in feature_names:
            vals2 = [f[name2] for f in features]
            
            # 计算皮尔逊相关系数
            n = len(vals1)
            mean1 = sum(vals1) / n
            mean2 = sum(vals2) / n
            
            numerator = sum((v1 - mean1) * (v2 - mean2) for v1, v2 in zip(vals1, vals2))
            denominator1 = sum((v - mean1) ** 2 for v in vals1) ** 0.5
            denominator2 = sum((v - mean2) ** 2 for v in vals2) ** 0.5
            
            if denominator1 > 0 and denominator2 > 0:
                corr = numerator / (denominator1 * denominator2)
            else:
                corr = 0
            
            correlations[name1][name2] = round(corr, 3)
    
    # 找出显著相关对
    significant_correlations = []
    for name1 in feature_names:
        for name2 in feature_names:
            if name1 < name2:  # 避免重复
                corr = correlations[name1][name2]
                if abs(corr) >= 0.3:
                    significant_correlations.append({
                        "feature1": name1,
                        "feature2": name2,
                        "correlation": corr,
                        "strength": "强" if abs(corr) >= 0.7 else ("中" if abs(corr) >= 0.5 else "弱")
                    })
    
    significant_correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    
    summary = f"发现{len(significant_correlations)}对显著相关特征"
    
    return {
        "data": {
            "correlation_matrix": correlations,
            "feature_names": feature_names,
            "significant_correlations": significant_correlations,
            "sample_size": len(features)
        },
        "summary": summary
    }


def interaction_heatmap(blog_data: List[Dict[str, Any]],
                        output_dir: str = "report/images",
                        dim1: str = "publisher",
                        dim2: str = "sentiment_polarity") -> Dict[str, Any]:
    """
    生成交互热力图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        dim1: 第一个维度
        dim2: 第二个维度
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取交叉矩阵数据
    matrix_result = cross_dimension_matrix(blog_data, dim1, dim2)
    matrix = matrix_result["data"].get("matrix", {})
    dim1_values = matrix_result["data"].get("dim1_values", [])
    dim2_values = matrix_result["data"].get("dim2_values", [])
    
    if not matrix or not dim1_values or not dim2_values:
        return {
            "charts": [],
            "summary": "没有足够的数据生成热力图"
        }
    
    # 构建数值矩阵
    data_matrix = []
    for v1 in dim1_values:
        row = []
        for v2 in dim2_values:
            count = matrix.get(v1, {}).get(v2, {}).get("count", 0)
            row.append(count)
        data_matrix.append(row)
    
    data_matrix = np.array(data_matrix)
    
    # 创建热力图
    fig, ax = plt.subplots(figsize=(max(10, len(dim2_values) * 0.8), max(8, len(dim1_values) * 0.5)))
    
    im = ax.imshow(data_matrix, cmap='YlOrRd', aspect='auto')
    
    # 设置坐标轴
    ax.set_xticks(np.arange(len(dim2_values)))
    ax.set_yticks(np.arange(len(dim1_values)))
    ax.set_xticklabels(dim2_values, fontsize=10)
    ax.set_yticklabels(dim1_values, fontsize=10)
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    
    # 添加数值标注
    for i in range(len(dim1_values)):
        for j in range(len(dim2_values)):
            value = data_matrix[i, j]
            color = "white" if value > data_matrix.max() * 0.5 else "black"
            ax.text(j, i, int(value), ha="center", va="center", 
                   color=color, fontsize=9, fontweight='bold')
    
    dim_labels = {
        "publisher": "发布者类型",
        "sentiment_polarity": "情感极性",
        "location": "地区",
        "topic": "主题"
    }
    
    ax.set_xlabel(dim_labels.get(dim2, dim2), fontsize=12)
    ax.set_ylabel(dim_labels.get(dim1, dim1), fontsize=12)
    ax.set_title(f'{dim_labels.get(dim1, dim1)} × {dim_labels.get(dim2, dim2)} 交叉分析热力图', 
                fontsize=14, fontweight='bold', pad=15)
    
    # 添加颜色条
    cbar = plt.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label('博文数量', fontsize=10)
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"interaction_heatmap_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"interaction_heatmap_{timestamp}",
            "type": "heatmap",
            "title": f"{dim_labels.get(dim1, dim1)} × {dim_labels.get(dim2, dim2)} 热力图",
            "file_path": file_path,
            "source_tool": "interaction_heatmap",
            "description": f"展示{dim_labels.get(dim1, dim1)}与{dim_labels.get(dim2, dim2)}的交叉分布"
        }],
        "summary": f"已生成交互热力图，保存至 {file_path}"
    }


def publisher_bar_chart(blog_data: List[Dict[str, Any]],
                        output_dir: str = "report/images") -> Dict[str, Any]:
    """
    生成发布者分布柱状图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取发布者分布数据
    pub_result = publisher_distribution_stats(blog_data)
    distribution = pub_result["data"].get("distribution", {})
    
    if not distribution:
        return {
            "charts": [],
            "summary": "没有发布者数据"
        }
    
    # 准备数据
    publishers = list(distribution.keys())
    counts = [distribution[p]["count"] for p in publishers]
    avg_sentiments = [distribution[p].get("avg_sentiment") for p in publishers]
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 8))
    
    x = np.arange(len(publishers))
    width = 0.6
    
    # 根据情感着色
    colors = []
    for sent in avg_sentiments:
        if sent is None:
            colors.append('#9e9e9e')
        elif sent >= 3.5:
            colors.append('#4caf50')
        elif sent <= 2.5:
            colors.append('#f44336')
        else:
            colors.append('#2196f3')
    
    bars = ax.bar(x, counts, width, color=colors, edgecolor='white', linewidth=0.5, alpha=0.8)
    
    # 添加数值标签和情感分数
    for bar, count, sent in zip(bars, counts, avg_sentiments):
        height = bar.get_height()
        label = f'{count}'
        if sent is not None:
            label += f'\n(情感:{sent:.1f})'
        ax.annotate(label,
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    ax.set_xlabel('发布者类型', fontsize=12)
    ax.set_ylabel('博文数量', fontsize=12)
    ax.set_title('发布者类型分布及情感特征', fontsize=16, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(publishers, rotation=30, ha='right', fontsize=10)
    
    # 添加图例
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#4caf50', edgecolor='white', label='正面倾向 (≥3.5)'),
        Patch(facecolor='#2196f3', edgecolor='white', label='中性 (2.5-3.5)'),
        Patch(facecolor='#f44336', edgecolor='white', label='负面倾向 (≤2.5)'),
        Patch(facecolor='#9e9e9e', edgecolor='white', label='无情感数据')
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)
    
    ax.set_ylim(0, max(counts) * 1.25)
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"publisher_bar_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"publisher_bar_{timestamp}",
            "type": "bar_chart",
            "title": "发布者类型分布图",
            "file_path": file_path,
            "source_tool": "publisher_bar_chart",
            "description": "展示不同发布者类型的博文数量及情感倾向"
        }],
        "summary": f"已生成发布者分布柱状图，保存至 {file_path}"
    }


def publisher_sentiment_bucket_chart(blog_data: List[Dict[str, Any]],
                                     output_dir: str = "report/images",
                                     top_n: int = 10) -> Dict[str, Any]:
    """
    按发布者的正/中/负面占比堆叠图，用于对齐示例中的情绪桶分布。
    """
    bucket_counts: Dict[str, Counter] = defaultdict(Counter)

    def _bucket(polarity: Any) -> str:
        if polarity is None:
            return "未知"
        try:
            v = float(polarity)
        except Exception:
            return "未知"
        if v <= 2:
            return "负面"
        if v >= 4:
            return "正面"
        return "中性"

    for post in blog_data:
        pub = post.get("publisher") or "未知"
        bucket = _bucket(post.get("sentiment_polarity"))
        bucket_counts[pub][bucket] += 1

    if not bucket_counts:
        return {"charts": [], "summary": "没有可用的发布者情绪桶数据"}

    sorted_pub = sorted(bucket_counts.items(), key=lambda x: sum(x[1].values()), reverse=True)[:top_n]
    publishers = [p for p, _ in sorted_pub]
    buckets = ["负面", "中性", "正面", "未知"]
    colors = {"负面": "#f44336", "中性": "#9e9e9e", "正面": "#4caf50", "未知": "#bdbdbd"}

    fig, ax = plt.subplots(figsize=(14, 8))
    bottom = np.zeros(len(publishers))
    for bucket in buckets:
        values = np.array([counts.get(bucket, 0) for _, counts in sorted_pub], dtype=float)
        ax.bar(publishers, values, bottom=bottom, label=bucket, color=colors.get(bucket, "#607d8b"), edgecolor="white", linewidth=0.5)
        bottom += values

    ax.set_ylabel("博文数", fontsize=12)
    ax.set_title(f"Top{top_n} 发布者情绪桶分布", fontsize=16, fontweight="bold", pad=15)
    ax.set_xticklabels(publishers, rotation=45, ha="right", fontsize=10)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"publisher_sentiment_bucket_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"publisher_sentiment_bucket_{timestamp}",
            "type": "bar_chart",
            "title": "发布者情绪桶分布",
            "file_path": file_path,
            "source_tool": "publisher_sentiment_bucket_chart",
            "description": "按发布者对比正/中/负面占比，突出情绪差异"
        }],
        "summary": f"已生成发布者情绪桶分布图，保存至 {file_path}"
    }


def publisher_focus_distribution_chart(blog_data: List[Dict[str, Any]],
                                       output_dir: str = "report/images",
                                       window_days: int = 14,
                                       top_n: int = 5) -> Dict[str, Any]:
    """
    焦点窗口内发布者类型发布量趋势（仅绘制窗口内数据）
    """
    if not blog_data:
        return {"charts": [], "summary": "没有可分析的博文数据"}

    import pandas as pd
    df = pd.DataFrame(blog_data)
    if df.empty or "publish_time" not in df.columns:
        return {"charts": [], "summary": "缺少时间字段"}
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df["publisher"] = df.get("publisher", "未知").fillna("未知")

    daily = df.set_index("publish_time").resample("D").size()
    if daily.empty:
        return {"charts": [], "summary": "无有效时间数据"}
    rolling = daily.rolling(window_days, min_periods=1).sum()
    if rolling.empty or rolling.isna().all():
        return {"charts": [], "summary": "无有效滚动窗口数据"}
    end = rolling.idxmax(skipna=True)
    if pd.isna(end):
        return {"charts": [], "summary": "无法确定焦点窗口"}
    start = end - pd.Timedelta(days=window_days - 1)

    fdf = df[(df["publish_time"] >= start) & (df["publish_time"] <= end)].copy()
    if fdf.empty:
        return {"charts": [], "summary": "焦点窗口内无数据"}
    fdf["date"] = fdf["publish_time"].dt.strftime("%Y-%m-%d")

    top_publishers = fdf["publisher"].value_counts().head(top_n).index.tolist()
    fdf = fdf[fdf["publisher"].isin(top_publishers)]

    pivot = fdf.pivot_table(index="date", columns="publisher", values="content", aggfunc="count").fillna(0)

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"publisher_focus_distribution_{timestamp}.png")

    ax = pivot.plot(figsize=(12, 6), marker='o')
    ax.set_title(f"焦点窗口发布者类型发布量趋势（{start.date()} - {end.date()}）")
    ax.set_ylabel("发布量")
    ax.set_xlabel("日期")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"publisher_focus_distribution_{timestamp}",
            "type": "line_chart",
            "title": "焦点窗口发布者类型发布趋势",
            "file_path": file_path,
            "source_tool": "publisher_focus_distribution_chart",
            "description": f"焦点窗口内 Top{top_n} 发布者类型的日发布量趋势"
        }],
        "data": {
            "focus_window": {"start": str(start.date()), "end": str(end.date())},
            "series": pivot.reset_index().to_dict(orient="records"),
            "top_publishers": top_publishers
        },
        "summary": f"焦点窗口（{start.date()}~{end.date()}）内发布者类型发布趋势。"
    }


def publisher_topic_distribution_chart(blog_data: List[Dict[str, Any]],
                                       output_dir: str = "report/images",
                                       top_publishers: int = 8,
                                       top_topics: int = 8) -> Dict[str, Any]:
    """
    发布者 × 主题堆叠图，对齐示例中的主题偏好分布。
    """
    pub_topic = defaultdict(Counter)
    for post in blog_data:
        pub = post.get("publisher") or "未知"
        for topic in post.get("topics", []) or []:
            parent = topic.get("parent_topic")
            if parent:
                pub_topic[pub][parent] += 1

    if not pub_topic:
        return {"charts": [], "summary": "没有可用的发布者主题数据"}

    top_pub_items = sorted(pub_topic.items(), key=lambda x: sum(x[1].values()), reverse=True)[:top_publishers]
    topic_counter = Counter()
    for _, counter in top_pub_items:
        topic_counter.update(counter)
    topic_names = [t for t, _ in topic_counter.most_common(top_topics)]

    publishers = [p for p, _ in top_pub_items]
    fig, ax = plt.subplots(figsize=(14, 8))
    bottom = np.zeros(len(publishers))
    palette = plt.cm.tab20(np.linspace(0, 1, len(topic_names)))
    for topic, color in zip(topic_names, palette):
        values = np.array([counter.get(topic, 0) for _, counter in top_pub_items], dtype=float)
        ax.bar(publishers, values, bottom=bottom, label=topic, color=color, edgecolor="white", linewidth=0.3)
        bottom += values

    ax.set_ylabel("博文数", fontsize=12)
    ax.set_title("发布者话题偏好分布", fontsize=16, fontweight="bold", pad=15)
    ax.set_xticklabels(publishers, rotation=45, ha="right", fontsize=10)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=9, ncol=2)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"publisher_topic_distribution_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"publisher_topic_distribution_{timestamp}",
            "type": "bar_chart",
            "title": "发布者话题偏好分布",
            "file_path": file_path,
            "source_tool": "publisher_topic_distribution_chart",
            "description": "堆叠展示不同发布者的主题偏好差异"
        }],
        "summary": f"已生成发布者主题分布图，保存至 {file_path}"
    }


def participant_trend_chart(blog_data: List[Dict[str, Any]],
                            output_dir: str = "report/images",
                            granularity: str = "day") -> Dict[str, Any]:
    """
    参与人数趋势：按时间粒度累计唯一 user_id，用于评估参与规模演化。
    """
    df = pd.DataFrame(blog_data)
    if df.empty or "user_id" not in df.columns or "publish_time" not in df.columns:
        return {"charts": [], "summary": "缺少用户或时间字段"}

    df = df.copy()
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df = df[pd.notna(df["publish_time"])]
    fmt = "%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d"
    df["time_key"] = df["publish_time"].dt.strftime(fmt)
    df = df.sort_values("publish_time")

    seen: set = set()
    rows = []
    for time_key, group in df.groupby("time_key"):
        user_ids = group["user_id"].astype(str).tolist()
        new_users = [u for u in user_ids if u not in seen]
        seen.update(new_users)
        rows.append({
            "time": time_key,
            "active_posts": int(len(group)),
            "active_users": int(group["user_id"].nunique()),
            "new_users": int(len(new_users)),
            "cumulative_users": int(len(seen))
        })

    if not rows:
        return {"charts": [], "summary": "没有可用的参与数据"}

    trend_df = pd.DataFrame(rows).sort_values("time")
    x_idx = range(len(trend_df))
    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax1.plot(x_idx, trend_df["cumulative_users"], color="#4caf50", linewidth=2, marker="o", markersize=4, label="累计参与人数")
    ax1.bar(x_idx, trend_df["new_users"], color="#2196f3", alpha=0.6, label="当期新增用户")
    ax1.set_ylabel("人数", fontsize=12)
    ax1.set_xlabel("时间", fontsize=12)
    ax1.set_title("参与人数演化趋势", fontsize=16, fontweight="bold", pad=15)
    if len(trend_df) > 20:
        step = max(1, len(trend_df) // 10)
        ax1.set_xticks(list(x_idx)[::step])
        ax1.set_xticklabels(trend_df["time"].tolist()[::step], rotation=45, ha="right")
    else:
        ax1.set_xticks(list(x_idx))
        ax1.set_xticklabels(trend_df["time"].tolist(), rotation=45, ha="right")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"participant_trend_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"participant_trend_{timestamp}",
            "type": "line_chart",
            "title": "参与人数演化趋势",
            "file_path": file_path,
            "source_tool": "participant_trend_chart",
            "description": "按时间粒度统计新增与累计参与用户，定位参与规模变化"
        }],
        "summary": f"已生成参与人数趋势图，保存至 {file_path}"
    }
