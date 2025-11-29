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
            ]
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
        
        features.append({
            "sentiment_polarity": polarity,
            "repost_count": post.get("repost_count", 0),
            "comment_count": post.get("comment_count", 0),
            "like_count": post.get("like_count", 0),
            "content_length": len(post.get("content", "")),
            "image_count": len(post.get("image_urls", [])),
            "topic_count": len(post.get("topics", []))
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

