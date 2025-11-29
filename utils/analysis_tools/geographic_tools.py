"""
地理分布分析工具集

提供舆情地理分布和热点识别的工具函数，包括：
- geographic_distribution_stats: 地理分布统计
- geographic_hotspot_detection: 热点区域识别
- geographic_sentiment_analysis: 地区情感差异分析
- geographic_heatmap: 地理热力图生成
- geographic_bar_chart: 地区分布柱状图生成
"""

import os
from datetime import datetime
from typing import List, Dict, Any
from collections import Counter, defaultdict

import matplotlib.pyplot as plt
import numpy as np

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def geographic_distribution_stats(blog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    地理分布统计
    
    统计博文的地理位置分布情况。
    
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
    
    location_counts = Counter()
    posts_with_location = 0
    
    for post in blog_data:
        location = post.get("location", "").strip()
        if location:
            posts_with_location += 1
            location_counts[location] += 1
    
    # 构建分布数据
    distribution = {}
    for location, count in location_counts.most_common():
        distribution[location] = {
            "count": count,
            "percentage": round(count / total * 100, 2)
        }
    
    coverage = round(posts_with_location / total * 100, 2) if total > 0 else 0
    top_location = location_counts.most_common(1)[0][0] if location_counts else "无"
    
    summary = f"地理信息覆盖率{coverage}%，最热门地区「{top_location}」共{location_counts.get(top_location, 0)}条"
    
    return {
        "data": {
            "distribution": distribution,
            "total_posts": total,
            "posts_with_location": posts_with_location,
            "coverage_percentage": coverage,
            "unique_locations": len(location_counts),
            "top_locations": [
                {"location": loc, "count": cnt, "percentage": round(cnt/total*100, 2)}
                for loc, cnt in location_counts.most_common(10)
            ]
        },
        "summary": summary
    }


def geographic_hotspot_detection(blog_data: List[Dict[str, Any]],
                                  threshold_percentile: float = 90) -> Dict[str, Any]:
    """
    热点区域识别
    
    识别高密度的热点区域。
    
    Args:
        blog_data: 增强后的博文数据列表
        threshold_percentile: 热点阈值百分位数
        
    Returns:
        包含data、summary的标准字典结构
    """
    # 首先获取地理分布
    dist_result = geographic_distribution_stats(blog_data)
    distribution = dist_result["data"].get("distribution", {})
    
    if not distribution:
        return {
            "data": {"hotspots": []},
            "summary": "没有地理位置数据"
        }
    
    # 计算阈值
    counts = [v["count"] for v in distribution.values()]
    if not counts:
        return {
            "data": {"hotspots": []},
            "summary": "没有地理位置数据"
        }
    
    threshold = np.percentile(counts, threshold_percentile)
    
    # 识别热点
    hotspots = []
    for location, stats in distribution.items():
        if stats["count"] >= threshold:
            hotspots.append({
                "location": location,
                "count": stats["count"],
                "percentage": stats["percentage"],
                "is_hotspot": True
            })
    
    # 按数量排序
    hotspots.sort(key=lambda x: x["count"], reverse=True)
    
    # 计算热点区域占总量的比例
    hotspot_count = sum(h["count"] for h in hotspots)
    total = sum(counts)
    concentration = round(hotspot_count / total * 100, 2) if total > 0 else 0
    
    summary = f"识别出{len(hotspots)}个热点区域，占总量的{concentration}%"
    
    return {
        "data": {
            "hotspots": hotspots,
            "threshold": round(threshold, 2),
            "threshold_percentile": threshold_percentile,
            "hotspot_concentration": concentration
        },
        "summary": summary
    }


def geographic_sentiment_analysis(blog_data: List[Dict[str, Any]],
                                   min_posts: int = 5) -> Dict[str, Any]:
    """
    地区情感差异分析
    
    分析不同地区的情感倾向差异。
    
    Args:
        blog_data: 增强后的博文数据列表
        min_posts: 最小博文数阈值（低于此值的地区不纳入分析）
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 按地区分组统计情感
    location_sentiments = defaultdict(list)
    
    for post in blog_data:
        location = post.get("location", "").strip()
        polarity = post.get("sentiment_polarity")
        
        if location and polarity is not None:
            location_sentiments[location].append(polarity)
    
    # 计算每个地区的情感统计
    regional_analysis = {}
    for location, polarities in location_sentiments.items():
        if len(polarities) >= min_posts:
            avg_polarity = sum(polarities) / len(polarities)
            positive_count = len([p for p in polarities if p >= 4])
            negative_count = len([p for p in polarities if p <= 2])
            
            regional_analysis[location] = {
                "post_count": len(polarities),
                "avg_polarity": round(avg_polarity, 2),
                "positive_ratio": round(positive_count / len(polarities) * 100, 2),
                "negative_ratio": round(negative_count / len(polarities) * 100, 2),
                "sentiment_label": (
                    "正面主导" if avg_polarity >= 3.5 else 
                    ("负面主导" if avg_polarity <= 2.5 else "中性")
                )
            }
    
    # 找出情感最正面和最负面的地区
    if regional_analysis:
        sorted_by_polarity = sorted(
            regional_analysis.items(), 
            key=lambda x: x[1]["avg_polarity"],
            reverse=True
        )
        most_positive = sorted_by_polarity[0] if sorted_by_polarity else None
        most_negative = sorted_by_polarity[-1] if sorted_by_polarity else None
        
        summary_parts = []
        if most_positive:
            summary_parts.append(f"最正面地区「{most_positive[0]}」({most_positive[1]['avg_polarity']})")
        if most_negative and most_negative != most_positive:
            summary_parts.append(f"最负面地区「{most_negative[0]}」({most_negative[1]['avg_polarity']})")
        summary = "，".join(summary_parts) if summary_parts else "无显著差异"
    else:
        summary = "没有足够数据进行地区情感分析"
    
    return {
        "data": {
            "regional_analysis": regional_analysis,
            "min_posts_threshold": min_posts,
            "regions_analyzed": len(regional_analysis)
        },
        "summary": summary
    }


def geographic_heatmap(blog_data: List[Dict[str, Any]],
                       output_dir: str = "report/images") -> Dict[str, Any]:
    """
    生成地理热力图（模拟版本，使用矩阵热力图代替实际地图）
    
    注意：由于缺少真实地理坐标数据，这里使用情感-地区交叉热力图代替
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取地区情感分析数据
    sentiment_result = geographic_sentiment_analysis(blog_data)
    regional_analysis = sentiment_result["data"].get("regional_analysis", {})
    
    if len(regional_analysis) < 2:
        return {
            "charts": [],
            "summary": "地区数据不足，无法生成热力图"
        }
    
    # 准备热力图数据：地区 x 情感指标
    regions = list(regional_analysis.keys())[:15]  # 最多显示15个地区
    metrics = ["avg_polarity", "positive_ratio", "negative_ratio", "post_count"]
    metric_labels = ["平均极性", "正面占比(%)", "负面占比(%)", "博文数量"]
    
    # 构建数据矩阵
    data_matrix = []
    for region in regions:
        row = []
        for metric in metrics:
            value = regional_analysis[region].get(metric, 0)
            row.append(value)
        data_matrix.append(row)
    
    data_matrix = np.array(data_matrix)
    
    # 归一化处理（按列）
    normalized_matrix = np.zeros_like(data_matrix, dtype=float)
    for j in range(data_matrix.shape[1]):
        col = data_matrix[:, j]
        col_min, col_max = col.min(), col.max()
        if col_max > col_min:
            normalized_matrix[:, j] = (col - col_min) / (col_max - col_min)
        else:
            normalized_matrix[:, j] = 0.5
    
    # 创建热力图
    fig, ax = plt.subplots(figsize=(12, max(8, len(regions) * 0.5)))
    
    im = ax.imshow(normalized_matrix, cmap='RdYlGn', aspect='auto')
    
    # 设置坐标轴
    ax.set_xticks(np.arange(len(metrics)))
    ax.set_yticks(np.arange(len(regions)))
    ax.set_xticklabels(metric_labels, fontsize=11)
    ax.set_yticklabels(regions, fontsize=10)
    
    # 旋转x轴标签
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center")
    
    # 添加数值标注
    for i in range(len(regions)):
        for j in range(len(metrics)):
            value = data_matrix[i, j]
            if metrics[j] == "avg_polarity":
                text = f"{value:.2f}"
            elif metrics[j] == "post_count":
                text = f"{int(value)}"
            else:
                text = f"{value:.1f}"
            ax.text(j, i, text, ha="center", va="center", 
                   color="black", fontsize=9, fontweight='bold')
    
    ax.set_title('地区舆情热力图', fontsize=16, fontweight='bold', pad=15)
    
    # 添加颜色条
    cbar = plt.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label('归一化值 (红低绿高)', fontsize=10)
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"geographic_heatmap_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"geographic_heatmap_{timestamp}",
            "type": "heatmap",
            "title": "地区舆情热力图",
            "file_path": file_path,
            "source_tool": "geographic_heatmap",
            "description": "展示各地区在不同舆情指标上的表现差异"
        }],
        "summary": f"已生成地区舆情热力图，保存至 {file_path}"
    }


def geographic_bar_chart(blog_data: List[Dict[str, Any]],
                         output_dir: str = "report/images",
                         top_n: int = 15) -> Dict[str, Any]:
    """
    生成地区分布柱状图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        top_n: 显示的地区数量
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取地理分布统计
    dist_result = geographic_distribution_stats(blog_data)
    distribution = dist_result["data"].get("distribution", {})
    
    if not distribution:
        return {
            "charts": [],
            "summary": "没有地理位置数据"
        }
    
    # 准备数据
    sorted_locations = sorted(distribution.items(), key=lambda x: x[1]["count"], reverse=True)[:top_n]
    locations = [loc for loc, _ in sorted_locations]
    counts = [stats["count"] for _, stats in sorted_locations]
    percentages = [stats["percentage"] for _, stats in sorted_locations]
    
    # 获取情感数据
    sentiment_result = geographic_sentiment_analysis(blog_data)
    regional_analysis = sentiment_result["data"].get("regional_analysis", {})
    
    # 创建双轴图表
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    # 柱状图：博文数量
    x = np.arange(len(locations))
    width = 0.6
    
    # 根据情感倾向着色
    colors = []
    for loc in locations:
        if loc in regional_analysis:
            avg_polarity = regional_analysis[loc]["avg_polarity"]
            if avg_polarity >= 3.5:
                colors.append('#4caf50')  # 绿色-正面
            elif avg_polarity <= 2.5:
                colors.append('#f44336')  # 红色-负面
            else:
                colors.append('#2196f3')  # 蓝色-中性
        else:
            colors.append('#9e9e9e')  # 灰色-无数据
    
    bars = ax1.bar(x, counts, width, color=colors, edgecolor='white', linewidth=0.5, alpha=0.8)
    
    # 添加数值标签
    for bar, count, pct in zip(bars, counts, percentages):
        height = bar.get_height()
        ax1.annotate(f'{count}\n({pct}%)',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    ax1.set_xlabel('地区', fontsize=12)
    ax1.set_ylabel('博文数量', fontsize=12)
    ax1.set_title(f'Top{top_n} 地区分布及情感倾向', fontsize=16, fontweight='bold', pad=15)
    ax1.set_xticks(x)
    ax1.set_xticklabels(locations, rotation=45, ha='right', fontsize=10)
    
    # 添加图例
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#4caf50', edgecolor='white', label='正面主导'),
        Patch(facecolor='#2196f3', edgecolor='white', label='中性'),
        Patch(facecolor='#f44336', edgecolor='white', label='负面主导'),
        Patch(facecolor='#9e9e9e', edgecolor='white', label='数据不足')
    ]
    ax1.legend(handles=legend_elements, loc='upper right', fontsize=10)
    
    ax1.set_ylim(0, max(counts) * 1.2)
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"geographic_bar_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"geographic_bar_{timestamp}",
            "type": "bar_chart",
            "title": f"Top{top_n} 地区分布图",
            "file_path": file_path,
            "source_tool": "geographic_bar_chart",
            "description": "展示博文数量最多的地区分布，颜色表示情感倾向"
        }],
        "summary": f"已生成地区分布柱状图，保存至 {file_path}"
    }

