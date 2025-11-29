"""
情感趋势分析工具集

提供时间序列情感变化分析的工具函数，包括：
- sentiment_distribution_stats: 情感极性分布统计
- sentiment_time_series: 情感时序趋势分析
- sentiment_anomaly_detection: 情感异常点检测
- sentiment_trend_chart: 情感趋势图生成
- sentiment_pie_chart: 情感分布饼图生成
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 情感极性标签映射
POLARITY_LABELS = {
    1: "极度悲观",
    2: "悲观",
    3: "中性",
    4: "乐观",
    5: "极度乐观"
}

# 情感极性颜色映射
POLARITY_COLORS = {
    1: "#d32f2f",  # 红色
    2: "#f57c00",  # 橙色
    3: "#9e9e9e",  # 灰色
    4: "#4caf50",  # 绿色
    5: "#2196f3"   # 蓝色
}


def sentiment_distribution_stats(blog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    情感极性分布统计
    
    统计各情感极性档位的数量和占比。
    
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
    
    # 统计各极性数量
    polarity_counts = Counter()
    for post in blog_data:
        polarity = post.get("sentiment_polarity")
        if polarity is not None:
            polarity_counts[polarity] += 1
    
    # 计算占比
    distribution = {}
    for polarity in range(1, 6):
        count = polarity_counts.get(polarity, 0)
        distribution[POLARITY_LABELS[polarity]] = {
            "count": count,
            "percentage": round(count / total * 100, 2) if total > 0 else 0,
            "polarity_value": polarity
        }
    
    # 计算平均情感极性
    valid_polarities = [p.get("sentiment_polarity") for p in blog_data if p.get("sentiment_polarity") is not None]
    avg_polarity = sum(valid_polarities) / len(valid_polarities) if valid_polarities else 3
    
    # 确定主导情感
    dominant_polarity = max(polarity_counts.items(), key=lambda x: x[1])[0] if polarity_counts else 3
    
    summary = f"共分析{total}条博文，平均情感极性{avg_polarity:.2f}，主导情感为「{POLARITY_LABELS[dominant_polarity]}」"
    
    return {
        "data": {
            "distribution": distribution,
            "total_count": total,
            "valid_count": len(valid_polarities),
            "avg_polarity": round(avg_polarity, 2),
            "dominant_polarity": POLARITY_LABELS[dominant_polarity]
        },
        "summary": summary
    }


def sentiment_time_series(blog_data: List[Dict[str, Any]], 
                          granularity: str = "hour") -> Dict[str, Any]:
    """
    情感时序趋势分析
    
    按时间聚合分析情感极性变化趋势。
    
    Args:
        blog_data: 增强后的博文数据列表
        granularity: 时间粒度，"hour"按小时，"day"按天
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 按时间分组
    time_groups = defaultdict(list)
    
    for post in blog_data:
        publish_time = post.get("publish_time", "")
        polarity = post.get("sentiment_polarity")
        
        if not publish_time or polarity is None:
            continue
        
        try:
            dt = datetime.strptime(publish_time, "%Y-%m-%d %H:%M:%S")
            if granularity == "hour":
                key = dt.strftime("%Y-%m-%d %H:00")
            else:
                key = dt.strftime("%Y-%m-%d")
            time_groups[key].append(polarity)
        except ValueError:
            continue
    
    # 计算每个时间点的统计数据
    time_series = {}
    for time_key in sorted(time_groups.keys()):
        polarities = time_groups[time_key]
        time_series[time_key] = {
            "count": len(polarities),
            "avg_polarity": round(sum(polarities) / len(polarities), 2),
            "positive_ratio": round(len([p for p in polarities if p >= 4]) / len(polarities) * 100, 2),
            "negative_ratio": round(len([p for p in polarities if p <= 2]) / len(polarities) * 100, 2)
        }
    
    # 计算趋势变化
    if len(time_series) >= 2:
        time_keys = list(time_series.keys())
        first_avg = time_series[time_keys[0]]["avg_polarity"]
        last_avg = time_series[time_keys[-1]]["avg_polarity"]
        trend = "上升" if last_avg > first_avg else ("下降" if last_avg < first_avg else "平稳")
    else:
        trend = "数据不足"
    
    summary = f"时间范围内共{len(time_series)}个时间点，情感趋势整体{trend}"
    
    return {
        "data": {
            "time_series": time_series,
            "granularity": granularity,
            "time_range": {
                "start": min(time_groups.keys()) if time_groups else None,
                "end": max(time_groups.keys()) if time_groups else None
            },
            "trend": trend
        },
        "summary": summary
    }


def sentiment_anomaly_detection(blog_data: List[Dict[str, Any]], 
                                 threshold: float = 2.0) -> Dict[str, Any]:
    """
    情感异常点检测
    
    识别情感极性突变和峰值时刻。
    
    Args:
        blog_data: 增强后的博文数据列表
        threshold: 异常阈值（标准差倍数）
        
    Returns:
        包含data、summary的标准字典结构
    """
    # 首先进行时序分析
    time_series_result = sentiment_time_series(blog_data, "hour")
    time_series = time_series_result["data"].get("time_series", {})
    
    if len(time_series) < 3:
        return {
            "data": {"anomalies": []},
            "summary": "数据点不足，无法进行异常检测"
        }
    
    # 提取平均极性序列
    avg_polarities = [v["avg_polarity"] for v in time_series.values()]
    counts = [v["count"] for v in time_series.values()]
    
    # 计算均值和标准差
    mean_polarity = sum(avg_polarities) / len(avg_polarities)
    std_polarity = (sum((p - mean_polarity) ** 2 for p in avg_polarities) / len(avg_polarities)) ** 0.5
    
    mean_count = sum(counts) / len(counts)
    std_count = (sum((c - mean_count) ** 2 for c in counts) / len(counts)) ** 0.5
    
    # 检测异常点
    anomalies = []
    for i, (time_key, stats) in enumerate(time_series.items()):
        reasons = []
        
        # 情感极性异常
        if std_polarity > 0:
            z_polarity = abs(stats["avg_polarity"] - mean_polarity) / std_polarity
            if z_polarity > threshold:
                reasons.append(f"情感极性异常(偏离{z_polarity:.1f}个标准差)")
        
        # 发帖量异常
        if std_count > 0:
            z_count = abs(stats["count"] - mean_count) / std_count
            if z_count > threshold:
                reasons.append(f"发帖量异常(偏离{z_count:.1f}个标准差)")
        
        if reasons:
            anomalies.append({
                "time": time_key,
                "avg_polarity": stats["avg_polarity"],
                "count": stats["count"],
                "reasons": reasons
            })
    
    summary = f"检测到{len(anomalies)}个异常时间点" if anomalies else "未检测到明显异常"
    
    return {
        "data": {
            "anomalies": anomalies,
            "threshold": threshold,
            "baseline": {
                "mean_polarity": round(mean_polarity, 2),
                "std_polarity": round(std_polarity, 2),
                "mean_count": round(mean_count, 2),
                "std_count": round(std_count, 2)
            }
        },
        "summary": summary
    }


def sentiment_trend_chart(blog_data: List[Dict[str, Any]], 
                          output_dir: str = "report/images",
                          granularity: str = "hour") -> Dict[str, Any]:
    """
    生成情感趋势折线图/面积图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        granularity: 时间粒度
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取时序数据
    time_series_result = sentiment_time_series(blog_data, granularity)
    time_series = time_series_result["data"].get("time_series", {})
    
    if not time_series:
        return {
            "charts": [],
            "summary": "没有可绘制的数据"
        }
    
    # 准备数据
    times = list(time_series.keys())
    avg_polarities = [time_series[t]["avg_polarity"] for t in times]
    positive_ratios = [time_series[t]["positive_ratio"] for t in times]
    negative_ratios = [time_series[t]["negative_ratio"] for t in times]
    
    # 创建图表
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    
    # 图1：情感极性趋势折线图
    ax1 = axes[0]
    x_indices = range(len(times))
    ax1.plot(x_indices, avg_polarities, 'b-', linewidth=2, marker='o', markersize=4, label='平均情感极性')
    ax1.axhline(y=3, color='gray', linestyle='--', alpha=0.5, label='中性基准线')
    ax1.fill_between(x_indices, 3, avg_polarities, 
                     where=[p >= 3 for p in avg_polarities], 
                     alpha=0.3, color='green', label='正面情感')
    ax1.fill_between(x_indices, 3, avg_polarities,
                     where=[p < 3 for p in avg_polarities],
                     alpha=0.3, color='red', label='负面情感')
    ax1.set_ylabel('情感极性', fontsize=12)
    ax1.set_ylim(1, 5)
    ax1.set_title('情感极性时序变化趋势', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # 设置x轴标签
    if len(times) > 20:
        step = len(times) // 10
        ax1.set_xticks(x_indices[::step])
        ax1.set_xticklabels([times[i] for i in x_indices[::step]], rotation=45, ha='right')
    else:
        ax1.set_xticks(x_indices)
        ax1.set_xticklabels(times, rotation=45, ha='right')
    
    # 图2：正负面占比堆叠面积图
    ax2 = axes[1]
    neutral_ratios = [100 - p - n for p, n in zip(positive_ratios, negative_ratios)]
    ax2.stackplot(x_indices, negative_ratios, neutral_ratios, positive_ratios,
                  labels=['负面', '中性', '正面'],
                  colors=['#f44336', '#9e9e9e', '#4caf50'], alpha=0.8)
    ax2.set_ylabel('占比 (%)', fontsize=12)
    ax2.set_xlabel('时间', fontsize=12)
    ax2.set_title('正负面情感占比变化', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper right')
    ax2.set_ylim(0, 100)
    
    if len(times) > 20:
        step = len(times) // 10
        ax2.set_xticks(x_indices[::step])
        ax2.set_xticklabels([times[i] for i in x_indices[::step]], rotation=45, ha='right')
    else:
        ax2.set_xticks(x_indices)
        ax2.set_xticklabels(times, rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"sentiment_trend_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"sentiment_trend_{timestamp}",
            "type": "line_chart",
            "title": "情感趋势变化图",
            "file_path": file_path,
            "source_tool": "sentiment_trend_chart",
            "description": "展示情感极性随时间的变化趋势及正负面占比"
        }],
        "summary": f"已生成情感趋势图，保存至 {file_path}"
    }


def sentiment_pie_chart(blog_data: List[Dict[str, Any]],
                        output_dir: str = "report/images") -> Dict[str, Any]:
    """
    生成情感分布饼图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取分布统计
    dist_result = sentiment_distribution_stats(blog_data)
    distribution = dist_result["data"].get("distribution", {})
    
    if not distribution:
        return {
            "charts": [],
            "summary": "没有可绘制的数据"
        }
    
    # 准备数据
    labels = []
    sizes = []
    colors = []
    explode = []
    
    for polarity_value in range(1, 6):
        label = POLARITY_LABELS[polarity_value]
        if label in distribution:
            count = distribution[label]["count"]
            if count > 0:
                labels.append(f"{label}\n({count}条)")
                sizes.append(count)
                colors.append(POLARITY_COLORS[polarity_value])
                explode.append(0.02)
    
    if not sizes:
        return {
            "charts": [],
            "summary": "没有可绘制的数据"
        }
    
    # 创建饼图
    fig, ax = plt.subplots(figsize=(10, 8))
    
    wedges, texts, autotexts = ax.pie(
        sizes, 
        labels=labels, 
        colors=colors,
        explode=explode,
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 11}
    )
    
    # 设置百分比文字颜色
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    ax.set_title('情感极性分布', fontsize=16, fontweight='bold', pad=20)
    
    # 添加总数标注
    total = sum(sizes)
    ax.annotate(f'总计: {total}条博文', 
                xy=(0, 0), fontsize=12,
                ha='center', va='center')
    
    plt.tight_layout()
    
    # 保存图表
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"sentiment_pie_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"sentiment_pie_{timestamp}",
            "type": "pie_chart",
            "title": "情感分布饼图",
            "file_path": file_path,
            "source_tool": "sentiment_pie_chart",
            "description": "展示博文情感极性的整体分布情况"
        }],
        "summary": f"已生成情感分布饼图，保存至 {file_path}"
    }

