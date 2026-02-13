"""
主题演化分析工具集

提供主题热度变化和关联分析的工具函数，包括：
- topic_frequency_stats: 主题频次统计
- topic_time_evolution: 主题时序演化分析
- topic_cooccurrence_analysis: 主题共现关联分析
- topic_ranking_chart: 主题热度排行柱状图
- topic_evolution_chart: 主题演化时序图
- topic_network_chart: 主题关联网络图
"""

import os
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import Counter, defaultdict
from itertools import combinations

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils.path_manager import get_images_dir
# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

WORD_PATTERN = re.compile(r"[A-Za-z]{3,}|[\u4e00-\u9fff]{2,}")


def _normalize_topic_df(blog_data: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(blog_data)
    if df.empty:
        return df
    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    if "topics" not in df.columns:
        df["topics"] = []
    return df


def _detect_focus_window(df: pd.DataFrame, window_days: int = 14) -> Dict[str, Any]:
    if df.empty or "publish_time" not in df.columns:
        return {}
    daily = df.set_index("publish_time").resample("D").size()
    if daily.empty:
        return {}
    roll = daily.rolling(window_days, min_periods=1).sum()
    if roll.empty or roll.isna().all():
        return {}
    end = roll.idxmax(skipna=True)
    if pd.isna(end):
        return {}
    start = end - pd.Timedelta(days=window_days - 1)
    return {"start": start.normalize(), "end": end.normalize()}


def _tokenize_content(text: str) -> Counter:
    """轻量分词，匹配英文>=3 或中文>=2 连续字符，并过滤 url/数字等。"""
    if not isinstance(text, str) or not text:
        return Counter()
    tokens = []
    for raw in WORD_PATTERN.findall(text):
        token = raw.lower()
        if token.startswith(("http", "www")) or token.isdigit():
            continue
        tokens.append(token)
    return Counter(tokens)


def topic_frequency_stats(blog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    主题频次统计
    
    统计父主题和子主题的出现频次和占比。
    
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
    
    parent_topic_counts = Counter()
    sub_topic_counts = Counter()
    topic_pairs = Counter()  # 父-子主题组合
    posts_with_topics = 0
    
    for post in blog_data:
        topics = post.get("topics") or []
        if not isinstance(topics, list):
            continue
        if topics:
            posts_with_topics += 1
            for topic in topics:
                if not isinstance(topic, dict):
                    continue
                parent = topic.get("parent_topic", "")
                sub = topic.get("sub_topic", "")
                if parent:
                    parent_topic_counts[parent] += 1
                if sub:
                    sub_topic_counts[sub] += 1
                if parent and sub:
                    topic_pairs[f"{parent} > {sub}"] += 1
    
    # 构建父主题分布
    parent_distribution = {}
    for topic, count in parent_topic_counts.most_common():
        parent_distribution[topic] = {
            "count": count,
            "percentage": round(count / total * 100, 2)
        }
    
    # 构建子主题分布（按父主题分组）
    sub_distribution = {}
    for topic, count in sub_topic_counts.most_common():
        sub_distribution[topic] = {
            "count": count,
            "percentage": round(count / total * 100, 2)
        }
    
    # 热门主题组合
    top_pairs = [
        {"topic": pair, "count": count, "percentage": round(count / total * 100, 2)}
        for pair, count in topic_pairs.most_common(10)
    ]
    
    coverage = round(posts_with_topics / total * 100, 2) if total > 0 else 0
    top_parent = parent_topic_counts.most_common(1)[0][0] if parent_topic_counts else "无"
    
    summary = f"主题覆盖率{coverage}%，最热门父主题「{top_parent}」"
    
    return {
        "data": {
            "parent_topics": parent_distribution,
            "sub_topics": sub_distribution,
            "top_topic_pairs": top_pairs,
            "coverage": {
                "posts_with_topics": posts_with_topics,
                "total_posts": total,
                "percentage": coverage
            }
        },
        "summary": summary
    }


def topic_time_evolution(blog_data: List[Dict[str, Any]], 
                         granularity: str = "day",
                         top_n: int = 5) -> Dict[str, Any]:
    """
    主题时序演化分析
    
    分析主题热度随时间的变化趋势。
    
    Args:
        blog_data: 增强后的博文数据列表
        granularity: 时间粒度，"hour"或"day"
        top_n: 显示的热门主题数量
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }

    df_norm = _normalize_topic_df(blog_data)
    # 首先获取最热门的父主题
    parent_topic_counts = Counter()
    for post in blog_data:
        topics = post.get("topics") or []
        if not isinstance(topics, list):
            continue
        for topic in topics:
            if not isinstance(topic, dict):
                continue
            parent = topic.get("parent_topic", "")
            if parent:
                parent_topic_counts[parent] += 1
    
    top_topics = [t[0] for t in parent_topic_counts.most_common(top_n)]
    
    # 按时间分组统计每个主题的出现次数
    time_topic_counts = defaultdict(lambda: defaultdict(int))
    
    for post in blog_data:
        publish_time = post.get("publish_time", "")
        topics = post.get("topics") or []
        
        if not publish_time:
            continue
        
        try:
            dt = datetime.strptime(publish_time, "%Y-%m-%d %H:%M:%S")
            if granularity == "hour":
                time_key = dt.strftime("%Y-%m-%d %H:00")
            else:
                time_key = dt.strftime("%Y-%m-%d")
            
            if isinstance(topics, list):
                for topic in topics:
                    if not isinstance(topic, dict):
                        continue
                    parent = topic.get("parent_topic", "")
                    if parent in top_topics:
                        time_topic_counts[time_key][parent] += 1
        except ValueError:
            continue
    
    # 构建时序数据
    time_series = {}
    for time_key in sorted(time_topic_counts.keys()):
        time_series[time_key] = dict(time_topic_counts[time_key])
    
    # 计算每个主题的增长趋势
    trends = {}
    time_keys = sorted(time_topic_counts.keys())
    if len(time_keys) >= 2:
        first_period = time_keys[:len(time_keys)//3] if len(time_keys) >= 3 else [time_keys[0]]
        last_period = time_keys[-len(time_keys)//3:] if len(time_keys) >= 3 else [time_keys[-1]]
        
        for topic in top_topics:
            first_avg = sum(time_topic_counts[t].get(topic, 0) for t in first_period) / len(first_period)
            last_avg = sum(time_topic_counts[t].get(topic, 0) for t in last_period) / len(last_period)
            
            if first_avg > 0:
                change = (last_avg - first_avg) / first_avg * 100
                trends[topic] = {
                    "first_period_avg": round(first_avg, 2),
                    "last_period_avg": round(last_avg, 2),
                    "change_percentage": round(change, 2),
                    "trend": "上升" if change > 10 else ("下降" if change < -10 else "平稳")
                }
            else:
                trends[topic] = {
                    "first_period_avg": 0,
                    "last_period_avg": round(last_avg, 2),
                    "change_percentage": 0,
                    "trend": "新兴" if last_avg > 0 else "平稳"
                }
    
    # 焦点窗口（按滚动14天）与焦点期关键词/趋势
    focus = _detect_focus_window(df_norm[["publish_time"]], window_days=14)
    focus_trend = []
    focus_keywords = {}
    if focus:
        start = focus["start"]
        end = focus["end"] + pd.Timedelta(days=1)
        fdf = df_norm[(df_norm["publish_time"] >= start) & (df_norm["publish_time"] < end)].copy()
        if not fdf.empty:
            fdf["focus_time_key"] = fdf["publish_time"].dt.strftime("%Y-%m-%d")
            fseries = defaultdict(Counter)
            for _, row in fdf.iterrows():
                key = row.get("focus_time_key")
                topics_row = row.get("topics") or []
                if not isinstance(topics_row, list):
                    continue
                for topic in topics_row:
                    if not isinstance(topic, dict):
                        continue
                    parent = topic.get("parent_topic", "")
                    if parent and key:
                        fseries[key][parent] += 1
            for key in sorted(fseries.keys()):
                entry = {"time": key}
                entry.update({k: v for k, v in fseries[key].most_common(top_n)})
                focus_trend.append(entry)

            daily_counter: Dict[pd.Timestamp, Counter] = defaultdict(Counter)
            total_counter: Counter = Counter()
            fdf["date"] = fdf["publish_time"].dt.normalize()
            for _, row in fdf.iterrows():
                tokens = _tokenize_content(row.get("content", ""))
                if not tokens:
                    continue
                day = row["date"]
                daily_counter[day].update(tokens)
                total_counter.update(tokens)
            if total_counter:
                top_words = [w for w, _ in total_counter.most_common(12)]
                kw_trend = []
                for day in pd.date_range(start.normalize(), (end - pd.Timedelta(days=1)).normalize()):
                    row = {"time": day.strftime("%Y-%m-%d")}
                    counts_row = daily_counter.get(day, Counter())
                    for word in top_words:
                        if counts_row.get(word, 0):
                            row[word] = counts_row[word]
                    kw_trend.append(row)
                focus_keywords = {
                    "window": {"start": str(focus["start"].date()), "end": str(focus["end"].date())},
                    "top_words": top_words,
                    "trend": kw_trend,
                }

    rising_topics = [t for t, v in trends.items() if v.get("trend") == "上升"]
    summary = f"分析Top{top_n}主题演化，上升趋势主题: {', '.join(rising_topics) if rising_topics else '无'}"
    
    return {
        "data": {
            "time_series": time_series,
            "top_topics": top_topics,
            "trends": trends,
            "granularity": granularity,
            "time_range": {
                "start": min(time_topic_counts.keys()) if time_topic_counts else None,
                "end": max(time_topic_counts.keys()) if time_topic_counts else None
            },
            "focus_window": {"start": str(focus["start"].date()), "end": str(focus["end"].date())} if focus else {},
            "focus_trend": focus_trend,
            "focus_keywords": focus_keywords,
        },
        "summary": summary
    }


def topic_cooccurrence_analysis(blog_data: List[Dict[str, Any]], 
                                 min_support: int = 2) -> Dict[str, Any]:
    """
    主题共现关联分析
    
    分析主题之间的共现关系。
    
    Args:
        blog_data: 增强后的博文数据列表
        min_support: 最小支持度（共现次数阈值）
        
    Returns:
        包含data、summary的标准字典结构
    """
    if not blog_data:
        return {
            "data": {},
            "summary": "没有可分析的博文数据"
        }
    
    # 收集每篇博文的父主题集合
    post_topics = []
    for post in blog_data:
        topics = post.get("topics") or []
        if not isinstance(topics, list):
            continue
        parent_topics = set()
        for t in topics:
            if isinstance(t, dict):
                parent = t.get("parent_topic", "")
                if parent:
                    parent_topics.add(parent)
        if len(parent_topics) >= 2:
            post_topics.append(parent_topics)
    
    # 统计主题对共现次数
    cooccurrence_counts = Counter()
    for topics in post_topics:
        for pair in combinations(sorted(topics), 2):
            cooccurrence_counts[pair] += 1
    
    # 过滤低于阈值的共现对
    significant_pairs = [
        {
            "topic_pair": list(pair),
            "count": count,
            "support": round(count / len(blog_data) * 100, 2)
        }
        for pair, count in cooccurrence_counts.most_common()
        if count >= min_support
    ]
    
    # 构建关联矩阵
    all_topics = set()
    for pair, count in cooccurrence_counts.items():
        if count >= min_support:
            all_topics.update(pair)
    
    association_matrix = {}
    for topic1 in all_topics:
        association_matrix[topic1] = {}
        for topic2 in all_topics:
            if topic1 != topic2:
                pair = tuple(sorted([topic1, topic2]))
                association_matrix[topic1][topic2] = cooccurrence_counts.get(pair, 0)
    
    summary = f"发现{len(significant_pairs)}对显著共现主题（阈值≥{min_support}）"
    
    return {
        "data": {
            "cooccurrence_pairs": significant_pairs[:20],  # 取Top20
            "association_matrix": association_matrix,
            "min_support": min_support,
            "total_pairs_analyzed": len(cooccurrence_counts)
        },
        "summary": summary
    }


def topic_ranking_chart(blog_data: List[Dict[str, Any]],
                        output_dir: str = "report/images",
                        top_n: int = 10) -> Dict[str, Any]:
    """
    生成主题热度排行柱状图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        top_n: 显示的主题数量
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取主题统计
    freq_result = topic_frequency_stats(blog_data)
    parent_topics = freq_result["data"].get("parent_topics", {})
    
    if not parent_topics:
        return {
            "charts": [],
            "summary": "没有可绘制的数据"
        }
    
    # 准备数据
    sorted_topics = sorted(parent_topics.items(), key=lambda x: x[1]["count"], reverse=True)[:top_n]
    topics = [t[0] for t in sorted_topics]
    counts = [t[1]["count"] for t in sorted_topics]
    percentages = [t[1]["percentage"] for t in sorted_topics]
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # 水平柱状图
    y_pos = np.arange(len(topics))
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(topics)))
    
    bars = ax.barh(y_pos, counts, color=colors, edgecolor='white', linewidth=0.5)
    
    # 添加数值标签
    for i, (bar, count, pct) in enumerate(zip(bars, counts, percentages)):
        ax.text(bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height()/2,
                f'{count} ({pct}%)', va='center', fontsize=10)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(topics, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel('博文数量', fontsize=12)
    ax.set_title(f'Top{top_n} 主题热度排行', fontsize=16, fontweight='bold', pad=15)
    ax.set_xlim(0, max(counts) * 1.2)
    
    plt.tight_layout()
    
    # 保存图表
    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_ranking_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"topic_ranking_{timestamp}",
            "type": "bar_chart",
            "title": f"Top{top_n} 主题热度排行",
            "file_path": file_path,
            "source_tool": "topic_ranking_chart",
            "description": f"展示热度最高的{top_n}个主题及其博文数量"
        }],
        "summary": f"已生成主题热度排行图，保存至 {file_path}"
    }


def topic_evolution_chart(blog_data: List[Dict[str, Any]],
                          output_dir: str = "report/images",
                          granularity: str = "day",
                          top_n: int = 5) -> Dict[str, Any]:
    """
    生成主题演化时序图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        granularity: 时间粒度
        top_n: 显示的主题数量
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取时序演化数据
    evolution_result = topic_time_evolution(blog_data, granularity, top_n)
    time_series = evolution_result["data"].get("time_series", {})
    top_topics = evolution_result["data"].get("top_topics", [])
    
    if not time_series or not top_topics:
        return {
            "charts": [],
            "summary": "没有可绘制的数据"
        }
    
    # 准备数据
    times = list(time_series.keys())
    x_indices = range(len(times))
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 为每个主题绘制折线
    colors = plt.cm.Set2(np.linspace(0, 1, len(top_topics)))
    
    for topic, color in zip(top_topics, colors):
        counts = [time_series.get(t, {}).get(topic, 0) for t in times]
        ax.plot(x_indices, counts, marker='o', markersize=4, 
                linewidth=2, label=topic, color=color)
    
    ax.set_ylabel('博文数量', fontsize=12)
    ax.set_xlabel('时间', fontsize=12)
    ax.set_title('主题热度时序演化', fontsize=16, fontweight='bold', pad=15)
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # 设置x轴标签
    if len(times) > 15:
        step = len(times) // 10
        ax.set_xticks(list(x_indices)[::step])
        ax.set_xticklabels([times[i] for i in range(0, len(times), step)], rotation=45, ha='right')
    else:
        ax.set_xticks(x_indices)
        ax.set_xticklabels(times, rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_evolution_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"topic_evolution_{timestamp}",
            "type": "line_chart",
            "title": "主题热度时序演化",
            "file_path": file_path,
            "source_tool": "topic_evolution_chart",
            "description": f"展示Top{top_n}主题随时间的热度变化趋势"
        }],
        "summary": f"已生成主题演化时序图，保存至 {file_path}"
    }


def topic_focus_distribution_chart(blog_data: List[Dict[str, Any]],
                                   output_dir: str = "report/images",
                                   window_days: int = 14,
                                   top_n: int = 5) -> Dict[str, Any]:
    """
    焦点窗口主题占比趋势（只绘制焦点窗口内的父主题趋势）
    """
    if not blog_data:
        return {"charts": [], "summary": "没有可分析的博文数据"}

    df_norm = _normalize_topic_df(blog_data)
    focus = _detect_focus_window(df_norm[["publish_time"]], window_days=window_days)
    if not focus:
        return {"charts": [], "summary": "未找到焦点窗口，无法绘制"}

    start = focus["start"]
    end = focus["end"] + pd.Timedelta(days=1)
    fdf = df_norm[(df_norm["publish_time"] >= start) & (df_norm["publish_time"] < end)].copy()
    if fdf.empty:
        return {"charts": [], "summary": "焦点窗口内无数据"}

    fdf["date"] = fdf["publish_time"].dt.strftime("%Y-%m-%d")

    # 统计父主题出现次数
    parent_counts = Counter()
    for _, row in fdf.iterrows():
        topics_row = row.get("topics") or []
        if not isinstance(topics_row, list):
            continue
        for topic in topics_row:
            if not isinstance(topic, dict):
                continue
            parent = topic.get("parent_topic", "")
            if parent:
                parent_counts[parent] += 1

    top_parents = [t for t, _ in parent_counts.most_common(top_n)]
    if not top_parents:
        return {"charts": [], "summary": "焦点窗口内无主题数据"}

    series = {p: [] for p in top_parents}
    dates = sorted(fdf["date"].unique())
    for d in dates:
        day_df = fdf[fdf["date"] == d]
        counts = Counter()
        for _, row in day_df.iterrows():
            topics_row = row.get("topics") or []
            if not isinstance(topics_row, list):
                continue
            for topic in topics_row:
                if not isinstance(topic, dict):
                    continue
                parent = topic.get("parent_topic", "")
                if parent in top_parents:
                    counts[parent] += 1
        for p in top_parents:
            series[p].append(counts[p])

    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_focus_distribution_{timestamp}.png")

    import numpy as np
    x_idx = np.arange(len(dates))
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.tab20(np.linspace(0, 1, len(top_parents)))
    for color, p in zip(colors, top_parents):
        ax.plot(x_idx, series[p], marker='o', label=p, color=color)
    ax.set_xticks(x_idx)
    ax.set_xticklabels(dates, rotation=45, ha="right")
    ax.set_title(f"焦点窗口主题发布趋势（{start.date()} - {focus['end'].date()}）")
    ax.set_ylabel("发布量")
    ax.set_xlabel("日期")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
    plt.tight_layout()
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    data_rows = []
    for i, d in enumerate(dates):
        row = {"time": d}
        for p in top_parents:
            row[p] = series[p][i]
        data_rows.append(row)

    return {
        "charts": [{
            "id": f"topic_focus_distribution_{timestamp}",
            "type": "line_chart",
            "title": "焦点窗口主题发布趋势",
            "file_path": file_path,
            "source_tool": "topic_focus_distribution_chart",
            "description": f"焦点窗口内 Top{top_n} 父主题的发布趋势"
        }],
        "data": {
            "focus_window": {"start": str(start.date()), "end": str(focus["end"].date())},
            "series": data_rows,
            "top_topics": top_parents
        },
        "summary": f"焦点窗口（{start.date()}~{focus['end'].date()}）内 Top{top_n} 主题的发布趋势。"
    }


def topic_network_chart(blog_data: List[Dict[str, Any]],
                        output_dir: str = "report/images",
                        min_support: int = 3) -> Dict[str, Any]:
    """
    生成主题关联网络图
    
    Args:
        blog_data: 增强后的博文数据列表
        output_dir: 图表输出目录
        min_support: 最小支持度阈值
        
    Returns:
        包含图表路径和描述的字典
    """
    # 获取共现分析数据
    cooccurrence_result = topic_cooccurrence_analysis(blog_data, min_support)
    pairs = cooccurrence_result["data"].get("cooccurrence_pairs", [])
    
    if not pairs:
        return {
            "charts": [],
            "summary": f"没有满足阈值({min_support})的共现主题对"
        }
    
    # 统计每个主题的总频次（防御空值/类型异常）
    topic_counts = Counter()
    for post in blog_data:
        topics = post.get("topics") or []
        if not isinstance(topics, list):
            continue
        for topic in topics:
            if not isinstance(topic, dict):
                continue
            parent = topic.get("parent_topic", "")
            if parent:
                topic_counts[parent] += 1
    
    # 收集所有涉及的主题
    all_topics = set()
    for pair in pairs:
        all_topics.update(pair["topic_pair"])
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 12))
    
    # 计算节点位置（圆形布局）
    n_topics = len(all_topics)
    topics_list = list(all_topics)
    angles = np.linspace(0, 2 * np.pi, n_topics, endpoint=False)
    radius = 5
    
    positions = {
        topic: (radius * np.cos(angle), radius * np.sin(angle))
        for topic, angle in zip(topics_list, angles)
    }
    
    # 绘制边
    max_count = max(p["count"] for p in pairs)
    for pair in pairs:
        t1, t2 = pair["topic_pair"]
        if t1 in positions and t2 in positions:
            x1, y1 = positions[t1]
            x2, y2 = positions[t2]
            # 线宽与共现次数成正比
            linewidth = 1 + (pair["count"] / max_count) * 4
            alpha = 0.3 + (pair["count"] / max_count) * 0.5
            ax.plot([x1, x2], [y1, y2], 'gray', linewidth=linewidth, alpha=alpha, zorder=1)
    
    # 绘制节点
    max_topic_count = max(topic_counts.values()) if topic_counts else 1
    for topic in topics_list:
        x, y = positions[topic]
        size = 200 + (topic_counts.get(topic, 0) / max_topic_count) * 800
        ax.scatter(x, y, s=size, c='steelblue', alpha=0.7, edgecolors='white', linewidth=2, zorder=2)
        ax.annotate(topic, (x, y), fontsize=10, ha='center', va='center', 
                   fontweight='bold', zorder=3)
    
    ax.set_xlim(-radius * 1.5, radius * 1.5)
    ax.set_ylim(-radius * 1.5, radius * 1.5)
    ax.set_aspect('equal')
    ax.axis('off')
    ax.set_title('主题关联网络图', fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # 保存图表
    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_network_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return {
        "charts": [{
            "id": f"topic_network_{timestamp}",
            "type": "network_chart",
            "title": "主题关联网络图",
            "file_path": file_path,
            "source_tool": "topic_network_chart",
            "description": "展示主题之间的共现关联关系，线条粗细表示关联强度"
        }],
        "summary": f"已生成主题关联网络图，保存至 {file_path}"
    }


def topic_focus_evolution_chart(blog_data: List[Dict[str, Any]],
                                output_dir: str = "report/images",
                                granularity: str = "day",
                                top_n: int = 5) -> Dict[str, Any]:
    """
    结合热点窗口高亮的主题演化趋势图，方便在报告中标注关键阶段。
    """
    evo_result = topic_time_evolution(blog_data, granularity, top_n)
    time_series = evo_result["data"].get("time_series", {})
    top_topics = evo_result["data"].get("top_topics", [])
    if not time_series or not top_topics:
        return {"charts": [], "summary": "没有可绘制的主题演化数据"}

    times = list(time_series.keys())
    x_idx = range(len(times))
    focus = _detect_focus_window(_normalize_topic_df(blog_data)[["publish_time"]], window_days=14)

    fig, ax = plt.subplots(figsize=(14, 8))
    colors = plt.cm.Set2(np.linspace(0, 1, len(top_topics)))
    for topic, color in zip(top_topics, colors):
        counts = [time_series.get(t, {}).get(topic, 0) for t in times]
        ax.plot(x_idx, counts, marker="o", markersize=4, linewidth=2, label=topic, color=color)

    if focus:
        try:
            start_str, end_str = focus["start"].strftime("%Y-%m-%d"), focus["end"].strftime("%Y-%m-%d")
            time_to_idx = {t: i for i, t in enumerate(times)}
            start_idx = time_to_idx.get(start_str)
            end_idx = time_to_idx.get(end_str)
            if start_idx is not None and end_idx is not None:
                ax.axvspan(start_idx, end_idx, color="gold", alpha=0.08, label="焦点窗口")
        except Exception:
            pass

    ax.set_ylabel("博文数量", fontsize=12)
    ax.set_xlabel("时间", fontsize=12)
    ax.set_title("主题演化趋势（焦点窗口高亮）", fontsize=16, fontweight="bold", pad=15)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)
    if len(times) > 15:
        step = max(1, len(times) // 10)
        ax.set_xticks(list(x_idx)[::step])
        ax.set_xticklabels([times[i] for i in range(0, len(times), step)], rotation=45, ha="right")
    else:
        ax.set_xticks(list(x_idx))
        ax.set_xticklabels(times, rotation=45, ha="right")

    plt.tight_layout()
    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_focus_evolution_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"topic_focus_evolution_{timestamp}",
            "type": "line_chart",
            "title": "主题演化趋势（焦点窗口）",
            "file_path": file_path,
            "source_tool": "topic_focus_evolution_chart",
            "description": "Top主题的时序变化，突出焦点窗口与峰值阶段"
        }],
        "summary": f"已生成带焦点窗口的主题演化图，保存至 {file_path}"
    }


def topic_keyword_trend_chart(blog_data: List[Dict[str, Any]],
                              output_dir: str = "report/images",
                              granularity: str = "day",
                              top_n: int = 8) -> Dict[str, Any]:
    """
    基于内容关键词的时间演化趋势，用于支撑焦点词热度变化。
    """
    df = _normalize_topic_df(blog_data)
    if df.empty or "publish_time" not in df.columns:
        return {"charts": [], "summary": "没有可用的内容或时间字段"}

    df = df.copy()
    fmt = "%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d"
    df["time_key"] = df["publish_time"].dt.strftime(fmt)

    time_keyword_counts: Dict[str, Counter] = defaultdict(Counter)
    total_counter = Counter()
    for _, row in df.iterrows():
        tokens = _tokenize_content(row.get("content", ""))
        time_key = row["time_key"]
        for token, cnt in tokens.items():
            time_keyword_counts[time_key][token] += cnt
            total_counter[token] += cnt

    if not total_counter:
        return {"charts": [], "summary": "未提取到有效关键词"}

    top_keywords = [kw for kw, _ in total_counter.most_common(top_n)]
    rows = []
    for time_key in sorted(time_keyword_counts.keys()):
        row = {"time": time_key}
        for kw in top_keywords:
            row[kw] = time_keyword_counts[time_key].get(kw, 0)
        rows.append(row)

    df_kw = pd.DataFrame(rows).fillna(0)
    x_idx = range(len(df_kw))
    fig, ax = plt.subplots(figsize=(14, 8))
    palette = plt.cm.tab10(np.linspace(0, 1, len(top_keywords)))
    for kw, color in zip(top_keywords, palette):
        ax.plot(x_idx, df_kw[kw], label=kw, linewidth=2, marker="o", markersize=4, color=color)

    ax.set_title("焦点关键词热度趋势", fontsize=16, fontweight="bold", pad=15)
    ax.set_ylabel("出现次数", fontsize=12)
    if len(df_kw) > 20:
        step = max(1, len(df_kw) // 10)
        ax.set_xticks(list(x_idx)[::step])
        ax.set_xticklabels(df_kw["time"].tolist()[::step], rotation=45, ha="right")
    else:
        ax.set_xticks(list(x_idx))
        ax.set_xticklabels(df_kw["time"].tolist(), rotation=45, ha="right")
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    output_dir = get_images_dir(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"topic_keyword_trend_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"topic_keyword_trend_{timestamp}",
            "type": "line_chart",
            "title": "焦点关键词热度趋势",
            "file_path": file_path,
            "source_tool": "topic_keyword_trend_chart",
            "description": "按时间粒度展示焦点关键词热度，支持地理与主题差异解读"
        }],
        "summary": f"已生成关键词趋势图，保存至 {file_path}"
    }
