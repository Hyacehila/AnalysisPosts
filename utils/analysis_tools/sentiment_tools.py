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
import pandas as pd

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


def _normalize_blog_df(blog_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """将 blog_data 转为 DataFrame 并标准化时间/极性/列表字段，便于扩展分析。"""
    df = pd.DataFrame(blog_data)
    if df.empty:
        return df
    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    if "sentiment_polarity" in df.columns:
        df["sentiment_polarity"] = pd.to_numeric(df["sentiment_polarity"], errors="coerce")
    if "sentiment_bucket" not in df.columns and "sentiment_polarity" in df.columns:
        def _bucket(v):
            if pd.isna(v):
                return "未知"
            if v <= 2:
                return "负面"
            if v >= 4:
                return "正面"
            return "中性"
        df["sentiment_bucket"] = df["sentiment_polarity"].apply(_bucket)
    if "sentiment_attribute" in df.columns:
        df["sentiment_attribute"] = df["sentiment_attribute"].apply(
            lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x])
        )
    df["publisher"] = df.get("publisher", "未知")
    df["publisher"] = df["publisher"].fillna("未知")
    return df


def _detect_focus_window(df: pd.DataFrame, window_days: int = 14) -> Dict[str, Any]:
    """按照滚动窗口找到发帖高峰期。"""
    if df.empty or "publish_time" not in df.columns:
        return {}
    daily = df.set_index("publish_time").resample("D").size()
    if daily.empty:
        return {}
    rolling = daily.rolling(window_days, min_periods=1).sum()
    end = rolling.idxmax()
    start = end - pd.Timedelta(days=window_days - 1)
    return {"start": start.normalize(), "end": end.normalize()}


def _detect_turning_points(series: List[Dict[str, Any]],
                           field: str = "avg_polarity",
                           min_change: float = 0.1,
                           window: int = 3) -> List[Dict[str, Any]]:
    """
    åœ¨æ—¶åºä¸­ç®€å•è¯†åˆ«è¶‹åŠ¿è½¬æŠ˜ç‚¹ï¼ˆå…ä¾èµ–é¢„æ£€å•ä¾§æŒ‡æ ‡ï¼‰
    æŠŠçŸ­æ»šåŠ¨çª—å£å¹³å‡çš„æ­£/è´Ÿå¢žé•¿æ”¹å˜ä½œä¸ºè½¬æŠ˜ç‚¹ï¼Œå¹¶è¿”å›žç®€æ´çš„æ–¹å‘/å·®å€¼ä¿¡æ¯ã€?
    """
    if len(series) < 3:
        return []

    values = pd.Series([row.get(field) for row in series], dtype="float")
    times = [row.get("time") for row in series]
    smooth = values.rolling(window, min_periods=1).mean()
    diffs = smooth.diff()

    turning_points: List[Dict[str, Any]] = []
    for i in range(1, len(diffs)):
        prev = diffs.iloc[i - 1]
        curr = diffs.iloc[i]
        if pd.isna(prev) or pd.isna(curr):
            continue
        if np.sign(prev) != np.sign(curr) and abs(curr - prev) >= min_change:
            turning_points.append({
                "time": times[i],
                field: round(values.iloc[i], 2) if not pd.isna(values.iloc[i]) else None,
                "direction": "up_to_down" if prev > 0 else "down_to_up",
                "delta": round(float(curr - prev), 3)
            })

    return turning_points


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
    df = _normalize_blog_df(blog_data)
    if df.empty or "publish_time" not in df.columns or "sentiment_polarity" not in df.columns:
        return {"data": {}, "summary": "没有可分析的博文数据"}

    fmt = "%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d"
    df = df.copy()
    df["time_key"] = df["publish_time"].dt.strftime(fmt)
    grouped = df.groupby("time_key")
    time_series = []
    for key, g in grouped:
        time_series.append(
            {
                "time": key,
                "count": len(g),
                "avg_polarity": round(g["sentiment_polarity"].mean(), 2),
                "positive_ratio": round((g["sentiment_polarity"] >= 4).mean() * 100, 2),
                "negative_ratio": round((g["sentiment_polarity"] <= 2).mean() * 100, 2),
            }
        )
    time_series = sorted(time_series, key=lambda x: x["time"])

    # 情感桶趋势
    bucket_counts = df.groupby(["time_key", "sentiment_bucket"]).size().unstack(fill_value=0)
    bucket_trend = []
    for idx, row in bucket_counts.sort_index().iterrows():
        entry = {"time": idx}
        entry.update(row.to_dict())
        bucket_trend.append(entry)

    # 焦点窗口
    focus = _detect_focus_window(df[["publish_time"]], window_days=14)
    focus_bucket_trend = []
    focus_numeric_trend = []
    focus_pub_mean = []
    focus_pub_median = []
    if focus:
        start = focus["start"]
        end = focus["end"] + pd.Timedelta(days=1)
        fdf = df[(df["publish_time"] >= start) & (df["publish_time"] < end)].copy()
        if not fdf.empty:
            fdf["focus_time_key"] = fdf["publish_time"].dt.strftime("%Y-%m-%d")
            fb = fdf.groupby(["focus_time_key", "sentiment_bucket"]).size().unstack(fill_value=0)
            for idx, row in fb.sort_index().iterrows():
                entry = {"time": idx}
                entry.update(row.to_dict())
                focus_bucket_trend.append(entry)

            fn = (
                fdf.groupby("focus_time_key")["sentiment_polarity"]
                .agg(["mean", "count"])
                .reset_index()
                .rename(columns={"focus_time_key": "time"})
            )
            for _, row in fn.iterrows():
                focus_numeric_trend.append(
                    {"time": row["time"], "avg_polarity": round(row["mean"], 2), "count": int(row["count"])}
                )

            pub_group = (
                fdf.groupby(["focus_time_key", "publisher"])["sentiment_polarity"]
                .agg(["mean", "median", "count"])
                .reset_index()
                .rename(columns={"focus_time_key": "time"})
            )
            for _, row in pub_group.iterrows():
                focus_pub_mean.append(
                    {
                        "time": row["time"],
                        "publisher": row["publisher"],
                        "value": round(row["mean"], 2),
                        "count": int(row["count"]),
                    }
                )
                focus_pub_median.append(
                    {
                        "time": row["time"],
                        "publisher": row["publisher"],
                        "value": round(row["median"], 2),
                        "count": int(row["count"]),
                    }
                )

    # 情感属性趋势（Top10）
    attribute_trend = []
    if "sentiment_attribute" in df.columns:
        exploded = df.explode("sentiment_attribute")
        exploded = exploded[pd.notna(exploded["sentiment_attribute"])]
        if not exploded.empty:
            top_attrs = exploded["sentiment_attribute"].value_counts().head(10).index.tolist()
            sub = exploded[exploded["sentiment_attribute"].isin(top_attrs)].copy()
            sub["attr_time_key"] = sub["publish_time"].dt.strftime(fmt)
            attr_counts = sub.groupby(["attr_time_key", "sentiment_attribute"]).size().unstack(fill_value=0)
            for idx, row in attr_counts.sort_index().iterrows():
                entry = {"time": idx}
                entry.update(row.to_dict())
                attribute_trend.append(entry)

    trend_desc = "数据不足"
    if len(time_series) >= 2:
        first_avg = time_series[0]["avg_polarity"]
        last_avg = time_series[-1]["avg_polarity"]
        trend_desc = "上升" if last_avg > first_avg else ("下降" if last_avg < first_avg else "平稳")

    # 高峰时间段与突发点
    peak_periods = sorted(time_series, key=lambda x: x["count"], reverse=True)[:3]
    counts_only = [row["count"] for row in time_series]
    volume_spikes = []
    if counts_only:
        mean_count = float(np.mean(counts_only))
        std_count = float(np.std(counts_only))
        for row in time_series:
            if std_count > 0:
                z = (row["count"] - mean_count) / std_count
                if z >= 2:
                    volume_spikes.append({
                        "time": row["time"],
                        "count": row["count"],
                        "zscore": round(float(z), 2)
                    })
            row["growth_rate_vs_prev"] = None
        for i in range(1, len(time_series)):
            prev = time_series[i - 1]["count"]
            curr = time_series[i]["count"]
            time_series[i]["growth_rate_vs_prev"] = round((curr - prev) / prev, 2) if prev else None

    turning_points = _detect_turning_points(
        time_series,
        field="avg_polarity",
        min_change=0.05 if granularity == "hour" else 0.1
    )

    attribute_turning_points: List[Dict[str, Any]] = []
    if attribute_trend:
        attr_df = pd.DataFrame(attribute_trend).set_index("time")
        diff_df = attr_df.diff()
        for col in [c for c in attr_df.columns if c != "time"]:
            if col not in diff_df.columns:
                continue
            max_idx = diff_df[col].abs().idxmax()
            change = diff_df.loc[max_idx, col]
            if not pd.isna(change) and abs(change) >= 1:
                attribute_turning_points.append({
                    "attribute": col,
                    "time": max_idx,
                    "change": round(float(change), 2)
                })
        attribute_turning_points = sorted(attribute_turning_points, key=lambda x: -abs(x["change"]))[:5]

    hourly_distribution = {}
    peak_hours: List[Any] = []
    if not df.empty:
        hour_counts = df["publish_time"].dt.hour.value_counts().sort_index()
        hourly_distribution = {int(h): int(c) for h, c in hour_counts.items()}
        peak_hours = sorted(hourly_distribution.items(), key=lambda x: -x[1])[:3]

    summary_parts = [
        f"时间范围内共{len(time_series)}个时间点，情感趋势整体{trend_desc}"
    ]
    if peak_periods:
        summary_parts.append(f"核心高峰: {peak_periods[0]['time']} ({peak_periods[0]['count']}条)")
    if volume_spikes:
        summary_parts.append(f"检测到{len(volume_spikes)}个发布量激增点")
    summary = "；".join(summary_parts)

    return {
        "data": {
            "time_series": {row["time"]: {k: v for k, v in row.items() if k != "time"} for row in time_series},
            "granularity": granularity,
            "time_range": {
                "start": time_series[0]["time"] if time_series else None,
                "end": time_series[-1]["time"] if time_series else None,
            },
            "trend": trend_desc,
            "bucket_trend": bucket_trend,
            "focus_window": {"start": str(focus["start"].date()), "end": str(focus["end"].date())} if focus else {},
            "focus_bucket_trend": focus_bucket_trend,
            "focus_numeric_polarity": focus_numeric_trend,
            "focus_publisher_mean": focus_pub_mean,
            "focus_publisher_median": focus_pub_median,
            "attribute_trend": attribute_trend,
            "turning_points": turning_points,
            "attribute_turning_points": attribute_turning_points,
            "peak_periods": peak_periods,
            "volume_spikes": volume_spikes,
            "hourly_distribution": hourly_distribution,
            "peak_hours": peak_hours,
        },
        "summary": summary,
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
    volume_spikes = time_series_result["data"].get("volume_spikes", [])
    
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
    if volume_spikes:
        summary += f"，发现{len(volume_spikes)}个发布量爆点"
    
    return {
        "data": {
            "anomalies": anomalies,
            "volume_spikes": volume_spikes,
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


def sentiment_bucket_trend_chart(blog_data: List[Dict[str, Any]],
                                 output_dir: str = "report/images",
                                 granularity: str = "hour") -> Dict[str, Any]:
    """
    将正/负/中性情绪桶的时序变化以堆叠面积图展示，便于比对阶段差异和峰值。
    """
    ts_result = sentiment_time_series(blog_data, granularity)
    bucket_trend = ts_result.get("data", {}).get("bucket_trend", [])
    if not bucket_trend:
        return {"charts": [], "summary": "没有可绘制的情绪桶数据"}

    df = pd.DataFrame(bucket_trend).fillna(0).sort_values("time")
    time_labels = df["time"].tolist()
    bucket_cols = [c for c in df.columns if c != "time"]
    if not bucket_cols:
        return {"charts": [], "summary": "没有正负中性情绪数据"}

    preferred_order = ["负面", "中性", "正面", "未知"]
    bucket_cols = [c for c in preferred_order if c in bucket_cols] + [c for c in bucket_cols if c not in preferred_order]

    x_idx = range(len(time_labels))
    series = [df[col].astype(float).tolist() for col in bucket_cols]
    colors = {
        "负面": "#f44336",
        "中性": "#9e9e9e",
        "正面": "#4caf50",
        "未知": "#bdbdbd"
    }
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.stackplot(x_idx, series, labels=bucket_cols, colors=[colors.get(c, "#607d8b") for c in bucket_cols], alpha=0.85)
    ax.set_title("情绪桶随时间变化", fontsize=16, fontweight="bold", pad=15)
    ax.set_ylabel("条数", fontsize=12)
    if len(time_labels) > 20:
        step = max(1, len(time_labels) // 10)
        ax.set_xticks(list(x_idx)[::step])
        ax.set_xticklabels([time_labels[i] for i in range(0, len(time_labels), step)], rotation=45, ha="right")
    else:
        ax.set_xticks(list(x_idx))
        ax.set_xticklabels(time_labels, rotation=45, ha="right")
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"sentiment_bucket_trend_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"sentiment_bucket_trend_{timestamp}",
            "type": "area_chart",
            "title": "正负中性占比时序趋势",
            "file_path": file_path,
            "source_tool": "sentiment_bucket_trend_chart",
            "description": "正/负/中性情绪占比的阶段差异和峰值"
        }],
        "summary": f"已生成情绪桶时序图，保存至 {file_path}"
    }


def sentiment_attribute_trend_chart(blog_data: List[Dict[str, Any]],
                                    output_dir: str = "report/images",
                                    granularity: str = "day",
                                    top_n: int = 6) -> Dict[str, Any]:
    """
    把最活跃的情绪属性随时间的变化绘制成折线，兼容日/小时粒度。
    """
    ts_result = sentiment_time_series(blog_data, granularity)
    attribute_trend = ts_result.get("data", {}).get("attribute_trend", [])
    if not attribute_trend:
        return {"charts": [], "summary": "没有可绘制的情绪属性趋势数据"}

    df = pd.DataFrame(attribute_trend).fillna(0).sort_values("time")
    attr_cols = [c for c in df.columns if c != "time"]
    if not attr_cols:
        return {"charts": [], "summary": "没有可绘制的情绪属性数据"}

    top_cols = sorted(attr_cols, key=lambda c: df[c].sum(), reverse=True)[:top_n]
    x_idx = range(len(df))
    fig, ax = plt.subplots(figsize=(14, 8))
    palette = plt.cm.Set1(np.linspace(0, 1, len(top_cols)))
    for attr, color in zip(top_cols, palette):
        ax.plot(x_idx, df[attr], label=attr, linewidth=2, marker="o", markersize=4, color=color)
    ax.set_title("情绪属性热点趋势", fontsize=16, fontweight="bold", pad=15)
    ax.set_ylabel("条数", fontsize=12)
    if len(df) > 20:
        step = max(1, len(df) // 10)
        ax.set_xticks(list(x_idx)[::step])
        ax.set_xticklabels(df["time"].tolist()[::step], rotation=45, ha="right")
    else:
        ax.set_xticks(list(x_idx))
        ax.set_xticklabels(df["time"].tolist(), rotation=45, ha="right")

    focus_window = ts_result.get("data", {}).get("focus_window")
    if focus_window:
        time_to_idx = {t: i for i, t in enumerate(df["time"].tolist())}
        start_idx = time_to_idx.get(focus_window.get("start"))
        end_idx = time_to_idx.get(focus_window.get("end"))
        if start_idx is not None and end_idx is not None:
            ax.axvspan(start_idx, end_idx, color="gold", alpha=0.08, label="焦点窗口")

    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"sentiment_attribute_trend_{timestamp}.png")
    plt.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "charts": [{
            "id": f"sentiment_attribute_trend_{timestamp}",
            "type": "line_chart",
            "title": "情绪属性热点趋势",
            "file_path": file_path,
            "source_tool": "sentiment_attribute_trend_chart",
            "description": "Top情绪属性时间热度变化与焦点窗口标注"
        }],
        "summary": f"已生成情绪属性趋势图，保存至 {file_path}"
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
    turning_points = time_series_result["data"].get("turning_points", [])
    volume_spikes = time_series_result["data"].get("volume_spikes", [])
    
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

    # 标注拐点与爆点
    if turning_points:
        tp_indices = [times.index(tp["time"]) for tp in turning_points if tp.get("time") in times]
        ax1.scatter(tp_indices, [avg_polarities[i] for i in tp_indices], color='orange', s=60, zorder=5, label='情感拐点')
    if volume_spikes:
        spike_idx = [times.index(v["time"]) for v in volume_spikes if v.get("time") in times]
        if spike_idx:
            ax1.vlines(spike_idx, 1, 5, colors='purple', linestyles='--', alpha=0.35, label='量级爆点')
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
