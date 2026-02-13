"""
NLP-driven analysis tools (keywords/entities/lexicon/similarity).
"""
from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils.nlp import (
    clean_text,
    extract_keywords,
    extract_entities,
    lexicon_sentiment,
    cluster_similar_texts,
)
from utils.path_manager import get_images_dir


def keyword_wordcloud(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
    top_n: int = 30,
) -> Dict[str, Any]:
    """Keyword frequency bar chart (lightweight wordcloud substitute)."""
    output_dir = get_images_dir(output_dir)
    counter = Counter()
    for post in blog_data:
        keywords = post.get("keywords")
        if not keywords:
            keywords = extract_keywords(post.get("content", ""), top_n=top_n)
        counter.update([k for k in keywords if k])

    top_items = counter.most_common(top_n)
    data = {"keywords": top_items}

    if not top_items:
        return {"data": data, "charts": []}

    labels = [k for k, _ in top_items]
    values = [v for _, v in top_items]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(range(len(values)), values)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_title("Keyword Frequency (Top)")
    fig.tight_layout()

    path = os.path.join(output_dir, "keyword_wordcloud.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return {
        "data": data,
        "charts": [{
            "id": "keyword_wordcloud",
            "title": "关键词频次分布",
            "type": "bar",
            "path": path,
            "file_path": path,
            "description": "Top keywords frequency bar chart",
            "source_tool": "keyword_wordcloud",
        }],
        "summary": "关键词频次统计完成",
    }


def entity_cooccurrence_network(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
    top_n: int = 15,
) -> Dict[str, Any]:
    """Entity co-occurrence heatmap + edge list."""
    output_dir = get_images_dir(output_dir)
    entity_counts = Counter()
    entity_lists = []
    for post in blog_data:
        ents = post.get("entities")
        if not ents:
            ents = extract_entities(post.get("content", ""))
        ents = [e for e in ents if e]
        if ents:
            entity_lists.append(ents)
            entity_counts.update(ents)

    top_entities = [e for e, _ in entity_counts.most_common(top_n)]
    if not top_entities:
        return {"data": {"nodes": [], "edges": []}, "charts": []}

    idx_map = {e: i for i, e in enumerate(top_entities)}
    matrix = np.zeros((len(top_entities), len(top_entities)), dtype=int)
    edges = defaultdict(int)
    for ents in entity_lists:
        filtered = [e for e in ents if e in idx_map]
        for i in range(len(filtered)):
            for j in range(i + 1, len(filtered)):
                a, b = filtered[i], filtered[j]
                key = tuple(sorted((a, b)))
                edges[key] += 1
                ai, bi = idx_map[a], idx_map[b]
                matrix[ai, bi] += 1
                matrix[bi, ai] += 1

    fig, ax = plt.subplots(figsize=(6, 5))
    im = ax.imshow(matrix, cmap="Blues")
    ax.set_xticks(range(len(top_entities)))
    ax.set_yticks(range(len(top_entities)))
    ax.set_xticklabels(top_entities, rotation=45, ha="right")
    ax.set_yticklabels(top_entities)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    ax.set_title("Entity Co-occurrence Heatmap")
    fig.tight_layout()

    path = os.path.join(output_dir, "entity_cooccurrence.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    edge_list = [
        {"source": a, "target": b, "weight": w}
        for (a, b), w in edges.items()
    ]

    return {
        "data": {"nodes": top_entities, "edges": edge_list},
        "charts": [{
            "id": "entity_cooccurrence_network",
            "title": "实体共现热力图",
            "type": "heatmap",
            "path": path,
            "file_path": path,
            "description": "Entity co-occurrence heatmap",
            "source_tool": "entity_cooccurrence_network",
        }],
        "summary": "实体共现统计完成",
    }


def text_cluster_analysis(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
    threshold: float = 0.85,
    min_cluster_size: int = 2,
) -> Dict[str, Any]:
    """Cluster size distribution based on text similarity."""
    output_dir = get_images_dir(output_dir)
    groups = [post.get("text_similarity_group") for post in blog_data]
    if not any(isinstance(g, int) for g in groups):
        texts = [clean_text(p.get("content", "")) for p in blog_data]
        groups = cluster_similar_texts(texts, threshold=threshold, min_cluster_size=min_cluster_size)

    counts = Counter([g for g in groups if g is not None and g >= 0])
    data = {"clusters": counts}

    if not counts:
        return {"data": data, "charts": []}

    labels = [str(k) for k in counts.keys()]
    values = list(counts.values())

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(labels, values)
    ax.set_title("Text Cluster Sizes")
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Posts")
    fig.tight_layout()

    path = os.path.join(output_dir, "text_cluster_analysis.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return {
        "data": data,
        "charts": [{
            "id": "text_cluster_analysis",
            "title": "文本相似聚类分布",
            "type": "bar",
            "path": path,
            "file_path": path,
            "description": "Cluster size distribution",
            "source_tool": "text_cluster_analysis",
        }],
        "summary": "文本聚类统计完成",
    }


def sentiment_lexicon_comparison(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
) -> Dict[str, Any]:
    """Compare lexicon-based sentiment distribution."""
    output_dir = get_images_dir(output_dir)
    labels = []
    for post in blog_data:
        lex = post.get("lexicon_sentiment")
        if not lex:
            lex = lexicon_sentiment(post.get("content", ""))
        labels.append(lex.get("label", "neutral"))

    counts = Counter(labels)
    data = {"distribution": dict(counts)}

    if not counts:
        return {"data": data, "charts": []}

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(list(counts.keys()), list(counts.values()))
    ax.set_title("Lexicon Sentiment Distribution")
    ax.set_xlabel("Label")
    ax.set_ylabel("Count")
    fig.tight_layout()

    path = os.path.join(output_dir, "sentiment_lexicon_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return {
        "data": data,
        "charts": [{
            "id": "sentiment_lexicon_comparison",
            "title": "词典情感分布",
            "type": "bar",
            "path": path,
            "file_path": path,
            "description": "Lexicon sentiment distribution",
            "source_tool": "sentiment_lexicon_comparison",
        }],
        "summary": "词典情感分布统计完成",
    }


def temporal_keyword_heatmap(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
    top_n: int = 10,
    granularity: str = "day",
) -> Dict[str, Any]:
    """Heatmap of top keywords over time."""
    output_dir = get_images_dir(output_dir)
    rows = []
    for post in blog_data:
        publish_time = post.get("publish_time")
        if not publish_time:
            continue
        try:
            dt = pd.to_datetime(publish_time, errors="coerce")
        except Exception:
            dt = None
        if dt is None or pd.isna(dt):
            continue
        key = dt.strftime("%Y-%m-%d" if granularity == "day" else "%Y-%m-%d %H:00")
        keywords = post.get("keywords")
        if not keywords:
            keywords = extract_keywords(post.get("content", ""), top_n=top_n)
        for kw in keywords:
            rows.append({"time_key": key, "keyword": kw})

    if not rows:
        return {"data": {"matrix": []}, "charts": []}

    df = pd.DataFrame(rows)
    top_keywords = df["keyword"].value_counts().head(top_n).index.tolist()
    df = df[df["keyword"].isin(top_keywords)]
    pivot = df.pivot_table(
        index="time_key", columns="keyword", values="keyword",
        aggfunc="count", fill_value=0
    )
    pivot = pivot.sort_index()

    fig, ax = plt.subplots(figsize=(8, 4 + 0.2 * len(pivot)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title("Temporal Keyword Heatmap")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()

    path = os.path.join(output_dir, "temporal_keyword_heatmap.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return {
        "data": {
            "matrix": pivot.to_dict(),
            "time_keys": list(pivot.index),
            "keywords": list(pivot.columns),
        },
        "charts": [{
            "id": "temporal_keyword_heatmap",
            "title": "关键词时间热力图",
            "type": "heatmap",
            "path": path,
            "file_path": path,
            "description": "Keyword temporal heatmap",
            "source_tool": "temporal_keyword_heatmap",
        }],
        "summary": "关键词时间热力图生成完成",
    }
