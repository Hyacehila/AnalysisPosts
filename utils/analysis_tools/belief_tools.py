import os
from datetime import datetime
from itertools import combinations
from typing import Any, Dict, List, Tuple

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib import patheffects
import networkx as nx
import numpy as np
import pandas as pd

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False


def _extract_belief_signals(belief_signals: Any) -> Tuple[List[str], Dict[str, str]]:
    """提取信念信号列表与子类->类别映射."""
    if not belief_signals:
        return [], {}

    signals: List[str] = []
    mapping: Dict[str, str] = {}

    for item in belief_signals:
        if isinstance(item, dict):
            category = item.get("category") or item.get("Category") or ""
            subcategory = item.get("subcategory") or item.get("Subcategory") or ""
            subcategories = item.get("subcategories") or item.get("Subcategories") or []

            if subcategory:
                signals.append(subcategory)
                mapping[subcategory] = category or subcategory

            if subcategories:
                for sub in subcategories:
                    if not sub:
                        continue
                    signals.append(sub)
                    mapping[sub] = category or sub
            elif not subcategory and category:
                signals.append(category)
                mapping[category] = category
        elif isinstance(item, str):
            signals.append(item)
            mapping[item] = mapping.get(item, item)

    unique_signals = list(dict.fromkeys(signals))
    return unique_signals, mapping


def build_belief_network_data(
    blogs_data: List[Dict[str, Any]],
    event_name: str = "belief_network",
    data_dir: str = "report",
) -> Tuple[nx.Graph, pd.DataFrame, pd.DataFrame]:
    """
    输入：打好标签的博文列表
    输出：NetworkX 图对象 + 节点/边数据表
    """
    G = nx.Graph()
    W_REPOST, W_COMMENT, W_LIKE = 5, 3, 1

    for blog in blogs_data:
        signals, sig_to_cat = _extract_belief_signals(blog.get("belief_signals", []))
        if not signals:
            continue

        raw_score = (
            blog.get("repost_count", 0) * W_REPOST
            + blog.get("comment_count", 0) * W_COMMENT
            + blog.get("like_count", 0) * W_LIKE
        )
        blog_weight = np.log1p(raw_score)

        if len(signals) == 1:
            node = signals[0]
            if not G.has_node(node):
                G.add_node(node, category=sig_to_cat.get(node, ""), co_occurrence_weight=0)

            if G.has_edge(node, node):
                G[node][node]["weight"] += blog_weight
            else:
                G.add_edge(node, node, weight=blog_weight)
        else:
            for u, v in combinations(signals, 2):
                for n in (u, v):
                    if not G.has_node(n):
                        G.add_node(n, category=sig_to_cat.get(n, ""), co_occurrence_weight=0)
                    G.nodes[n]["co_occurrence_weight"] += blog_weight

                if G.has_edge(u, v):
                    G[u][v]["weight"] += blog_weight
                else:
                    G.add_edge(u, v, weight=blog_weight)

    nodes_df = pd.DataFrame(
        [
            {
                "ID": n,
                "Category": d.get("category", ""),
                "Co-Occurrence-Weight": d.get("co_occurrence_weight", 0),
            }
            for n, d in G.nodes(data=True)
        ]
    )

    edges_list = []
    for u, v, d in G.edges(data=True):
        edges_list.append(
            {
                "Source": u,
                "Target": v,
                "Weight": d.get("weight", 0),
                "Type": "Loop" if u == v else "Regular",
            }
        )
    edges_df = pd.DataFrame(edges_list)

    os.makedirs(data_dir, exist_ok=True)
    nodes_path = os.path.join(data_dir, f"nodes_{event_name}.csv")
    edges_path = os.path.join(data_dir, f"edges_{event_name}.csv")
    nodes_df.to_csv(nodes_path, index=False, encoding="utf_8")
    edges_df.to_csv(edges_path, index=False, encoding="utf_8")

    return G, nodes_df, edges_df


def _draw_belief_network_graph(G: nx.Graph, file_path: str, event_name: str) -> None:
    if G.number_of_nodes() == 0:
        return

    num_nodes = G.number_of_nodes()
    dynamic_k = 2.5 / np.sqrt(num_nodes)
    pos = nx.spring_layout(G, k=dynamic_k, iterations=10, seed=114514)

    plt.figure(figsize=(15, 12), facecolor="white")

    nodes = list(G.nodes())
    color_map = {"风险感知类": "#FF6B6B", "归因信念类": "#4D96FF", "行动/政策类": "#6BCB77"}
    border_color_map = {"风险感知类": "#C0392B", "归因信念类": "#2980B9", "行动/政策类": "#27AE60"}

    self_loop_weights = {u: d["weight"] for u, v, d in G.edges(data=True) if u == v}
    regular_edges = [(u, v) for u, v in G.edges() if u != v]

    total_sizes = []
    core_sizes = []
    for n in nodes:
        cw = G.nodes[n].get("co_occurrence_weight", 0)
        sw = self_loop_weights.get(n, 0)
        total_w = cw + sw

        t_size = np.power(total_w, 0.5) * 400 + 1500
        total_sizes.append(t_size)

        ratio = (cw / total_w) if total_w > 0 else 0
        core_sizes.append(t_size * (ratio * 0.8 + 0.2))

    edge_widths = [np.power((G[u][v]["weight"] / 5), 0.7) * 10 for u, v in regular_edges]

    nx.draw_networkx_edges(
        G, pos, edgelist=regular_edges, width=edge_widths, alpha=0.5, edge_color="#5F5F5F"
    )

    nx.draw_networkx_nodes(
        G,
        pos,
        node_size=total_sizes,
        node_color=[border_color_map.get(G.nodes[n].get("category"), "#747474") for n in nodes],
        alpha=1.0,
    )

    nx.draw_networkx_nodes(
        G,
        pos,
        node_size=core_sizes,
        node_color=[color_map.get(G.nodes[n].get("category"), "#cccccc") for n in nodes],
        edgecolors="none",
        alpha=1.0,
    )

    for node, (x, y) in pos.items():
        txt = plt.text(
            x,
            y,
            s=node,
            fontsize=12,
            fontfamily="SimHei",
            fontweight="bold",
            ha="center",
            va="center",
            zorder=30,
        )
        txt.set_path_effects([patheffects.withStroke(linewidth=3, foreground="white")])

    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            label=cat,
            markerfacecolor=color,
            markersize=12,
            markeredgecolor=border_color_map[cat],
            markeredgewidth=1.5,
        )
        for cat, color in color_map.items()
    ]

    plt.legend(
        handles=legend_elements,
        loc="upper right",
        title="信念类别",
        fontsize=11,
        title_fontsize=13,
        frameon=True,
        edgecolor="#D3D3D3",
    )
    plt.title(f"信念网络图（{event_name}）", fontsize=18, fontfamily="SimHei")
    plt.margins(0.05)
    plt.axis("off")
    plt.savefig(file_path, bbox_inches="tight", dpi=300)
    plt.close()


def belief_network_chart(
    blog_data: List[Dict[str, Any]],
    output_dir: str = "report/images",
    data_dir: str = "report",
    event_name: str = "belief_network",
) -> Dict[str, Any]:
    """
    生成信念系统共现网络图与节点/边表格
    """
    if not blog_data:
        return {"charts": [], "summary": "没有可绘制的信念数据"}

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chart_id = f"belief_network_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{chart_id}.png")

    G, nodes_df, edges_df = build_belief_network_data(
        blog_data, event_name=event_name, data_dir=data_dir
    )
    if G.number_of_nodes() == 0:
        return {"charts": [], "summary": "没有可绘制的信念数据"}

    _draw_belief_network_graph(G, file_path, event_name=event_name)

    return {
        "charts": [
            {
                "id": chart_id,
                "type": "network",
                "title": f"信念网络图（{event_name}）",
                "file_path": file_path,
                "source_tool": "belief_network_chart",
                "description": "信念子类的共现关系与强度",
            }
        ],
        "data": {
            "nodes": nodes_df.to_dict(orient="records"),
            "edges": edges_df.to_dict(orient="records"),
        },
        "summary": f"生成信念网络图与节点/边数据（节点{len(nodes_df)}、边{len(edges_df)}）",
    }
