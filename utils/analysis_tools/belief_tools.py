import os
import json
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


def build_belief_network_data(
    blogs_data: List[Dict[str, Any]],
    event_name: str = "belief_network",
    data_dir: str = "report",
) -> Tuple[nx.Graph, pd.DataFrame, pd.DataFrame]:
    """
    输入：打好标签的博文列表
    输出：NetworkX 图对象 (包含节点、普通边、环边及其权重)
    """
    G = nx.Graph()
    # 转发、评论、点赞的权重
    # W_REPOST, W_COMMENT, W_LIKE = 5, 3, 1

    for blog in blogs_data:
        # 1. 提取信号并处理嵌套的 subcategories 列表
        raw_signals_list = blog.get('belief_signals', [])
        if not raw_signals_list:
            continue

        signals = []
        sig_to_cat = {}
        
        for item in raw_signals_list:
            if isinstance(item, dict):
                cat = item.get('category', '其他')
                # 关键修复：遍历 subcategories 列表
                subs = item.get('subcategories', [])
                if isinstance(subs, list):
                    for s in subs:
                        if s:
                            signals.append(s)
                            sig_to_cat[s] = cat
                elif isinstance(subs, str): # 防御性编程：万一以后出现了字符串
                    if subs:
                        signals.append(subs)
                        sig_to_cat[subs] = cat
            elif isinstance(item, str):
                if item:
                    signals.append(item)
                    sig_to_cat[item] = '其他'

        # 对单篇博文内的信号去重
        signals = list(set(signals))
        if not signals:
            continue

        # 互动总分
        interaction_sum = (blog.get('repost_count', 0) * 5 + 
                           blog.get('comment_count', 0) * 3 + 
                           blog.get('like_count', 0) * 1)
        blog_weight = np.log(np.e + interaction_sum)    # 自然对数平滑

        # 3. 构建网络结构
        if len(signals) == 1:
            # 单个信号：构造“自环”边，表示独立出现的强度
            node = signals[0]
            if not G.has_node(node):
                G.add_node(node, category=sig_to_cat.get(node, "其他"), co_occurrence_weight=0)
            
            if G.has_edge(node, node):
                G[node][node]['weight'] += blog_weight
            else:
                G.add_edge(node, node, weight=blog_weight)
        
        else:
            # 多个信号：构造两两之间的普通边，并增加节点的“共现”权重
            for u, v in combinations(signals, 2):
                for n in [u, v]:
                    if not G.has_node(n):
                        G.add_node(n, category=sig_to_cat.get(n, "其他"), co_occurrence_weight=0)
                    # 记录节点参与“共现”的总强度
                    G.nodes[n]['co_occurrence_weight'] += blog_weight
                
                if G.has_edge(u, v):
                    G[u][v]['weight'] += blog_weight
                else:
                    G.add_edge(u, v, weight=blog_weight)
    
    nodes_df = pd.DataFrame([
        {
            "ID": n, 
            "Category": d.get('category', '未知'), 
            "Co-Occurrence-Weight": d.get('co_occurrence_weight', 0)
        }
        for n, d in G.nodes(data=True)
    ])
    
    os.makedirs(data_dir, exist_ok=True)
    nodes_path = os.path.join(data_dir, f"nodes_{event_name}.csv")
    edges_path = os.path.join(data_dir, f"edges_{event_name}.csv")
    nodes_df.to_csv(nodes_path, index=False, encoding='utf_8_sig')

    edges_list = []
    for u, v, d in G.edges(data=True):
        edges_list.append({
            "Source": u, 
            "Target": v, 
            "Weight": d['weight'], 
            "Type": "Loop" if u == v else "Regular"
        })
    edges_df = pd.DataFrame(edges_list)
    edges_df.to_csv(edges_path, index=False, encoding='utf_8_sig')
    
    return G, nodes_df, edges_df


def _draw_belief_network_graph(G: nx.Graph, file_path: str, event_name: str) -> None:
    if G.number_of_nodes() == 0: return

    num_nodes = G.number_of_nodes()
    fig_side = max(15, int(np.sqrt(num_nodes) * 3.5))
    fig, ax = plt.subplots(figsize=(fig_side, fig_side * 1), facecolor='white')

    dynamic_k = 7.0 / np.sqrt(num_nodes) 
    pos = nx.spring_layout(G, k=dynamic_k, iterations=500, seed=114514, weight=None)

    nodes = list(G.nodes())
    self_loop_weights = {u: d['weight'] for u, v, d in G.edges(data=True) if u == v}
    co_occurrence_weights = {n: G.nodes[n].get('co_occurrence_weight', 0) for n in nodes}

    total_weights = [co_occurrence_weights[n] + self_loop_weights.get(n, 0) for n in nodes]
    avg_total_w = np.mean(total_weights) if total_weights else 1
    
    base_node_size = (fig_side * 1000) / np.sqrt(num_nodes) 
    outer_sizes = [base_node_size * np.clip(np.sqrt(w / avg_total_w), 0.8, 3.5) for w in total_weights]

    # 3. 边宽五档化 (针对外部边)
    regular_edges = [(u, v) for u, v in G.edges() if u != v]
    w_levels = [1.0, 2.5, 4.5, 7.0, 10.0]
    
    edge_widths = []
    edge_range_labels = []

    if regular_edges:
        weights = np.array([G[u][v]['weight'] for u, v in regular_edges])
        p_points = [0, 20, 40, 60, 80, 100]
        thresholds = [np.percentile(weights, p) for p in p_points]
        edge_range_labels = [f"{int(thresholds[i])} - {int(thresholds[i+1])}" for i in range(5)]
        
        for u, v in regular_edges:
            w = G[u][v]['weight']
            idx = next((i for i, t in enumerate(thresholds[1:]) if w <= t), 4)
            edge_widths.append(w_levels[idx])
    else:
        edge_range_labels = ["0-0"] * 5

    color_map = {"风险感知类": "#FF6B6B", "归因信念类": "#4D96FF", "行动/政策类": "#6BCB77"}
    dark_color_map = {"风险感知类": "#C0392B", "归因信念类": "#2980B9", "行动/政策类": "#27AE60"}

    nx.draw_networkx_edges(G, pos, edgelist=regular_edges, width=edge_widths, alpha=0.25, edge_color="#5D5C5C")

    for i, node in enumerate(nodes):
        cat = G.nodes[node].get('category', '其他')
        base_color = color_map.get(cat, "#cccccc")
        dark_color = dark_color_map.get(cat, "#747474")
        
        nx.draw_networkx_nodes(G, pos, nodelist=[node], node_size=outer_sizes[i], 
                               node_color=base_color, edgecolors=dark_color, linewidths=1.0, alpha=0.8)
        
        self_w = self_loop_weights.get(node, 0)
        total_w = total_weights[i]
        inner_ratio = np.sqrt(self_w / total_w) if total_w > 0 else 0
        inner_size = outer_sizes[i] * inner_ratio
        
        if inner_size > 0:
            nx.draw_networkx_nodes(G, pos, nodelist=[node], node_size=inner_size, 
                                   node_color=dark_color, edgecolors=dark_color, linewidths=0.5, alpha=1)

    for i, (node, (x, y)) in enumerate(pos.items()):
        d_fontsize = np.clip(np.sqrt(outer_sizes[i]) / 3.5, 10, 16)
        txt = plt.text(x, y - 0.03, s=node, fontsize=d_fontsize, fontfamily='SimHei', 
                        fontweight='bold', ha='center', va='center', zorder=150)
        txt.set_path_effects([patheffects.withStroke(linewidth=2.5, foreground="white")])

    cat_legends = [Line2D([0], [0], marker='o', color='w', label=cat, markerfacecolor=color, 
                          markersize=14) 
                   for cat, color in color_map.items()]
    
    if regular_edges:
        edge_legends = [Line2D([0], [0], color='#767676', lw=w_levels[i], label=edge_range_labels[i])
                        for i in range(4, -1, -1)]
    else:
        edge_legends = []

    node_composition_legends = [
        Line2D([0], [0], marker='o', color='w', label='共现部分', 
               markerfacecolor='#DDDDDD', markersize=14),
        Line2D([0], [0], marker='o', color='w', label='自现部分', 
               markerfacecolor='#747474', markersize=10)
    ]

    leg1 = ax.legend(handles=cat_legends, loc='upper right', title="信念类别", shadow=True)
    ax.add_artist(leg1) 
    leg2 = ax.legend(handles=node_composition_legends, loc='upper right', 
                     title="节点构成", bbox_to_anchor=(1, 0.85), shadow=True)
    ax.add_artist(leg2)
    
    if edge_legends:
        ax.legend(handles=edge_legends, loc='lower right', title="关联强度", shadow=True)

    # plt.title(f"信念网络图（{event_name}）", fontsize=22, pad=30)
    plt.axis('off')
    plt.margins(0.05)
    plt.savefig(file_path, dpi=600, bbox_inches='tight', transparent=True)
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
