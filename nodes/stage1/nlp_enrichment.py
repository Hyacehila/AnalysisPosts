"""
Stage 1 NLP enrichment node.
"""
from __future__ import annotations

from nodes.base import MonitoredNode

from utils.nlp import (
    clean_text,
    extract_keywords,
    extract_entities,
    lexicon_sentiment,
    cluster_similar_texts,
)


class NLPEnrichmentNode(MonitoredNode):
    """
    可选的 NLP 预处理节点（Stage 1）。

    输出字段：
    - keywords
    - entities
    - text_similarity_group
    - lexicon_sentiment
    """

    def prep(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        cfg = shared.get("config", {}) or {}
        nlp_cfg = cfg.get("stage1_nlp") or (cfg.get("stage1", {}) or {}).get("nlp", {})
        return {
            "blog_data": blog_data,
            "enabled": nlp_cfg.get("enabled", False),
            "keyword_top_n": nlp_cfg.get("keyword_top_n", 8),
            "similarity_threshold": nlp_cfg.get("similarity_threshold", 0.85),
            "min_cluster_size": nlp_cfg.get("min_cluster_size", 2),
        }

    def exec(self, prep_res):
        if not prep_res.get("enabled", False):
            return {"skipped": True}

        blog_data = prep_res["blog_data"]
        top_n = int(prep_res.get("keyword_top_n", 8))
        threshold = float(prep_res.get("similarity_threshold", 0.85))
        min_cluster_size = int(prep_res.get("min_cluster_size", 2))

        texts = [clean_text(post.get("content", "")) for post in blog_data]
        keywords_list = [extract_keywords(text, top_n=top_n) for text in texts]
        entities_list = [extract_entities(text) for text in texts]
        lexicon_list = [lexicon_sentiment(text) for text in texts]
        groups = cluster_similar_texts(texts, threshold=threshold, min_cluster_size=min_cluster_size)

        return {
            "keywords_list": keywords_list,
            "entities_list": entities_list,
            "lexicon_list": lexicon_list,
            "groups": groups,
        }

    def exec_fallback(self, prep_res, exc):
        return {"failed": True, "error": str(exc)}

    def post(self, shared, prep_res, exec_res):
        if exec_res.get("skipped"):
            return "default"

        if exec_res.get("failed"):
            shared.setdefault("stage1_results", {}).setdefault("nlp", {})
            shared["stage1_results"]["nlp"] = {
                "enabled": prep_res.get("enabled", False),
                "failed": True,
                "error": exec_res.get("error", ""),
            }
            print(f"[NLPEnrichment] failed, fallback to LLM-only: {exec_res.get('error')}")
            return "default"

        blog_data = prep_res.get("blog_data", [])
        keywords_list = exec_res.get("keywords_list", [])
        entities_list = exec_res.get("entities_list", [])
        lexicon_list = exec_res.get("lexicon_list", [])
        groups = exec_res.get("groups", [])

        for idx, post in enumerate(blog_data):
            if idx < len(keywords_list):
                post["keywords"] = keywords_list[idx]
            if idx < len(entities_list):
                post["entities"] = entities_list[idx]
            if idx < len(lexicon_list):
                post["lexicon_sentiment"] = lexicon_list[idx]
            if idx < len(groups):
                post["text_similarity_group"] = groups[idx]

        shared.setdefault("stage1_results", {}).setdefault("nlp", {})
        shared["stage1_results"]["nlp"] = {
            "enabled": prep_res.get("enabled", False),
            "failed": False,
            "processed": len(blog_data),
        }

        print(f"[NLPEnrichment] enriched {len(blog_data)} posts")
        return "default"
