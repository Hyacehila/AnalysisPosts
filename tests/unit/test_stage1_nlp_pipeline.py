"""
Tests for Stage1 NLP enrichment node.
"""
from unittest.mock import patch

from nodes.stage1.nlp_enrichment import NLPEnrichmentNode


def test_nlp_enrichment_adds_fields(sample_blog_data):
    shared = {
        "data": {"blog_data": sample_blog_data},
        "config": {"stage1_nlp": {"enabled": True, "keyword_top_n": 3}},
        "stage1_results": {},
    }

    node = NLPEnrichmentNode()
    p = node.prep(shared)
    e = node.exec(p)
    node.post(shared, p, e)

    post = shared["data"]["blog_data"][0]
    assert "keywords" in post
    assert "entities" in post
    assert "lexicon_sentiment" in post
    assert "text_similarity_group" in post


def test_nlp_enrichment_failure_fallback(sample_blog_data):
    shared = {
        "data": {"blog_data": sample_blog_data},
        "config": {"stage1_nlp": {"enabled": True}},
        "stage1_results": {},
    }

    node = NLPEnrichmentNode()

    with patch("nodes.stage1.nlp_enrichment.extract_keywords", side_effect=RuntimeError("boom")):
        action = node.run(shared)

    assert action == "default"
    assert shared["stage1_results"]["nlp"]["failed"] is True
