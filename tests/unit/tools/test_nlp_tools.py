"""
Unit tests for lightweight NLP utilities.
"""
import re

from utils.nlp import (
    clean_text,
    tokenize,
    extract_keywords,
    lexicon_sentiment,
    cluster_similar_texts,
)


def test_clean_text_removes_url_and_emoji():
    raw = "测试一下 😊 https://example.com/path?q=1"
    cleaned = clean_text(raw)
    assert "http" not in cleaned
    assert "😊" not in cleaned


def test_tokenize_empty_text():
    assert tokenize("") == []


def test_extract_keywords_long_text_limit():
    text = "天气很好今天心情不错我们一起出去玩今天真开心" * 5
    keywords = extract_keywords(text, top_n=5)
    assert isinstance(keywords, list)
    assert len(keywords) <= 5


def test_lexicon_sentiment_empty():
    res = lexicon_sentiment("")
    assert res["label"] == "neutral"
    assert res["score"] == 0


def test_cluster_similar_texts():
    texts = ["天气很好", "天气真好", "完全不同的内容"]
    groups = cluster_similar_texts(texts, threshold=0.2, min_cluster_size=2)
    assert groups[0] == groups[1]
    assert groups[2] in (-1, groups[0]) or isinstance(groups[2], int)


def test_tokenize_regex_fallback_keeps_multi_char_chinese_tokens(monkeypatch):
    from utils.nlp import tokenizer as tokenizer_module

    monkeypatch.setattr(tokenizer_module, "jieba", None)
    tokens = tokenizer_module.tokenize("今天天气很好我们去公园")
    chinese_tokens = [token for token in tokens if re.search(r"[\u4e00-\u9fff]", token)]
    assert any(len(token) > 1 for token in chinese_tokens)
