"""
Lightweight NLP utilities (tokenization, keywords, NER, similarity).
"""
from utils.nlp.text_cleaner import clean_text
from utils.nlp.tokenizer import tokenize, tokenize_batch
from utils.nlp.keyword_extractor import extract_keywords, extract_keywords_batch
from utils.nlp.ner import extract_entities
from utils.nlp.sentiment_lexicon import lexicon_sentiment
from utils.nlp.similarity import cluster_similar_texts

__all__ = [
    "clean_text",
    "tokenize",
    "tokenize_batch",
    "extract_keywords",
    "extract_keywords_batch",
    "extract_entities",
    "lexicon_sentiment",
    "cluster_similar_texts",
]
