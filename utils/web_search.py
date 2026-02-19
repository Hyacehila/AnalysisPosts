"""
Web search wrapper utilities for Stage2 search ingestion.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests


_TAVILY_SEARCH_URL = "https://api.tavily.com/search"


def _normalize_provider(provider: str) -> str:
    normalized = (provider or "").strip().lower()
    if normalized != "tavily":
        raise ValueError(f"Unsupported search provider: {provider}")
    return normalized


def _resolve_api_key(provider: str, api_key: Optional[str]) -> str:
    if provider != "tavily":
        raise ValueError(f"Unsupported search provider: {provider}")
    resolved = (api_key or "").strip() or os.environ.get("TAVILY_API_KEY", "").strip()
    if not resolved:
        raise EnvironmentError(
            "Tavily API key is required (set stage2.search_api_key in config.yaml "
            "or environment variable TAVILY_API_KEY)."
        )
    return resolved


def _normalize_tavily_results(results: List[Dict[str, Any]], max_results: int) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for item in (results or [])[:max_results]:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "title": str(item.get("title", "")),
                "url": str(item.get("url", "")),
                "snippet": str(item.get("content", item.get("snippet", ""))),
                "date": str(item.get("published_date", item.get("date", ""))),
                "source": str(item.get("source", "")),
            }
        )
    return normalized


def search_web(
    query: str,
    *,
    provider: str = "tavily",
    api_key: Optional[str] = None,
    max_results: int = 5,
    timeout_seconds: int = 20,
) -> Dict[str, Any]:
    """
    Execute a web search request and normalize result payload.
    """
    clean_query = (query or "").strip()
    if not clean_query:
        raise ValueError("query must not be empty")

    provider_name = _normalize_provider(provider)
    api_key_value = _resolve_api_key(provider_name, api_key)
    safe_max = max(1, int(max_results))
    safe_timeout = max(1, int(timeout_seconds))

    if provider_name == "tavily":
        payload = {
            "api_key": api_key_value,
            "query": clean_query,
            "max_results": safe_max,
            "include_answer": False,
            "include_images": False,
        }
        try:
            response = requests.post(_TAVILY_SEARCH_URL, json=payload, timeout=safe_timeout)
            response.raise_for_status()
            body = response.json() or {}
            normalized_results = _normalize_tavily_results(body.get("results", []), safe_max)
            return {
                "query": clean_query,
                "provider": provider_name,
                "results": normalized_results,
                "error": "",
            }
        except Exception as exc:  # noqa: B902
            return {
                "query": clean_query,
                "provider": provider_name,
                "results": [],
                "error": str(exc),
            }

    raise ValueError(f"Unsupported search provider: {provider_name}")


def batch_search(
    queries: List[str],
    *,
    provider: str = "tavily",
    api_key: Optional[str] = None,
    max_results: int = 5,
    timeout_seconds: int = 20,
) -> Dict[str, Any]:
    """
    Execute multiple queries via search_web and aggregate outputs.
    """
    provider_name = _normalize_provider(provider)
    normalized_queries = [(q or "").strip() for q in (queries or []) if (q or "").strip()]
    results_by_query: List[Dict[str, Any]] = []
    total_results = 0

    for query in normalized_queries:
        result = search_web(
            query,
            provider=provider_name,
            api_key=api_key,
            max_results=max_results,
            timeout_seconds=timeout_seconds,
        )
        results_by_query.append(result)
        total_results += len(result.get("results", []))

    return {
        "queries": normalized_queries,
        "provider": provider_name,
        "results_by_query": results_by_query,
        "total_results": total_results,
    }


__all__ = ["search_web", "batch_search"]
