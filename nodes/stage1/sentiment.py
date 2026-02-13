"""
Stage 1 sentiment analysis nodes.
"""
import asyncio
import json
import os
from typing import Any

from nodes.base import AsyncParallelBatchNode
from utils.call_llm import call_glm_45_air, call_glm4v_plus


class AsyncSentimentPolarityAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感极性分析节点
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict):
            item["sentiment_polarity"] = result

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        self._configure_checkpoint(shared, blog_data)
        return blog_data

    async def exec_async(self, prep_res):
        blog_post = prep_res
        existing = blog_post.get("sentiment_polarity")
        if existing is not None:
            return existing

        prompt = f"""你是社交媒体分析师，请依据下表判断博文整体情感极性：
- 1=极度悲观，2=悲观，3=中性，4=乐观，5=极度乐观，0=无法判断
- 仅输出一个数字（0-5），不得附加解释或其他字符
博文内容：
{blog_post.get('content', '')}"""

        image_paths = blog_post.get("image_urls", [])
        image_paths = [img for img in image_paths if img and img.strip()]

        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                processed_image_paths.append(os.path.join("data", img_path))
            else:
                processed_image_paths.append(img_path)

        if processed_image_paths:
            response = await asyncio.to_thread(
                call_glm4v_plus, prompt, image_paths=processed_image_paths, temperature=0.3
            )
        else:
            response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.3)

        response = response.strip()
        if not response.isdigit():
            raise ValueError(f"模型输出不是数字: {response}")

        score = int(response)
        if not 1 <= score <= 5:
            raise ValueError(f"模型输出数字不在1-5范围内: {score}")

        return score

    async def exec_fallback_async(self, prep_res, exc):
        print(f"情感极性分析失败，使用默认值: {str(exc)}")
        return 3

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])
        if len(exec_res) != len(blog_data):
            print("警告：情感极性分析结果数量与博文数量不匹配")
            return "default"

        for i, blog_post in enumerate(blog_data):
            blog_post["sentiment_polarity"] = exec_res[i] if i < len(exec_res) else None

        print(f"[AsyncSentimentPolarity] 完成 {len(exec_res)} 条博文的情感极性分析")
        return "default"


class AsyncSentimentAttributeAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感属性分析节点
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["sentiment_attribute"] = result

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        sentiment_attributes = shared.get("data", {}).get("sentiment_attributes", [])

        self._configure_checkpoint(shared, blog_data)

        return [
            {"blog_data": blog_post, "sentiment_attributes": sentiment_attributes}
            for blog_post in blog_data
        ]

    async def exec_async(self, prep_res):
        blog_post = prep_res["blog_data"]
        sentiment_attributes = prep_res["sentiment_attributes"]
        existing = blog_post.get("sentiment_attribute")
        if existing is not None:
            return existing

        attributes_str = "、".join(sentiment_attributes)
        prompt = f"""从候选情感属性中选出最贴切的1-3个，按JSON数组输出（示例：["支持","期待"]）。
候选：{attributes_str}
仅输出JSON数组，不得添加解释或其他文本。
博文：
{blog_post.get('content', '')}"""

        response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.3)

        attributes = json.loads(response.strip())
        if not isinstance(attributes, list):
            raise ValueError(f"模型输出不是列表格式: {attributes}")

        return [attr for attr in attributes if attr in sentiment_attributes]

    async def exec_fallback_async(self, prep_res, exc):
        print(f"情感属性分析失败，使用默认值: {str(exc)}")
        return ["中立"]

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])
        if len(exec_res) != len(blog_data):
            print("警告：情感属性分析结果数量与博文数量不匹配")
            return "default"

        for i, blog_post in enumerate(blog_data):
            blog_post["sentiment_attribute"] = exec_res[i] if i < len(exec_res) else None

        print(f"[AsyncSentimentAttribute] 完成 {len(exec_res)} 条博文的情感属性分析")
        return "default"
