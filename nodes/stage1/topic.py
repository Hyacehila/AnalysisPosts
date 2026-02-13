"""
Stage 1 topic analysis node.
"""
import asyncio
import json
import os
from typing import Any

from nodes.base import AsyncParallelBatchNode
from utils.call_llm import call_glm_45_air, call_glm4v_plus


class AsyncTwoLevelTopicAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步两级主题分析节点
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["topics"] = result

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        topics_hierarchy = shared.get("data", {}).get("topics_hierarchy", [])

        self._configure_checkpoint(shared, blog_data)

        return [
            {"blog_data": blog_post, "topics_hierarchy": topics_hierarchy}
            for blog_post in blog_data
        ]

    async def exec_async(self, prep_res):
        blog_post = prep_res["blog_data"]
        topics_hierarchy = prep_res["topics_hierarchy"]
        existing = blog_post.get("topics")
        if existing is not None:
            return existing

        topics_lines = []
        for topic_group in topics_hierarchy:
            parent_topic = topic_group.get("parent_topic", "")
            sub_topics = "、".join(topic_group.get("sub_topics", []))
            topics_lines.append(f"{parent_topic} -> {sub_topics}")
        topics_str = "\n".join(topics_lines)

        prompt = f"""请根据以下主题层次，从候选中选1-2个最贴切的父/子主题组合，使用JSON数组输出：
[{{"parent_topic": "父主题", "sub_topic": "子主题"}}]
若无匹配输出 []，不得添加解释。
候选主题：
{topics_str}
博文：
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

        topics = json.loads(response.strip())
        if not isinstance(topics, list):
            raise ValueError(f"模型输出不是列表格式: {topics}")

        valid_topics = []
        for topic_item in topics:
            parent_topic = topic_item.get("parent_topic", "")
            sub_topic = topic_item.get("sub_topic", "")

            for topic_group in topics_hierarchy:
                if topic_group.get("parent_topic") == parent_topic:
                    if sub_topic in topic_group.get("sub_topics", []):
                        valid_topics.append({"parent_topic": parent_topic, "sub_topic": sub_topic})
                    break

        return valid_topics

    async def exec_fallback_async(self, prep_res, exc):
        print(f"主题分析失败，使用默认值: {str(exc)}")
        return []

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])
        if len(exec_res) != len(blog_data):
            print("警告：主题分析结果数量与博文数量不匹配")
            return "default"

        for i, blog_post in enumerate(blog_data):
            blog_post["topics"] = exec_res[i] if i < len(exec_res) else None

        print(f"[AsyncTopic] 完成 {len(exec_res)} 条博文的主题分析")
        return "default"
