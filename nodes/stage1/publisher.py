"""
Stage 1 publisher analysis nodes.
"""
import asyncio
import json
from typing import Any

from nodes.base import AsyncParallelBatchNode
from utils.call_llm import call_glm_45_air


class AsyncPublisherObjectAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步发布者对象分析节点
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["publisher"] = result

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_objects = shared.get("data", {}).get("publisher_objects", [])

        self._configure_checkpoint(shared, blog_data)

        return [
            {"blog_data": blog_post, "publisher_objects": publisher_objects}
            for blog_post in blog_data
        ]

    async def exec_async(self, prep_res):
        blog_post = prep_res["blog_data"]
        publisher_objects = prep_res["publisher_objects"]
        existing = blog_post.get("publisher")
        if existing is not None:
            return existing

        publishers_str = "、".join(publisher_objects)
        username = blog_post.get("username", "") or blog_post.get("user_name", "")
        extra_user = f"\n发布者昵称/账号：{username}" if username else ""

        prompt = f"""候选发布者：{publishers_str}
判断该博文最可能的发布者类型，直接输出候选列表中的一个条目，不得添加解释。
博文：
{blog_post.get('content', '')}{extra_user}"""

        response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.3)
        publisher = response.strip()

        if publisher in publisher_objects:
            return publisher
        return "个人用户" if "个人用户" in publisher_objects else None

    async def exec_fallback_async(self, prep_res, exc):
        print(f"发布者分析失败，使用默认值: {str(exc)}")
        return "个人用户"

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])
        if len(exec_res) != len(blog_data):
            print("警告：发布者分析结果数量与博文数量不匹配")
            return "default"

        for i, blog_post in enumerate(blog_data):
            blog_post["publisher"] = exec_res[i] if i < len(exec_res) else None

        print(f"[AsyncPublisher] 完成 {len(exec_res)} 条博文的发布者分析")
        return "default"


class AsyncPublisherDecisionAnalysisBatchNode(AsyncParallelBatchNode):
    """
    发布者事件关联身份分类（四选一）
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["publisher_decision"] = result

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_decisions = shared.get("data", {}).get("publisher_decisions", [])
        self._configure_checkpoint(shared, blog_data)
        if not publisher_decisions:
            print("[PublisherDecision] 警告: publisher_decisions 数据为空，将跳过LLM调用")
        else:
            print(f"[PublisherDecision] 准备处理 {len(blog_data)} 条博文，关联身份分类包含 {len(publisher_decisions)} 个类别")
        return [
            {"blog_data": blog_post, "publisher_decisions": publisher_decisions}
            for blog_post in blog_data
        ]

    async def exec_async(self, prep_res):
        blog_post = prep_res["blog_data"]
        existing = blog_post.get("publisher_decision")
        if existing is not None:
            return existing

        publisher_decisions_raw = prep_res["publisher_decisions"]

        def _clean(text: str) -> str:
            if not isinstance(text, str):
                return text
            try:
                return text.encode("latin1").decode("utf-8")
            except Exception:
                return text

        publisher_decisions = [
            {"category": _clean(item.get("category", ""))}
            for item in (publisher_decisions_raw or [])
        ]
        if not publisher_decisions:
            print("[PublisherDecision] 警告: 处理博文时 publisher_decisions 为空，返回None")
            return None

        username = blog_post.get("username", "") or blog_post.get("user_name", "")
        extra_user = f"\n发布者昵称/账号：{username}" if username else ""

        prompt = f"""请选择博文发布者与舆情事件“河南大学生夜骑事件”的“事件关联身份”，只能从下列候选中选1个：
{json.dumps(publisher_decisions, ensure_ascii=False, indent=2)}
若无法判断，选择最接近的类型。只输出候选中的category文本，不要输出解释。
博文内容：{blog_post.get('content', '')}{extra_user}"""

        try:
            response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.2)
            chosen = response.strip().replace('"', "")
            candidates = [item.get("category") for item in publisher_decisions]
            for cand in candidates:
                if cand and cand in chosen:
                    return cand
            return candidates[0] if candidates else None
        except Exception as e:
            print(f"[PublisherDecision] 调用失败: {e}")
            candidates = [item.get("category") for item in publisher_decisions if item.get("category")]
            return candidates[0] if candidates else None

    async def exec_fallback_async(self, prep_res, exc):
        print(f"[PublisherDecision] 调用失败，返回None: {exc}")
        candidates = [
            item.get("category")
            for item in prep_res.get("publisher_decisions", [])
            if item.get("category")
        ]
        return candidates[0] if candidates else None

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])

        def _clean(text: str):
            if not isinstance(text, str):
                return text
            try:
                return text.encode("latin1").decode("utf-8")
            except Exception:
                return text

        candidates = [
            _clean(item.get("category"))
            for item in shared.get("data", {}).get("publisher_decisions", [])
            if item.get("category")
        ]
        fallback_value = candidates[0] if candidates else "未知"

        if len(exec_res) < len(blog_data):
            exec_res = list(exec_res) + [fallback_value for _ in range(len(blog_data) - len(exec_res))]
        elif len(exec_res) > len(blog_data):
            exec_res = exec_res[:len(blog_data)]

        for i, blog_post in enumerate(blog_data):
            value = exec_res[i] if i < len(exec_res) else fallback_value
            if value is None:
                value = fallback_value
            blog_post["publisher_decision"] = value

        print(f"[PublisherDecision] 完成 {len(blog_data)} 条博文的事件关联身份增强")
        return "default"
