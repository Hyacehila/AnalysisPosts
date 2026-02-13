"""
Stage 1 belief system analysis node.
"""
import asyncio
import json
import re
from typing import Any

from nodes.base import AsyncParallelBatchNode
from utils.call_llm import call_glm_45_air


class AsyncBeliefSystemAnalysisBatchNode(AsyncParallelBatchNode):
    """
    信念体系分类识别（多选）
    """

    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["belief_signals"] = result if result is not None else []

    async def prep_async(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        belief_system = shared.get("data", {}).get("belief_system", [])
        self._configure_checkpoint(shared, blog_data)
        if not belief_system:
            print("[BeliefSystem] 警告: belief_system 数据为空，将跳过LLM调用")
        else:
            print(f"[BeliefSystem] 准备处理 {len(blog_data)} 条博文，信念系统包含 {len(belief_system)} 个类别")
        return [
            {"blog_data": blog_post, "belief_system": belief_system}
            for blog_post in blog_data
        ]

    async def exec_async(self, prep_res):
        blog_post = prep_res["blog_data"]
        existing = blog_post.get("belief_signals")
        if existing is not None:
            return existing
        belief_system_raw = prep_res["belief_system"]
        belief_system = []

        def _clean(text: str) -> str:
            if not isinstance(text, str):
                return text
            try:
                return text.encode("latin1").decode("utf-8")
            except Exception:
                return text

        for item in belief_system_raw or []:
            belief_system.append({
                "category": _clean(item.get("category", "")),
                "subcategories": [_clean(sub) for sub in item.get("subcategories", [])],
            })
        if not belief_system:
            print("[BeliefSystem] 警告: 处理博文时 belief_system 为空，返回空结果")
            return []

        belief_lines = []
        for bs in belief_system:
            cat = bs.get("category", "")
            subs = "、".join(bs.get("subcategories", []))
            belief_lines.append(f"{cat} -> {subs}")
        belief_str = "\n".join(belief_lines)

        prompt = f"""请从下列信念体系主题列表中为博文的讨论内容选择1-3个最贴切的 category/subcategory 组合。
必须以 [ 开头、以 ] 结尾，仅输出 JSON 数组，格式示例：
[{{"category": "类别", "subcategories": ["子类1","子类2"]}}]
若无匹配输出 []，不得添加任何说明、Markdown 代码块或多余文本。子类必须来自给定列表。
信念体系：
{belief_str}
博文：
{blog_post.get('content', '')}"""

        response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.2)

        candidate = response.strip()
        fence_match = re.search(r"```(?:json)?\s*(.*?)\s*```", candidate, re.S)
        if fence_match:
            candidate = fence_match.group(1).strip()
        else:
            start = candidate.find("[")
            end = candidate.rfind("]")
            if start != -1 and end != -1 and end > start:
                candidate = candidate[start:end + 1].strip()

        try:
            parsed = json.loads(candidate)
        except Exception:
            arr_match = re.search(r"\[\s*{.*}\s*]", candidate, re.S)
            if arr_match:
                candidate = arr_match.group(0).strip()
                try:
                    parsed = json.loads(candidate)
                except Exception as e:
                    print(f"[BeliefSystem] 解析失败（二次）: {e}, 原始输出: {response!r}")
                    return []
            else:
                print(f"[BeliefSystem] 解析失败：未找到有效JSON数组，原始输出: {response!r}")
                return []
        if not isinstance(parsed, list):
            raise ValueError(f"信念分类模型输出不是列表格式: {parsed}")

        valid_results = []
        for item in parsed:
            cat = item.get("category", "")
            subs = item.get("subcategories", []) or []
            matched_def = next((bs for bs in belief_system if bs.get("category") == cat), None)
            if not matched_def:
                continue
            valid_subs = [sub for sub in subs if sub in matched_def.get("subcategories", [])]
            valid_results.append({"category": cat, "subcategories": valid_subs})

        return valid_results

    async def exec_fallback_async(self, prep_res, exc):
        print(f"[BeliefSystem] 调用失败，返回空: {exc}")
        return []

    async def post_async(self, shared, prep_res, exec_res):
        blog_data = shared.get("data", {}).get("blog_data", [])
        if len(exec_res) < len(blog_data):
            exec_res = list(exec_res) + [[] for _ in range(len(blog_data) - len(exec_res))]
        elif len(exec_res) > len(blog_data):
            exec_res = exec_res[:len(blog_data)]

        for i, blog_post in enumerate(blog_data):
            blog_post["belief_signals"] = exec_res[i] if exec_res[i] is not None else []

        print(f"[BeliefSystem] 完成 {len(blog_data)} 条博文的信念分类增强")
        return "default"
