"""
base.py - AsyncParallelBatchNode 基类

带并发限制的异步并行批处理节点基类，用于阶段1增强处理。
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

from pocketflow import AsyncNode, BatchNode

from utils.data_loader import save_enhanced_blog_data


class AsyncParallelBatchNode(AsyncNode, BatchNode):
    """
    带并发限制的异步并行批处理节点
    
    用于阶段1的异步批量并行处理路径 (enhancement_mode="async")
    支持通过 max_concurrent 参数控制并发执行数量，避免触发API限流
    """
    
    def __init__(self, max_concurrent: Optional[int] = None, **kwargs):
        """
        初始化异步并行批处理节点
        
        Args:
            max_concurrent: 最大并发数，None表示不限制
        """
        super().__init__(**kwargs)
        self.max_concurrent = max_concurrent
        # 在构造时创建信号量（实例级别共享）
        self._semaphore = (
            asyncio.Semaphore(max_concurrent) 
            if max_concurrent else None
        )
        # === stage1 checkpoint（防断连/防中断丢失）===
        self._checkpoint_enabled: bool = False
        self._checkpoint_output_path: str = ""
        self._checkpoint_save_every: int = 0
        self._checkpoint_min_interval_seconds: float = 0.0
        self._checkpoint_last_save_time: float = 0.0
        self._checkpoint_lock = asyncio.Lock()
        self._checkpoint_data_ref: Optional[List[Dict[str, Any]]] = None

    def _configure_checkpoint(self, shared: Dict[str, Any], blog_data_ref: List[Dict[str, Any]]) -> None:
        """
        从 shared["config"] 读取阶段1的 checkpoint 配置，并绑定需要保存的数据引用。

        支持的配置入口（任选其一）：
        - shared["config"]["stage1_checkpoint"]
        - shared["config"]["checkpoint"]["stage1"]

        字段：
        - enabled: bool，默认 True
        - save_every: int，每完成 N 条就保存一次，默认 200（想“每条都保存”可设为 1）
        - min_interval_seconds: float，最小保存间隔（秒），默认 20
        - output_path: str，覆盖输出路径；默认使用 config.data_source.enhanced_data_path
        """
        config = shared.get("config", {})
        checkpoint_cfg = config.get("stage1_checkpoint")
        if checkpoint_cfg is None:
            checkpoint_cfg = (config.get("checkpoint", {}) or {}).get("stage1", {})

        enabled = checkpoint_cfg.get("enabled", True)
        save_every = checkpoint_cfg.get("save_every", 200)
        min_interval_seconds = checkpoint_cfg.get("min_interval_seconds", 20)
        output_path = checkpoint_cfg.get(
            "output_path",
            config.get("data_source", {}).get("enhanced_data_path", "data/enhanced_blogs.json"),
        )

        try:
            save_every = int(save_every)
        except Exception:
            save_every = 200
        try:
            min_interval_seconds = float(min_interval_seconds)
        except Exception:
            min_interval_seconds = 20.0

        self._checkpoint_enabled = bool(enabled) and bool(output_path) and save_every > 0
        self._checkpoint_output_path = str(output_path)
        self._checkpoint_save_every = max(1, save_every)
        self._checkpoint_min_interval_seconds = max(0.0, min_interval_seconds)
        self._checkpoint_data_ref = blog_data_ref

    def apply_item_result(self, item: Any, result: Any) -> None:
        """
        允许子类在每条 item 完成后，将 result 立刻写回 blog_post（用于 checkpoint 立刻落盘）。
        默认不做任何事；子类按需 override。
        """
        return

    async def _checkpoint_save_async(self, completed: int, total: int, *, force: bool) -> None:
        if not self._checkpoint_enabled:
            return
        if not self._checkpoint_output_path:
            return
        if not isinstance(self._checkpoint_data_ref, list):
            return

        async with self._checkpoint_lock:
            now = time.time()
            if (
                not force
                and self._checkpoint_min_interval_seconds > 0
                and (now - self._checkpoint_last_save_time) < self._checkpoint_min_interval_seconds
            ):
                return

            ok = await asyncio.to_thread(
                save_enhanced_blog_data, self._checkpoint_data_ref, self._checkpoint_output_path
            )
            if ok:
                self._checkpoint_last_save_time = now
                print(f"[Checkpoint] saved {completed}/{total} -> {self._checkpoint_output_path}")
    
    async def _exec(self, items):
        """
        执行批量处理，支持并发控制 + 增量 checkpoint 保存。

        checkpoint 的保存由 apply_item_result() 决定“每条结果如何写回原数据”，
        写回后按 save_every / min_interval_seconds 规则落盘，避免中断导致丢失已增强结果。
        """
        if not items:
            return []

        total = len(items)
        results: List[Any] = [None] * total
        max_workers = self.max_concurrent or min(200, total)
        max_workers = max(1, int(max_workers))

        index_queue: asyncio.Queue[Optional[int]] = asyncio.Queue()
        done_queue: asyncio.Queue[tuple[int, Any, Any]] = asyncio.Queue()

        for idx in range(total):
            index_queue.put_nowait(idx)
        for _ in range(max_workers):
            index_queue.put_nowait(None)

        async def worker():
            while True:
                idx = await index_queue.get()
                if idx is None:
                    return
                item = items[idx]
                # 并发由 worker 数量控制（避免一次性创建 total 个 task）
                res = await AsyncNode._exec(self, item)
                await done_queue.put((idx, item, res))

        async def aggregator():
            completed = 0
            for _ in range(total):
                idx, item, res = await done_queue.get()
                results[idx] = res
                try:
                    self.apply_item_result(item, res)
                except Exception as e:
                    print(f"[Checkpoint] apply_item_result failed: {e}")

                completed += 1
                if self._checkpoint_enabled and (completed % self._checkpoint_save_every == 0):
                    await self._checkpoint_save_async(completed, total, force=False)

            if self._checkpoint_enabled:
                await self._checkpoint_save_async(completed, total, force=True)

        async with asyncio.TaskGroup() as tg:
            agg_task = tg.create_task(aggregator())
            for _ in range(max_workers):
                tg.create_task(worker())

        # TaskGroup exit indicates aggregator completed successfully (or raised).
        _ = agg_task.result()
        return results
