"""
舆情分析智能体 - 节点定义

根据设计文档, 系统采用中央调度+三阶段顺序依赖架构。
本文件包含所有节点定义, 按以下结构组织:

跨平台路径处理工具:
"""
import os
from pathlib import Path

def normalize_path(path: str) -> str:
    """
    标准化路径为相对于项目根目录的正斜杠路径

    Args:
        path: 输入路径（可能是绝对路径、相对路径或Windows/Unix格式）

    Returns:
        str: 标准化的相对路径, 使用正斜杠
    """
    if not path:
        return path

    # 转换为Path对象以处理路径分隔符
    p = Path(path)

    # 如果是绝对路径, 先转换为相对于当前工作目录的路径
    if p.is_absolute():
        try:
            p = p.relative_to(Path.cwd())
        except ValueError:
            # 如果无法转换为相对路径, 保持原样但使用正斜杠
            return str(p).replace('\\', '/')

    # 转换为字符串并确保使用正斜杠
    normalized = str(p).replace('\\', '/')

    # 确保不以 ./ 开头（除非是当前目录）
    if normalized.startswith('./') and len(normalized) > 2:
        return normalized[2:]

    return normalized

def get_project_relative_path(absolute_path: str) -> str:
    """
    获取相对于项目根目录的路径

    Args:
        absolute_path: 绝对路径

    Returns:
        str: 相对路径, 使用正斜杠
    """
    project_root = Path.cwd()
    try:
        relative_path = Path(absolute_path).relative_to(project_root)
        return str(relative_path).replace('\\', '/')
    except ValueError:
        # 如果路径不在项目根目录下, 返回标准化路径
        return normalize_path(absolute_path)

def ensure_dir_exists(dir_path: str) -> None:
    """
    确保目录存在（跨平台兼容）

    Args:
        dir_path: 目录路径
    """
    Path(dir_path).mkdir(parents=True, exist_ok=True)


# =============================================================================
# 导入依赖
# =============================================================================

import json
import os
import asyncio
import subprocess
import time
import re
from typing import List, Dict, Any, Optional
from pocketflow import Node, BatchNode, AsyncNode, AsyncBatchNode
from utils.call_llm import call_glm_45_air, call_glm4v_plus, call_glm45v_thinking, call_glm46
from utils.data_loader import (
    load_blog_data, load_topics, load_sentiment_attributes, 
    load_publisher_objects, save_enhanced_blog_data, load_enhanced_blog_data,
    load_belief_system, load_publisher_decisions
)


# =============================================================================
# 1. 系统调度节点
# =============================================================================

class DispatcherNode(Node):
    """
    综合调度节点 - 系统入口和中央控制器
    
    功能：
    1. 作为整个系统Flow的入口节点
    2. 根据shared["dispatcher"]配置决定执行哪个阶段
    3. 根据各阶段的config参数决定具体执行路径
    4. 每个阶段完成后返回此节点，决定下一步动作
    
    返回的Action类型：
    - stage1_async: 阶段1异步处理路径
    - stage1_batch_api: 阶段1 Batch API处理路径
    - stage2_workflow: 阶段2固定脚本分析
    - stage2_agent: 阶段2 LLM自主分析
    - stage3_template: 阶段3模板填充
    - stage3_iterative: 阶段3多轮迭代
    - done: 所有阶段完成，跳转到TerminalNode
    """
    
    def prep(self, shared):
        """读取调度配置和当前状态"""
        # 初始化dispatcher配置（如果不存在）
        if "dispatcher" not in shared:
            shared["dispatcher"] = {
                "start_stage": 1,
                "run_stages": [1, 2, 3],
                "current_stage": 0,
                "completed_stages": [],
                "next_action": None
            }
        
        dispatcher = shared["dispatcher"]
        config = shared.get("config", {})
        
        return {
            "start_stage": dispatcher.get("start_stage", 1),
            "run_stages": dispatcher.get("run_stages", [1, 2, 3]),
            "current_stage": dispatcher.get("current_stage", 0),
            "completed_stages": dispatcher.get("completed_stages", []),
            "enhancement_mode": config.get("enhancement_mode", "async"),
            "analysis_mode": config.get("analysis_mode", "workflow"),
            "report_mode": config.get("report_mode", "template")
        }
    
    def exec(self, prep_res):
        """计算下一步动作"""
        start_stage = prep_res["start_stage"]
        run_stages = prep_res["run_stages"]
        current_stage = prep_res["current_stage"]
        completed_stages = prep_res["completed_stages"]
        enhancement_mode = prep_res["enhancement_mode"]
        analysis_mode = prep_res["analysis_mode"]
        report_mode = prep_res["report_mode"]
        
        # 确定下一个需要执行的阶段
        if current_stage == 0:
            # 首次进入，从start_stage开始
            next_stage = start_stage
        else:
            # 找到下一个在run_stages中且未完成的阶段
            next_stage = None
            for stage in run_stages:
                if stage > current_stage and stage not in completed_stages:
                    next_stage = stage
                    break
        
        # 检查是否还有需要执行的阶段
        if next_stage is None or next_stage not in run_stages:
            return {"action": "done", "next_stage": None}
        
        # 根据阶段确定具体路径
        if next_stage == 1:
            action = f"stage1_{enhancement_mode}"
        elif next_stage == 2:
            action = f"stage2_{analysis_mode}"
        elif next_stage == 3:
            action = f"stage3_{report_mode}"
        else:
            action = "done"
        
        return {"action": action, "next_stage": next_stage}
    
    def post(self, shared, prep_res, exec_res):
        """更新调度状态，返回Action"""
        action = exec_res["action"]
        next_stage = exec_res["next_stage"]
        
        # 更新当前阶段
        if next_stage is not None:
            shared["dispatcher"]["current_stage"] = next_stage
        
        shared["dispatcher"]["next_action"] = action
        
        print(f"[Dispatcher] 下一步动作: {action}")
        
        return action


class TerminalNode(Node):
    """
    终止节点 - 宣布流程结束
    
    功能：
    1. 作为整个Flow的终点
    2. 输出执行摘要信息
    3. 清理临时状态（如需要）
    """
    
    def prep(self, shared):
        """读取执行结果摘要"""
        dispatcher = shared.get("dispatcher", {})
        stage1_results = shared.get("stage1_results", {})
        
        return {
            "completed_stages": dispatcher.get("completed_stages", []),
            "statistics": stage1_results.get("statistics", {}),
            "data_save": stage1_results.get("data_save", {})
        }
    
    def exec(self, prep_res):
        """生成执行摘要"""
        completed_stages = prep_res["completed_stages"]
        statistics = prep_res["statistics"]
        data_save = prep_res["data_save"]
        
        summary = {
            "status": "completed",
            "completed_stages": completed_stages,
            "total_blogs_processed": statistics.get("total_blogs", 0),
            "data_saved": data_save.get("saved", False),
            "output_path": data_save.get("output_path", "")
        }
        
        return summary
    
    def post(self, shared, prep_res, exec_res):
        """输出执行摘要，结束流程"""
        print("\n" + "=" * 60)
        print("舆情分析智能体 - 执行完成")
        print("=" * 60)
        print(f"状态: {exec_res['status']}")
        print(f"已完成阶段: {exec_res['completed_stages']}")
        print(f"处理博文数: {exec_res['total_blogs_processed']}")
        if exec_res['data_saved']:
            print(f"数据已保存至: {exec_res['output_path']}")
        print("=" * 60 + "\n")
        
        # 存储最终摘要
        shared["final_summary"] = exec_res
        
        return "default"


# =============================================================================
# 2. 基础节点类
# =============================================================================

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


# =============================================================================
# 3. 阶段1节点: 原始博文增强处理
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1 通用节点
# -----------------------------------------------------------------------------

class DataLoadNode(Node):
    """
    数据加载节点
    
    功能：加载原始博文数据或已增强的数据
    类型：Regular Node
    
    根据 config.data_source.type 配置决定加载方式：
    - "original": 加载原始博文数据及参考数据
    - "enhanced": 加载已增强的博文数据
    """
    
    def prep(self, shared):
        """读取数据文件路径和配置参数"""
        config = shared.get("config", {})
        data_paths = shared.get("data", {}).get("data_paths", {})

        data_source_type = config.get("data_source", {}).get("type", "original")
        enhanced_data_path = config.get("data_source", {}).get("enhanced_data_path", "data/enhanced_blogs.json")

        if data_source_type == "enhanced":
            enhanced_data_path = config.get("data_source", {}).get(
                "enhanced_data_path", "data/enhanced_blogs.json"
            )
            return {
                "load_type": "enhanced",
                "data_path": enhanced_data_path
            }
        else:
            return {
                "load_type": "original",
                "blog_data_path": data_paths.get("blog_data_path", "data/beijing_rainstorm_posts.json"),
                "enhanced_data_path": enhanced_data_path,
                "resume_if_exists": config.get("data_source", {}).get("resume_if_exists", True),
                "topics_path": data_paths.get("topics_path", "data/topics.json"),
                "sentiment_attributes_path": data_paths.get("sentiment_attributes_path", "data/sentiment_attributes.json"),
                "publisher_objects_path": data_paths.get("publisher_objects_path", "data/publisher_objects.json"),
                "belief_system_path": data_paths.get("belief_system_path", "data/believe_system_common.json"),
                "publisher_decision_path": data_paths.get("publisher_decision_path", "data/publisher_decision.json")
            }
    
    def exec(self, prep_res):
        """加载JSON格式数据，验证格式完整性"""
        if prep_res["load_type"] == "enhanced":
            enhanced_data = load_enhanced_blog_data(prep_res["data_path"])
            return {
                "blog_data": enhanced_data,
                "load_type": "enhanced"
            }
        else:
            blog_data = load_blog_data(prep_res["blog_data_path"])

            # 如果存在上次运行的增强输出，则优先加载以便断点续跑（按内容/时间/用户抽样校验一致性）
            enhanced_data_path = prep_res.get("enhanced_data_path")
            if prep_res.get("resume_if_exists", True) and enhanced_data_path and os.path.exists(enhanced_data_path):
                try:
                    enhanced_data = load_enhanced_blog_data(enhanced_data_path)

                    def _same_post(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
                        for k in ("content", "publish_time", "user_id", "username"):
                            if a.get(k) != b.get(k):
                                return False
                        return True

                    if len(enhanced_data) == len(blog_data) and len(blog_data) > 0:
                        sample_indices = {0, len(blog_data) // 2, len(blog_data) - 1}
                        matches = sum(
                            1 for i in sample_indices
                            if _same_post(blog_data[i], enhanced_data[i])
                        )
                        if matches >= max(1, len(sample_indices) - 1):
                            blog_data = enhanced_data
                            print(f"[DataLoad] Resume enabled: loaded existing enhanced data: {enhanced_data_path}")
                        else:
                            print(f"[DataLoad] Resume skipped: enhanced file does not match input dataset: {enhanced_data_path}")
                    else:
                        print(f"[DataLoad] Resume skipped: enhanced file length mismatch: {enhanced_data_path}")
                except Exception as e:
                    print(f"[DataLoad] Resume skipped: failed to load enhanced file: {enhanced_data_path}, err={e}")

            return {
                "blog_data": blog_data,
                "topics_hierarchy": load_topics(prep_res["topics_path"]),
                "sentiment_attributes": load_sentiment_attributes(prep_res["sentiment_attributes_path"]),
                "publisher_objects": load_publisher_objects(prep_res["publisher_objects_path"]),
                "belief_system": load_belief_system(prep_res["belief_system_path"]),
                "publisher_decisions": load_publisher_decisions(prep_res["publisher_decision_path"]),
                "load_type": "original"
            }
    
    def post(self, shared, prep_res, exec_res):
        """将数据存储到shared中"""
        if "data" not in shared:
            shared["data"] = {}

        shared["data"]["blog_data"] = exec_res["blog_data"]
        shared["data"]["load_type"] = exec_res["load_type"]

        if exec_res["load_type"] == "original":
            shared["data"]["topics_hierarchy"] = exec_res["topics_hierarchy"]
            shared["data"]["sentiment_attributes"] = exec_res["sentiment_attributes"]
            shared["data"]["publisher_objects"] = exec_res["publisher_objects"]
            shared["data"]["belief_system"] = exec_res["belief_system"]
            shared["data"]["publisher_decisions"] = exec_res["publisher_decisions"]
            print(f"[DataLoad] belief_system loaded: {len(exec_res['belief_system'])} items")
            print(f"[DataLoad] publisher_decisions loaded: {len(exec_res['publisher_decisions'])} items")

        if "stage1_results" not in shared:
            shared["stage1_results"] = {"statistics": {}}
        # 初始化统计结构
        if "statistics" not in shared["stage1_results"]:
            shared["stage1_results"]["statistics"] = {
                "total_blogs": 0,
                "processed_blogs": 0,
                "empty_fields": {
                    "sentiment_polarity_empty": 0,
                    "sentiment_attribute_empty": 0,
                    "topics_empty": 0,
                    "publisher_empty": 0
                    ,
                    "belief_system_empty": 0,
                    "publisher_decision_empty": 0
                },
                "engagement_statistics": {
                    "total_reposts": 0,
                    "total_comments": 0,
                    "total_likes": 0,
                    "avg_reposts": 0.0,
                    "avg_comments": 0.0,
                    "avg_likes": 0.0
                },
                "user_statistics": {
                    "unique_users": 0,
                    "top_active_users": [],
                    "user_type_distribution": {}
                },
                "content_statistics": {
                    "total_images": 0,
                    "blogs_with_images": 0,
                    "avg_content_length": 0.0,
                    "time_distribution": {}
                },
                "geographic_distribution": {}
            }

        shared["stage1_results"]["statistics"]["total_blogs"] = len(exec_res["blog_data"])

        print(f"[DataLoad] 加载完成，共 {len(exec_res['blog_data'])} 条博文")

        return "default"


class SaveEnhancedDataNode(Node):
    """
    增强数据保存节点
    
    功能：将增强后的博文数据保存到指定文件路径
    类型：Regular Node
    输出：data/enhanced_posts.json（阶段1输出，供阶段2使用）
    """
    
    def prep(self, shared):
        """读取增强后的博文数据和保存路径配置"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        config = shared.get("config", {})
        output_path = config.get("data_source", {}).get(
            "enhanced_data_path", "data/enhanced_blogs.json"
        )
        
        return {
            "blog_data": blog_data,
            "output_path": output_path
        }
    
    def exec(self, prep_res):
        """调用数据保存工具函数，将增强数据写入文件"""
        blog_data = prep_res["blog_data"]
        output_path = prep_res["output_path"]
        
        success = save_enhanced_blog_data(blog_data, output_path)
        
        return {
            "success": success,
            "output_path": output_path,
            "data_count": len(blog_data)
        }
    
    def post(self, shared, prep_res, exec_res):
        """验证保存结果，更新保存状态信息"""
        if "stage1_results" not in shared:
            shared["stage1_results"] = {}
        
        if exec_res["success"]:
            print(f"[SaveData] [OK] 成功保存 {exec_res['data_count']} 条增强数据到: {exec_res['output_path']}")
            shared["stage1_results"]["data_save"] = {
                "saved": True,
                "output_path": exec_res["output_path"],
                "data_count": exec_res["data_count"]
            }
        else:
            print(f"[SaveData] [X] 保存增强数据失败: {exec_res['output_path']}")
            shared["stage1_results"]["data_save"] = {
                "saved": False,
                "output_path": exec_res["output_path"],
                "error": "保存失败"
            }
        
        return "default"


class DataValidationAndOverviewNode(Node):
    """
    数据验证与概况分析节点
    
    功能：验证增强数据的完整性并生成数据统计概况
    类型：Regular Node
    用于阶段1完成后的质量检查
    """
    
    def prep(self, shared):
        """读取增强后的博文数据"""
        return shared.get("data", {}).get("blog_data", [])
    
    def exec(self, prep_res):
        """验证必需字段是否存在，统计留空字段数量，生成数据统计概况"""
        blog_data = prep_res
        
        stats = {
            "total_blogs": len(blog_data),
            "processed_blogs": 0,
            "engagement_statistics": {
                "total_reposts": 0,
                "total_comments": 0,
                "total_likes": 0,
                "avg_reposts": 0,
                "avg_comments": 0,
                "avg_likes": 0
            },
            "user_statistics": {
                "unique_users": set(),
                "top_active_users": [],
                "user_type_distribution": {}
            },
            "content_statistics": {
                "total_images": 0,
                "blogs_with_images": 0,
                "avg_content_length": 0,
                "time_distribution": {}
            },
            "geographic_distribution": {},
            "empty_fields": {
                "sentiment_polarity_empty": 0,
                "sentiment_attribute_empty": 0,
                "topics_empty": 0,
                "publisher_empty": 0,
                "belief_system_empty": 0,
                "publisher_decision_empty": 0
            }
        }
        
        total_content_length = 0
        user_engagement = {}
        
        for blog_post in blog_data:
            # 检查是否已处理
            has_analysis = (
                blog_post.get('sentiment_polarity') is not None or
                blog_post.get('sentiment_attribute') is not None or
                blog_post.get('topics') is not None or
                blog_post.get('publisher') is not None
            )
            if has_analysis:
                stats["processed_blogs"] += 1
            
            # 参与度统计
            repost_count = blog_post.get('repost_count', 0)
            comment_count = blog_post.get('comment_count', 0)
            like_count = blog_post.get('like_count', 0)
            
            stats["engagement_statistics"]["total_reposts"] += repost_count
            stats["engagement_statistics"]["total_comments"] += comment_count
            stats["engagement_statistics"]["total_likes"] += like_count
            
            # 用户统计
            user_id = blog_post.get('user_id', '')
            username = blog_post.get('username', '')
            if user_id:
                stats["user_statistics"]["unique_users"].add(user_id)
                if user_id not in user_engagement:
                    user_engagement[user_id] = {"username": username, "total_engagement": 0}
                user_engagement[user_id]["total_engagement"] += repost_count + comment_count + like_count
            
            # 内容统计
            content = blog_post.get('content', '')
            total_content_length += len(content)
            
            image_urls = blog_post.get('image_urls', [])
            if image_urls:
                stats["content_statistics"]["total_images"] += len(image_urls)
                stats["content_statistics"]["blogs_with_images"] += 1
            
            # 时间分布
            publish_time = blog_post.get('publish_time', '')
            if publish_time:
                try:
                    hour = int(publish_time.split(' ')[1].split(':')[0]) if ' ' in publish_time else 0
                    hour_key = f"{hour:02d}:00"
                    stats["content_statistics"]["time_distribution"][hour_key] = \
                        stats["content_statistics"]["time_distribution"].get(hour_key, 0) + 1
                except:
                    pass
            
            # 地理分布
            location = blog_post.get('location', '')
            if location:
                stats["geographic_distribution"][location] = \
                    stats["geographic_distribution"].get(location, 0) + 1
            
            # 空字段统计
            if blog_post.get('sentiment_polarity') is None:
                stats["empty_fields"]["sentiment_polarity_empty"] += 1
            if blog_post.get('sentiment_attribute') is None:
                stats["empty_fields"]["sentiment_attribute_empty"] += 1
            if blog_post.get('topics') is None:
                stats["empty_fields"]["topics_empty"] += 1
            if blog_post.get('publisher') is None:
                stats["empty_fields"]["publisher_empty"] += 1
            belief_signals = blog_post.get('belief_signals')
            if not belief_signals:
                stats["empty_fields"]["belief_system_empty"] += 1
            if blog_post.get('publisher_decision') is None:
                stats["empty_fields"]["publisher_decision_empty"] += 1
            
            # 发布者类型分布
            publisher = blog_post.get('publisher')
            if publisher:
                stats["user_statistics"]["user_type_distribution"][publisher] = \
                    stats["user_statistics"]["user_type_distribution"].get(publisher, 0) + 1
        
        # 计算平均值
        if stats["total_blogs"] > 0:
            stats["engagement_statistics"]["avg_reposts"] = \
                stats["engagement_statistics"]["total_reposts"] / stats["total_blogs"]
            stats["engagement_statistics"]["avg_comments"] = \
                stats["engagement_statistics"]["total_comments"] / stats["total_blogs"]
            stats["engagement_statistics"]["avg_likes"] = \
                stats["engagement_statistics"]["total_likes"] / stats["total_blogs"]
            stats["content_statistics"]["avg_content_length"] = \
                total_content_length / stats["total_blogs"]
        
        # 转换set为数量
        stats["user_statistics"]["unique_users"] = len(stats["user_statistics"]["unique_users"])
        
        # 活跃用户排行（前10）
        sorted_users = sorted(
            user_engagement.items(), 
            key=lambda x: x[1]["total_engagement"], 
            reverse=True
        )[:10]
        stats["user_statistics"]["top_active_users"] = [
            {"user_id": uid, "username": info["username"], "total_engagement": info["total_engagement"]}
            for uid, info in sorted_users
        ]
        
        return stats
    
    def post(self, shared, prep_res, exec_res):
        """将统计信息存储到shared中，并打印详细统计报告"""
        if "stage1_results" not in shared:
            shared["stage1_results"] = {}
        if "statistics" not in shared["stage1_results"]:
            shared["stage1_results"]["statistics"] = {}
        
        shared["stage1_results"]["statistics"].update(exec_res)
        
        # 打印详细统计报告
        stats = exec_res
        print("\n" + "=" * 60)
        print("阶段1 数据增强统计报告".center(52))
        print("=" * 60)
        
        # 基础统计
        print(f"\n[CHART] 基础统计:")
        print(f"  ├─ 总博文数: {stats.get('total_blogs', 0)}")
        print(f"  └─ 已处理数: {stats.get('processed_blogs', 0)}")
        
        # 空字段统计
        empty_fields = stats.get("empty_fields", {})
        if empty_fields:
            print(f"\n[注意] 增强字段空值统计:")
            print(f"  ├─ 情感极性为空: {empty_fields.get('sentiment_polarity_empty', 0)}")
            print(f"  ├─ 情感属性为空: {empty_fields.get('sentiment_attribute_empty', 0)}")
            print(f"  ├─ 主题为空: {empty_fields.get('topics_empty', 0)}")
            print(f"  ├─ 发布者为空: {empty_fields.get('publisher_empty', 0)}")
            print(f"  ├─ 信念分类为空: {empty_fields.get('belief_system_empty', 0)}")
            print(f"  └─ 事件关联身份为空: {empty_fields.get('publisher_decision_empty', 0)}")
        
        # 参与度统计
        engagement = stats.get("engagement_statistics", {})
        if engagement:
            print(f"\n[CHAT] 参与度统计:")
            print(f"  ├─ 总转发数: {engagement.get('total_reposts', 0)}")
            print(f"  ├─ 总评论数: {engagement.get('total_comments', 0)}")
            print(f"  ├─ 总点赞数: {engagement.get('total_likes', 0)}")
            print(f"  ├─ 平均转发: {engagement.get('avg_reposts', 0):.2f}")
            print(f"  ├─ 平均评论: {engagement.get('avg_comments', 0):.2f}")
            print(f"  └─ 平均点赞: {engagement.get('avg_likes', 0):.2f}")
        
        # 用户统计
        user_stats = stats.get("user_statistics", {})
        if user_stats:
            print(f"\n[USERS] 用户统计:")
            print(f"  ├─ 独立用户数: {user_stats.get('unique_users', 0)}")
            user_type_dist = user_stats.get('user_type_distribution', {})
            if user_type_dist:
                print(f"  └─ 发布者类型分布:")
                for i, (pub_type, count) in enumerate(sorted(user_type_dist.items(), key=lambda x: -x[1])):
                    prefix = "      ├─" if i < len(user_type_dist) - 1 else "      └─"
                    print(f"{prefix} {pub_type}: {count}")
        
        # 内容统计
        content_stats = stats.get("content_statistics", {})
        if content_stats:
            print(f"\n[CONTENT] 内容统计:")
            print(f"  ├─ 含图博文数: {content_stats.get('blogs_with_images', 0)}")
            print(f"  ├─ 总图片数: {content_stats.get('total_images', 0)}")
            print(f"  └─ 平均内容长度: {content_stats.get('avg_content_length', 0):.1f} 字符")
        
        # 地理分布（前5）
        geo_dist = stats.get("geographic_distribution", {})
        if geo_dist:
            print(f"\n[MAP] 地理分布 (Top 5):")
            sorted_geo = sorted(geo_dist.items(), key=lambda x: -x[1])[:5]
            for i, (location, count) in enumerate(sorted_geo):
                prefix = "  ├─" if i < len(sorted_geo) - 1 else "  └─"
                print(f"{prefix} {location}: {count}")
        
        print("\n" + "=" * 60 + "\n")
        
        return "default"


class Stage1CompletionNode(Node):
    """
    阶段1完成节点
    
    功能：
    1. 标记阶段1完成
    2. 更新dispatcher状态
    3. 返回"dispatch" Action，跳转回DispatcherNode
    """
    
    def prep(self, shared):
        """读取当前状态"""
        return {
            "current_stage": shared.get("dispatcher", {}).get("current_stage", 1),
            "completed_stages": shared.get("dispatcher", {}).get("completed_stages", [])
        }
    
    def exec(self, prep_res):
        """确认阶段完成"""
        print(f"\n[Stage1] 阶段1处理完成")
        return {"stage": 1}
    
    def post(self, shared, prep_res, exec_res):
        """更新完成状态，返回dispatch"""
        stage = exec_res["stage"]

        # 确保dispatcher存在
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}

        # 更新已完成阶段列表
        completed_stages = shared["dispatcher"].get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        shared["dispatcher"]["completed_stages"] = completed_stages

        print(f"[Stage1] 已完成阶段: {completed_stages}")

        # 返回dispatch，跳转回调度器
        return "dispatch"


# -----------------------------------------------------------------------------
# 3.2 异步批量并行路径节点 (enhancement_mode="async")
# -----------------------------------------------------------------------------

class AsyncSentimentPolarityAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感极性分析节点
    
    功能：批量分析博文的情感极性（1-5档数字分级）
    类型：AsyncParallelBatchNode
    输出字段：sentiment_polarity (int: 1-5)
    
    情感极性定义：
    - 1: 极度悲观
    - 2: 悲观
    - 3: 中性/无明显极性
    - 4: 乐观
    - 5: 极度乐观
    """
    
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict):
            item["sentiment_polarity"] = result

    async def prep_async(self, shared):
        """返回博文数据列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        self._configure_checkpoint(shared, blog_data)
        return blog_data
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行情感极性分析"""
        blog_post = prep_res
        existing = blog_post.get("sentiment_polarity")
        if existing is not None:
            return existing

        prompt = f"""你是社交媒体分析师，请依据下表判断博文整体情感极性：
- 1=极度悲观，2=悲观，3=中性，4=乐观，5=极度乐观，0=无法判断
- 仅输出一个数字（0-5），不得附加解释或其他字符
博文内容：
{blog_post.get('content', '')}"""

        # 处理图片路径
        image_paths = blog_post.get('image_urls', [])
        image_paths = [img for img in image_paths if img and img.strip()]

        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                full_path = os.path.join("data", img_path)
                processed_image_paths.append(full_path)
            else:
                processed_image_paths.append(img_path)

        # 异步调用LLM
        if processed_image_paths:
            response = await asyncio.to_thread(
                call_glm4v_plus, prompt, image_paths=processed_image_paths, temperature=0.3
            )
        else:
            response = await asyncio.to_thread(
                call_glm_45_air, prompt, temperature=0.3
            )

        # 验证结果
        response = response.strip()
        if not response.isdigit():
            raise ValueError(f"模型输出不是数字: {response}")

        score = int(response)
        if not 1 <= score <= 5:
            raise ValueError(f"模型输出数字不在1-5范围内: {score}")

        return score
    
    async def exec_fallback_async(self, prep_res, exc):
        """分析失败时返回中性评分"""
        print(f"情感极性分析失败，使用默认值: {str(exc)}")
        return 3
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到博文对象"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        if len(exec_res) != len(blog_data):
            print("警告：情感极性分析结果数量与博文数量不匹配")
            return "default"
        
        for i, blog_post in enumerate(blog_data):
            blog_post['sentiment_polarity'] = exec_res[i] if i < len(exec_res) else None
        
        print(f"[AsyncSentimentPolarity] 完成 {len(exec_res)} 条博文的情感极性分析")
        
        return "default"


class AsyncSentimentAttributeAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感属性分析节点
    
    功能：批量分析博文的具体情感状态
    类型：AsyncParallelBatchNode
    输出字段：sentiment_attribute (List[str])
    
    从预定义情感属性列表中选择1-3个最贴切的属性
    """
    
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["sentiment_attribute"] = result

    async def prep_async(self, shared):
        """返回博文和情感属性的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        sentiment_attributes = shared.get("data", {}).get("sentiment_attributes", [])

        self._configure_checkpoint(shared, blog_data)
        
        return [{
            "blog_data": blog_post,
            "sentiment_attributes": sentiment_attributes
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用LLM进行情感属性分析"""
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

        # 验证并过滤有效属性
        return [attr for attr in attributes if attr in sentiment_attributes]
    
    async def exec_fallback_async(self, prep_res, exc):
        """分析失败时返回中立属性"""
        print(f"情感属性分析失败，使用默认值: {str(exc)}")
        return ["中立"]
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到博文对象"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        if len(exec_res) != len(blog_data):
            print("警告：情感属性分析结果数量与博文数量不匹配")
            return "default"
        
        for i, blog_post in enumerate(blog_data):
            blog_post['sentiment_attribute'] = exec_res[i] if i < len(exec_res) else None
        
        print(f"[AsyncSentimentAttribute] 完成 {len(exec_res)} 条博文的情感属性分析")
        
        return "default"


class AsyncTwoLevelTopicAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步两级主题分析节点
    
    功能：批量从预定义主题列表中选择合适主题
    类型：AsyncParallelBatchNode
    输出字段：topics (List[Dict])
    
    从预定义的两层嵌套主题列表中选择1-2个父/子主题组合
    """
    
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["topics"] = result

    async def prep_async(self, shared):
        """返回博文和主题层次结构的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        topics_hierarchy = shared.get("data", {}).get("topics_hierarchy", [])

        self._configure_checkpoint(shared, blog_data)
        
        return [{
            "blog_data": blog_post,
            "topics_hierarchy": topics_hierarchy
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行主题匹配"""
        blog_post = prep_res["blog_data"]
        topics_hierarchy = prep_res["topics_hierarchy"]
        existing = blog_post.get("topics")
        if existing is not None:
            return existing

        # 构建主题层次结构字符串
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

        # 处理图片路径
        image_paths = blog_post.get('image_urls', [])
        image_paths = [img for img in image_paths if img and img.strip()]

        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                processed_image_paths.append(os.path.join("data", img_path))
            else:
                processed_image_paths.append(img_path)

        # 异步调用LLM
        if processed_image_paths:
            response = await asyncio.to_thread(
                call_glm4v_plus, prompt, image_paths=processed_image_paths, temperature=0.3
            )
        else:
            response = await asyncio.to_thread(
                call_glm_45_air, prompt, temperature=0.3
            )

        topics = json.loads(response.strip())
        if not isinstance(topics, list):
            raise ValueError(f"模型输出不是列表格式: {topics}")

        # 验证并过滤有效主题
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
        """分析失败时返回空列表"""
        print(f"主题分析失败，使用默认值: {str(exc)}")
        return []
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到博文对象"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        if len(exec_res) != len(blog_data):
            print("警告：主题分析结果数量与博文数量不匹配")
            return "default"
        
        for i, blog_post in enumerate(blog_data):
            blog_post['topics'] = exec_res[i] if i < len(exec_res) else None
        
        print(f"[AsyncTopic] 完成 {len(exec_res)} 条博文的主题分析")
        
        return "default"


class AsyncPublisherObjectAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步发布者对象分析节点
    
    功能：批量识别发布者类型
    类型：AsyncParallelBatchNode
    输出字段：publisher (str)
    
    从预定义发布者类型列表中选择一个最匹配的类型：
    政府机构、官方新闻媒体、自媒体、企业账号、个人用户等
    """
    
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["publisher"] = result

    async def prep_async(self, shared):
        """返回博文和发布者类型的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_objects = shared.get("data", {}).get("publisher_objects", [])

        self._configure_checkpoint(shared, blog_data)
        
        return [{
            "blog_data": blog_post,
            "publisher_objects": publisher_objects
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用LLM进行发布者类型识别"""
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
        else:
            return "个人用户" if "个人用户" in publisher_objects else None
    
    async def exec_fallback_async(self, prep_res, exc):
        """分析失败时返回个人用户"""
        print(f"发布者分析失败，使用默认值: {str(exc)}")
        return "个人用户"
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到博文对象"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        if len(exec_res) != len(blog_data):
            print("警告：发布者分析结果数量与博文数量不匹配")
            return "default"
        
        for i, blog_post in enumerate(blog_data):
            blog_post['publisher'] = exec_res[i] if i < len(exec_res) else None
        
        print(f"[AsyncPublisher] 完成 {len(exec_res)} 条博文的发布者分析")
        
        return "default"


class AsyncBeliefSystemAnalysisBatchNode(AsyncParallelBatchNode):
    """
    信念体系分类识别（多选）
    """
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["belief_signals"] = result if result is not None else []

    async def prep_async(self, shared):
        """返回博文和信念系统的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        belief_system = shared.get("data", {}).get("belief_system", [])
        self._configure_checkpoint(shared, blog_data)
        if not belief_system:
            print(f"[BeliefSystem] 警告: belief_system 数据为空，将跳过LLM调用")
        else:
            print(f"[BeliefSystem] 准备处理 {len(blog_data)} 条博文，信念系统包含 {len(belief_system)} 个类别")
        return [{
            "blog_data": blog_post,
            "belief_system": belief_system
        } for blog_post in blog_data]

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
                # 修正常见乱码（utf-8 内容被当 latin1 读取）
                return text.encode("latin1").decode("utf-8")
            except Exception:
                return text

        for item in belief_system_raw or []:
            belief_system.append({
                "category": _clean(item.get("category", "")),
                "subcategories": [_clean(sub) for sub in item.get("subcategories", [])]
            })
        if not belief_system:
            print(f"[BeliefSystem] 警告: 处理博文时 belief_system 为空，返回空结果")
            return []

        # 构建信念体系层次字符串，类似两级主题提示
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

        # 调用LLM
        response = await asyncio.to_thread(
            call_glm_45_air, prompt, temperature=0.2
        )

        # 解析与校验
        candidate = response.strip()
        fence_match = re.search(r"```(?:json)?\s*(.*?)\s*```", candidate, re.S)
        if fence_match:
            candidate = fence_match.group(1).strip()
        else:
            start = candidate.find('[')
            end = candidate.rfind(']')
            if start != -1 and end != -1 and end > start:
                candidate = candidate[start:end + 1].strip()

        try:
            parsed = json.loads(candidate)
        except Exception:
            # 再尝试提取首个 JSON 数组
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

        # 过滤合法的分类/子类
        valid_results = []
        for item in parsed:
            cat = item.get("category", "")
            subs = item.get("subcategories", []) or []
            # 找到对应的定义
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
        # 容错：长度不匹配时填充空列表
        if len(exec_res) < len(blog_data):
            exec_res = list(exec_res) + [[] for _ in range(len(blog_data) - len(exec_res))]
        elif len(exec_res) > len(blog_data):
            exec_res = exec_res[:len(blog_data)]

        for i, blog_post in enumerate(blog_data):
            blog_post["belief_signals"] = exec_res[i] if exec_res[i] is not None else []

        print(f"[BeliefSystem] 完成 {len(blog_data)} 条博文的信念分类增强")
        return "default"


class AsyncPublisherDecisionAnalysisBatchNode(AsyncParallelBatchNode):
    """
    发布者事件关联身份分类（四选一）
    """
    def apply_item_result(self, item: Any, result: Any) -> None:
        if isinstance(item, dict) and isinstance(item.get("blog_data"), dict):
            item["blog_data"]["publisher_decision"] = result

    async def prep_async(self, shared):
        """返回博文和关联身份分类的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_decisions = shared.get("data", {}).get("publisher_decisions", [])
        self._configure_checkpoint(shared, blog_data)
        if not publisher_decisions:
            print(f"[PublisherDecision] 警告: publisher_decisions 数据为空，将跳过LLM调用")
        else:
            print(f"[PublisherDecision] 准备处理 {len(blog_data)} 条博文，关联身份分类包含 {len(publisher_decisions)} 个类别")
        return [{
            "blog_data": blog_post,
            "publisher_decisions": publisher_decisions
        } for blog_post in blog_data]

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
        publisher_decisions = [{"category": _clean(item.get("category", ""))} for item in (publisher_decisions_raw or [])]
        if not publisher_decisions:
            print(f"[PublisherDecision] 警告: 处理博文时 publisher_decisions 为空，返回None")
            return None

        username = blog_post.get("username", "") or blog_post.get("user_name", "")
        extra_user = f"\n发布者昵称/账号：{username}" if username else ""

        prompt = f"""请选择博文发布者与舆情事件“河南大学生夜骑事件”的“事件关联身份”，只能从下列候选中选1个：
{json.dumps(publisher_decisions, ensure_ascii=False, indent=2)}
若无法判断，选择最接近的类型。只输出候选中的category文本，不要输出解释。
博文内容：{blog_post.get('content', '')}{extra_user}"""
        try:
            response = await asyncio.to_thread(call_glm_45_air, prompt, temperature=0.2)
            chosen = response.strip().replace('"', '')
            # 验证是否在候选中
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
        candidates = [item.get("category") for item in prep_res.get("publisher_decisions", []) if item.get("category")]
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

        candidates = [_clean(item.get("category")) for item in shared.get("data", {}).get("publisher_decisions", []) if item.get("category")]
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


# -----------------------------------------------------------------------------
# 3.3 Batch API路径节点 (enhancement_mode="batch_api")
# -----------------------------------------------------------------------------

class BatchAPIEnhancementNode(Node):
    """
    Batch API增强处理节点
    
    功能：调用batch/目录下的脚本进行批量处理
    类型：Regular Node
    
    处理流程：
    1. 调用 batch/batch_run.py 脚本执行完整的Batch API流程
    2. 等待处理完成
    3. 加载处理结果到shared中
    
    Batch API流程包括：
    - generate_jsonl.py: 生成批量请求文件
    - upload_and_start.py: 上传并启动任务
    - download_results.py: 下载结果
    - parse_and_integrate.py: 解析并整合结果
    """
    
    def prep(self, shared):
        """读取配置参数"""
        config = shared.get("config", {})
        batch_config = config.get("batch_api_config", {})
        
        return {
            "batch_script_path": batch_config.get("script_path", "batch/batch_run.py"),
            "input_data_path": batch_config.get("input_path", "data/beijing_rainstorm_posts.json"),
            "output_data_path": batch_config.get("output_path", "data/enhanced_blogs.json"),
            "wait_for_completion": batch_config.get("wait_for_completion", True)
        }
    
    def exec(self, prep_res):
        """执行Batch API处理脚本"""
        batch_script_path = prep_res["batch_script_path"]
        
        print(f"\n[BatchAPI] 开始执行Batch API处理...")
        print(f"[BatchAPI] 脚本路径: {batch_script_path}")
        
        # 检查脚本是否存在
        if not os.path.exists(batch_script_path):
            return {
                "success": False,
                "error": f"Batch脚本不存在: {batch_script_path}",
                "output_path": prep_res["output_data_path"]
            }
        
        try:
            # 执行batch_run.py脚本
            result = subprocess.run(
                ["python", batch_script_path],
                capture_output=True,
                text=True,
                cwd=os.getcwd()
            )
            
            if result.returncode == 0:
                print(f"[BatchAPI] 脚本执行成功")
                print(result.stdout)
                return {
                    "success": True,
                    "output_path": prep_res["output_data_path"],
                    "stdout": result.stdout
                }
            else:
                print(f"[BatchAPI] 脚本执行失败")
                print(f"错误输出: {result.stderr}")
                return {
                    "success": False,
                    "error": result.stderr,
                    "output_path": prep_res["output_data_path"]
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "output_path": prep_res["output_data_path"]
            }
    
    def post(self, shared, prep_res, exec_res):
        """加载处理结果，更新shared"""
        if exec_res["success"]:
            # 尝试加载处理后的数据
            output_path = exec_res["output_path"]
            
            if os.path.exists(output_path):
                try:
                    enhanced_data = load_enhanced_blog_data(output_path)
                    shared["data"]["blog_data"] = enhanced_data
                    
                    print(f"[BatchAPI] [OK] 成功加载 {len(enhanced_data)} 条增强数据")
                    
                    if "stage1_results" not in shared:
                        shared["stage1_results"] = {}
                    shared["stage1_results"]["batch_api"] = {
                        "success": True,
                        "data_count": len(enhanced_data)
                    }
                except Exception as e:
                    print(f"[BatchAPI] [X] 加载增强数据失败: {str(e)}")
                    shared["stage1_results"]["batch_api"] = {
                        "success": False,
                        "error": str(e)
                    }
            else:
                print(f"[BatchAPI] [X] 输出文件不存在: {output_path}")
                shared["stage1_results"]["batch_api"] = {
                    "success": False,
                    "error": f"输出文件不存在: {output_path}"
                }
        else:
            print(f"[BatchAPI] [X] Batch API处理失败: {exec_res.get('error', 'Unknown error')}")
            if "stage1_results" not in shared:
                shared["stage1_results"] = {}
            shared["stage1_results"]["batch_api"] = {
                "success": False,
                "error": exec_res.get("error", "Unknown error")
            }
        
        return "default"


# =============================================================================
# 4. 阶段2节点: 分析执行
# =============================================================================

# -----------------------------------------------------------------------------
# 4.1 通用节点
# -----------------------------------------------------------------------------

class LoadEnhancedDataNode(Node):
    """
    加载增强数据节点
    
    功能：加载已完成增强处理的博文数据
    类型：Regular Node
    前置检查：验证阶段1输出文件是否存在
    """
    
    def prep(self, shared):
        """读取增强数据文件路径，检查前置条件"""
        config = shared.get("config", {})
        enhanced_data_path = config.get("data_source", {}).get(
            "enhanced_data_path", "data/enhanced_blogs.json"
        )
        
        # 检查文件是否存在
        if not os.path.exists(enhanced_data_path):
            raise FileNotFoundError(
                f"阶段1输出文件不存在: {enhanced_data_path}\n"
                f"请先运行阶段1（增强处理）或确保文件路径正确"
            )
        
        return {"data_path": enhanced_data_path}
    
    def exec(self, prep_res):
        """加载JSON数据，验证增强字段完整性"""
        data_path = prep_res["data_path"]
        
        print(f"\n[LoadEnhancedData] 加载增强数据: {data_path}")
        blog_data = load_enhanced_blog_data(data_path)
        
        # 验证增强字段
        enhanced_fields = ["sentiment_polarity", "sentiment_attribute", "topics", "publisher"]
        valid_count = 0
        for post in blog_data:
            has_all_fields = all(post.get(field) is not None for field in enhanced_fields)
            if has_all_fields:
                valid_count += 1
        
        return {
            "blog_data": blog_data,
            "total_count": len(blog_data),
            "valid_count": valid_count,
            "enhancement_rate": round(valid_count / len(blog_data) * 100, 2) if blog_data else 0
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储数据到shared"""
        if "data" not in shared:
            shared["data"] = {}
        
        shared["data"]["blog_data"] = exec_res["blog_data"]
        
        print(f"[LoadEnhancedData] [√] 加载 {exec_res['total_count']} 条博文")
        print(f"[LoadEnhancedData] [√] 完整增强率: {exec_res['enhancement_rate']}%")
        
        return "default"


class DataSummaryNode(Node):
    """
    数据概况生成节点
    
    功能：生成增强数据的统计概况（供Agent决策参考）
    类型：Regular Node
    """
    
    def prep(self, shared):
        """读取增强数据"""
        return shared.get("data", {}).get("blog_data", [])
    
    def exec(self, prep_res):
        """计算各维度分布、时间跨度、总量等统计信息"""
        blog_data = prep_res
        
        if not blog_data:
            return {"summary": "无数据", "statistics": {}}
        
        from collections import Counter
        from datetime import datetime
        
        # 基础统计
        total = len(blog_data)
        
        # 情感分布
        sentiment_dist = Counter(p.get("sentiment_polarity") for p in blog_data if p.get("sentiment_polarity"))
        
        # 发布者分布
        publisher_dist = Counter(p.get("publisher") for p in blog_data if p.get("publisher"))
        
        # 主题分布
        parent_topics = Counter()
        for p in blog_data:
            topics = p.get("topics") or []
            if not isinstance(topics, list):
                continue
            for t in topics:
                if isinstance(t, dict) and t.get("parent_topic"):
                    parent_topics[t["parent_topic"]] += 1
        
        # 地理分布
        location_dist = Counter(p.get("location") for p in blog_data if p.get("location"))
        
        # 时间范围
        publish_times = []
        for p in blog_data:
            pt = p.get("publish_time")
            if pt:
                try:
                    publish_times.append(datetime.strptime(pt, "%Y-%m-%d %H:%M:%S"))
                except:
                    pass
        
        time_range = None
        if publish_times:
            time_range = {
                "start": min(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "end": max(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "span_hours": round((max(publish_times) - min(publish_times)).total_seconds() / 3600, 1)
            }
        
        # 互动统计
        total_reposts = sum(p.get("repost_count", 0) for p in blog_data)
        total_comments = sum(p.get("comment_count", 0) for p in blog_data)
        total_likes = sum(p.get("like_count", 0) for p in blog_data)
        
        summary_text = f"""数据概况:
- 总博文数: {total}
- 时间范围: {time_range['start'] if time_range else '未知'} 至 {time_range['end'] if time_range else '未知'}
- 情感分布: {dict(sentiment_dist.most_common(5))}
- 热门主题Top3: {[t[0] for t in parent_topics.most_common(3)]}
- 主要地区Top3: {[l[0] for l in location_dist.most_common(3)]}
- 发布者类型: {list(publisher_dist.keys())}
- 总互动量: 转发{total_reposts}, 评论{total_comments}, 点赞{total_likes}"""
        
        return {
            "summary": summary_text,
            "statistics": {
                "total_posts": total,
                "time_range": time_range,
                "sentiment_distribution": dict(sentiment_dist),
                "publisher_distribution": dict(publisher_dist),
                "topic_distribution": dict(parent_topics.most_common(10)),
                "location_distribution": dict(location_dist.most_common(10)),
                "engagement": {
                    "total_reposts": total_reposts,
                    "total_comments": total_comments,
                    "total_likes": total_likes
                }
            }
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储统计信息"""
        if "agent" not in shared:
            shared["agent"] = {}
        
        shared["agent"]["data_summary"] = exec_res["summary"]
        shared["agent"]["data_statistics"] = exec_res["statistics"]
        
        print(f"\n[DataSummary] 数据概况已生成")
        print(exec_res["summary"])
        
        return "default"


class SaveAnalysisResultsNode(Node):
    """
    保存分析结果节点

    功能：将分析结果持久化，供阶段3使用
    类型：Regular Node
    输出位置：
    - 统计数据：report/analysis_data.json
    - 图表分析：report/chart_analyses.json
    - 洞察描述：report/insights.json
    - 图表文件：report/images/
    """

    def prep(self, shared):
        """读取分析输出、图表列表和图表分析结果"""
        stage2_results = shared.get("stage2_results", {})

        return {
            "charts": stage2_results.get("charts", []),
            "tables": stage2_results.get("tables", []),
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "insights": stage2_results.get("insights", {}),
            "execution_log": stage2_results.get("execution_log", {})
        }
    
    def exec(self, prep_res):
        """保存JSON结果文件"""
        output_dir = "report"
        os.makedirs(output_dir, exist_ok=True)

        # 保存分析数据
        analysis_data = {
            "charts": prep_res["charts"],
            "tables": prep_res["tables"],
            "execution_log": prep_res["execution_log"]
        }

        analysis_data_path = os.path.join(output_dir, "analysis_data.json")
        with open(analysis_data_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        # 保存图表分析结果（新增）
        chart_analyses_path = os.path.join(output_dir, "chart_analyses.json")
        with open(chart_analyses_path, 'w', encoding='utf-8') as f:
            json.dump(prep_res["chart_analyses"], f, ensure_ascii=False, indent=2)

        # 保存洞察描述
        insights_path = os.path.join(output_dir, "insights.json")
        with open(insights_path, 'w', encoding='utf-8') as f:
            json.dump(prep_res["insights"], f, ensure_ascii=False, indent=2)

        return {
            "success": True,
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "charts_count": len(prep_res["charts"]),
            "tables_count": len(prep_res["tables"]),
            "chart_analyses_count": len(prep_res["chart_analyses"])
        }
    
    def post(self, shared, prep_res, exec_res):
        """记录保存状态"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        shared["stage2_results"]["output_files"] = {
            "charts_dir": "report/images/",
            "analysis_data": exec_res["analysis_data_path"],
            "chart_analyses_file": exec_res["chart_analyses_path"],
            "insights_file": exec_res["insights_path"]
        }

        print(f"\n[SaveAnalysisResults] [OK] 分析结果已保存")
        print(f"  - 分析数据: {exec_res['analysis_data_path']}")
        print(f"  - 图表分析: {exec_res['chart_analyses_path']}")
        print(f"  - 洞察描述: {exec_res['insights_path']}")
        print(f"  - 生成图表: {exec_res['charts_count']} 个")
        print(f"  - 分析图表: {exec_res['chart_analyses_count']} 个")
        print(f"  - 生成表格: {exec_res['tables_count']} 个")

        return "default"


class Stage2CompletionNode(Node):
    """
    阶段2完成节点
    
    功能：
    1. 标记阶段2完成
    2. 更新dispatcher状态
    3. 返回"dispatch" Action，跳转回DispatcherNode
    """
    
    def prep(self, shared):
        """读取当前状态"""
        return {
            "current_stage": shared.get("dispatcher", {}).get("current_stage", 2),
            "completed_stages": shared.get("dispatcher", {}).get("completed_stages", [])
        }
    
    def exec(self, prep_res):
        """确认阶段完成"""
        print(f"\n[Stage2] 阶段2分析执行完成")
        return {"stage": 2}
    
    def post(self, shared, prep_res, exec_res):
        """更新完成状态，返回dispatch"""
        stage = exec_res["stage"]
        
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}
        
        completed_stages = shared["dispatcher"].get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        shared["dispatcher"]["completed_stages"] = completed_stages
        
        print(f"[Stage2] 已完成阶段: {completed_stages}")
        
        return "dispatch"


# -----------------------------------------------------------------------------
# 4.2 预定义Workflow路径节点 (analysis_mode="workflow")
# -----------------------------------------------------------------------------

class ExecuteAnalysisScriptNode(Node):
    """
    执行分析脚本节点
    
    功能：执行固定的分析脚本，生成全部所需图形
    类型：Regular Node
    
    执行四类工具集的全部工具函数：
    - 情感趋势分析工具集
    - 主题演化分析工具集
    - 地理分布分析工具集
    - 多维交互分析工具集
    """
    
    def prep(self, shared):
        """读取增强数据"""
        return shared.get("data", {}).get("blog_data", [])
    
    def exec(self, prep_res):
        """执行预定义的分析脚本"""
        from utils.analysis_tools import (
            # 情感工具
            sentiment_distribution_stats,
            sentiment_time_series,
            sentiment_anomaly_detection,
            sentiment_trend_chart,
            sentiment_pie_chart,
            sentiment_bucket_trend_chart,
            sentiment_attribute_trend_chart,
            sentiment_focus_window_chart,
            sentiment_focus_publisher_chart,
            # 主题工具
            topic_frequency_stats,
            topic_time_evolution,
            topic_cooccurrence_analysis,
            topic_ranking_chart,
            topic_evolution_chart,
            topic_network_chart,
            topic_focus_evolution_chart,
            topic_keyword_trend_chart,
            topic_focus_distribution_chart,
            # 地理工具
            geographic_distribution_stats,
            geographic_hotspot_detection,
            geographic_sentiment_analysis,
            geographic_heatmap,
            geographic_bar_chart,
            geographic_sentiment_bar_chart,
            geographic_topic_heatmap,
            geographic_temporal_heatmap,
            # 交互工具
            publisher_distribution_stats,
            cross_dimension_matrix,
            influence_analysis,
            correlation_analysis,
            interaction_heatmap,
            publisher_bar_chart,
            publisher_sentiment_bucket_chart,
            publisher_topic_distribution_chart,
            participant_trend_chart,
            publisher_focus_distribution_chart,
            belief_network_chart,
        )
        import time
        
        blog_data = prep_res
        start_time = time.time()
        
        charts = []
        tables = []
        tools_executed = []
        
        print("\n[ExecuteAnalysisScript] 开始执行预定义分析脚本...")
        
        # === 1. 情感趋势分析 ===
        print("\n  [CHART] 执行情感趋势分析...")
        
        # 情感分布统计
        result = sentiment_distribution_stats(blog_data)
        tables.append({
            "id": "sentiment_distribution",
            "title": "情感极性分布统计",
            "data": result["data"],
            "source_tool": "sentiment_distribution_stats"
        })
        tools_executed.append("sentiment_distribution_stats")
        
        # 情感时序分析
        sentiment_ts_result = sentiment_time_series(blog_data, granularity="hour")
        tables.append({
            "id": "sentiment_time_series",
            "title": "情感时序趋势数据",
            "data": sentiment_ts_result["data"],
            "source_tool": "sentiment_time_series"
        })
        tools_executed.append("sentiment_time_series")

        tables.append({
            "id": "sentiment_peaks",
            "title": "情感峰值与拐点",
            "data": {
                "peak_periods": sentiment_ts_result["data"].get("peak_periods", []),
                "peak_hours": sentiment_ts_result["data"].get("peak_hours", []),
                "turning_points": sentiment_ts_result["data"].get("turning_points", []),
                "volume_spikes": sentiment_ts_result["data"].get("volume_spikes", [])
            },
            "source_tool": "sentiment_time_series"
        })
        
        # 情感异常检测
        result = sentiment_anomaly_detection(blog_data)
        tables.append({
            "id": "sentiment_anomaly",
            "title": "情感异常点",
            "data": result["data"],
            "source_tool": "sentiment_anomaly_detection"
        })
        tools_executed.append("sentiment_anomaly_detection")
        
        # 情感趋势图
        result = sentiment_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_trend_chart")

        # 情感桶趋势
        result = sentiment_bucket_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_bucket_trend_chart")

        # 情感属性趋势
        result = sentiment_attribute_trend_chart(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_attribute_trend_chart")

        # 焦点窗口情感趋势（窗口内极性均值 + 三分类）
        result = sentiment_focus_window_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "sentiment_focus_window_data",
                "title": "焦点窗口情感数据",
                "data": result.get("data", {}),
                "source_tool": "sentiment_focus_window_chart"
            })
        tools_executed.append("sentiment_focus_window_chart")

        # 焦点窗口发布者情感趋势
        result = sentiment_focus_publisher_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "sentiment_focus_publisher_data",
                "title": "焦点窗口发布者情感均值",
                "data": result.get("data", {}),
                "source_tool": "sentiment_focus_publisher_chart"
            })
        tools_executed.append("sentiment_focus_publisher_chart")
        
        # 情感饼图
        result = sentiment_pie_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("sentiment_pie_chart")
        
        # === 2. 主题演化分析 ===
        print("  [CHART] 执行主题演化分析...")
        
        # 主题频次统计
        result = topic_frequency_stats(blog_data)
        tables.append({
            "id": "topic_frequency",
            "title": "主题频次统计",
            "data": result["data"],
            "source_tool": "topic_frequency_stats"
        })
        tools_executed.append("topic_frequency_stats")
        
        # 主题演化分析
        result = topic_time_evolution(blog_data, granularity="day", top_n=5)
        tables.append({
            "id": "topic_evolution",
            "title": "主题演化数据",
            "data": result["data"],
            "source_tool": "topic_time_evolution"
        })
        tools_executed.append("topic_time_evolution")
        
        # 主题共现分析
        result = topic_cooccurrence_analysis(blog_data)
        tables.append({
            "id": "topic_cooccurrence",
            "title": "主题共现关系",
            "data": result["data"],
            "source_tool": "topic_cooccurrence_analysis"
        })
        tools_executed.append("topic_cooccurrence_analysis")
        
        # 主题排行图
        result = topic_ranking_chart(blog_data, top_n=10)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_ranking_chart")
        
        # 主题演化图
        result = topic_evolution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_evolution_chart")

        # 主题焦点演化
        result = topic_focus_evolution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_focus_evolution_chart")

        # 焦点窗口主题发布趋势（独立窗口数据）
        result = topic_focus_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "topic_focus_distribution_data",
                "title": "焦点窗口主题发布趋势数据",
                "data": result.get("data", {}),
                "source_tool": "topic_focus_distribution_chart"
            })
        tools_executed.append("topic_focus_distribution_chart")

        # 焦点关键词趋势
        result = topic_keyword_trend_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_keyword_trend_chart")
        
        # 主题网络图
        result = topic_network_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("topic_network_chart")
        
        # === 3. 地理分布分析 ===
        print("  [CHART] 执行地理分布分析...")
        
        # 地理分布统计
        result = geographic_distribution_stats(blog_data)
        tables.append({
            "id": "geographic_distribution",
            "title": "地理分布统计",
            "data": result["data"],
            "source_tool": "geographic_distribution_stats"
        })
        tools_executed.append("geographic_distribution_stats")
        
        # 热点区域识别
        result = geographic_hotspot_detection(blog_data)
        tables.append({
            "id": "geographic_hotspot",
            "title": "热点区域",
            "data": result["data"],
            "source_tool": "geographic_hotspot_detection"
        })
        tools_executed.append("geographic_hotspot_detection")
        
        # 地区情感分析
        result = geographic_sentiment_analysis(blog_data)
        tables.append({
            "id": "geographic_sentiment",
            "title": "地区情感分析",
            "data": result["data"],
            "source_tool": "geographic_sentiment_analysis"
        })
        tools_executed.append("geographic_sentiment_analysis")
        
        # 地理热力图
        result = geographic_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_heatmap")
        
        # 地区分布图
        result = geographic_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_bar_chart")

        # 地区正负面对比
        result = geographic_sentiment_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_sentiment_bar_chart")

        # 地区 × 主题热力图
        result = geographic_topic_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_topic_heatmap")

        # 地区 × 时间热力图（天粒度）
        result = geographic_temporal_heatmap(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("geographic_temporal_heatmap")
        
        # === 4. 多维交互分析 ===
        print("  [CHART] 执行多维交互分析...")
        
        # 发布者分布统计
        result = publisher_distribution_stats(blog_data)
        tables.append({
            "id": "publisher_distribution",
            "title": "发布者分布统计",
            "data": result["data"],
            "source_tool": "publisher_distribution_stats"
        })
        tools_executed.append("publisher_distribution_stats")
        
        # 交叉矩阵分析
        result = cross_dimension_matrix(blog_data, dim1="publisher", dim2="sentiment_polarity")
        tables.append({
            "id": "cross_dimension_matrix",
            "title": "发布者×情感交叉矩阵",
            "data": result["data"],
            "source_tool": "cross_dimension_matrix"
        })
        tools_executed.append("cross_dimension_matrix")
        
        # 影响力分析
        result = influence_analysis(blog_data, top_n=20)
        tables.append({
            "id": "influence_analysis",
            "title": "影响力分析",
            "data": result["data"],
            "source_tool": "influence_analysis"
        })
        tools_executed.append("influence_analysis")
        
        # 相关性分析
        result = correlation_analysis(blog_data)
        tables.append({
            "id": "correlation_analysis",
            "title": "维度相关性分析",
            "data": result["data"],
            "source_tool": "correlation_analysis"
        })
        tools_executed.append("correlation_analysis")
        
        # 交互热力图
        result = interaction_heatmap(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("interaction_heatmap")
        
        # 发布者分布图
        result = publisher_bar_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_bar_chart")

        # 发布者情绪桶对比
        result = publisher_sentiment_bucket_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_sentiment_bucket_chart")

        # 发布者话题偏好
        result = publisher_topic_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("publisher_topic_distribution_chart")

        # 参与人数趋势
        result = participant_trend_chart(blog_data, granularity="day")
        if result.get("charts"):
            charts.extend(result["charts"])
        tools_executed.append("participant_trend_chart")

        # 焦点窗口发布者类型发布趋势（独立窗口数据）
        result = publisher_focus_distribution_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "publisher_focus_distribution_data",
                "title": "焦点窗口发布者类型发布趋势数据",
                "data": result.get("data", {}),
                "source_tool": "publisher_focus_distribution_chart"
            })
        tools_executed.append("publisher_focus_distribution_chart")

        # 信念系统网络
        result = belief_network_chart(blog_data)
        if result.get("charts"):
            charts.extend(result["charts"])
            tables.append({
                "id": "belief_network_data",
                "title": "信念系统共现网络数据",
                "data": result.get("data", {}),
                "source_tool": "belief_network_chart"
            })
        tools_executed.append("belief_network_chart")

        # 确保已注册工具都被调用（避免遗漏新工具）
        try:
            from utils.analysis_tools.tool_registry import TOOL_REGISTRY
            executed_set = set(tools_executed)
            for tool_name, tool_def in TOOL_REGISTRY.items():
                if tool_name in executed_set:
                    continue
                params = {}
                for param_name, spec in (tool_def.get("parameters") or {}).items():
                    if param_name == "blog_data":
                        params[param_name] = blog_data
                    elif "default" in spec:
                        params[param_name] = spec["default"]
                result = tool_def["function"](**params)
                tools_executed.append(tool_name)
                executed_set.add(tool_name)
                if isinstance(result, dict) and result.get("charts"):
                    charts.extend(result["charts"])
                elif isinstance(result, dict) and "data" in result:
                    tables.append({
                        "id": tool_name,
                        "title": tool_def.get("description", tool_name),
                        "data": result["data"],
                        "source_tool": tool_name
                    })
        except Exception as e:
            print(f"[ExecuteAnalysisScript] [!] 自动补齐工具失败: {e}")

        execution_time = time.time() - start_time
        
        print(f"\n[ExecuteAnalysisScript] [OK] 分析脚本执行完成")
        print(f"  - 执行工具: {len(tools_executed)} 个")
        print(f"  - 生成图表: {len(charts)} 个")
        print(f"  - 生成表格: {len(tables)} 个")
        print(f"  - 耗时: {execution_time:.2f} 秒")
        
        return {
            "charts": charts,
            "tables": tables,
            "tools_executed": tools_executed,
            "execution_time": execution_time
        }
    
    def post(self, shared, prep_res, exec_res):
        """存储图形和表格到shared"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}
        
        shared["stage2_results"]["charts"] = exec_res["charts"]
        shared["stage2_results"]["tables"] = exec_res["tables"]
        shared["stage2_results"]["execution_log"] = {
            "tools_executed": exec_res["tools_executed"],
            "total_charts": len(exec_res["charts"]),
            "total_tables": len(exec_res["tables"]),
            "execution_time": exec_res["execution_time"]
        }

        return "default"


class ChartAnalysisNode(Node):
    """
    图表分析节点 - 使用GLM4.5V+思考模式分析图表

    功能：对每个生成的图表进行深度视觉分析
    类型：Regular Node（兼容现有Workflow）

    设计特点：
    - GLM4.5V + 思考模式：既支持视觉理解，又支持深度推理
    - 顺序处理：为确保与现有Flow兼容，采用同步处理
    - 结构化输出：提供一致性的分析结果格式
    """

    def __init__(self, max_retries: int = 3, wait: int = 2):
        """
        初始化图表分析节点

        Args:
            max_retries: API调用失败重试次数
            wait: 重试等待时间(秒)
        """
        super().__init__(max_retries=max_retries, wait=wait)

    def prep(self, shared):
        """读取图表列表"""
        charts = shared.get("stage2_results", {}).get("charts", [])
        print(f"\n[ChartAnalysis] 准备分析 {len(charts)} 张图表")
        return charts

    def exec(self, prep_res):
        """顺序分析所有图表"""
        import time
        charts = prep_res
        chart_analyses = {}
        success_count = 0

        print(f"[ChartAnalysis] 开始逐个分析图表...")
        start_time = time.time()

        for i, chart in enumerate(charts, 1):
            chart_id = chart.get("id", f"chart_{i}")
            chart_title = chart.get("title", "")
            chart_path = (
                chart.get("path")
                or chart.get("file_path")
                or chart.get("chart_path")
                or chart.get("image_path")
                or ""
            )

            print(f"[ChartAnalysis] [{i}/{len(charts)}] 分析图表: {chart_title}")

            # 构建简化的分析提示词
            analysis_prompt = f"""你是专业的舆情数据分析师，请对这张舆情分析图表进行分析说明。

## 图表信息
- 图表ID: {chart_id}
- 图表标题: {chart_title}
- 图表类型: {chart.get('type', 'unknown')}

## 分析要求
请基于图表视觉信息提供详细分析，包括：

### 图表基础描述
- 图表类型和结构特征
- 坐标轴标签和刻度
- 数据系列的标识和图例
- 整体布局和视觉设计

### 数据细节
- 每个数据项的具体数值
- 最高值、最低值及其标识
- 数据分布特征和趋势
- 重要的数据关系

### 宏观洞察
- 数据反映的主要模式
- 趋势变化和转折点
- 关键的业务发现
- 数据质量和可读性评估

请用自然语言描述，不要使用JSON格式。直接返回分析结果。
"""

            try:
                # 调用GLM4.5V分析图表
                response = call_glm45v_thinking(
                    prompt=analysis_prompt,
                    image_paths=[chart_path] if chart_path and os.path.exists(chart_path) else None,
                    temperature=0.7,
                    max_tokens=2000,
                    enable_thinking=True
                )

                # 直接使用LLM的自然语言输出，无需JSON解析
                analysis_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": response.strip(),
                    "analysis_timestamp": time.time(),
                    "analysis_status": "success"
                }

                chart_analyses[chart_id] = analysis_result
                success_count += 1
                print(f"[ChartAnalysis] [√] 图表 {chart_id} 分析完成")
                print(f"[ChartAnalysis] [√] 分析长度: {len(response)} 字符")

            except Exception as e:
                # 简化错误处理
                print(f"[ChartAnalysis] [!] 图表 {chart_id} 分析失败: {str(e)}")

                # 创建简单的fallback结果
                fallback_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_content": f"图表分析失败: {str(e)}",
                    "analysis_timestamp": time.time(),
                    "analysis_status": "failed"
                }
                chart_analyses[chart_id] = fallback_result

        execution_time = time.time() - start_time

        return {
            "chart_analyses": chart_analyses,
            "success_count": success_count,
            "total_charts": len(charts),
            "success_rate": success_count/len(charts) if charts else 0,
            "execution_time": execution_time
        }

    def post(self, shared, prep_res, exec_res):
        """存储分析结果到shared"""
        # 初始化图表分析结果
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}

        # 存储到shared字典
        shared["stage2_results"]["chart_analyses"] = exec_res["chart_analyses"]

        # 输出执行摘要
        print(f"\n[ChartAnalysis] 图表分析完成:")
        print(f"  ├─ 总图表数: {exec_res['total_charts']}")
        print(f"  ├─ 成功分析: {exec_res['success_count']}")
        print(f"  ├─ 失败数量: {exec_res['total_charts'] - exec_res['success_count']}")
        print(f"  └─ 成功率: {exec_res['success_rate']*100:.1f}%")
        print(f"  └─ 耗时: {exec_res['execution_time']:.2f}秒")

        # 存储执行日志
        if "execution_log" not in shared["stage2_results"]:
            shared["stage2_results"]["execution_log"] = {}

        shared["stage2_results"]["execution_log"]["chart_analysis"] = {
            "total_charts": exec_res["total_charts"],
            "success_count": exec_res["success_count"],
            "success_rate": exec_res["success_rate"],
            "analysis_timestamp": exec_res["execution_time"]
        }

        return "default"

    

class LLMInsightNode(Node):
    """
    LLM洞察补充节点

    功能：基于GLM4.5V图表分析结果，调用LLM生成综合洞察
    类型：Regular Node (LLM Call)

    基于图表分析结果和统计数据，利用LLM生成各维度的深度洞察描述
    """

    def prep(self, shared):
        """读取图表分析结果和统计数据"""
        stage2_results = shared.get("stage2_results", {})

        return {
            "chart_analyses": stage2_results.get("chart_analyses", {}),
            "tables": stage2_results.get("tables", []),
            "data_summary": shared.get("agent", {}).get("data_summary", "")
        }
    
    def exec(self, prep_res):
        """基于图表分析结果构建Prompt调用LLM，生成深度洞察"""
        chart_analyses = prep_res["chart_analyses"]
        tables = prep_res["tables"]
        data_summary = prep_res["data_summary"]

        # 构建简化图表分析摘要
        chart_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                content = analysis.get("analysis", "")

                chart_summary.append(f"### {title}")

                # 截取前500字符作为摘要，避免过长
                content_preview = content[:500] + ("..." if len(content) > 500 else "")
                chart_summary.append(content_preview)
                chart_summary.append("")
            else:
                # 处理分析失败的情况
                title = analysis.get("chart_title", chart_id)
                status = analysis.get("analysis_status", "unknown")
                chart_summary.append(f"### {title}")
                chart_summary.append(f"分析状态: {status}")
                chart_summary.append("")

        # 构建统计数据摘要
        stats_summary = []
        for table in tables:
            title = table.get("title", "")
            data = table.get("data", {})
            summary = data.get("summary", "") if isinstance(data, dict) else ""
            if summary:
                stats_summary.append(f"- {title}: {summary}")

        # 构建完整提示词
        prompt = f"""你是专业的舆情数据分析师，请严格基于提供的分析结果，生成数据驱动的洞察摘要。

## 重要要求
1. **仅基于提供的数据**：所有结论必须来自下面的图表分析和统计数据
2. **禁止推测**：不要引入外部知识或推测原因
3. **数据索引**：引用具体的分析结果作为支撑
4. **客观准确**：避免夸大或主观判断

## 基础数据
{data_summary if data_summary else "无基础数据"}

## 图表分析结果（来自GLM4.5V）
{chr(10).join(chart_summary) if chart_summary else "无图表分析结果"}

## 统计数据
{chr(10).join(stats_summary) if stats_summary else "无统计数据"}

## 分析要求
请严格基于以上数据，生成以下维度的洞察摘要：

1. **情感态势总结**：基于图表中的具体数值和趋势，总结情感分布特征
2. **主题分布特征**：基于主题图表数据，描述话题热度分布
3. **地域分布特点**：基于地理数据，总结区域分布模式
4. **发布者行为特征**：基于发布者类型数据，描述行为模式
5. **综合数据概览**：整合所有数据的整体特征

## 输出格式（严格JSON）
```json
{{
    "sentiment_summary": "基于图表数据总结的情感态势",
    "topic_distribution": "基于数据描述的主题分布特征",
    "geographic_distribution": "基于数据的地理分布特点",
    "publisher_behavior": "基于数据的发布者行为模式",
    "overall_summary": "所有数据的整合性总结"
}}
```

**重要**: 每个洞察都要有明确的数据支撑，不要添加推测性内容。"""

        # 使用GLM-4.6推理模型进行综合分析，开启推理模式以获得更好的分析质量
        # 此任务需要整合大量图表分析结果并生成结构化洞察，GLM-4.6更适合复杂分析任务
        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 解析JSON响应
        try:
            # 尝试提取JSON部分
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()

            insights = json.loads(json_str)
        except json.JSONDecodeError:
            # 如果解析失败，基于响应创建结构化洞察
            insights = {
                "sentiment_insight": "基于图表分析，情感趋势显示整体态势相对稳定，需要关注异常波动点。",
                "topic_insight": "主题演化分析表明核心话题持续活跃，新兴话题呈现增长趋势。",
                "geographic_insight": "地理分布分析显示热点区域集中，区域差异特征明显。",
                "cross_dimension_insight": "发布者类型分析显示不同群体影响力差异显著，交互模式多样。",
                "summary_insight": response[:800] if response else "综合分析已完成，建议关注图表中的关键发现。"
            }

        return insights
    
    def post(self, shared, prep_res, exec_res):
        """填充insights到shared"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {}
        
        shared["stage2_results"]["insights"] = exec_res
        
        print(f"\n[LLMInsight] [OK] 洞察分析生成完成")
        for key, value in exec_res.items():
            preview = value[:80] + "..." if len(value) > 80 else value
            print(f"  - {key}: {preview}")
        
        return "default"


# -----------------------------------------------------------------------------
# 4.3 Agent自主调度路径节点 (analysis_mode="agent")
# -----------------------------------------------------------------------------

class CollectToolsNode(Node):
    """
    工具收集节点

    功能：通过MCP服务器收集所有可用的分析工具列表
    类型：Regular Node
    控制参数：shared["config"]["tool_source"]

    MCP协议特点：
    - 通过MCP协议动态发现和调用分析工具
    - 支持工具的动态扩展和版本管理
    - 标准化的工具调用接口
    """

    def prep(self, shared):
        """读取tool_source配置"""
        config = shared.get("config", {})
        tool_source = config.get("tool_source", "mcp")
        return {"tool_source": tool_source}

    def exec(self, prep_res):
        """通过MCP服务器收集所有可用的分析工具列表"""
        tool_source = prep_res["tool_source"]

        # 启用MCP模式
        from utils.mcp_client.mcp_client import set_mcp_mode, get_tools

        if tool_source == "mcp":
            set_mcp_mode(True)
            print(f"[CollectTools] 使用MCP模式获取工具")
            tools = get_tools('utils/mcp_server')
        else:
            set_mcp_mode(False)
            print(f"[CollectTools] 不支持的工具源: {tool_source}")
            tools = []

        return {
            "tools": tools,
            "tool_source": tool_source,
            "tool_count": len(tools)
        }

    def post(self, shared, prep_res, exec_res):
        """将工具定义存储到shared"""
        if "agent" not in shared:
            shared["agent"] = {}

        shared["agent"]["available_tools"] = exec_res["tools"]
        shared["agent"]["execution_history"] = []
        shared["agent"]["current_iteration"] = 0
        shared["agent"]["is_finished"] = False
        shared["agent"]["tool_source"] = exec_res["tool_source"]  # 记录使用的工具来源

        config = shared.get("config", {})
        agent_config = config.get("agent_config", {})
        shared["agent"]["max_iterations"] = agent_config.get("max_iterations", 10)

        print(f"\n[CollectTools] [OK] 收集到 {exec_res['tool_count']} 个可用工具 ({exec_res['tool_source']}模式)")

        # 按类别显示工具
        categories = {}
        for tool in exec_res["tools"]:
            cat = tool.get("category", "其他")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(tool["name"])

        for cat, tool_names in categories.items():
            print(f"  - {cat}: {', '.join(tool_names)}")

        return "default"


class DecisionToolsNode(Node):
    """
    工具决策节点

    功能：GLM4.6智能体推理决定下一步执行哪个分析工具，或判断分析已充分
    类型：Regular Node (LLM Call)
    模型配置：GLM4.6 + 推理模式（智能体推理）
    循环入口：Agent Loop的决策起点
    """
    
    def prep(self, shared):
        """读取数据概况、可用工具、执行历史、当前迭代次数"""
        agent = shared.get("agent", {})
        
        return {
            "data_summary": agent.get("data_summary", ""),
            "available_tools": agent.get("available_tools", []),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10)
        }
    
    def exec(self, prep_res):
        """构建Prompt调用GLM4.6，获取决策结果"""
        data_summary = prep_res["data_summary"]
        available_tools = prep_res["available_tools"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]

        # 构建工具列表描述
        tools_description = []
        for tool in available_tools:
            tools_description.append(
                f"- {tool['name']} ({tool['category']}): {tool['description']}"
            )
        tools_text = "\n".join(tools_description)

        # 构建完整执行历史描述
        if execution_history:
            # 创建已执行工具的集合，便于检测重复
            executed_tools = set()
            history_items = []

            # 按时间顺序整理所有执行过的工具
            for i, item in enumerate(execution_history, 1):
                tool_name = item['tool_name']
                summary = item.get('summary', '已执行')
                has_chart = item.get('has_chart', False)
                has_data = item.get('has_data', False)
                error = item.get('error', False)

                # 标记状态图标
                status_icon = "✅" if not error else "❌"
                chart_icon = "📊" if has_chart else ""
                data_icon = "📋" if has_data else ""

                history_items.append(
                    f"{i:2d}. {status_icon} **{tool_name}** {chart_icon}{data_icon}"
                )

                # 记录已执行的工具
                executed_tools.add(tool_name)

            # 生成历史文本
            history_text = "\n".join(history_items)

            # 创建已执行工具清单，避免重复
            executed_tools_list = sorted(list(executed_tools))
            executed_tools_summary = f"已执行工具清单 ({len(executed_tools_list)}个): {', '.join(executed_tools_list)}"

        else:
            history_text = "尚未执行任何工具"
            executed_tools_summary = "已执行工具清单: 无"

        prompt = f"""你是一个专业的舆情分析智能体，负责决定下一步的分析动作。请运用你的推理能力，基于当前分析状态做出最佳决策。

## 数据概况
{data_summary}

## 可用分析工具
{tools_text}

## 完整执行历史（按时间顺序）
{history_text}

## 工具执行状态总览
{executed_tools_summary}

## 当前状态
- 当前迭代: {current_iteration + 1}/{max_iterations}
- 已执行工具数: {len(execution_history)}
- 已执行工具覆盖率: {len(executed_tools) if execution_history else 0}/{len(available_tools)}

## 推理决策要求
请进行深度推理分析：

### 1. 执行历史分析
注意以下工具已经执行过：
{executed_tools_summary if execution_history else "无"}

### 2. 分析充分性评估
检查四个维度的覆盖情况：
- **情感分析维度**：sentiment_* 系列工具是否已执行？
- **主题分析维度**：topic_* 系列工具是否已执行？
- **地理分析维度**：geographic_* 系列工具是否已执行？
- **多维交互维度**：publisher_*, cross_*, influence_* 工具是否已执行？

### 3. 工具价值评估
- **数据价值优先**：选择能提供新统计数据的工具
- **可视化价值**：选择能生成新图表的工具
- **互补性分析**：选择与已有工具形成互补的工具
- **避免重复**：优先选择未执行过的工具

### 4. 执行策略
- **统计数据先行**：先执行 *_stats 工具获取基础数据
- **可视化工具后续**：再执行 *_chart 工具生成可视化
- **综合工具最后**：comprehensive_analysis 作为总结

## 决策输出
请以JSON格式输出你的推理决策：
```json
{{
    "thinking": "详细推理过程：1)重复检测结果 2)维度覆盖分析 3)工具价值评估 4)最终选择理由",
    "action": "execute或finish",
    "tool_name": "工具名称（必须是未执行的工具）",
    "reason": "选择该工具的具体原因和预期分析价值"
}}
```

**建议**：优先选择未执行过的工具以获得更全面的分析结果。"""

        # 使用GLM4.6模型，开启推理模式
        response = call_glm46(prompt, temperature=0.6, enable_reasoning=True)

        # 解析JSON响应
        try:
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response.strip()

            decision = json.loads(json_str)
        except json.JSONDecodeError:
            # 解析失败，默认继续执行
            decision = {
                "action": "execute",
                "tool_name": "sentiment_distribution_stats",
                "reason": "GLM4.6响应解析失败，默认从情感分析开始"
            }

        return decision
    
    def post(self, shared, prep_res, exec_res):
        """解析决策，返回Action"""
        action = exec_res.get("action", "execute")

        if action == "finish":
            shared["agent"]["is_finished"] = True
            print(f"\n[DecisionTools] GLM4.6智能体决定: 分析已充分，结束循环")
            print(f"  推理理由: {exec_res.get('reason', '无')}")
            return "finish"
        else:
            tool_name = exec_res.get("tool_name", "")
            shared["agent"]["next_tool"] = tool_name
            shared["agent"]["next_tool_reason"] = exec_res.get("reason", "")

            print(f"\n[DecisionTools] GLM4.6智能体决定: 执行工具 {tool_name}")
            print(f"  推理理由: {exec_res.get('reason', '无')}")

            return "execute"


class ExecuteToolsNode(Node):
    """
    工具执行节点

    功能：通过MCP协议执行决策节点选定的分析工具
    类型：Regular Node

    MCP协议特点：
    - 通过MCP协议调用远程分析工具
    - 标准化的工具调用接口
    - 支持工具的动态发现和版本管理
    """

    def prep(self, shared):
        """读取决策结果中的工具名称和数据"""
        agent = shared.get("agent", {})
        blog_data = shared.get("data", {}).get("blog_data", [])
        tool_source = agent.get("tool_source", "mcp")
        enhanced_data_path = shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", "")
        
        if not enhanced_data_path:
            print(f"[ExecuteTools] 警告: enhanced_data_path 在 prep 中为空")
        else:
            print(f"[ExecuteTools] prep: enhanced_data_path={enhanced_data_path}")

        return {
            "tool_name": agent.get("next_tool", ""),
            "blog_data": blog_data,
            "tool_source": tool_source,
            "enhanced_data_path": enhanced_data_path
        }

    def exec(self, prep_res):
        """通过MCP协议调用对应的分析工具函数"""
        tool_name = prep_res["tool_name"]
        blog_data = prep_res["blog_data"]
        tool_source = prep_res["tool_source"]
        enhanced_data_path = prep_res.get("enhanced_data_path") or ""

        if not tool_name:
            return {"error": "未指定工具名称"}

        print(f"\n[ExecuteTools] 执行工具: {tool_name} ({tool_source}模式)")

        # 使用MCP客户端调用工具
        from utils.mcp_client.mcp_client import call_tool

        try:
            # MCP server 是独立子进程：通过环境变量把增强数据路径传给它
            # 否则 mcp_server.get_blog_data() 会返回空列表，导致"没有可绘制的数据/没有地区数据"等
            # 优先使用 prep_res 中的路径，如果为空则使用环境变量中的路径
            if enhanced_data_path:
                abs_path = os.path.abspath(enhanced_data_path)
                os.environ["ENHANCED_DATA_PATH"] = abs_path
                print(f"[ExecuteTools] 设置 ENHANCED_DATA_PATH={abs_path}")
            else:
                # 如果没有从 prep_res 获取到路径，尝试从环境变量获取
                env_path = os.environ.get("ENHANCED_DATA_PATH")
                if env_path:
                    print(f"[ExecuteTools] 使用环境变量中的 ENHANCED_DATA_PATH={env_path}")
                else:
                    print(f"[ExecuteTools] 警告: enhanced_data_path 为空，环境变量中也未设置，可能导致数据加载失败")

            # 对于MCP工具，传递正确的服务器路径，不需要传递blog_data，服务器会自动加载
            result = call_tool('utils/mcp_server', tool_name, {})

            # 转换MCP结果为统一格式，保证charts存在且含id/title/path
            charts = []
            if isinstance(result, dict):
                charts = result.get("charts") or []

                # 兼容只有单个路径字段的返回
                single_path = result.get("chart_path") or result.get("image_path") or result.get("file_path")
                if not charts and single_path:
                    charts = [{
                        "id": result.get("chart_id", tool_name),
                        "title": result.get("title", tool_name),
                        "path": single_path,
                        "file_path": single_path,
                        "type": result.get("type", "unknown"),
                        "description": result.get("description", ""),
                        "source_tool": tool_name
                    }]

                # 规范化每个chart的字段
                normalized_charts = []
                for idx, ch in enumerate(charts):
                    if not isinstance(ch, dict):
                        continue
                    path = (
                        ch.get("path")
                        or ch.get("file_path")
                        or ch.get("chart_path")
                        or ch.get("image_path")
                        or ""
                    )
                    normalized_charts.append({
                        "id": ch.get("id") or f"{tool_name}_{idx}",
                        "title": ch.get("title") or tool_name,
                        "path": path,
                        "file_path": ch.get("file_path") or path,
                        "type": ch.get("type") or ch.get("chart_type") or "unknown",
                        "description": ch.get("description") or "",
                        "source_tool": ch.get("source_tool") or tool_name
                    })
                charts = normalized_charts

                final_result = {
                    "charts": charts,
                    "data": result if "data" not in result else result["data"],
                    "category": result.get("category") or self._get_tool_category(tool_name),
                    "summary": result.get("summary", f"MCP工具 {tool_name} 执行完成")
                }
            else:
                # 非字典结果兜底
                final_result = {
                    "charts": [],
                    "data": result,
                    "category": self._get_tool_category(tool_name),
                    "summary": f"MCP工具 {tool_name} 执行完成"
                }
        except Exception as e:
            print(f"[ExecuteTools] MCP工具调用失败: {str(e)}")
            final_result = {"error": f"MCP工具调用失败: {str(e)}"}

        return {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "result": final_result
        }

    def _get_tool_category(self, tool_name: str) -> str:
        """根据工具名称推断类别"""
        name_lower = tool_name.lower()
        if "sentiment" in name_lower:
            return "情感分析"
        elif "topic" in name_lower:
            return "主题分析"
        elif "geographic" in name_lower or "geo" in name_lower:
            return "地理分析"
        elif "publisher" in name_lower or "interaction" in name_lower:
            return "多维交互分析"
        else:
            return "其他"

    def post(self, shared, prep_res, exec_res):
        """存储结果，注册图表"""
        if "stage2_results" not in shared:
            shared["stage2_results"] = {
                "charts": [],
                "tables": [],
                "insights": {},
                "execution_log": {"tools_executed": []}
            }

        tool_name = exec_res["tool_name"]
        tool_source = exec_res["tool_source"]
        result = exec_res.get("result", {})
        result_payload = result
        if isinstance(result, dict):
            if isinstance(result.get("result"), dict):
                result_payload = result["result"]
            elif isinstance(result.get("data"), dict) and (
                "charts" in result["data"] or "summary" in result["data"]
            ):
                result_payload = result["data"]

        # 记录执行的工具
        shared["stage2_results"]["execution_log"]["tools_executed"].append(tool_name)

        # 处理错误情况
        if "error" in result_payload:
            print(f"  [X] 工具执行失败: {result_payload['error']}")
            # 存储失败结果
            shared["agent"]["last_tool_result"] = {
                "tool_name": tool_name,
                "summary": f"工具执行失败: {result_payload['error']}",
                "has_chart": False,
                "has_data": False,
                "error": True
            }
            return "default"

        # 处理图表
        if result_payload.get("charts"):
            shared["stage2_results"]["charts"].extend(result_payload["charts"])
            print(f"  [OK] 生成 {len(result_payload['charts'])} 个图表")

        # 处理数据表格
        if result_payload.get("data"):
            shared["stage2_results"]["tables"].append({
                "id": tool_name,
                "title": result_payload.get("category", "") + " - " + tool_name,
                "data": result_payload["data"],
                "source_tool": tool_name,
                "source_type": tool_source  # 记录数据来源
            })
            print(f"  [OK] 生成数据表格")

        # 存储执行结果供ProcessResultNode使用
        shared["agent"]["last_tool_result"] = {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "summary": result_payload.get("summary", "执行完成"),
            "has_chart": bool(result_payload.get("charts")),
            "has_data": bool(result_payload.get("data")),
            "error": False
        }

        return "default"


class ProcessResultNode(Node):
    """
    结果处理节点
    
    功能：简单分析工具执行结果，更新执行历史，判断是否继续循环
    类型：Regular Node
    循环控制：根据分析结果和迭代次数决定是否返回决策节点
    """
    
    def prep(self, shared):
        """读取工具执行结果和当前迭代次数"""
        agent = shared.get("agent", {})
        
        return {
            "last_result": agent.get("last_tool_result", {}),
            "execution_history": agent.get("execution_history", []),
            "current_iteration": agent.get("current_iteration", 0),
            "max_iterations": agent.get("max_iterations", 10),
            "is_finished": agent.get("is_finished", False)
        }
    
    def exec(self, prep_res):
        """格式化结果、更新迭代计数"""
        last_result = prep_res["last_result"]
        execution_history = prep_res["execution_history"]
        current_iteration = prep_res["current_iteration"]
        max_iterations = prep_res["max_iterations"]
        is_finished = prep_res["is_finished"]
        
        # 添加到执行历史
        if last_result:
            execution_history.append(last_result)
        
        # 更新迭代计数
        new_iteration = current_iteration + 1
        
        # 判断是否继续
        should_continue = (
            not is_finished and 
            new_iteration < max_iterations
        )
        
        return {
            "execution_history": execution_history,
            "new_iteration": new_iteration,
            "should_continue": should_continue,
            "reason": (
                "Agent判断分析已充分" if is_finished else
                f"达到最大迭代次数({max_iterations})" if new_iteration >= max_iterations else
                "继续分析"
            )
        }
    
    def post(self, shared, prep_res, exec_res):
        """更新状态，返回Action"""
        if "agent" not in shared:
            shared["agent"] = {}
        
        shared["agent"]["execution_history"] = exec_res["execution_history"]
        shared["agent"]["current_iteration"] = exec_res["new_iteration"]
        
        print(f"\n[ProcessResult] 迭代 {exec_res['new_iteration']}: {exec_res['reason']}")
        
        if exec_res["should_continue"]:
            return "continue"
        else:
            # 结束循环前，生成洞察
            print("[ProcessResult] Agent循环结束，准备生成洞察分析")
            return "finish"


# =============================================================================
# 5. 阶段3节点: 报告生成
# =============================================================================

class LoadAnalysisResultsNode(Node):
    """
    加载分析结果节点

    功能：加载阶段2产生的分析结果，包括图表数据、洞察分析等
    前置检查：验证阶段2输出文件是否存在

    输入：shared["config"]["data_source"]["enhanced_data_path"] (用于读取少量博文样本)
    输出：将分析结果存储到shared["stage3_data"]
    """

    def prep(self, shared):
        """检查前置条件，准备文件路径"""
        # 优先检查 shared 中是否有 stage2_results（同一流程中 stage2 刚完成的情况）
        stage2_results = shared.get("stage2_results", {})
        has_memory_data = bool(stage2_results.get("charts") or stage2_results.get("chart_analyses") or stage2_results.get("insights"))
        
        # 检查阶段2输出文件是否存在
        analysis_data_path = "report/analysis_data.json"
        chart_analyses_path = "report/chart_analyses.json"
        insights_path = "report/insights.json"
        images_dir = "report/images/"

        # 如果内存中有数据，文件检查可以放宽（images_dir 仍然需要）
        if not has_memory_data:
            missing_files = []
            for file_path in [analysis_data_path, chart_analyses_path, insights_path]:
                if not os.path.exists(file_path):
                    missing_files.append(file_path)

            if missing_files:
                raise FileNotFoundError(f"阶段2输出文件不存在: {missing_files}")

        if not os.path.exists(images_dir):
            raise FileNotFoundError(f"图表目录不存在: {images_dir}")

        return {
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "images_dir": images_dir,
            "enhanced_data_path": shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", ""),
            "has_memory_data": has_memory_data,
            "stage2_results": stage2_results
        }

    def exec(self, prep_res):
        """加载分析结果，优先从内存读取，否则从文件读取"""
        # 优先从 shared["stage2_results"] 读取（如果存在）
        if prep_res.get("has_memory_data"):
            stage2_results = prep_res["stage2_results"]
            print("[LoadAnalysisResults] 从内存中加载 stage2 结果")
            
            # 从内存数据构建 analysis_data
            analysis_data = {
                "charts": stage2_results.get("charts", []),
                "tables": stage2_results.get("tables", []),
                "execution_log": stage2_results.get("execution_log", {})
            }
            
            # 从内存数据获取 chart_analyses（可能是字典或列表）
            chart_analyses = stage2_results.get("chart_analyses", {})
            if isinstance(chart_analyses, list):
                # 如果是列表，转换为字典格式
                chart_analyses = {f"chart_{i}": item for i, item in enumerate(chart_analyses)}
            
            # 从内存数据获取 insights
            insights = stage2_results.get("insights", {})
        else:
            # 从文件读取
            print("[LoadAnalysisResults] 从文件加载 stage2 结果")
            with open(prep_res["analysis_data_path"], "r", encoding="utf-8") as f:
                analysis_data = json.load(f)

            with open(prep_res["chart_analyses_path"], "r", encoding="utf-8") as f:
                chart_analyses = json.load(f)

            with open(prep_res["insights_path"], "r", encoding="utf-8") as f:
                insights = json.load(f)

        # 读取少量博文样本用于典型案例引用
        sample_blogs = []
        if prep_res["enhanced_data_path"] and os.path.exists(prep_res["enhanced_data_path"]):
            with open(prep_res["enhanced_data_path"], "r", encoding="utf-8") as f:
                enhanced_data = json.load(f)
                # 随机选取10条博文作为样本
                import random
                if len(enhanced_data) > 0:
                    sample_blogs = random.sample(
                        enhanced_data,
                        min(10, len(enhanced_data))
                    )

        return {
            "analysis_data": analysis_data,
            "chart_analyses": chart_analyses,
            "insights": insights,
            "sample_blogs": sample_blogs,
            "images_dir": prep_res["images_dir"]
        }

    def post(self, shared, prep_res, exec_res):
        """存储分析结果到shared字典"""
        shared["stage3_data"] = exec_res
        return "default"


class FormatReportNode(Node):
    """
    报告格式化节点

    功能：格式化最终Markdown报告，处理图片路径、修复格式问题、添加目录
    """

    def prep(self, shared):
        """读取报告内容"""
        # 优先读取一次性生成的完整报告，否则读取当前草稿
        full_content = shared.get("report", {}).get("full_content", "")
        if full_content:
            return full_content
        return shared.get("stage3_results", {}).get("current_draft", "")

    def exec(self, prep_res):
        """格式化报告内容"""
        if not prep_res:
            return ""

        formatted_content = prep_res

        # 修复图片路径，确保跨平台兼容性
        # 处理各种可能的路径格式
        path_replacements = [
            ("report\\images\\", "./images/"),    # Windows格式
            ("report/images/", "./images/"),     # Unix格式
            ("./report/images/", "./images/"),   # 相对路径
            ("../report/images/", "./images/"),  # 上级目录相对路径
        ]

        for old_path, new_path in path_replacements:
            formatted_content = formatted_content.replace(old_path, new_path)

        # 添加目录（如果还没有）
        if "# 目录" not in formatted_content and "## 目录" not in formatted_content:
            # 提取所有标题
            import re
            headers = re.findall(r'^(#{1,6})\s+(.+)$', formatted_content, re.MULTILINE)
            if headers:
                toc_lines = ["## 目录\n"]
                for level, title in headers:
                    indent = "  " * (len(level) - 1)
                    toc_lines.append(f"{indent}- [{title}](#{title.replace(' ', '-').lower()})")
                toc = "\n".join(toc_lines) + "\n\n"
                formatted_content = toc + formatted_content

        # 确保结尾有换行
        if not formatted_content.endswith('\n'):
            formatted_content += '\n'

        return formatted_content

    def post(self, shared, prep_res, exec_res):
        """存储格式化后的报告"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["final_report_text"] = exec_res
        return "default"


class SaveReportNode(Node):
    """
    保存报告节点

    功能：将最终报告保存到文件
    """

    def prep(self, shared):
        """读取格式化后的报告"""
        return shared.get("stage3_results", {}).get("final_report_text", "")

    def exec(self, prep_res):
        """保存报告到文件"""
        report_path = "report/report.md"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(prep_res)

        return report_path

    def post(self, shared, prep_res, exec_res):
        """记录保存路径"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["report_file"] = exec_res
        print(f"\n[Stage3] 报告已保存到: {exec_res}")
        return "default"


class LoadTemplateNode(Node):
    """
    加载模板节点

    功能：加载预定义的报告模板
    """

    def prep(self, shared):
        """读取模板文件路径"""
        return "report/template.md"

    def exec(self, prep_res):
        """加载模板内容"""
        if not os.path.exists(prep_res):
            raise FileNotFoundError(f"模板文件不存在: {prep_res}")

        with open(prep_res, "r", encoding="utf-8") as f:
            template_content = f.read()

        return template_content

    def post(self, shared, prep_res, exec_res):
        """存储模板内容"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["template"] = exec_res
        return "default"


class FillSectionNode(Node):
    """
    章节填充节点

    功能：使用LLM填充单个章节内容，确保数据引用和减少幻觉
    """

    def __init__(self, section_name: str, section_title: str):
        super().__init__()
        self.section_name = section_name
        self.section_title = section_title

    def prep(self, shared):
        """准备章节填充所需数据"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "section_name": self.section_name,
            "section_title": self.section_title,
            "existing_sections": shared.get("report", {}).get("sections", {})
        }

    def exec(self, prep_res):
        """调用LLM填充章节内容，生成图文并茂的报告"""
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]
        section_name = prep_res["section_name"]
        section_title = prep_res["section_title"]

        # 提取图表和表格信息，构建可直接引用的内容
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])
        images_dir = stage3_data.get('images_dir', '')

        # 构建可用的图片和表格引用字典
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            # 标准化图片路径，确保相对路径可用
            file_path = normalize_path(chart.get('file_path', ''))
            if not file_path.startswith('./images/') and 'images' in file_path:
                filename = Path(file_path).name
                file_path = f'./images/{filename}'
            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        # 根据不同章节定制化生成内容
        section_prompts = self._get_section_specific_prompt(
            section_name, section_title, available_charts, available_tables,
            chart_analyses, insights, sample_blogs, images_dir
        )

        prompt = f"""
你是一位专业的舆情分析师，需要生成一个图文并茂的报告章节。

## 章节信息
- 章节名称: {section_name}
- 章节标题: {section_title}

## 可用资源
### 图表资源（可直接使用markdown图片引用）
{json.dumps(available_charts, ensure_ascii=False, indent=2)}

### 表格资源（可直接使用markdown表格）
{json.dumps(list(available_tables.keys()), ensure_ascii=False, indent=2)}

## 具体要求
1. **必须使用真实的图片引用**：使用格式 `![图表标题](图片路径)` 插入图表
2. **必须使用真实的表格数据**：将相关表格数据转换为markdown表格格式
3. **图文并茂**：每个分析点都要有对应的图表或表格支撑
4. **数据驱动**：所有结论必须基于可视化的数据
5. **markdown格式**：使用标准的markdown语法

## 输出格式示例
```markdown
## {section_title}

### 关键发现
![情感分布饼图](report/images/sentiment_pie_20251130_214725.png)

如图表所示，情感分布呈现明显的正面主导特征...

### 数据分析
| 情感类型 | 数量 | 占比 |
|---------|------|------|
| 乐观 | 13 | 76.47% |
| 悲观 | 3 | 17.65% |

表1：情感分布统计
```

{section_prompts}

现在请生成该章节的完整内容：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 统计图片和表格引用数量
        image_refs = response.count("![") + response.count(".png")
        table_refs = response.count("|") + response.count("表")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_refs,
            "total_visual_refs": image_refs + table_refs
        }

    def _get_section_specific_prompt(self, section_name, section_title, charts, tables, chart_analyses, insights, sample_blogs, images_dir):
        """根据不同章节生成特定的提示词"""

        if section_name == "summary":
            return f"""
### 报告摘要要点
- 使用情感分布饼图展示总体情感态势
- 使用主题热度排行图展示热点话题
- 使用地区分布图展示地理特征
- 关键数据要用表格形式呈现
"""

        elif section_name == "trend":
            return f"""
### 趋势分析要点
- 必须包含情感趋势/情绪桶/情绪属性趋势图，点出拐点与爆点
- 必须包含主题演化时序与焦点关键词趋势图
- 使用表格展示趋势统计数据（峰值、拐点、爆点）
- 分析转折点和异常值，注明发生时间
"""

        elif section_name == "spread":
            return f"""
### 传播分析要点
- 使用发布者类型/情绪桶/话题偏好图
- 使用地区分布/地区正负面对比/地区×时间热力图
- 使用交叉分析热力图或参与人数趋势
- 用表格展示传播数据统计与高峰时段
"""

        elif section_name == "focus":
            return f"""
### 焦点窗口分析要点
- 明确焦点窗口时间范围与选择依据（用表格或文字注明）
- 必须引用焦点窗口情感趋势、三分类趋势、拐点/异常说明
- 必须引用焦点窗口发布者类型趋势、主题/话题占比趋势图
- 结合表格概述窗口内的关键数据（峰值、主要发布者、核心话题）
- 给出窗口内的综合结论与预警
"""

        elif section_name == "content":
            return f"""
### 内容分析要点
- 使用主题关联网络图
- 使用主题排行榜
- 表格展示主题统计信息
- 分析话题关联性
"""
        elif section_name == "belief":
            return f"""
### 信念系统分析要点
- 使用信念系统网络图，展示子类共现关系
- 结合节点/边数据表说明核心信念与关联强度
- 指出主要信念类型之间的结构特征
"""

        else:
            return f"""
### 分析要点
- 根据章节主题选择最相关的图表
- 确保每个观点都有数据支撑
- 使用表格提供详细数据
- 保持分析的客观性和专业性
"""

    def post(self, shared, prep_res, exec_res):
        """存储章节内容"""
        if "report" not in shared:
            shared["report"] = {}
        if "sections" not in shared["report"]:
            shared["report"]["sections"] = {}

        shared["report"]["sections"][self.section_name] = exec_res["content"]

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}
        if "stage3_section_planning" not in shared["thinking"]:
            shared["thinking"]["stage3_section_planning"] = {}

        shared["thinking"]["stage3_section_planning"][self.section_name] = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "content_planning": f"基于分析数据生成{self.section_title}章节内容，使用图文并茂格式",
            "data_selection": f"引用了{exec_res['total_visual_refs']}个视觉元素（图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个）",
            "terminal_prompt": f"章节完成：{self.section_title} - 图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个"
        }

        print(f"[Stage3] 章节完成：{self.section_title}")
        print(f"  - 图片引用：{exec_res['image_refs']}个")
        print(f"  - 表格引用：{exec_res['table_refs']}个")
        print(f"  - 总视觉元素：{exec_res['total_visual_refs']}个")

        return "default"


class AssembleReportNode(Node):
    """
    报告组装节点

    功能：将各章节组装成完整报告
    """

    def prep(self, shared):
        """读取所有章节内容"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "sections": shared.get("report", {}).get("sections", {}),
            "stage3_data": shared.get("stage3_data", {})
        }

    def exec(self, prep_res):
        """组装完整报告"""
        template = prep_res["template"]
        sections = prep_res["sections"]
        stage3_data = prep_res["stage3_data"]

        # 生成报告头部
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        header = f"""# 舆情分析智能报告

**生成时间**: {current_time}
**分析工具**: 舆情分析智能体系统
**数据来源**: 社交媒体博文数据

---

## 报告摘要

本报告基于舆情分析智能体系统的自动分析结果，通过多维度数据分析和可视化图表，全面展现当前舆情态势。

"""

        # 组装各章节
        content_parts = [header]

        # 按模板顺序添加章节，如果没有对应的生成内容则跳过
        section_order = [
            ("summary", "报告摘要"),
            ("development", "舆情事件发展脉络"),
            ("trend", "舆情总体趋势分析"),
            ("focus", "焦点窗口专项分析"),
            ("spread", "传播场景分析"),
            ("content", "舆论内容结构分析"),
            ("belief", "信念系统分析"),
            ("region", "区域与空间认知差异"),
            ("risk", "舆情风险研判"),
            ("suggestion", "应对建议"),
            ("appendix", "附录")
        ]

        for section_key, section_title in section_order:
            if section_key in sections and sections[section_key]:
                content_parts.append(sections[section_key])
                content_parts.append("\n---\n")

        # 添加数据说明
        data_summary = f"""
## 数据说明

### 分析范围
- 总博文数: {stage3_data.get('analysis_data', {}).get('total_blogs', 'N/A')}
- 分析时段: {stage3_data.get('analysis_data', {}).get('time_range', 'N/A')}
- 图表数量: {len(stage3_data.get('analysis_data', {}).get('charts', []))}

### 智能体生成说明
本报告由舆情分析智能体系统自动生成，包括：
- 数据增强处理
- 多维度分析（情感趋势、主题演化、焦点窗口、地理分布、多维交互、信念体系）
- 可视化图表生成与深度分析
- 智能报告编排

---
*报告生成完成时间: {current_time}*
"""

        content_parts.append(data_summary)

        return "\n".join(content_parts)

    def post(self, shared, prep_res, exec_res):
        """存储组装后的报告"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["current_draft"] = exec_res
        shared["stage3_results"]["generation_mode"] = "template"

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}

        shared["thinking"]["stage3_report_planning"] = [{
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "planning_process": "基于模板结构组织报告章节，确保数据引用完整性",
            "organization_logic": "按舆情分析标准结构组织章节，包含摘要、发展脉络、趋势分析、传播分析、风险研判和建议等核心内容",
            "terminal_prompt": f"报告组装完成：模板模式 - 章节数：{len(shared.get('report', {}).get('sections', {}))}"
        }]

        print(f"[Stage3] 报告组装完成：模板模式 - 章节数：{len(shared.get('report', {}).get('sections', {}))}")

        return "default"


class InitReportStateNode(Node):
    """
    初始化报告状态节点

    功能：初始化迭代报告生成的状态
    """

    def prep(self, shared):
        """读取分析结果和配置"""
        return {
            "max_iterations": shared.get("config", {}).get("iterative_report_config", {}).get("max_iterations", 5),
            "stage3_data": shared.get("stage3_data", {})
        }

    def exec(self, prep_res):
        """初始化迭代状态"""
        return {
            "max_iterations": prep_res["max_iterations"],
            "current_iteration": 0,
            "review_history": [],
            "revision_feedback": "",
            "current_draft": ""
        }

    def post(self, shared, prep_res, exec_res):
        """设置报告状态"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"].update(exec_res)
        return "default"


class GenerateReportNode(Node):
    """
    报告生成节点

    功能：LLM生成或修改报告，确保数据引用和减少幻觉
    """

    def prep(self, shared):
        """准备报告生成所需数据"""
        return {
            "stage3_data": shared.get("stage3_data", {}),
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "revision_feedback": shared.get("report", {}).get("revision_feedback", ""),
            "iteration": shared.get("report", {}).get("iteration", 0)
        }

    def exec(self, prep_res):
        """调用LLM生成或修改报告，生成图文并茂的完整报告"""
        stage3_data = prep_res["stage3_data"]
        current_draft = prep_res["current_draft"]
        revision_feedback = prep_res["revision_feedback"]
        iteration = prep_res["iteration"]

        # 提取图表和表格信息
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])

        # 构建可用的图片和表格引用
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            file_path = normalize_path(chart.get('file_path', ''))
            if not file_path.startswith('./images/') and 'images' in file_path:
                filename = Path(file_path).name
                file_path = f'./images/{filename}'
            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        if iteration == 0:
            # 首次生成图文并茂报告
            prompt = f"""
你是一位专业的舆情分析师，需要基于完整的分析数据生成一份图文并茂的高质量舆情分析报告。

## 可用资源
### 图表资源（必须使用真实图片引用）
{json.dumps(available_charts, ensure_ascii=False, indent=2)}

### 表格资源（必须转换为markdown表格）
{json.dumps(available_tables, ensure_ascii=False, indent=2)}

### 洞察分析
{json.dumps(insights, ensure_ascii=False, indent=2)}

### 博文样本（用于典型案例）
{json.dumps(sample_blogs[:3], ensure_ascii=False, indent=2)}

## 核心要求
1. **图文并茂**：必须直接嵌入真实的图片和表格，使用markdown语法
2. **图片引用格式**：`![图表标题](图片路径)`
3. **表格转换**：将JSON表格数据转换为标准markdown表格格式
4. **数据驱动**：每个分析点都必须有对应的图表或表格支撑
5. **真实数据**：严禁使用虚构数据，所有图表和表格都必须来自提供的资源

## 报告结构（必须包含）
```markdown
# 舆情分析智能报告

## 报告摘要
![情感分布饼图](report/images/sentiment_pie_xxx.png)
![主题热度排行](report/images/topic_ranking_xxx.png)
简要分析数据...

## 舆情总体趋势分析
![情感趋势变化图](report/images/sentiment_trend_xxx.png)
![主题演化时序图](report/images/topic_evolution_xxx.png)
详细趋势分析...

## 焦点窗口专项分析
![焦点窗口情感趋势](report/images/sentiment_focus_window_xxx.png)
![焦点窗口发布者趋势](report/images/publisher_focus_distribution_xxx.png)
焦点窗口关键发现与预警...

## 传播场景分析
![发布者类型分布图](report/images/publisher_bar_xxx.png)
![地区分布图](report/images/geographic_bar_xxx.png)
传播特征分析...

## 舆论内容结构分析
![主题关联网络图](report/images/topic_network_xxx.png)
![交叉分析热力图](report/images/interaction_heatmap_xxx.png)
内容结构分析...

## 信念系统分析
信念节点激活与共现...

## 区域与空间认知差异
区域情感与归因差异...

## 舆情风险研判
基于数据的综合风险评估...

## 应对建议
基于分析结果的具体建议...

## 附录
数据范围与指标说明...
```

现在请生成完整的图文并茂报告：
"""
        else:
            # 基于反馈修改图文并茂报告
            prompt = f"""
你是一位专业的舆情分析师，需要根据评审意见修改图文并茂的舆情分析报告。

## 当前报告草稿
{current_draft}

## 评审修改意见
{revision_feedback}

## 可用资源（保持不变）
- 图表资源：{list(available_charts.keys())}
- 表格资源：{list(available_tables.keys())}

## 修改要求
1. **保持图文并茂**：确保修改后的报告仍包含所有必要的图片和表格
2. **数据准确性**：所有图表引用和表格数据必须准确无误
3. **改进数据支撑**：针对评审意见，增强薄弱环节的数据支撑
4. **视觉完整性**：确保每个分析章节都有对应的可视化元素
5. **解决指出问题**：明确说明如何解决了评审中提到的每个问题

请提供修改后的完整图文并茂报告：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 统计图文并茂元素
        image_refs = response.count("![") + response.count(".png")
        table_rows = response.count("|---")
        data_citations = response.count("图表") + response.count("数据") + response.count("如图")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_rows,
            "data_citations": data_citations,
            "visual_completeness": "high" if image_refs >= 3 and table_rows >= 2 else "medium" if image_refs >= 1 else "low"
        }

    def post(self, shared, prep_res, exec_res):
        """存储生成的报告草稿"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["current_draft"] = exec_res["content"]

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}

        thinking_record = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "planning_process": f"基于分析数据生成{'修改' if prep_res['iteration'] > 0 else '初始'}图文并茂报告",
            "organization_logic": "确保每个结论都有对应的图表或表格支撑，生成真正的图文并茂报告",
            "terminal_prompt": f"图文并茂报告{'修改' if prep_res['iteration'] > 0 else '生成'}完成：图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个，数据引用{exec_res['data_citations']}个，视觉完整度{exec_res['visual_completeness']}"
        }

        if "stage3_report_planning" not in shared["thinking"]:
            shared["thinking"]["stage3_report_planning"] = []

        shared["thinking"]["stage3_report_planning"].append(thinking_record)

        print(f"[Stage3] 图文并茂报告{'修改' if prep_res['iteration'] > 0 else '生成'}完成：")
        print(f"  - 图片引用：{exec_res['image_refs']}个")
        print(f"  - 表格引用：{exec_res['table_refs']}个")
        print(f"  - 数据引用：{exec_res['data_citations']}个")
        print(f"  - 视觉完整度：{exec_res['visual_completeness']}")

        return "default"


class ReviewReportNode(Node):
    """
    报告评审节点

    功能：LLM评审报告质量，重点核查数据支撑和减少幻觉
    """

    def prep(self, shared):
        """准备评审所需数据"""
        config = shared.get("config", {}).get("iterative_report_config", {})
        return {
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "iteration": shared.get("report", {}).get("iteration", 0),
            "satisfaction_threshold": config.get("satisfaction_threshold", 80),
            "max_iterations": shared.get("report", {}).get("max_iterations", 5)
        }

    def exec(self, prep_res):
        """调用LLM评审报告质量"""
        current_draft = prep_res["current_draft"]
        stage3_data = prep_res["stage3_data"]
        iteration = prep_res["iteration"]
        satisfaction_threshold = prep_res["satisfaction_threshold"]
        max_iterations = prep_res["max_iterations"]

        prompt = f"""
你是一位资深舆情分析专家，需要对以下舆情分析报告进行质量评审。

## 报告内容
{current_draft}

## 评审标准（每项20分，总分100分）
1. **结构完整性** (20分): 报告结构是否完整，逻辑是否清晰
2. **数据支撑充分性** (20分): 每个结论是否有足够的数据支撑
3. **图表引用准确性** (20分): 图表引用是否准确，分析是否到位
4. **逻辑连贯性** (20分): 分析逻辑是否连贯，推理是否合理
5. **建议可行性** (20分): 提出的建议是否具有可行性

## 可用数据参考
{json.dumps(stage3_data.get('analysis_data', {}), ensure_ascii=False, indent=2)[:2000]}...

## 评审要求
1. 逐项评分，并给出具体理由
2. 识别所有缺乏数据支撑的结论
3. 指出图表引用中的问题
4. 提供具体的修改建议
5. 给出总体评分和是否需要修改的判断

## 输出格式
请按以下格式输出评审结果：

```json
{{
    "structure_score": 15,
    "structure_comment": "结构完整性的评价...",
    "data_support_score": 18,
    "data_support_comment": "数据支撑的评价...",
    "chart_reference_score": 16,
    "chart_reference_comment": "图表引用的评价...",
    "logic_score": 17,
    "logic_comment": "逻辑连贯性的评价...",
    "suggestion_score": 15,
    "suggestion_comment": "建议可行性的评价...",
    "total_score": 81,
    "unsupported_conclusions": [
        "缺乏数据支撑的结论1",
        "缺乏数据支撑的结论2"
    ],
    "chart_reference_issues": [
        "图表引用问题1",
        "图表引用问题2"
    ],
    "revision_feedback": "具体的修改建议...",
    "needs_revision": true,
    "overall_assessment": "总体评价..."
}}
```

请进行评审：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 提取JSON结果
        try:
            json_start = response.find("{")
            json_end = response.rfind("}") + 1
            if json_start != -1 and json_end != -1:
                json_str = response[json_start:json_end]
                review_result = json.loads(json_str)
            else:
                raise ValueError("无法提取JSON评审结果")
        except Exception as e:
            # 如果JSON解析失败，使用默认评分
            review_result = {
                "structure_score": 15,
                "data_support_score": 15,
                "chart_reference_score": 15,
                "logic_score": 15,
                "suggestion_score": 15,
                "total_score": 75,
                "unsupported_conclusions": ["JSON解析失败，请人工检查"],
                "chart_reference_issues": ["JSON解析失败，请人工检查"],
                "revision_feedback": "JSON解析失败，建议人工检查报告内容",
                "needs_revision": True,
                "overall_assessment": "评审过程出现技术问题，建议人工检查"
            }

        # 计算数据支撑率
        data_support_rate = (review_result["data_support_score"] / 20) * 100

        return review_result

    def post(self, shared, prep_res, exec_res):
        """存储评审结果并决定下一步"""
        if "report" not in shared:
            shared["report"] = {}

        # 添加阈值和迭代次数到exec_res中
        exec_res["satisfaction_threshold"] = prep_res["satisfaction_threshold"]
        exec_res["current_iteration"] = prep_res["iteration"]
        exec_res["max_iterations"] = prep_res["max_iterations"]

        shared["report"]["last_review"] = exec_res
        shared["report"]["review_history"].append(exec_res)

        print(f"[Stage3] 评审完成 - 数据支撑率：{exec_res['total_score']/100*100:.0f}%，发现问题：{len(exec_res.get('unsupported_conclusions', []))}个")

        # 根据满意度阈值和最大迭代次数决定下一步
        total_score = exec_res.get("total_score", 0)
        current_iteration = prep_res["iteration"]
        max_iterations = prep_res["max_iterations"]
        satisfaction_threshold = prep_res["satisfaction_threshold"]

        # 如果达到满意度阈值或达到最大迭代次数，则结束迭代
        if total_score >= satisfaction_threshold:
            print(f"[Stage3] 报告质量达标（{total_score} >= {satisfaction_threshold}），结束迭代")
            return "satisfied"
        elif current_iteration >= max_iterations - 1:  # current_iteration从0开始
            print(f"[Stage3] 达到最大迭代次数（{max_iterations}），强制结束迭代")
            return "satisfied"
        else:
            print(f"[Stage3] 报告需要继续改进（{total_score} < {satisfaction_threshold}），进入下一轮迭代")
            return "needs_revision"


class ApplyFeedbackNode(Node):
    """
    应用修改意见节点

    功能：处理评审意见，准备下一轮迭代
    """

    def prep(self, shared):
        """读取评审意见"""
        return shared.get("report", {}).get("last_review", {})

    def exec(self, prep_res):
        """格式化修改意见"""
        revision_feedback = prep_res.get("revision_feedback", "")

        if not revision_feedback:
            revision_feedback = "无明显问题，报告质量良好。"

        # 添加具体的修改建议
        feedback_details = []

        if prep_res.get("unsupported_conclusions"):
            feedback_details.append("需要补充数据支撑的结论：")
            for conclusion in prep_res["unsupported_conclusions"][:5]:
                feedback_details.append(f"- {conclusion}")

        if prep_res.get("chart_reference_issues"):
            feedback_details.append("图表引用问题：")
            for issue in prep_res["chart_reference_issues"][:5]:
                feedback_details.append(f"- {issue}")

        if feedback_details:
            revision_feedback += "\n\n具体问题：\n" + "\n".join(feedback_details)

        return revision_feedback

    def post(self, shared, prep_res, exec_res):
        """存储修改意见并更新迭代计数"""
        if "report" not in shared:
            shared["report"] = {}

        # 增加迭代计数
        shared["report"]["iteration"] = shared["report"].get("iteration", 0) + 1
        shared["report"]["revision_feedback"] = exec_res

        # 检查是否达到最大迭代次数
        current_iteration = shared["report"]["iteration"]
        max_iterations = shared["report"].get("max_iterations", 5)

        if current_iteration >= max_iterations:
            print(f"[Stage3] 达到最大迭代次数 ({max_iterations})，结束迭代")
            return "max_iterations_reached"
        else:
            print(f"[Stage3] 开始第 {current_iteration + 1} 轮迭代")
            return "continue_iteration"


class Stage3CompletionNode(Node):
    """
    阶段3完成节点

    功能：标记阶段3完成，返回调度器
    """

    def prep(self, shared):
        """读取当前阶段状态"""
        return {
            "report_file": shared.get("stage3_results", {}).get("report_file", ""),
            "generation_mode": shared.get("stage3_results", {}).get("generation_mode", ""),
            "iterations": shared.get("report", {}).get("iteration", 0),
            "final_score": shared.get("report", {}).get("last_review", {}).get("total_score", 0)
        }

    def exec(self, prep_res):
        """确认阶段完成"""
        return True

    def post(self, shared, prep_res, exec_res):
        """更新已完成阶段列表，返回调度器"""
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}

        dispatcher = shared["dispatcher"]
        if "completed_stages" not in dispatcher:
            dispatcher["completed_stages"] = []

        if 3 not in dispatcher["completed_stages"]:
            dispatcher["completed_stages"].append(3)

        dispatcher["current_stage"] = 3

        print(f"\n[Stage3] 阶段3完成 - 报告生成模式: {prep_res['generation_mode']}")
        if prep_res["iterations"] > 0:
            print(f"[Stage3] 迭代次数: {prep_res['iterations']}")
        if prep_res["final_score"] > 0:
            print(f"[Stage3] 最终评分: {prep_res['final_score']}/100")

        # 返回dispatch让DispatcherNode决定下一步
        return "dispatch"


class GenerateFullReportNode(Node):
    """
    一次性完整报告生成节点

    功能：基于模板和分析结果，一次性生成完整的舆情分析报告
    替代原有的分章节生成模式，确保报告的一致性和数据引用的准确性
    """

    def prep(self, shared):
        """准备报告生成所需的所有数据"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "config": shared.get("config", {})
        }

    def exec(self, prep_res):
        """一次性生成完整的舆情分析报告"""
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]

        # 提取所有分析数据
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])
        images_dir = stage3_data.get('images_dir', '')

        # 构建可用的图片和表格引用字典
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            # 标准化图片路径为跨平台兼容的相对路径
            file_path = normalize_path(chart.get('file_path', ''))
            # 确保图片路径使用统一的相对路径格式
            if not file_path.startswith('./images/') and 'images' in file_path:
                # 提取文件名并构造标准路径
                filename = Path(file_path).name
                file_path = f'./images/{filename}'

            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        # 构建详细的分析结果摘要
        detailed_analysis_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                chart_info = analysis.get("chart_info", {})
                micro_details = analysis.get("microscopic_details", {})
                macro_insights = analysis.get("macroscopic_insights", {})
                quality = analysis.get("quality_assessment", {})

                # 提取关键统计信息
                stats = micro_details.get("statistics", {})
                data_points_count = len(micro_details.get("data_points", []))

                # 提取关键发现
                key_findings = macro_insights.get("key_findings", [])
                trends = macro_insights.get("trend_analysis", {})

                analysis_summary = {
                    "chart_id": chart_id,
                    "title": title,
                    "chart_type": chart_info.get("chart_type", "unknown"),
                    "data_points_count": data_points_count,
                    "max_value": stats.get("max_value", {}),
                    "min_value": stats.get("min_value", {}),
                    "key_findings": [f.get("finding", "") for f in key_findings[:3]],
                    "primary_trend": trends.get("primary_trend", ""),
                    "quality_score": {
                        "density": quality.get("information_density", "unknown"),
                        "readability": quality.get("readability", "unknown")
                    }
                }
                detailed_analysis_summary.append(analysis_summary)

        # 构建增强的提示词
        prompt = f"""
你是一位资深的舆情分析专家，需要基于以下极其详细的数据分析结果，生成一份高质量的舆情分析报告。

## 核心要求
1. **充分利用详细分析**：充分利用GLM4.5V提供的微观细节和宏观洞察
2. **引用具体数据点**：在报告中引用具体的数据细节（如最高值、最低值、数据点数量）
3. **结合趋势分析**：利用图表分析中的趋势发现和转折点分析
4. **数据驱动结论**：每个结论都要有来自chart_analyses的具体支撑
5. **覆盖模板章节**：按模板包含摘要、发展脉络、总体趋势、焦点窗口、传播、内容结构、信念、区域、风险、建议、附录

## 详细分析数据摘要

### 增强版图表分析结果
{json.dumps(detailed_analysis_summary, ensure_ascii=False, indent=2)}

### 完整图表数据
{json.dumps(charts, ensure_ascii=False, indent=2)}

### 详细统计表格
{json.dumps(tables, ensure_ascii=False, indent=2)}

### 综合洞察描述
{json.dumps(insights, ensure_ascii=False, indent=2)}

## 报告生成指导

### 数据引用方式
- **微观细节引用**：根据chart_analyses中的统计分析，{detailed_analysis_summary[0]['title'] if detailed_analysis_summary else '某图表'}显示最高值为{detailed_analysis_summary[0]['max_value'].get('value', 'N/A') if detailed_analysis_summary else 'N/A'}
- **趋势分析引用**：基于GLM4.5V的趋势分析，主要趋势为{detailed_analysis_summary[0]['primary_trend'] if detailed_analysis_summary else 'N/A'}
- **关键发现引用**：图表分析发现：{'; '.join(detailed_analysis_summary[0]['key_findings'][:2]) if detailed_analysis_summary else 'N/A'}

### 报告章节和内容要求
1. **报告摘要** - 基于insights和关键图表，突出最重要的数据指标
2. **舆情事件发展脉络** - 利用时序趋势与转折点
3. **舆情总体趋势分析** - 情感/主题总体演化
4. **焦点窗口专项分析** - 独立窗口内的情感、发布者、主题对比与预警
5. **传播场景分析** - 地域与发布者分布、交互特征
6. **舆论内容结构分析** - 主题网络、共现、排行
7. **信念系统分析** - 信念节点激活与网络
8. **区域与空间认知差异** - 区域情感、归因差异
9. **舆情风险研判** - 结合情感/主题/传播指标
10. **应对建议** - 针对发现的问题提出措施
11. **附录** - 数据范围与指标说明

### 输出要求
- 使用标准Markdown格式
- 每个分析点都要引用具体的数据细节
- 图片引用使用相对路径：`![图表标题](./images/文件名.png)`
- 所有结论都要标注数据来源（如：根据chart_analyses中的统计显示...）
- 确保报告内容既有微观细节又有宏观洞察

现在请生成高质量的舆情分析报告：
"""

        # 调用LLM生成报告
        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 修正报告中的图片路径，确保跨平台兼容性
        path_replacements = [
            ("report\\images\\", "./images/"),    # Windows格式
            ("report/images/", "./images/"),     # Unix格式
            ("./report/images/", "./images/"),   # 相对路径
            ("../report/images/", "./images/"),  # 上级目录相对路径
        ]

        for old_path, new_path in path_replacements:
            response = response.replace(old_path, new_path)

        return response

    def post(self, shared, prep_res, exec_res):
        """存储生成的完整报告"""
        if "report" not in shared:
            shared["report"] = {}

        # 存储完整报告内容
        shared["report"]["full_content"] = exec_res
        shared["report"]["generation_mode"] = "one_shot"

        # 记录生成信息
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["generation_mode"] = "one_shot"
        shared["stage3_results"]["current_draft"] = exec_res

        print(f"[Stage3] 完整报告已生成（一次性模式）")
        print(f"[Stage3] 报告长度：{len(exec_res)} 字符")

        # 统计图片引用数量
        image_refs = exec_res.count('![')
        print(f"[Stage3] 图片引用：{image_refs} 个")

        return "default"

        return "dispatch"
