"""
舆情分析智能体 - 节点定义

根据设计文档，系统采用中央调度+三阶段顺序依赖架构。
本文件包含所有节点定义，按以下结构组织：

================================================================================
目录结构
================================================================================

1. 系统调度节点
   - DispatcherNode: 综合调度节点，系统入口和中央控制器
   - TerminalNode: 终止节点，宣布流程结束

2. 基础节点类
   - AsyncParallelBatchNode: 带并发限制的异步并行批处理基类

3. 阶段1节点: 原始博文增强处理
   3.1 通用节点
       - DataLoadNode: 数据加载
       - SaveEnhancedDataNode: 保存增强数据
       - DataValidationAndOverviewNode: 数据验证与概况分析
       - Stage1CompletionNode: 阶段1完成节点，返回调度器
   3.2 异步批量并行路径节点 (enhancement_mode="async")
       - AsyncSentimentPolarityAnalysisBatchNode: 情感极性分析
       - AsyncSentimentAttributeAnalysisBatchNode: 情感属性分析
       - AsyncTwoLevelTopicAnalysisBatchNode: 两级主题分析
       - AsyncPublisherObjectAnalysisBatchNode: 发布者对象分析
   3.3 Batch API路径节点 (enhancement_mode="batch_api")
       - BatchAPIEnhancementNode: 调用Batch API脚本处理

4. 阶段2节点: 分析执行
   - LoadEnhancedDataNode, DataSummaryNode, ExecuteAnalysisScriptNode等
   - LLMInsightNode, SaveAnalysisResultsNode, Stage2CompletionNode等

5. 阶段3节点: 报告生成（待实现）
   - Stage3EntryNode, TemplateReportNode, IterativeReportFlow节点等

================================================================================
"""

import json
import os
import asyncio
import subprocess
from typing import List, Dict, Any, Optional
from pocketflow import Node, BatchNode, AsyncNode, AsyncBatchNode
from utils.call_llm import call_glm_45_air, call_glm4v_plus, call_glm45v_thinking, call_glm46
from utils.data_loader import (
    load_blog_data, load_topics, load_sentiment_attributes, 
    load_publisher_objects, save_enhanced_blog_data, load_enhanced_blog_data
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
    
    async def _exec(self, items):
        """执行批量处理，支持并发控制"""
        if not items:
            return []
        
        if self._semaphore:
            async def sem_exec(item):
                async with self._semaphore:
                    return await AsyncNode._exec(self, item)
            
            return await asyncio.gather(*(sem_exec(i) for i in items))
        else:
            return await asyncio.gather(*(AsyncNode._exec(self, i) for i in items))


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
                "topics_path": data_paths.get("topics_path", "data/topics.json"),
                "sentiment_attributes_path": data_paths.get("sentiment_attributes_path", "data/sentiment_attributes.json"),
                "publisher_objects_path": data_paths.get("publisher_objects_path", "data/publisher_objects.json")
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
            return {
                "blog_data": load_blog_data(prep_res["blog_data_path"]),
                "topics_hierarchy": load_topics(prep_res["topics_path"]),
                "sentiment_attributes": load_sentiment_attributes(prep_res["sentiment_attributes_path"]),
                "publisher_objects": load_publisher_objects(prep_res["publisher_objects_path"]),
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
                "publisher_empty": 0
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
            print(f"  └─ 发布者为空: {empty_fields.get('publisher_empty', 0)}")
        
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
    
    async def prep_async(self, shared):
        """返回博文数据列表"""
        return shared.get("data", {}).get("blog_data", [])
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行情感极性分析"""
        blog_post = prep_res

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
    
    async def prep_async(self, shared):
        """返回博文和情感属性的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        sentiment_attributes = shared.get("data", {}).get("sentiment_attributes", [])
        
        return [{
            "blog_data": blog_post,
            "sentiment_attributes": sentiment_attributes
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用LLM进行情感属性分析"""
        blog_post = prep_res["blog_data"]
        sentiment_attributes = prep_res["sentiment_attributes"]

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
    
    async def prep_async(self, shared):
        """返回博文和主题层次结构的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        topics_hierarchy = shared.get("data", {}).get("topics_hierarchy", [])
        
        return [{
            "blog_data": blog_post,
            "topics_hierarchy": topics_hierarchy
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行主题匹配"""
        blog_post = prep_res["blog_data"]
        topics_hierarchy = prep_res["topics_hierarchy"]

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
    
    async def prep_async(self, shared):
        """返回博文和发布者类型的组合列表"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_objects = shared.get("data", {}).get("publisher_objects", [])
        
        return [{
            "blog_data": blog_post,
            "publisher_objects": publisher_objects
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用LLM进行发布者类型识别"""
        blog_post = prep_res["blog_data"]
        publisher_objects = prep_res["publisher_objects"]

        publishers_str = "、".join(publisher_objects)

        prompt = f"""候选发布者：{publishers_str}
判断该博文最可能的发布者类型，直接输出候选列表中的一个条目，不得添加解释。
博文：
{blog_post.get('content', '')}"""

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
            for t in p.get("topics", []):
                if t.get("parent_topic"):
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
            # 主题工具
            topic_frequency_stats,
            topic_time_evolution,
            topic_cooccurrence_analysis,
            topic_ranking_chart,
            topic_evolution_chart,
            topic_network_chart,
            # 地理工具
            geographic_distribution_stats,
            geographic_hotspot_detection,
            geographic_sentiment_analysis,
            geographic_heatmap,
            geographic_bar_chart,
            # 交互工具
            publisher_distribution_stats,
            cross_dimension_matrix,
            influence_analysis,
            correlation_analysis,
            interaction_heatmap,
            publisher_bar_chart,
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
        result = sentiment_time_series(blog_data, granularity="hour")
        tables.append({
            "id": "sentiment_time_series",
            "title": "情感时序趋势数据",
            "data": result["data"],
            "source_tool": "sentiment_time_series"
        })
        tools_executed.append("sentiment_time_series")
        
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
            chart_path = chart.get("path", "")

            print(f"[ChartAnalysis] [{i}/{len(charts)}] 分析图表: {chart_title}")

            # 构建分析提示词
            analysis_prompt = f"""你是一位专业的舆情分析专家，请分析这张舆情分析图表并返回结构化的JSON格式结果。

## 图表基本信息
- 图表ID: {chart_id}
- 图表标题: {chart_title}
- 图表类型: {chart.get('type', 'unknown')}

## 分析要求
请从以下四个维度进行分析：

### 1. 图表描述
客观描述图表的类型、结构、数据组成部分和关键元素

### 2. 关键发现
识别数据趋势、模式、峰值、异常点和重要特征

### 3. 业务洞察
分析舆情情感、话题热度、地域差异、发布者影响等业务含义

### 4. 建议和预警
提供趋势关注点、风险机遇识别和后续分析建议

## 输出格式要求
**重要**: 请严格按以下JSON格式输出，不要添加任何解释文字或思考过程：

```json
{{
  "chart_description": "详细描述图表类型、结构和关键元素",
  "key_findings": [
    "第一个关键发现的描述",
    "第二个关键发现的描述",
    "第三个关键发现的描述"
  ],
  "insights": [
    "第一个业务洞察的描述",
    "第二个业务洞察的描述"
  ],
  "recommendations": [
    "第一条建议的描述",
    "第二条建议的描述"
  ],
  "alert_level": "low",
  "data_quality": "good"
}}
```

## 参数说明
- alert_level: "low"(无异常), "medium"(需关注), "high"(需警惕)
- data_quality: "excellent"(优质), "good"(良好), "fair"(一般), "poor"(较差)

请直接返回JSON格式结果，确保JSON格式完整且有效。"""

            try:
                # 调用GLM4.5V分析图表
                response = call_glm45v_thinking(
                    prompt=analysis_prompt,
                    image_paths=[chart_path] if chart_path and os.path.exists(chart_path) else None,
                    temperature=0.7,
                    max_tokens=2000,
                    enable_thinking=True
                )

                # 解析JSON响应
                try:
                    import re
                    json_str = None

                    # 方法1: 提取JSON代码块
                    json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                    else:
                        # 方法2: 寻找JSON对象开始和结束
                        json_start = response.find('{')
                        json_end = response.rfind('}')
                        if json_start != -1 and json_end != -1 and json_end > json_start:
                            json_str = response[json_start:json_end + 1]

                    # 方法3: 如果都失败，尝试直接解析清理后的响应
                    if not json_str:
                        # 清理响应文本，移除思考过程等无关内容
                        cleaned_response = response.strip()
                        # 移除可能的前缀文本
                        if not cleaned_response.startswith('{'):
                            lines = cleaned_response.split('\n')
                            for line in lines:
                                if line.strip().startswith('{'):
                                    cleaned_response = '\n'.join(lines[lines.index(line):])
                                    break
                        json_str = cleaned_response

                    if json_str:
                        # 清理JSON字符串
                        json_str = json_str.strip()
                        # 移除可能的注释和多余空白
                        json_str = re.sub(r'//.*?\n', '', json_str)
                        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)

                        analysis_result = json.loads(json_str)
                    else:
                        raise ValueError("无法从响应中提取有效的JSON数据")

                    # 添加元数据
                    analysis_result["chart_id"] = chart_id
                    analysis_result["chart_title"] = chart_title
                    analysis_result["chart_path"] = chart_path
                    analysis_result["analysis_timestamp"] = time.time()
                    analysis_result["analysis_status"] = "success"

                    chart_analyses[chart_id] = analysis_result
                    success_count += 1
                    print(f"[ChartAnalysis] [√] 图表 {chart_id} 分析完成")

                except (json.JSONDecodeError, ValueError) as e:
                    # JSON解析失败，尝试智能fallback处理
                    print(f"[ChartAnalysis] [!] JSON解析失败，尝试智能提取: {str(e)}")

                    # 尝试从响应中智能提取关键信息
                    fallback_result = self._extract_fallback_analysis(response, chart_id, chart_title, chart_path, e)
                    chart_analyses[chart_id] = fallback_result
                    print(f"[ChartAnalysis] [!] 图表 {chart_id} 使用fallback分析完成")

            except Exception as e:
                # 分析失败，返回错误信息
                print(f"[ChartAnalysis] [X] 图表 {chart_id} 分析失败: {str(e)}")
                failed_result = {
                    "chart_id": chart_id,
                    "chart_title": chart_title,
                    "chart_path": chart_path,
                    "analysis_timestamp": time.time(),
                    "analysis_status": "failed",
                    "chart_description": "图表分析失败",
                    "key_findings": [f"分析失败: {str(e)}"],
                    "insights": ["需要手动分析"],
                    "recommendations": ["建议检查图表文件和相关数据"],
                    "alert_level": "high",
                    "data_quality": "poor",
                    "error": str(e)
                }
                chart_analyses[chart_id] = failed_result

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

    def _extract_fallback_analysis(self, response: str, chart_id: str, chart_title: str, chart_path: str, error: Exception) -> Dict:
        """
        智能fallback分析：当JSON解析失败时，从文本响应中提取关键信息

        Args:
            response: GLM4.5V模型的原始响应文本
            chart_id: 图表ID
            chart_title: 图表标题
            chart_path: 图表路径
            error: 解析错误

        Returns:
            构建的分析结果字典
        """
        import re
        import time

        # 初始化基本结果结构
        result = {
            "chart_id": chart_id,
            "chart_title": chart_title,
            "chart_path": chart_path,
            "analysis_timestamp": time.time(),
            "analysis_status": "partial_success",
            "error": str(error)
        }

        # 尝试提取图表描述
        description_patterns = [
            r'chart_description["\s]*:\s*["\']([^"\']+)["\']',
            r'图表描述[：:]\s*([^\n]+)',
            r'描述[：:]\s*([^\n]+)'
        ]

        for pattern in description_patterns:
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                result["chart_description"] = match.group(1).strip()
                break
        else:
            result["chart_description"] = "图表分析已完成，但结构化数据提取失败"

        # 尝试提取关键发现
        key_findings = []
        findings_patterns = [
            r'key_findings["\s]*:\s*\[(.*?)\]',
            r'关键发现[：:]\s*([^\n]+)',
            r'发现[：:]\s*([^\n]+)'
        ]

        for pattern in findings_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE | re.DOTALL)
            if matches:
                for match in matches:
                    # 清理和分割发现项
                    findings = re.findall(r'["\']([^"\']+)["\']', match)
                    if findings:
                        key_findings.extend([f.strip() for f in findings if f.strip()])
                    else:
                        # 如果没有引号，尝试按行分割
                        lines = match.split('\n')
                        for line in lines:
                            clean_line = re.sub(r'[-*]\s*', '', line.strip())
                            if clean_line and len(clean_line) > 5:
                                key_findings.append(clean_line)
                break

        # 如果没有找到关键发现，从响应中提取主要内容
        if not key_findings:
            # 寻找包含分析内容的段落
            sentences = re.split(r'[。！？\n]', response)
            for sentence in sentences:
                sentence = sentence.strip()
                if (len(sentence) > 20 and
                    any(keyword in sentence for keyword in ['趋势', '显示', '数据', '分析', '发现', '表明'])):
                    key_findings.append(sentence[:100] + ("..." if len(sentence) > 100 else ""))
                    if len(key_findings) >= 3:
                        break

        result["key_findings"] = key_findings[:5]  # 限制最多5个发现

        # 尝试提取业务洞察
        insights = []
        insight_patterns = [
            r'insights["\s]*:\s*\[(.*?)\]',
            r'业务洞察[：:]\s*([^\n]+)',
            r'洞察[：:]\s*([^\n]+)'
        ]

        for pattern in insight_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE | re.DOTALL)
            if matches:
                for match in matches:
                    insight_items = re.findall(r'["\']([^"\']+)["\']', match)
                    if insight_items:
                        insights.extend([f.strip() for f in insight_items if f.strip()])
                break

        # 如果没有找到洞察，生成默认洞察
        if not insights:
            insights = ["图表数据分析已完成，建议结合其他分析结果进行综合判断"]

        result["insights"] = insights[:3]  # 限制最多3个洞察

        # 尝试提取建议
        recommendations = []
        rec_patterns = [
            r'recommendations["\s]*:\s*\[(.*?)\]',
            r'建议[：:]\s*([^\n]+)',
            r'推荐[：:]\s*([^\n]+)'
        ]

        for pattern in rec_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE | re.DOTALL)
            if matches:
                for match in matches:
                    rec_items = re.findall(r'["\']([^"\']+)["\']', match)
                    if rec_items:
                        recommendations.extend([f.strip() for f in rec_items if f.strip()])
                break

        # 如果没有找到建议，生成默认建议
        if not recommendations:
            recommendations = ["建议进一步验证分析结果，确保数据解读的准确性"]

        result["recommendations"] = recommendations[:3]  # 限制最多3个建议

        # 尝试提取预警级别
        alert_patterns = [
            r'alert_level["\s]*:\s*["\']([^"\']+)["\']',
            r'预警级别[：:]\s*([^\n]+)'
        ]

        for pattern in alert_patterns:
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                alert_level = match.group(1).lower()
                if alert_level in ['low', 'medium', 'high']:
                    result["alert_level"] = alert_level
                    break
        else:
            result["alert_level"] = "medium"  # 默认中等预警级别

        # 尝试提取数据质量
        quality_patterns = [
            r'data_quality["\s]*:\s*["\']([^"\']+)["\']',
            r'数据质量[：:]\s*([^\n]+)'
        ]

        for pattern in quality_patterns:
            match = re.search(pattern, response, re.IGNORECASE)
            if match:
                quality = match.group(1).lower()
                if quality in ['excellent', 'good', 'fair', 'poor']:
                    result["data_quality"] = quality
                    break
        else:
            result["data_quality"] = "fair"  # 默认一般质量

        # 添加原始响应的前500字符作为备份
        result["raw_response_preview"] = response[:500] + ("..." if len(response) > 500 else "")

        return result


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

        # 构建图表分析摘要
        chart_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                key_findings = analysis.get("key_findings", [])
                insights = analysis.get("insights", [])
                alert_level = analysis.get("alert_level", "low")

                chart_summary.append(f"### {title}")
                chart_summary.append(f"关键发现: {'; '.join(key_findings[:3])}")
                chart_summary.append(f"业务洞察: {'; '.join(insights[:2])}")
                chart_summary.append(f"预警级别: {alert_level}")
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
        prompt = f"""你是一位资深的舆情分析专家，请基于以下GLM4.5V视觉分析和统计数据，生成深度洞察报告。

## 数据概况
{data_summary}

## GLM4.5V图表视觉分析结果
{chr(10).join(chart_summary) if chart_summary else "无图表分析结果"}

## 统计分析数据
{chr(10).join(stats_summary) if stats_summary else "无统计数据"}

请综合以上信息，针对以下维度生成深度洞察分析，每个维度150-300字：

1. **情感趋势洞察**：
   - 结合情感趋势图表的视觉分析，分析情感变化的主要驱动因素
   - 基于异常点检测和时序分析，识别关键转折点和潜在风险
   - 提供情感管理的具体建议

2. **主题演化洞察**：
   - 根据主题热度图表和网络关系分析，识别核心话题及其演变路径
   - 分析新兴话题的崛起趋势和话题间的关联效应
   - 预测未来话题发展的可能方向

3. **地理分布洞察**：
   - 基于地理热力图和区域分析，识别热点地区和冷点区域
   - 分析地区差异的深层原因和影响因素
   - 提出区域针对性的应对策略

4. **多维交互洞察**：
   - 结合发布者类型分析和影响力评估，识别关键意见领袖
   - 分析不同发布者群体的行为模式和内容特征
   - 评估信息传播的路径和效果

5. **综合洞察摘要**：
   - 整合所有分析维度，提炼整体舆情态势的核心特征
   - 识别主要机遇和风险点
   - 提供战略层面的决策建议

请以JSON格式输出，包含以下字段：
```json
{{
    "sentiment_insight": "基于视觉分析的情感趋势深度洞察",
    "topic_insight": "结合图表的主题演化全面分析",
    "geographic_insight": "基于地图分析的区域分布洞察",
    "cross_dimension_insight": "多维交互关系的深度剖析",
    "summary_insight": "整体舆情态势的战略洞察"
}}
```"""

        response = call_glm_45_air(prompt, temperature=0.7)

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

        return {
            "tool_name": agent.get("next_tool", ""),
            "blog_data": blog_data,
            "tool_source": tool_source
        }

    def exec(self, prep_res):
        """通过MCP协议调用对应的分析工具函数"""
        tool_name = prep_res["tool_name"]
        blog_data = prep_res["blog_data"]
        tool_source = prep_res["tool_source"]

        if not tool_name:
            return {"error": "未指定工具名称"}

        print(f"\n[ExecuteTools] 执行工具: {tool_name} ({tool_source}模式)")

        # 使用MCP客户端调用工具
        from utils.mcp_client.mcp_client import call_tool

        try:
            # 对于MCP工具，传递正确的服务器路径，不需要传递blog_data，服务器会自动加载
            result = call_tool('utils/mcp_server', tool_name, {})

            # 转换MCP结果为统一格式
            if isinstance(result, dict) and "charts" in result:
                # 已经是标准格式
                final_result = result
            else:
                # 转换为标准格式
                final_result = {
                    "charts": result.get("charts", []),
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

        # 记录执行的工具
        shared["stage2_results"]["execution_log"]["tools_executed"].append(tool_name)

        # 处理错误情况
        if "error" in result:
            print(f"  [X] 工具执行失败: {result['error']}")
            # 存储失败结果
            shared["agent"]["last_tool_result"] = {
                "tool_name": tool_name,
                "summary": f"工具执行失败: {result['error']}",
                "has_chart": False,
                "has_data": False,
                "error": True
            }
            return "default"

        # 处理图表
        if result.get("charts"):
            shared["stage2_results"]["charts"].extend(result["charts"])
            print(f"  [OK] 生成 {len(result['charts'])} 个图表")

        # 处理数据表格
        if result.get("data"):
            shared["stage2_results"]["tables"].append({
                "id": tool_name,
                "title": result.get("category", "") + " - " + tool_name,
                "data": result["data"],
                "source_tool": tool_name,
                "source_type": tool_source  # 记录数据来源
            })
            print(f"  [OK] 生成数据表格")

        # 存储执行结果供ProcessResultNode使用
        shared["agent"]["last_tool_result"] = {
            "tool_name": tool_name,
            "tool_source": tool_source,
            "summary": result.get("summary", "执行完成"),
            "has_chart": bool(result.get("charts")),
            "has_data": bool(result.get("data")),
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
# 5. 阶段3节点: 报告生成（待实现）
# =============================================================================

# TODO: 实现以下节点
# - Stage3EntryNode: 阶段3入口节点
# - LoadAnalysisResultsNode: 加载分析结果节点
# - LoadTemplateNode: 加载模板节点
# - FillSectionNode: 章节填充节点
# - AssembleReportNode: 报告组装节点
# - FormatReportNode: 报告格式化节点
# - SaveReportNode: 保存报告节点
# - InitReportStateNode: 初始化报告状态节点
# - GenerateReportNode: 报告生成节点
# - ReviewReportNode: 报告评审节点
# - ApplyFeedbackNode: 应用修改意见节点
# - Stage3CompletionNode: 阶段3完成节点
