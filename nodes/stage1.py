"""
stage1.py - 阶段1节点：原始博文增强处理

包含通用数据加载/保存/验证节点 + 6个异步分析批处理节点。
"""

import json
import os
import asyncio
from typing import Any, Dict, List, Optional
from collections import Counter, defaultdict
from datetime import datetime

from pocketflow import Node

from nodes.base import AsyncParallelBatchNode
from nodes._utils import normalize_path
from utils.call_llm import call_glm_45_air, call_glm4v_plus
from utils.data_loader import (
    load_blog_data, load_topics, load_sentiment_attributes,
    load_publisher_objects, save_enhanced_blog_data, load_enhanced_blog_data,
    load_belief_system, load_publisher_decisions
)




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
