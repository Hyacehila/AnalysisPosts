"""
数据处理模块节点

根据设计文档要求，实现以下节点：
1. 数据加载节点 (DataLoadNode)
2. 情感极性分析BatchNode (SentimentPolarityAnalysisBatchNode)
3. 情感属性分析BatchNode (SentimentAttributeAnalysisBatchNode)
4. 两级主题分析BatchNode (TwoLevelTopicAnalysisBatchNode)
5. 发布者对象分析BatchNode (PublisherObjectAnalysisBatchNode)
6. 数据验证与概况分析节点 (DataValidationAndOverviewNode)
"""

import json
import os
import asyncio
from typing import List, Dict, Any, Optional
from pocketflow import Node, BatchNode, AsyncNode
from utils.call_llm import call_glm_45_air, call_glm4v_plus
from utils.data_loader import (
    load_blog_data, load_topics, load_sentiment_attributes, 
    load_publisher_objects, save_enhanced_blog_data, load_enhanced_blog_data
)


class AsyncParallelBatchNode(AsyncNode, BatchNode):
    """
    带并发限制的异步并行批处理节点
    支持通过 max_concurrent 参数控制并发执行数量
    """
    
    def __init__(self, max_concurrent=None, **kwargs):
        super().__init__(**kwargs)
        self.max_concurrent = max_concurrent
        # ✅ 正确：在构造时创建信号量（实例级别共享）
        self._semaphore = (
            asyncio.Semaphore(max_concurrent) 
            if max_concurrent else None
        )
    
    async def _exec(self, items):
        if not items:
            return []
            
        # 使用实例属性中的信号量
        if self._semaphore:
            async def sem_exec(item):
                async with self._semaphore:  # 同一实例的所有调用共享
                    return await AsyncNode._exec(self, item)
            
            return await asyncio.gather(*(sem_exec(i) for i in items))
        else:
            return await asyncio.gather(*(AsyncNode._exec(self, i) for i in items))


class DataLoadNode(Node):
    """
    数据加载节点
    Purpose: 加载原始博文数据或增强数据
    Type: Regular Node
    """
    
    def prep(self, shared):
        """读取数据文件路径和配置参数，根据配置判断加载原始数据还是增强数据"""
        config = shared.get("config", {})
        data_paths = shared.get("data", {}).get("data_paths", {})
        
        # 根据配置决定数据源类型
        data_source_type = config.get("data_source", {}).get("type", "original")
        
        if data_source_type == "enhanced":
            # 加载增强数据
            enhanced_data_path = config.get("data_source", {}).get("enhanced_data_path", "data/enhanced_blogs.json")
            return {
                "load_type": "enhanced",
                "data_path": enhanced_data_path
            }
        else:
            # 加载原始数据
            blog_data_path = data_paths.get("blog_data_path", "data/beijing_rainstorm_posts.json")
            topics_path = data_paths.get("topics_path", "data/topics.json")
            sentiment_attributes_path = data_paths.get("sentiment_attributes_path", "data/sentiment_attributes.json")
            publisher_objects_path = data_paths.get("publisher_objects_path", "data/publisher_objects.json")
            
            return {
                "load_type": "original",
                "blog_data_path": blog_data_path,
                "topics_path": topics_path,
                "sentiment_attributes_path": sentiment_attributes_path,
                "publisher_objects_path": publisher_objects_path
            }
    
    def exec(self, prep_res):
        """加载JSON格式博文数据，验证数据格式完整性"""
        if prep_res["load_type"] == "enhanced":
            # 加载增强数据
            enhanced_data = load_enhanced_blog_data(prep_res["data_path"])
            return {
                "blog_data": enhanced_data,
                "load_type": "enhanced"
            }
        else:
            # 加载原始数据和相关配置数据
            blog_data = load_blog_data(prep_res["blog_data_path"])
            topics_hierarchy = load_topics(prep_res["topics_path"])
            sentiment_attributes = load_sentiment_attributes(prep_res["sentiment_attributes_path"])
            publisher_objects = load_publisher_objects(prep_res["publisher_objects_path"])
            
            return {
                "blog_data": blog_data,
                "topics_hierarchy": topics_hierarchy,
                "sentiment_attributes": sentiment_attributes,
                "publisher_objects": publisher_objects,
                "load_type": "original"
            }
    
    def post(self, shared, prep_res, exec_res):
        """将数据存储到shared中"""
        # 初始化data结构（如果不存在）
        if "data" not in shared:
            shared["data"] = {}
        
        # 存储博文数据
        shared["data"]["blog_data"] = exec_res["blog_data"]
        # 存储加载类型
        shared["data"]["load_type"] = exec_res["load_type"]
        
        # 如果是原始数据，还需要存储配置数据
        if exec_res["load_type"] == "original":
            shared["data"]["topics_hierarchy"] = exec_res["topics_hierarchy"]
            shared["data"]["sentiment_attributes"] = exec_res["sentiment_attributes"]
            shared["data"]["publisher_objects"] = exec_res["publisher_objects"]
        
        # 初始化统计信息
        if "results" not in shared:
            shared["results"] = {"statistics": {}}
        
        shared["results"]["statistics"]["total_blogs"] = len(exec_res["blog_data"])
        
        return "default"


class SaveEnhancedDataNode(Node):
    """
    数据保存节点
    Purpose: 将增强后的博文数据保存到指定文件路径
    Type: Regular Node
    """
    
    def prep(self, shared):
        """读取增强后的博文数据和保存路径配置"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        config = shared.get("config", {})
        data_source = config.get("data_source", {})
        
        # 获取保存路径
        output_path = data_source.get("enhanced_data_path", "data/enhanced_blogs.json")
        
        return {
            "blog_data": blog_data,
            "output_path": output_path
        }
    
    def exec(self, prep_res):
        """调用数据保存工具函数，将增强数据写入文件"""
        blog_data = prep_res["blog_data"]
        output_path = prep_res["output_path"]
        
        # 调用data loader中的保存函数
        success = save_enhanced_blog_data(blog_data, output_path)
        
        return {
            "success": success,
            "output_path": output_path,
            "data_count": len(blog_data)
        }
    
    def post(self, shared, prep_res, exec_res):
        """验证保存结果，更新保存状态信息"""
        if exec_res["success"]:
            print(f"成功保存 {exec_res['data_count']} 条增强数据到: {exec_res['output_path']}")
            
            # 更新保存状态信息到shared中
            if "results" not in shared:
                shared["results"] = {}
            if "data_save" not in shared["results"]:
                shared["results"]["data_save"] = {}
            
            shared["results"]["data_save"] = {
                "saved": True,
                "output_path": exec_res["output_path"],
                "data_count": exec_res["data_count"]
            }
        else:
            print(f"保存增强数据失败: {exec_res['output_path']}")
            
            # 更新失败状态
            if "results" not in shared:
                shared["results"] = {}
            if "data_save" not in shared["results"]:
                shared["results"]["data_save"] = {}
            
            shared["results"]["data_save"] = {
                "saved": False,
                "output_path": exec_res["output_path"],
                "error": "保存失败"
            }
        
        return "default"


class AsyncSentimentPolarityAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感极性分析BatchNode
    Purpose: 批量分析博文的情感极性（1-5档数字分级）
    Type: AsyncParallelBatchNode
    """
    
    async def prep_async(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        return blog_data
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行情感极性分析"""
        blog_post = prep_res
        # 构建提示词（压缩版）
        prompt = f"""
你是社交媒体分析师，请依据下表判断博文整体情感极性：
- 1=极度悲观，2=悲观，3=中性，4=乐观，5=极度乐观，0=无法判断
- 仅输出一个数字（0-5），不得附加解释或其他字符
博文内容：
{blog_post.get('content', '')}
"""
        
        # 准备图片路径（如果有）
        image_paths = blog_post.get('image_urls', [])
        # 过滤掉空字符串
        image_paths = [img for img in image_paths if img and img.strip()]
        
        # 处理相对路径：如果路径不是绝对路径，则假设相对于data目录
        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                # 相对路径，添加data目录前缀
                full_path = os.path.join("data", img_path)
                processed_image_paths.append(full_path)
            else:
                processed_image_paths.append(img_path)
        
        # 使用 asyncio.to_thread 来在单独线程中运行同步的LLM调用，让出协程
        if processed_image_paths:
            # 有图片时使用多模态模型
            response = await asyncio.to_thread(
                call_glm4v_plus, prompt, image_paths=processed_image_paths, temperature=0.3
            )
        else:
            # 无图片时使用纯文本模型
            response = await asyncio.to_thread(
                call_glm_45_air, prompt, temperature=0.3
            )
        
        # 提取数字并验证格式
        response = response.strip()
        
        # 检查是否为单一数字
        if not response.isdigit():
            raise ValueError(f"模型输出不是数字: {response}")
        
        score = int(response)
        if not 1 <= score <= 5:
            raise ValueError(f"模型输出数字不在1-5范围内: {score}")
        
        return score
    
    async def exec_fallback_async(self, prep_res, exc):
        """当情感极性分析失败时的回退处理"""
        print(f"情感极性分析失败，使用默认值: {str(exc)}")
        # 返回中性评分作为默认值
        return 3
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的sentiment_polarity字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：情感极性分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['sentiment_polarity'] = exec_res[i]
            else:
                blog_post['sentiment_polarity'] = None
        
        return "default"


class SentimentPolarityAnalysisBatchNode(BatchNode):
    """
    情感极性分析BatchNode
    Purpose: 批量分析博文的情感极性（1-5档数字分级）
    Type: BatchNode
    """
    
    def prep(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        return blog_data
    
    def exec(self, prep_res):
        """对单条博文调用多模态LLM进行情感极性分析"""
        blog_post = prep_res
        # 构建提示词（压缩版）
        prompt = f"""
你是社交媒体分析师，请依据下表判断博文整体情感极性：
- 1=极度悲观，2=悲观，3=中性，4=乐观，5=极度乐观，0=无法判断
- 仅输出一个数字（0-5），不得附加解释或其他字符
博文内容：
{blog_post.get('content', '')}
"""
        
        # 准备图片路径（如果有）
        image_paths = blog_post.get('image_urls', [])
        # 过滤掉空字符串
        image_paths = [img for img in image_paths if img and img.strip()]
        
        # 处理相对路径：如果路径不是绝对路径，则假设相对于data目录
        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                # 相对路径，添加data目录前缀
                full_path = os.path.join("data", img_path)
                processed_image_paths.append(full_path)
            else:
                processed_image_paths.append(img_path)
        
        # 调用多模态LLM
        if processed_image_paths:
            # 有图片时使用多模态模型
            response = call_glm4v_plus(prompt, image_paths=processed_image_paths, temperature=0.3)
        else:
            # 无图片时使用纯文本模型
            response = call_glm_45_air(prompt, temperature=0.3)
        
        # 提取数字并验证格式
        response = response.strip()
        
        # 检查是否为单一数字
        if not response.isdigit():
            raise ValueError(f"模型输出不是数字: {response}")
        
        score = int(response)
        if not 1 <= score <= 5:
            raise ValueError(f"模型输出数字不在1-5范围内: {score}")
        
        return score
    
    def exec_fallback(self, prep_res, exc):
        """当情感极性分析失败时的回退处理"""
        print(f"情感极性分析失败，使用默认值: {str(exc)}")
        # 返回中性评分作为默认值
        return 3
    
    def post(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的sentiment_polarity字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：情感极性分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['sentiment_polarity'] = exec_res[i]
            else:
                blog_post['sentiment_polarity'] = None
        
        return "default"


class AsyncSentimentAttributeAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步情感属性分析BatchNode
    Purpose: 批量分析博文的具体情感状态和强度
    Type: AsyncParallelBatchNode
    """
    
    async def prep_async(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        sentiment_attributes = shared.get("data", {}).get("sentiment_attributes", [])
        
        # 返回包含博文和情感属性的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "sentiment_attributes": sentiment_attributes
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用单模态LLM进行情感属性分析"""
        blog_post = prep_res["blog_data"]
        sentiment_attributes = prep_res["sentiment_attributes"]
        
        # 构建情感属性列表字符串
        attributes_str = "、".join(sentiment_attributes)
        
        # 构建提示词（压缩版）
        prompt = f"""
从候选情感属性中选出最贴切的1-3个，按JSON数组输出（示例：["支持","期待"]）。
候选：{attributes_str}
仅输出JSON数组，不得添加解释或其他文本。
博文：
{blog_post.get('content', '')}
"""
        
        # 使用 asyncio.to_thread 来在单独线程中运行同步的LLM调用，让出协程
        response = await asyncio.to_thread(
            call_glm_45_air, prompt, temperature=0.3
        )
        
        # 解析JSON响应 - 让PocketFlow处理JSON解析错误
        attributes = json.loads(response.strip())
        
        # 验证是否为列表
        if not isinstance(attributes, list):
            raise ValueError(f"模型输出不是列表格式: {attributes}")
        
        # 验证结果是否在预定义列表中
        valid_attributes = []
        for attr in attributes:
            if attr in sentiment_attributes:
                valid_attributes.append(attr)
        
        return valid_attributes 
    
    async def exec_fallback_async(self, prep_res, exc):
        """当情感属性分析失败时的回退处理"""
        print(f"情感属性分析失败，使用默认值: {str(exc)}")
        # 返回中性属性作为默认值
        return ["中立"]
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的sentiment_attribute字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：情感属性分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['sentiment_attribute'] = exec_res[i]
            else:
                blog_post['sentiment_attribute'] = None
        
        return "default"


class SentimentAttributeAnalysisBatchNode(BatchNode):
    """
    情感属性分析BatchNode
    Purpose: 批量分析博文的具体情感状态和强度
    Type: BatchNode
    """
    
    def prep(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        sentiment_attributes = shared.get("data", {}).get("sentiment_attributes", [])
        
        # 返回包含博文和情感属性的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "sentiment_attributes": sentiment_attributes
        } for blog_post in blog_data]
    
    def exec(self, prep_res):
        """对单条博文调用单模态LLM进行情感属性分析"""
        blog_post = prep_res["blog_data"]
        sentiment_attributes = prep_res["sentiment_attributes"]
        
        attributes_str = "、".join(sentiment_attributes)
        
        # 构建提示词（压缩版）
        prompt = f"""
从候选情感属性中选出最贴切的1-3个，按JSON数组输出（示例：["支持","期待"]）。
候选：{attributes_str}
仅输出JSON数组，不得添加解释或其他文本。
博文：
{blog_post.get('content', '')}
"""
        
        # 调用纯文本LLM
        response = call_glm_45_air(prompt, temperature=0.3)
        
        # 解析JSON响应 - 让PocketFlow处理JSON解析错误
        attributes = json.loads(response.strip())
        
        # 验证是否为列表
        if not isinstance(attributes, list):
            raise ValueError(f"模型输出不是列表格式: {attributes}")
        
        # 验证结果是否在预定义列表中
        valid_attributes = []
        for attr in attributes:
            if attr in sentiment_attributes:
                valid_attributes.append(attr)
        
        return valid_attributes 
    
    def exec_fallback(self, prep_res, exc):
        """当情感属性分析失败时的回退处理"""
        print(f"情感属性分析失败，使用默认值: {str(exc)}")
        # 返回中性属性作为默认值
        return ["中立"]
    
    def post(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的sentiment_attribute字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：情感属性分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['sentiment_attribute'] = exec_res[i]
            else:
                blog_post['sentiment_attribute'] = None
        
        return "default"


class AsyncTwoLevelTopicAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步两级主题分析BatchNode
    Purpose: 批量从预定义主题列表中选择合适主题
    Type: AsyncParallelBatchNode
    """
    
    async def prep_async(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        topics_hierarchy = shared.get("data", {}).get("topics_hierarchy", [])
        
        # 返回包含博文和主题层次结构的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "topics_hierarchy": topics_hierarchy
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用多模态LLM进行主题匹配和选择"""
        blog_post = prep_res["blog_data"]
        topics_hierarchy = prep_res["topics_hierarchy"]
        
        # 构建主题层次结构字符串
        topics_lines = []
        for topic_group in topics_hierarchy:
            parent_topic = topic_group.get("parent_topic", "")
            sub_topics = "、".join(topic_group.get("sub_topics", []))
            topics_lines.append(f"{parent_topic} -> {sub_topics}")
        topics_str = "\n".join(topics_lines)
        
        # 构建提示词（压缩版）
        prompt = f"""
请根据以下主题层次，从候选中选1-2个最贴切的父/子主题组合，使用JSON数组输出：
[{{"parent_topic": "父主题", "sub_topic": "子主题"}}]
若无匹配输出 []，不得添加解释。
候选主题：
{topics_str}
博文：
{blog_post.get('content', '')}
"""
        
        # 准备图片路径（如果有）
        image_paths = blog_post.get('image_urls', [])
        image_paths = [img for img in image_paths if img and img.strip()]
        
        # 处理相对路径：如果路径不是绝对路径，则假设相对于data目录
        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                # 相对路径，添加data目录前缀
                full_path = os.path.join("data", img_path)
                processed_image_paths.append(full_path)
            else:
                processed_image_paths.append(img_path)
        
        # 使用 asyncio.to_thread 来在单独线程中运行同步的LLM调用，让出协程
        if processed_image_paths:
            response = await asyncio.to_thread(
                call_glm4v_plus, prompt, image_paths=processed_image_paths, temperature=0.3
            )
        else:
            response = await asyncio.to_thread(
                call_glm_45_air, prompt, temperature=0.3
            )
        
        # 解析JSON响应
        topics = json.loads(response.strip())
        
        # 验证是否为列表
        if not isinstance(topics, list):
            raise ValueError(f"模型输出不是列表格式: {topics}")
        
        # 验证结果是否在预定义列表中
        valid_topics = []
        for topic_item in topics:
            parent_topic = topic_item.get("parent_topic", "")
            sub_topic = topic_item.get("sub_topic", "")
            
            # 验证父主题和子主题是否在预定义列表中
            for topic_group in topics_hierarchy:
                if topic_group.get("parent_topic") == parent_topic:
                    if sub_topic in topic_group.get("sub_topics", []):
                        valid_topics.append({
                            "parent_topic": parent_topic,
                            "sub_topic": sub_topic
                        })
                    break
        
        return valid_topics  # 允许返回空列表，表示找不到符合的主题
    
    async def exec_fallback_async(self, prep_res, exc):
        """当主题分析失败时的回退处理"""
        print(f"主题分析失败，使用默认值: {str(exc)}")
        # 返回空列表作为默认值，表示无法识别主题
        return []
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的topics字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：主题分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['topics'] = exec_res[i]
            else:
                blog_post['topics'] = None
        
        return "default"


class TwoLevelTopicAnalysisBatchNode(BatchNode):
    """
    两级主题分析BatchNode
    Purpose: 批量从预定义主题列表中选择合适主题
    Type: BatchNode
    """
    
    def prep(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        topics_hierarchy = shared.get("data", {}).get("topics_hierarchy", [])
        
        # 返回包含博文和主题层次结构的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "topics_hierarchy": topics_hierarchy
        } for blog_post in blog_data]
    
    def exec(self, prep_res):
        """对单条博文调用多模态LLM进行主题匹配和选择"""
        blog_post = prep_res["blog_data"]
        topics_hierarchy = prep_res["topics_hierarchy"]
        
        topics_lines = []
        for topic_group in topics_hierarchy:
            parent_topic = topic_group.get("parent_topic", "")
            sub_topics = "、".join(topic_group.get("sub_topics", []))
            topics_lines.append(f"{parent_topic} -> {sub_topics}")
        topics_str = "\n".join(topics_lines)
        
        # 构建提示词（压缩版）
        prompt = f"""
请根据以下主题层次，从候选中选1-2个最贴切的父/子主题组合，使用JSON数组输出：
[{{"parent_topic": "父主题", "sub_topic": "子主题"}}]
若无匹配输出 []，不得添加解释。
候选主题：
{topics_str}
博文：
{blog_post.get('content', '')}
"""
        
        # 准备图片路径（如果有）
        image_paths = blog_post.get('image_urls', [])
        image_paths = [img for img in image_paths if img and img.strip()]
        
        # 处理相对路径：如果路径不是绝对路径，则假设相对于data目录
        processed_image_paths = []
        for img_path in image_paths:
            if not os.path.isabs(img_path):
                # 相对路径，添加data目录前缀
                full_path = os.path.join("data", img_path)
                processed_image_paths.append(full_path)
            else:
                processed_image_paths.append(img_path)
        
        # 调用多模态LLM
        if processed_image_paths:
            response = call_glm4v_plus(prompt, image_paths=processed_image_paths, temperature=0.3)
        else:
            response = call_glm_45_air(prompt, temperature=0.3)
        
        # 解析JSON响应
        topics = json.loads(response.strip())
        
        # 验证是否为列表
        if not isinstance(topics, list):
            raise ValueError(f"模型输出不是列表格式: {topics}")
        
        # 验证结果是否在预定义列表中
        valid_topics = []
        for topic_item in topics:
            parent_topic = topic_item.get("parent_topic", "")
            sub_topic = topic_item.get("sub_topic", "")
            
            # 验证父主题和子主题是否在预定义列表中
            for topic_group in topics_hierarchy:
                if topic_group.get("parent_topic") == parent_topic:
                    if sub_topic in topic_group.get("sub_topics", []):
                        valid_topics.append({
                            "parent_topic": parent_topic,
                            "sub_topic": sub_topic
                        })
                    break
        
        return valid_topics  # 允许返回空列表，表示找不到符合的主题
    
    def exec_fallback(self, prep_res, exc):
        """当主题分析失败时的回退处理"""
        print(f"主题分析失败，使用默认值: {str(exc)}")
        # 返回空列表作为默认值，表示无法识别主题
        return []
    
    def post(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的topics字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：主题分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['topics'] = exec_res[i]
            else:
                blog_post['topics'] = None
        
        return "default"


class AsyncPublisherObjectAnalysisBatchNode(AsyncParallelBatchNode):
    """
    异步发布者对象分析BatchNode
    Purpose: 批量识别发布者类型和特征
    Type: AsyncParallelBatchNode
    """
    
    async def prep_async(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_objects = shared.get("data", {}).get("publisher_objects", [])
        
        # 返回包含博文和发布者对象的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "publisher_objects": publisher_objects
        } for blog_post in blog_data]
    
    async def exec_async(self, prep_res):
        """对单条博文调用单模态LLM进行发布者类型识别"""
        blog_post = prep_res["blog_data"]
        publisher_objects = prep_res["publisher_objects"]
        
        # 构建发布者对象列表字符串
        publishers_str = "、".join(publisher_objects)
        
        # 构建提示词（压缩版）
        prompt = f"""
候选发布者：{publishers_str}
判断该博文最可能的发布者类型，直接输出候选列表中的一个条目，不得添加解释。
博文：
{blog_post.get('content', '')}
"""
        
        # 使用 asyncio.to_thread 来在单独线程中运行同步的LLM调用，让出协程
        response = await asyncio.to_thread(
            call_glm_45_air, prompt, temperature=0.3
        )
        
        # 直接解析字符串响应
        publisher = response.strip()
        
        # 验证结果是否在预定义列表中
        if publisher in publisher_objects:
            return publisher
        else:
            # 如果不在列表中，返回默认值
            return "个人用户" if "个人用户" in publisher_objects else None
    
    async def exec_fallback_async(self, prep_res, exc):
        """当发布者分析失败时的回退处理"""
        print(f"发布者分析失败，使用默认值: {str(exc)}")
        # 返回个人用户作为默认值
        return "个人用户"
    
    async def post_async(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的publisher字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：发布者分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['publisher'] = exec_res[i]
            else:
                blog_post['publisher'] = None
        
        return "default"


class PublisherObjectAnalysisBatchNode(BatchNode):
    """
    发布者对象分析BatchNode
    Purpose: 批量识别发布者类型和特征
    Type: BatchNode
    """
    
    def prep(self, shared):
        """返回博文数据列表，每条博文作为独立处理单元"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        publisher_objects = shared.get("data", {}).get("publisher_objects", [])
        
        # 返回包含博文和发布者对象的列表，每个元素包含处理单条博文所需的所有信息
        return [{
            "blog_data": blog_post,
            "publisher_objects": publisher_objects
        } for blog_post in blog_data]
    
    def exec(self, prep_res):
        """对单条博文调用单模态LLM进行发布者类型识别"""
        blog_post = prep_res["blog_data"]
        publisher_objects = prep_res["publisher_objects"]
        
        publishers_str = "、".join(publisher_objects)
        
        # 构建提示词（压缩版）
        prompt = f"""
候选发布者：{publishers_str}
判断该博文最可能的发布者类型，直接输出候选列表中的一个条目，不得添加解释。
博文：
{blog_post.get('content', '')}
"""
        
        # 调用纯文本LLM
        response = call_glm_45_air(prompt, temperature=0.3)
        
        # 直接解析字符串响应
        publisher = response.strip()
        
        # 验证结果是否在预定义列表中
        if publisher in publisher_objects:
            return publisher
        else:
            # 如果不在列表中，返回默认值
            return "个人用户" if "个人用户" in publisher_objects else None
    
    def exec_fallback(self, prep_res, exc):
        """当发布者分析失败时的回退处理"""
        print(f"发布者分析失败，使用默认值: {str(exc)}")
        # 返回个人用户作为默认值
        return "个人用户"
    
    def post(self, shared, prep_res, exec_res):
        """将分析结果附加到对应博文对象的publisher字段中"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        
        # 确保结果列表长度与博文数据长度一致
        if len(exec_res) != len(blog_data):
            print("警告：发布者分析结果数量与博文数量不匹配")
            return "default"
        
        # 将结果附加到博文数据中
        for i, blog_post in enumerate(blog_data):
            if i < len(exec_res):
                blog_post['publisher'] = exec_res[i]
            else:
                blog_post['publisher'] = None
        
        return "default"


class DataValidationAndOverviewNode(Node):
    """
    数据验证与概况分析节点
    Purpose: 验证增强数据的完整性并生成数据统计概况
    Type: Regular Node
    """
    
    def prep(self, shared):
        """读取增强后的博文数据"""
        blog_data = shared.get("data", {}).get("blog_data", [])
        return blog_data
    
    def exec(self, prep_res):
        """验证必需字段是否存在，统计留空字段数量，生成数据统计概况"""
        blog_data = prep_res
        # 初始化统计信息
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
        
        # 遍历所有博文进行统计
        total_content_length = 0
        user_engagement = {}
        
        for blog_post in blog_data:
            # 检查是否已处理（至少有一个分析字段不为空）
            has_analysis = (
                blog_post.get('sentiment_polarity') is not None or
                blog_post.get('sentiment_attribute') is not None or
                blog_post.get('topics') is not None or
                blog_post.get('publisher') is not None
            )
            
            if has_analysis:
                stats["processed_blogs"] += 1
            
            # 统计参与度
            repost_count = blog_post.get('repost_count', 0)
            comment_count = blog_post.get('comment_count', 0)
            like_count = blog_post.get('like_count', 0)
            
            stats["engagement_statistics"]["total_reposts"] += repost_count
            stats["engagement_statistics"]["total_comments"] += comment_count
            stats["engagement_statistics"]["total_likes"] += like_count
            
            # 统计用户信息
            user_id = blog_post.get('user_id', '')
            username = blog_post.get('username', '')
            if user_id:
                stats["user_statistics"]["unique_users"].add(user_id)
                
                # 统计用户活跃度
                if user_id not in user_engagement:
                    user_engagement[user_id] = {
                        "username": username,
                        "total_engagement": 0
                    }
                user_engagement[user_id]["total_engagement"] += repost_count + comment_count + like_count
            
            # 统计内容信息
            content = blog_post.get('content', '')
            total_content_length += len(content)
            
            image_urls = blog_post.get('image_urls', [])
            if image_urls:
                stats["content_statistics"]["total_images"] += len(image_urls)
                stats["content_statistics"]["blogs_with_images"] += 1
            
            # 统计时间分布
            publish_time = blog_post.get('publish_time', '')
            if publish_time:
                # 提取小时（简化处理）
                try:
                    hour = int(publish_time.split(' ')[1].split(':')[0]) if ' ' in publish_time else 0
                    hour_key = f"{hour:02d}:00"
                    stats["content_statistics"]["time_distribution"][hour_key] = \
                        stats["content_statistics"]["time_distribution"].get(hour_key, 0) + 1
                except:
                    pass
            
            # 统计地理分布
            location = blog_post.get('location', '')
            if location:
                stats["geographic_distribution"][location] = \
                    stats["geographic_distribution"].get(location, 0) + 1
            
            # 统计空字段
            if blog_post.get('sentiment_polarity') is None:
                stats["empty_fields"]["sentiment_polarity_empty"] += 1
            if blog_post.get('sentiment_attribute') is None:
                stats["empty_fields"]["sentiment_attribute_empty"] += 1
            if blog_post.get('topics') is None:
                stats["empty_fields"]["topics_empty"] += 1
            if blog_post.get('publisher') is None:
                stats["empty_fields"]["publisher_empty"] += 1
            
            # 统计发布者类型分布
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
        
        # 获取活跃用户排行（前10）
        sorted_users = sorted(user_engagement.items(), 
                            key=lambda x: x[1]["total_engagement"], 
                            reverse=True)[:10]
        stats["user_statistics"]["top_active_users"] = [
            {
                "user_id": user_id,
                "username": info["username"],
                "total_engagement": info["total_engagement"]
            }
            for user_id, info in sorted_users
        ]
        
        return stats
    
    def post(self, shared, prep_res, exec_res):
        """将统计信息存储到shared中"""
        # 更新统计信息到shared中
        if "results" not in shared:
            shared["results"] = {}
        if "statistics" not in shared["results"]:
            shared["results"]["statistics"] = {}
        
        # 合并统计信息
        shared["results"]["statistics"].update(exec_res)
        
        return "default"
