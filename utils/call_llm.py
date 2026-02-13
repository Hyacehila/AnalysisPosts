"""
GLM模型调用工具函数

支持四个模型：
1. glm-4.5-air: 纯文本模型，不开启推理模式
2. glm-4v-plus: 视觉分析模型，不开启推理模式  
3. glm-4.5v: 思考模式模型，支持图像理解，用于报告分析智能体
4. glm-4.6: 智能体推理模型，纯文本，开启推理模式

使用zai-sdk作为调用模型的package
"""

import os
import json
import base64
import threading
from typing import List, Dict, Any, Optional, Union
from zai import ZaiClient

from utils.llm_retry import llm_retry
# API密钥配置
GLM_API_KEY_ENV = "GLM_API_KEY"

# 使用 ThreadLocal 存储客户端实例
# 这样每个线程会有自己独立的 Client，既避免了锁竞争，又实现了连接复用
_thread_local = threading.local()

def get_client():
    """
    获取当前线程的 ZaiClient 实例
    如果在当前线程是第一次调用，则创建一个新实例并缓存
    """
    api_key = os.getenv(GLM_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"{GLM_API_KEY_ENV} environment variable is not set.")
    if not hasattr(_thread_local, "client") or getattr(_thread_local, "api_key", None) != api_key:
        _thread_local.client = ZaiClient(api_key=api_key)
        _thread_local.api_key = api_key
    return _thread_local.client


@llm_retry(max_retries=3, retry_delay=5.0, backoff="linear")
def call_glm_45_air(prompt: str, temperature: float = 0.7, max_tokens: Optional[int] = None, enable_thinking: bool = False, timeout: int = 30) -> str:
    """
    调用glm-4.5-air纯文本模型
    
    Args:
        prompt: 输入提示词
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        enable_thinking: 是否开启思考模式，默认False（不开启）
        timeout: 请求超时时间（秒），默认30秒
        
    Returns:
        模型生成的文本响应
    """
    client = get_client()  # 创建新的客户端实例

    messages = [
        {"role": "user", "content": prompt}
    ]

    params = {
        "model": "glm-4.5-air",
        "messages": messages,
        "temperature": temperature,
        "stream": False,
        "timeout": timeout  # 添加超时设置
    }

    if max_tokens:
        params["max_tokens"] = max_tokens

    response = client.chat.completions.create(**params)
    return response.choices[0].message.content


@llm_retry(max_retries=3, retry_delay=5.0, backoff="linear")
def call_glm4v_plus(prompt: str, image_paths: Optional[List[str]] = None,
                   image_data: Optional[List[bytes]] = None,
                   temperature: float = 0.7, max_tokens: Optional[int] = None, timeout: int = 30) -> str:
    """
    调用glm-4v-plus视觉模型（支持图像理解）

    Args:
        prompt: 输入提示词
        image_paths: 图片文件路径列表
        image_data: 图片二进制数据列表（与image_paths二选一）
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        timeout: 请求超时时间（秒），默认30秒

    Returns:
        模型生成的文本响应
    """
    client = get_client()

    # 构建消息内容（文本 + 图片）
    content = [{"type": "text", "text": prompt}]

    if image_paths:
        for img_path in image_paths:
            with open(img_path, "rb") as f:
                img_bytes = f.read()
            base64_image = base64.b64encode(img_bytes).decode("utf-8")
            if img_path.lower().endswith(".png"):
                mime_type = "image/png"
            else:
                mime_type = "image/jpeg"
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}
            })
    elif image_data:
        for img_bytes in image_data:
            base64_image = base64.b64encode(img_bytes).decode("utf-8")
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
            })

    messages = [{"role": "user", "content": content}]

    params = {
        "model": "glm-4v-plus",
        "messages": messages,
        "temperature": temperature,
        "stream": False,
        "timeout": timeout
    }
    if max_tokens:
        params["max_tokens"] = max_tokens

    response = client.chat.completions.create(**params)
    return response.choices[0].message.content


@llm_retry(max_retries=3, retry_delay=5.0, backoff="linear")
def call_glm45v_thinking(prompt: str, image_paths: Optional[List[str]] = None, 
                        image_data: Optional[List[bytes]] = None, 
                        temperature: float = 0.7, max_tokens: Optional[int] = None, 
                        enable_thinking: bool = True) -> str:
    """
    调用glm4.5v思考模式模型 - 用于视觉理解的思考模型，作为glm4.6模型的配套来辅助最终的智能分析与报告生成
    
    Args:
        prompt: 输入提示词
        image_paths: 图片文件路径列表
        image_data: 图片二进制数据列表（与image_paths二选一）
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        enable_thinking: 是否开启思考模式
        
    Returns:
        模型生成的文本响应
    """
    client = get_client()  # 创建新的客户端实例

    # 构建消息内容
    content = [{"type": "text", "text": prompt}]

    # 处理图片输入
    if image_paths:
        for img_path in image_paths:
            with open(img_path, "rb") as f:
                img_data = f.read()
            base64_image = base64.b64encode(img_data).decode('utf-8')
            # 根据文件扩展名确定MIME类型
            if img_path.lower().endswith(('.png', '.PNG')):
                mime_type = "image/png"
            else:
                mime_type = "image/jpeg"
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{base64_image}"
                }
            })

    elif image_data:
        for img_bytes in image_data:
            base64_image = base64.b64encode(img_bytes).decode('utf-8')
            content.append({
                "type": "image_url", 
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })

    messages = [
        {"role": "user", "content": content}
    ]

    params = {
        "model": "glm-4.5v",
        "messages": messages,
        "temperature": temperature,
        "stream": False
    }

    if max_tokens:
        params["max_tokens"] = max_tokens

    # glm4.5v开启思考模式的参数设置
    if enable_thinking:
        params["thinking"] = {"enabled": True}

    response = client.chat.completions.create(**params)
    return response.choices[0].message.content


@llm_retry(max_retries=3, retry_delay=5.0, backoff="linear")
def call_glm46(prompt: str, temperature: float = 0.8, 
               max_tokens: Optional[int] = None, enable_reasoning: bool = True,
               max_retries: int = 3, retry_delay: int = 5) -> str:
    """
    调用glm4.6智能体推理模型
    
    Args:
        prompt: 输入提示词
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        enable_reasoning: 是否开启推理模式
        max_retries: 保留参数（由 llm_retry 统一重试策略）
        retry_delay: 保留参数（由 llm_retry 统一重试策略）
        
    Returns:
        模型生成的文本响应
    """
    client = get_client()  # 创建新的客户端实例

    messages = [
        {"role": "user", "content": prompt}
    ]

    params = {
        "model": "glm-4.6",
        "messages": messages,
        "temperature": temperature,
        "stream": False
    }

    if max_tokens:
        params["max_tokens"] = max_tokens

    # glm4.6开启推理模式的参数设置
    if enable_reasoning:
        params["thinking"] = {"enabled": True}

    response = client.chat.completions.create(**params)
    return response.choices[0].message.content
