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
import time
from typing import List, Dict, Any, Optional, Union
from zai import ZaiClient

# API密钥配置
#GLM_API_KEY = "c020d461aacc492aba4ce1e9b5071962.uFJuF63k31GRGs2l"  # API密钥
GLM_API_KEY = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"   # 武大团队API密钥

# 使用 ThreadLocal 存储客户端实例
# 这样每个线程会有自己独立的 Client，既避免了锁竞争，又实现了连接复用
_thread_local = threading.local()

def get_client():
    """
    获取当前线程的 ZaiClient 实例
    如果在当前线程是第一次调用，则创建一个新实例并缓存
    """
    if not hasattr(_thread_local, "client"):
        _thread_local.client = ZaiClient(api_key=GLM_API_KEY)
    return _thread_local.client


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
    try:
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
        
    except Exception as e:
        # 更详细的错误信息，帮助诊断问题
        error_msg = str(e)
        if "429" in error_msg or "rate" in error_msg.lower():
            raise Exception(f"API速率限制: {error_msg}")
        elif "timeout" in error_msg.lower():
            raise Exception(f"请求超时({timeout}秒): {error_msg}")
        else:
            raise Exception(f"调用glm-4.5-air模型失败: {error_msg}")


def call_glm4v_plus(prompt: str, image_paths: Optional[List[str]] = None, 
                   image_data: Optional[List[bytes]] = None, 
                   temperature: float = 0.7, max_tokens: Optional[int] = None, timeout: int = 30) -> str:
    """
    调用glm-4.5-air纯文本模型（原为glm4v-plus，已切换）
    
    注意：此函数已改为调用 glm-4.5-air 模型，忽略所有图像参数
    
    Args:
        prompt: 输入提示词
        image_paths: 图片文件路径列表（已忽略，保留参数以兼容旧代码）
        image_data: 图片二进制数据列表（已忽略，保留参数以兼容旧代码）
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        timeout: 请求超时时间（秒），默认30秒
        
    Returns:
        模型生成的文本响应
    """
    try:
        client = get_client()  # 创建新的客户端实例
        
        # 直接使用文本提示词，忽略所有图像参数
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        params = {
            "model": "glm-4.5-air",
            "messages": messages,
            "temperature": temperature,
            "stream": False,
            "timeout": 90  # 添加超时设置
        }
        
        if max_tokens:
            params["max_tokens"] = max_tokens
        
        response = client.chat.completions.create(**params)
        return response.choices[0].message.content
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "rate" in error_msg.lower():
            raise Exception(f"API速率限制: {error_msg}")
        elif "timeout" in error_msg.lower():
            raise Exception(f"请求超时({timeout}秒): {error_msg}")
        else:
            raise Exception(f"调用glm-4.5-air模型失败: {error_msg}")


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
    try:
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
        
    except Exception as e:
        raise Exception(f"调用glm4.5v思考模式模型失败: {str(e)}")


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
        max_retries: 最大重试次数（遇到429错误时），默认3次
        retry_delay: 重试延迟时间（秒），默认5秒
        
    Returns:
        模型生成的文本响应
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
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
            
        except Exception as e:
            last_error = e
            error_msg = str(e)
            
            # 检查是否是429并发限制错误
            if "429" in error_msg or "concurrency" in error_msg.lower() or "rate" in error_msg.lower():
                if attempt < max_retries - 1:
                    # 指数退避：第1次等5秒，第2次等10秒，第3次等15秒
                    delay = retry_delay * (attempt + 1)
                    print(f"[call_glm46] API并发限制，等待 {delay} 秒后重试 (尝试 {attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(f"API并发限制，已重试 {max_retries} 次仍失败: {error_msg}")
            else:
                # 其他错误直接抛出
                raise Exception(f"调用glm4.6模型失败: {error_msg}")
    
    # 如果所有重试都失败
    raise Exception(f"调用glm4.6模型失败（已重试{max_retries}次）: {str(last_error)}")
