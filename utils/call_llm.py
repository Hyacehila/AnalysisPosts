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
from typing import List, Dict, Any, Optional, Union
from zai import ZaiClient

# API密钥配置
GLM_API_KEY = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"  # API密钥


def call_glm_45_air(prompt: str, temperature: float = 0.7, max_tokens: Optional[int] = None, enable_thinking: bool = False) -> str:
    """
    调用glm-4.5-air纯文本模型
    
    Args:
        prompt: 输入提示词
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        enable_thinking: 是否开启思考模式，默认False（不开启）
        
    Returns:
        模型生成的文本响应
    """
    try:
        client = ZaiClient(api_key=GLM_API_KEY)
        
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        params = {
            "model": "glm-4.5-air",
            "messages": messages,
            "temperature": temperature,
            "stream": False
        }
        
        if max_tokens:
            params["max_tokens"] = max_tokens
        
        response = client.chat.completions.create(**params)
        return response.choices[0].message.content
        
    except Exception as e:
        raise Exception(f"调用glm-4.5-air模型失败: {str(e)}")


def call_glm4v_plus(prompt: str, image_paths: Optional[List[str]] = None, 
                   image_data: Optional[List[bytes]] = None, 
                   temperature: float = 0.7, max_tokens: Optional[int] = None) -> str:
    """
    调用glm4v-plus视觉分析模型
    
    Args:
        prompt: 输入提示词
        image_paths: 图片文件路径列表
        image_data: 图片二进制数据列表（与image_paths二选一）
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        
    Returns:
        模型生成的文本响应
    """
    try:
        client = ZaiClient(api_key=GLM_API_KEY)
        
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
            "model": "glm-4v-plus",
            "messages": messages,
            "temperature": temperature,
            "stream": False
        }
        
        if max_tokens:
            params["max_tokens"] = max_tokens
        
        response = client.chat.completions.create(**params)
        return response.choices[0].message.content
        
    except Exception as e:
        raise Exception(f"调用glm4v-plus模型失败: {str(e)}")


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
        client = ZaiClient(api_key=GLM_API_KEY)
        
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
               max_tokens: Optional[int] = None, enable_reasoning: bool = True) -> str:
    """
    调用glm4.6智能体推理模型
    
    Args:
        prompt: 输入提示词
        temperature: 温度参数，控制随机性
        max_tokens: 最大生成token数
        enable_reasoning: 是否开启推理模式
        
    Returns:
        模型生成的文本响应
    """
    try:
        client = ZaiClient(api_key=GLM_API_KEY)
        
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
        raise Exception(f"调用glm4.6模型失败: {str(e)}")
