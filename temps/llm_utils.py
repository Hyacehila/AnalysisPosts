# -*- coding: utf-8 -*-

import os
import base64
import requests
from mimetypes import guess_type
from typing import List, Union

# --- 配置 ---
API_KEY = "sk-mcqppjzifeekculmknsttugxfwputvzfqlfdmdhhviwxgqrf" # 直接保存KEY
API_URL = "https://api.siliconflow.cn/v1/chat/completions"
# 默认使用一个支持视觉的模型 (VLM)
DEFAULT_MODEL = "Qwen/Qwen3-VL-235B-A22B-Instruct" 

# --- 辅助函数 ---

def encode_image_to_base64(image_path: str) -> Union[str, None]:
    """
    将本地图片文件编码为 Base64 格式的数据 URI。

    :param image_path: 图片文件的本地路径。
    :return: Base64 格式的数据 URI 字符串，如果失败则返回 None。
    """
    try:
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
        
        mime_type, _ = guess_type(image_path)
        if not mime_type or not mime_type.startswith('image/'):
            print(f"警告: 无法识别的图片类型或文件不是图片: {image_path}")
            return None
            
        base64_string = base64.b64encode(image_data).decode('utf-8')
        return f"data:{mime_type};base64,{base64_string}"
    except FileNotFoundError:
        print(f"错误：文件未找到 '{image_path}'")
        return None
    except Exception as e:
        print(f"编码图片时发生错误 '{image_path}': {e}")
        return None

# --- 核心函数 (已修改) ---

def call_llm(prompt: str, image_paths_or_urls: List[str] = None, model: str = DEFAULT_MODEL) -> str:
    """
    调用 SiliconFlow LLM API，支持文本和多张图像输入。

    :param prompt: 用户的文本提示。
    :param image_paths_or_urls: 可选。包含图片本地路径或公网 URL 的列表。
    :param model: 使用的模型名称，必须是支持视觉的模型 (VLM)。
    :return: LLM 的文本回复，或在出错时返回错误信息。
    """
    if not API_KEY:
        return "错误：未设置 SILICONFLOW_API_KEY 环境变量。"

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    # 1. 构建消息内容
    # content 必须是一个对象列表，以支持多种类型（文本、图像）
    content = [{"type": "text", "text": prompt}]

    # **核心改动：遍历图片列表**
    if image_paths_or_urls:
        for image_path_or_url in image_paths_or_urls:
            image_url = None
            # 判断是本地路径还是网络URL
            if image_path_or_url.startswith(('http://', 'https://')):
                image_url = image_path_or_url
            else:
                # 如果是本地路径，转换为Base64
                image_url = encode_image_to_base64(image_path_or_url)
            
            if image_url:
                # 将每张有效的图像信息添加到 content 列表中
                content.append({
                    "type": "image_url",
                    "image_url": {"url": image_url}
                })
            else:
                # 如果单张图片处理失败，打印警告并跳过这张图
                print(f"警告：处理图片 '{image_path_or_url}' 失败，已跳过。")

    messages = [{"role": "user", "content": content}]

    # 2. 构建请求体
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
    }

    try:
        # 3. 发送请求
        response = requests.post(API_URL, headers=headers, json=payload, timeout=120) # 增加超时时间
        response.raise_for_status()

        # 4. 解析响应
        result = response.json()
        return result['choices'][0]['message']['content']

    except requests.exceptions.RequestException as e:
        return f"API 请求出错: {e}"
    except (KeyError, IndexError) as e:
        return f"解析 API 响应时出错: {e}\n原始响应: {response.text}"

# --- 测试代码 (已更新) ---

if __name__ == "__main__":
    print("--- SiliconFlow LLM 多模态调用测试 (支持多图片) ---")

    # 测试场景1：仅使用文本 (不变)
    print("\n[场景1] 仅使用文本提问...")
    text_prompt = "请用一句话解释什么是大语言模型。"
    response_text_only = call_llm(prompt=text_prompt)
    print(f"提问: {text_prompt}")
    print(f"回答: {response_text_only}")
    print("-" * 20)

    # 测试场景2：使用一张公网图片 URL
    print("\n[场景2] 使用一张公网图片 URL 提问...")
    image_url_single = "https://wx4.sinaimg.cn/mw690/a5dc5be4gy1i6e59tnc36j21o00xr162.jpg"
    prompt_with_url = "根据这张图片，写一首五言绝句。"
    # 注意：现在即使只有一张图，也要将其放入列表中
    response_with_url = call_llm(prompt=prompt_with_url, image_paths_or_urls=[image_url_single])
    print(f"提问: {prompt_with_url}")
    print(f"图片URL: {image_url_single}")
    print(f"回答: {response_with_url}")
    print("-" * 20)
    
    # 测试场景3：使用一张本地图片
    local_image_path_1 = "test_image.jpg"
    print(f"\n[场景3] 使用一张本地图片提问 (路径: {local_image_path_1})...")
    prompt_with_local_image = "这张图片里有什么？请详细描述一下。"
    
    if os.path.exists(local_image_path_1):
        # 同样，放入列表中
        response_with_local_image = call_llm(prompt=prompt_with_local_image, image_paths_or_urls=[local_image_path_1])
        print(f"提问: {prompt_with_local_image}")
        print(f"回答: {response_with_local_image}")
    else:
        print(f"本地图片 '{local_image_path_1}' 不存在，跳过此测试。")
    print("-" * 20)

    # 新增测试场景4：混合使用多张本地图片和URL
    print("\n[新增场景4] 混合使用多张图片 (本地 + URL) 提问...")
    # 为了测试，请准备两张本地图片： 'test_image_1.jpg' 和 'test_image_2.png'
    # 或者修改下面的文件路径为你自己的图片
    local_image_path_2 = "test_image.jpg"
    image_url_another = "https://wx4.sinaimg.cn/mw690/a5dc5be4gy1i6e59tnc36j21o00xr162.jpg" # 
    
    multi_image_list = []
    
    # 检查本地文件是否存在，存在才加入列表
    if os.path.exists(local_image_path_1):
        multi_image_list.append(local_image_path_1)
    else:
        print(f"提示: 本地图片 '{local_image_path_1}' 不存在，已在多图测试中跳过。")
        
    if os.path.exists(local_image_path_2):
        multi_image_list.append(local_image_path_2)
    else:
        print(f"提示: 本地图片 '{local_image_path_2}' 不存在，已在多图测试中跳过。")
        
    multi_image_list.append(image_url_another)

    prompt_multi_image = "这里有几张图片，请按顺序分别描述它们的内容，并总结一下它们之间有什么共同点或不同点。"

    if len(multi_image_list) > 1: # 确保至少有图片可以测试
        response_multi = call_llm(prompt=prompt_multi_image, image_paths_or_urls=multi_image_list)
        print(f"提问: {prompt_multi_image}")
        print(f"图片列表: {multi_image_list}")
        print(f"回答: {response_multi}")
    else:
        print("没有足够的图片来执行多图测试，已跳过。")
        print("提示：请确保 'test_image_1.jpg' 或 'test_image_2.png' 至少有一个存在于脚本目录中。")

    print("-" * 20)

    print("\n--- 所有测试完成 ---")