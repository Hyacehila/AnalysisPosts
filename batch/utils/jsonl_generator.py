"""
JSONL文件生成工具

根据nodes.py中的提示词设计，创建四个对应的JSONL文件
支持文件拆分以符合API限制（1万条/500MB）
"""

import os
import json
import base64
from typing import List, Dict, Any, Tuple


# API限制常量
MAX_LINES_PER_FILE = 10000  # 每个文件最大条数
MAX_FILE_SIZE_BYTES = 500 * 1024 * 1024  # 500MB


def encode_image_to_base64(image_path: str) -> str:
    """将图片文件编码为Base64字符串"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


def get_image_mime_type(image_path: str) -> str:
    """获取图片的MIME类型"""
    ext = os.path.splitext(image_path)[1].lower()
    mime_types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.webp': 'image/webp'
    }
    return mime_types.get(ext, 'image/jpeg')


def process_images_for_batch(blog_post: Dict[str, Any], data_dir: str) -> List[Dict[str, Any]]:
    """为批处理请求处理图片"""
    image_content = []
    image_urls = blog_post.get('image_urls', [])
    
    for img_url in image_urls:
        if not img_url or not img_url.strip():
            continue
            
        # 构建完整的图片路径
        if img_url.startswith('http'):
            # 如果是网络图片，需要先下载（这里暂不处理）
            continue
        else:
            # 本地图片路径
            full_path = os.path.join(data_dir, img_url.lstrip('/'))
            
            if os.path.exists(full_path):
                try:
                    base64_image = encode_image_to_base64(full_path)
                    mime_type = get_image_mime_type(full_path)
                    data_url = f"data:{mime_type};base64,{base64_image}"
                    
                    image_content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": data_url
                        }
                    })
                except Exception as e:
                    print(f"处理图片失败 {full_path}: {e}")
                    continue
    
    return image_content


def select_model_for_analysis(analysis_type: str, has_images: bool) -> str:
    """根据分析类型和是否有图片选择模型"""
    if analysis_type in ["sentiment_polarity", "topic_analysis"]:
        # 情感极性分析和主题分析任务，无论是否包含图片都使用多模态视觉模型
        return "glm-4v-plus"
    elif analysis_type == "sentiment_attribute":
        return "glm-4-air"
    elif analysis_type == "publisher_analysis":
        return "glm-4-air"
    else:
        return "glm-4-air"


def get_temperature_for_analysis(analysis_type: str) -> float:
    """根据分析类型获取温度参数"""
    return 0.3


def split_requests_by_limits(requests: List[Dict[str, Any]], base_path: str) -> List[str]:
    """
    根据API限制拆分请求列表为多个文件
    
    Args:
        requests: 请求列表
        base_path: 基础文件路径（不带扩展名）
        
    Returns:
        生成的文件路径列表
    """
    if not requests:
        return []
    
    file_paths = []
    current_requests = []
    current_size = 0
    
    for request in requests:
        # 计算当前请求的大小（估算）
        request_str = json.dumps(request, ensure_ascii=False) + '\n'
        request_size = len(request_str.encode('utf-8'))
        
        # 检查是否需要拆分
        if (len(current_requests) >= MAX_LINES_PER_FILE or 
            current_size + request_size > MAX_FILE_SIZE_BYTES):
            
            # 保存当前文件
            if current_requests:
                file_path = f"{base_path}_part{len(file_paths) + 1}.jsonl"
                with open(file_path, 'w', encoding='utf-8') as f:
                    for req in current_requests:
                        f.write(json.dumps(req, ensure_ascii=False) + '\n')
                file_paths.append(file_path)
                
                # 重置
                current_requests = []
                current_size = 0
        
        # 添加当前请求
        current_requests.append(request)
        current_size += request_size
    
    # 保存最后一个文件
    if current_requests:
        if len(file_paths) == 0:
            # 只有一个文件时，不添加part后缀
            file_path = f"{base_path}.jsonl"
        else:
            file_path = f"{base_path}_part{len(file_paths) + 1}.jsonl"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for req in current_requests:
                f.write(json.dumps(req, ensure_ascii=False) + '\n')
        file_paths.append(file_path)
    
    return file_paths


def generate_sentiment_polarity_requests(posts: List[Dict[str, Any]], data_dir: str) -> List[Dict[str, Any]]:
    """生成情感极性分析请求列表"""
    requests = []
    
    for i, post in enumerate(posts):
        # 检查是否有图片
        image_urls = post.get('image_urls', [])
        image_urls = [img for img in image_urls if img and img.strip()]
        has_images = len(image_urls) > 0
        
        # 构建提示词（与nodes.py中完全一致）
        prompt = f"""你是一个专业的社交媒体内容分析师。
你的任务是分析以下博文内容（包括文本和图片）的整体情感极性。

情感极性评分标准如下：
1 - 极度悲观 (例如：愤怒、绝望、极度不满)
2 - 悲观 (例如：失望、担忧、轻微不满)
3 - 无明显极性 (例如：事实陈述、客观描述、无明显情感倾向)
4 - 乐观 (例如：开心、满意、期待)
5 - 极度乐观 (例如：兴奋、感激、极度喜悦)

要求：
1. 请仔细阅读文本、观察图片，并结合上述评分标准，对这篇博文的整体情感倾向做出判断
2. 你的最终输出必须只包含一个代表判断结果的阿拉伯数字（1-5）
3. 不要添加任何解释、标题、引言、JSON格式或其他任何文本
4. 如果内容无法判断或超出理解范围，请输出数字0

--- 示例 1：极度悲观 (1分) ---
博文内容：我家被洪水淹了！所有的家具都毁了，一年的积蓄就这样没了！天啊，我该怎么办！
预期输出：1

--- 示例 2：悲观 (2分) ---
博文内容：这场雨下得太大了，出门很不方便，担心会影响明天的上班。
预期输出：2

--- 示例 3：无明显极性 (3分) ---
博文内容：北京市气象台发布暴雨红色预警信号，预计未来3小时内将出现特大暴雨。
预期输出：3

--- 示例 4：乐观 (4分) ---
博文内容：虽然下大雨，但看到邻居们互相帮助，心里很温暖，相信很快就会好起来的。
预期输出：4

--- 示例 5：极度乐观 (5分) ---
博文内容：太棒了！救援队及时赶到，所有人都安全了！感谢所有救援人员，你们是最棒的！
预期输出：5

--- 示例 6：无法判断 (0分) ---
博文内容：asdfghjkl123456789
预期输出：0

现在请分析以下博文内容：

{post.get('content', '')}"""
        
        # 选择模型
        model = select_model_for_analysis("sentiment_polarity", has_images)
        temperature = get_temperature_for_analysis("sentiment_polarity")
        
        # 构建请求
        if has_images:
            # 有图片时的多模态请求
            image_content = process_images_for_batch(post, data_dir)
            content = [
                {"type": "text", "text": prompt}
            ] + image_content
        else:
            # 无图片时的纯文本请求
            content = prompt
        
        request = {
            "custom_id": f"sentiment_polarity_{i:06d}",
            "method": "POST",
            "url": "/v4/chat/completions",
            "body": {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的社交媒体内容分析师。"
                    },
                    {
                        "role": "user",
                        "content": content
                    }
                ],
                "temperature": temperature
            }
        }
        
        requests.append(request)
    
    return requests


def generate_sentiment_attribute_requests(posts: List[Dict[str, Any]], attributes: List[str]) -> List[Dict[str, Any]]:
    """生成情感属性分析请求列表"""
    requests = []
    
    for i, post in enumerate(posts):
        # 构建情感属性列表字符串
        attributes_str = "、".join(attributes)
        
        # 构建提示词（与nodes.py中完全一致）
        prompt = f"""你是一个专业的社交媒体内容分析师。
你的任务是分析以下博文内容（包括文本和图片）的整体情感属性。

可选择的情感属性：{attributes_str}

请从上述列表中选择1-3个最贴切的情感属性，严格按照以下JSON格式输出：

["属性1", "属性2"]

请直接输出JSON数组，不要添加任何其他文字

--- 示例 1：愤怒内容 ---
博文内容：这场暴雨太让人愤怒了！政府为什么不提前预警！
预期输出：["生气"]

--- 示例 2：支持赞赏内容 ---
博文内容：感谢救援人员的辛勤付出，你们是最棒的！
预期输出：["支持", "赞赏"]

--- 示例 3：中立客观内容 ---
博文内容：北京市气象台发布暴雨红色预警信号。
预期输出：["中立"]

--- 示例 4：担忧焦虑内容 ---
博文内容：担心这场雨会造成更大的损失，希望大家都平安。
预期输出：["担忧", "焦虑"]

--- 示例 5：希望期待内容 ---
博文内容：希望雨快点停，明天能正常上班。
预期输出：["希望", "期待"]


博文内容：
{post.get('content', '')}
"""
        
        # 选择模型和参数
        model = select_model_for_analysis("sentiment_attribute", False)
        temperature = get_temperature_for_analysis("sentiment_attribute")
        
        request = {
            "custom_id": f"sentiment_attribute_{i:06d}",
            "method": "POST",
            "url": "/v4/chat/completions",
            "body": {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的社交媒体内容分析师。"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": temperature
            }
        }
        
        requests.append(request)
    
    return requests


def generate_topic_analysis_requests(posts: List[Dict[str, Any]], topics: List[Dict[str, Any]], data_dir: str) -> List[Dict[str, Any]]:
    """生成两级主题分析请求列表"""
    requests = []
    
    for i, post in enumerate(posts):
        # 构建主题层次结构字符串
        topics_str = ""
        for topic_group in topics:
            parent_topic = topic_group.get("parent_topic", "")
            sub_topics = topic_group.get("sub_topics", [])
            topics_str += f"\n父主题：{parent_topic}\n子主题：{'、'.join(sub_topics)}\n"
        
        # 构建提示词（与nodes.py中完全一致）
        prompt = f"""你是一个专业的社交媒体内容分析师。
你的任务是分析以下博文内容（包括文本和图片）的主题层次结构。

候选的主题层次结构：{topics_str}

请从上述主题列表中选择1-2个最贴切的父主题和对应的子主题，严格按照以下JSON格式输出，请直接输出JSON数组，不要添加任何其他文字：

[{{"parent_topic": "父主题", "sub_topic": "子主题"}}, ...]

如果没有找到符合的主题，请输出空数组：[]

--- 示例 1：暴雨灾害内容 ---
博文内容：北京遭遇特大暴雨，多地出现严重内涝。
预期输出：[{{"parent_topic": "自然灾害", "sub_topic": "暴雨"}}]

--- 示例 2：地铁运营内容 ---
博文内容：因暴雨影响，地铁部分线路暂停运营。
预期输出：[{{"parent_topic": "交通运输", "sub_topic": "地铁运营"}}]

--- 示例 3：政府预警内容 ---
博文内容：市政府发布暴雨红色预警，请市民注意防范。
预期输出：[{{"parent_topic": "政府工作", "sub_topic": "预警发布"}}]

--- 示例 4：多主题内容 ---
博文内容：暴雨导致地铁停运，政府启动应急响应。
预期输出：[{{"parent_topic": "自然灾害", "sub_topic": "暴雨"}}, {{"parent_topic": "交通运输", "sub_topic": "地铁运营"}}]

--- 示例 5：无相关主题内容 ---
博文内容：今天天气不错，适合出门散步。
预期输出：[]

博文内容：
{post.get('content', '')}
"""
        
        # 检查是否有图片
        image_urls = post.get('image_urls', [])
        image_urls = [img for img in image_urls if img and img.strip()]
        has_images = len(image_urls) > 0
        
        # 选择模型和参数
        model = select_model_for_analysis("topic_analysis", has_images)
        temperature = get_temperature_for_analysis("topic_analysis")
        
        # 构建请求
        if has_images:
            # 有图片时的多模态请求
            image_content = process_images_for_batch(post, data_dir)
            content = [
                {"type": "text", "text": prompt}
            ] + image_content
        else:
            # 无图片时的纯文本请求
            content = prompt
        
        request = {
            "custom_id": f"topic_analysis_{i:06d}",
            "method": "POST",
            "url": "/v4/chat/completions",
            "body": {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的社交媒体内容分析师。"
                    },
                    {
                        "role": "user",
                        "content": content
                    }
                ],
                "temperature": temperature
            }
        }
        
        requests.append(request)
    
    return requests


def generate_publisher_analysis_requests(posts: List[Dict[str, Any]], publishers: List[str]) -> List[Dict[str, Any]]:
    """生成发布者对象分析请求列表"""
    requests = []
    
    for i, post in enumerate(posts):
        # 构建发布者对象列表字符串
        publishers_str = "、".join(publishers)
        
        # 构建提示词（与nodes.py中完全一致）
        prompt = f"""分析博文发布者类型

你是一个专业的社交媒体内容分析师。
你的任务是分析以下博文内容（包括文本和图片）的博文发布者类型。
可选择的发布者对象：{publishers_str}


请从上述列表中选择1个最贴切的发布者对象，直接输出发布者类型字符串。请直接输出发布者类型字符串，不要添加任何其他文字

--- 示例 1：气象台内容 ---
博文内容：北京市气象台发布暴雨红色预警信号。
预期输出：政府机构

--- 示例 2：媒体内容 ---
博文内容：本报记者现场报道暴雨情况。
预期输出：官方新闻媒体

--- 示例 3：普通用户内容 ---
博文内容：今天雨好大，出门要小心。
预期输出：个人用户

--- 示例 4：事业单位内容 ---
博文内容：北京地铁发布运营调整通知。
预期输出：事业单位

--- 示例 5：应急管理部门内容 ---
博文内容：消防部门提醒市民注意安全。
预期输出：应急管理部门

博文内容：
{post.get('content', '')}
"""
        
        # 选择模型和参数
        model = select_model_for_analysis("publisher_analysis", False)
        temperature = get_temperature_for_analysis("publisher_analysis")
        
        request = {
            "custom_id": f"publisher_analysis_{i:06d}",
            "method": "POST",
            "url": "/v4/chat/completions",
            "body": {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的社交媒体内容分析师。"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": temperature
            }
        }
        
        requests.append(request)
    
    return requests


def create_all_jsonl_files(posts: List[Dict[str, Any]], 
                         topics: List[Dict[str, Any]],
                         sentiment_attributes: List[str],
                         publishers: List[str],
                         data_dir: str,
                         temp_dir: str) -> Dict[str, List[str]]:
    """
    创建所有四个JSONL文件，支持自动拆分
    
    Args:
        posts: 博文数据列表
        topics: 主题层次结构
        sentiment_attributes: 情感属性列表
        publishers: 发布者对象列表
        data_dir: 数据目录路径
        temp_dir: 临时文件目录路径
        
    Returns:
        分析类型到文件路径列表的映射
    """
    jsonl_files = {}
    
    print(f"开始生成JSONL文件，总共 {len(posts)} 条博文")
    
    # 创建情感极性分析JSONL
    print("生成情感极性分析请求...")
    sentiment_polarity_requests = generate_sentiment_polarity_requests(posts, data_dir)
    sentiment_polarity_files = split_requests_by_limits(
        sentiment_polarity_requests, 
        os.path.join(temp_dir, "sentiment_polarity_batch")
    )
    jsonl_files["sentiment_polarity"] = sentiment_polarity_files
    print(f"创建情感极性分析JSONL文件: {len(sentiment_polarity_requests)} 条请求 -> {len(sentiment_polarity_files)} 个文件")
    
    # 创建情感属性分析JSONL
    print("生成情感属性分析请求...")
    sentiment_attribute_requests = generate_sentiment_attribute_requests(posts, sentiment_attributes)
    sentiment_attribute_files = split_requests_by_limits(
        sentiment_attribute_requests,
        os.path.join(temp_dir, "sentiment_attribute_batch")
    )
    jsonl_files["sentiment_attribute"] = sentiment_attribute_files
    print(f"创建情感属性分析JSONL文件: {len(sentiment_attribute_requests)} 条请求 -> {len(sentiment_attribute_files)} 个文件")
    
    # 创建主题分析JSONL
    print("生成主题分析请求...")
    topic_analysis_requests = generate_topic_analysis_requests(posts, topics, data_dir)
    topic_analysis_files = split_requests_by_limits(
        topic_analysis_requests,
        os.path.join(temp_dir, "topic_analysis_batch")
    )
    jsonl_files["topic_analysis"] = topic_analysis_files
    print(f"创建主题分析JSONL文件: {len(topic_analysis_requests)} 条请求 -> {len(topic_analysis_files)} 个文件")
    
    # 创建发布者对象分析JSONL
    print("生成发布者对象分析请求...")
    publisher_analysis_requests = generate_publisher_analysis_requests(posts, publishers)
    publisher_analysis_files = split_requests_by_limits(
        publisher_analysis_requests,
        os.path.join(temp_dir, "publisher_analysis_batch")
    )
    jsonl_files["publisher_analysis"] = publisher_analysis_files
    print(f"创建发布者对象分析JSONL文件: {len(publisher_analysis_requests)} 条请求 -> {len(publisher_analysis_files)} 个文件")
    
    # 显示文件大小信息
    print("\n文件大小信息:")
    total_size = 0
    for analysis_type, files in jsonl_files.items():
        for file_path in files:
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                total_size += size
                print(f"  {file_path}: {size/1024/1024:.2f} MB")
    
    print(f"总大小: {total_size/1024/1024:.2f} MB")
    
    return jsonl_files


# 保持向后兼容的旧函数
def generate_sentiment_polarity_jsonl(posts: List[Dict[str, Any]], data_dir: str, output_path: str) -> int:
    """生成情感极性分析JSONL文件（向后兼容）"""
    requests = generate_sentiment_polarity_requests(posts, data_dir)
    with open(output_path, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    return len(requests)


def generate_sentiment_attribute_jsonl(posts: List[Dict[str, Any]], attributes: List[str], output_path: str) -> int:
    """生成情感属性分析JSONL文件（向后兼容）"""
    requests = generate_sentiment_attribute_requests(posts, attributes)
    with open(output_path, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    return len(requests)


def generate_topic_analysis_jsonl(posts: List[Dict[str, Any]], topics: List[Dict[str, Any]], data_dir: str, output_path: str) -> int:
    """生成两级主题分析JSONL文件（向后兼容）"""
    requests = generate_topic_analysis_requests(posts, topics, data_dir)
    with open(output_path, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    return len(requests)


def generate_publisher_analysis_jsonl(posts: List[Dict[str, Any]], publishers: List[str], output_path: str) -> int:
    """生成发布者对象分析JSONL文件（向后兼容）"""
    requests = generate_publisher_analysis_requests(posts, publishers)
    with open(output_path, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    return len(requests)


if __name__ == "__main__":
    print("JSONL生成工具模块加载成功")
