#!/usr/bin/env python3
"""
从 posts.json 文件中提取前20条数据作为测试数据
"""

import json
import os
from typing import List, Dict, Any

def extract_test_posts(input_file: str, output_file: str, num_posts: int = 20) -> bool:
    """
    从输入文件中提取前N条数据并保存到输出文件
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
        num_posts: 要提取的条目数量
        
    Returns:
        是否成功
    """
    try:
        # 检查输入文件是否存在
        if not os.path.exists(input_file):
            print(f"错误：找不到输入文件 {input_file}")
            return False
        
        # 读取原始数据
        print(f"正在读取 {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"读取完成，共 {len(data)} 条记录")
        
        # 检查是否有足够的数据
        if len(data) < num_posts:
            print(f"警告：只有 {len(data)} 条记录，少于请求的 {num_posts} 条")
            num_posts = len(data)
        
        # 提取前N条数据
        test_posts = data[:num_posts]
        
        # 保存测试数据
        print(f"正在保存前 {num_posts} 条数据到 {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(test_posts, f, ensure_ascii=False, indent=2)
        
        print(f"成功提取 {num_posts} 条测试数据！")
        
        # 显示提取的数据统计信息
        print("\n提取的数据统计：")
        print(f"总条目数: {len(test_posts)}")
        
        # 统计有图片的条目
        posts_with_images = sum(1 for post in test_posts if post.get('image_urls') and len(post['image_urls']) > 0)
        print(f"有图片的条目数: {posts_with_images}")
        
        # 统计总图片数
        total_images = sum(len(post.get('image_urls', [])) for post in test_posts)
        print(f"总图片数: {total_images}")
        
        # 显示前几个条目的基本信息
        print("\n前3个条目预览：")
        for i, post in enumerate(test_posts[:3]):
            print(f"\n条目 {i+1}:")
            print(f"  用户名: {post.get('username', 'Unknown')}")
            print(f"  发布时间: {post.get('publish_time', 'Unknown')}")
            print(f"  位置: {post.get('location', 'Unknown')}")
            print(f"  图片数量: {len(post.get('image_urls', []))}")
            content = post.get('content', '')
            # 清理可能导致编码问题的字符
            clean_content = content.replace('\u200b', '').replace('\ufeff', '')[:50]
            print(f"  内容预览: {clean_content}...")
        
        return True
        
    except Exception as e:
        print(f"错误：{e}")
        return False

def main():
    """主函数"""
    
    # 文件路径
    input_file = 'data/posts.json'
    output_file = 'data/test_posts.json'
    
    print("=== 提取测试数据脚本 ===")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"提取数量: 20 条")
    print("-" * 40)
    
    # 提取测试数据
    success = extract_test_posts(input_file, output_file, 20)
    
    if success:
        print("\n[成功] 测试数据提取完成！")
        print(f"您现在可以使用 {output_file} 作为测试数据文件。")
    else:
        print("\n[失败] 测试数据提取失败！")

if __name__ == "__main__":
    main()
