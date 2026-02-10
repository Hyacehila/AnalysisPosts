#!/usr/bin/env python3
"""
修复posts数据中的图片数量
确保每个条目的图片数量不超过3张，超过的则移除多余的图片
"""

import json
import os
from typing import List, Dict, Any


def load_posts_data(file_path: str) -> List[Dict[str, Any]]:
    """加载posts数据"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到文件 {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"错误: JSON文件格式错误 - {e}")
        return []


def save_posts_data(data: List[Dict[str, Any]], file_path: str) -> bool:
    """保存posts数据"""
    try:
        # 创建备份
        backup_path = file_path.replace('.json', '_backup.json')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as original:
                with open(backup_path, 'w', encoding='utf-8') as backup:
                    backup.write(original.read())
            print(f"已创建备份文件: {backup_path}")
        
        # 保存修复后的数据
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"错误: 保存文件失败 - {e}")
        return False


def fix_post_images(posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    修复posts中的图片数量
    返回修复统计信息
    """
    stats = {
        'total_posts': len(posts),
        'posts_with_images': 0,
        'posts_with_too_many_images': 0,
        'total_images_removed': 0,
        'posts_modified': 0
    }
    
    for i, post in enumerate(posts):
        image_urls = post.get('image_urls', [])
        
        # 统计有图片的帖子
        if image_urls:
            stats['posts_with_images'] += 1
        
        # 检查是否超过3张图片
        if len(image_urls) > 3:
            stats['posts_with_too_many_images'] += 1
            original_count = len(image_urls)
            
            # 只保留前3张图片
            post['image_urls'] = image_urls[:3]
            removed_count = original_count - 3
            
            stats['total_images_removed'] += removed_count
            stats['posts_modified'] += 1
            
            print(f"帖子 {i+1} (用户: {post.get('username', 'Unknown')}): "
                  f"从 {original_count} 张图片减少到 3 张，移除了 {removed_count} 张")
    
    return stats


def print_statistics(stats: Dict[str, Any]):
    """打印统计信息"""
    print("\n" + "="*50)
    print("修复统计信息")
    print("="*50)
    print(f"总帖子数量: {stats['total_posts']}")
    print(f"包含图片的帖子: {stats['posts_with_images']}")
    print(f"图片数量超过3张的帖子: {stats['posts_with_too_many_images']}")
    print(f"总共移除的图片数量: {stats['total_images_removed']}")
    print(f"修改的帖子数量: {stats['posts_modified']}")
    print("="*50)


def main():
    """主函数"""
    # 数据文件路径
    posts_file = 'data/posts.json'
    test_posts_file = 'data/test_posts.json'
    
    print("开始修复posts数据中的图片数量...")
    
    # 处理主posts文件
    if os.path.exists(posts_file):
        print(f"\n正在处理: {posts_file}")
        posts = load_posts_data(posts_file)
        if posts:
            stats = fix_post_images(posts)
            if save_posts_data(posts, posts_file):
                print_statistics(stats)
                print(f"[完成] {posts_file} 修复完成")
            else:
                print(f"[失败] {posts_file} 保存失败")
        else:
            print(f"[失败] {posts_file} 加载失败或为空")
    else:
        print(f"[警告] 文件不存在: {posts_file}")
    
    # 处理test_posts文件
    if os.path.exists(test_posts_file):
        print(f"\n正在处理: {test_posts_file}")
        test_posts = load_posts_data(test_posts_file)
        if test_posts:
            stats = fix_post_images(test_posts)
            if save_posts_data(test_posts, test_posts_file):
                print_statistics(stats)
                print(f"[完成] {test_posts_file} 修复完成")
            else:
                print(f"[失败] {test_posts_file} 保存失败")
        else:
            print(f"[失败] {test_posts_file} 加载失败或为空")
    else:
        print(f"[警告] 文件不存在: {test_posts_file}")
    
    print("\n[成功] 所有文件处理完成！")


if __name__ == "__main__":
    main()
