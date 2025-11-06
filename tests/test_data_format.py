import json
import sys
import os
import re
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def validate_blog_data(data_file_path: str) -> bool:
    """
    验证博文数据格式是否符合design文档要求
    
    Args:
        data_file_path: JSON数据文件路径
        
    Returns:
        bool: 验证是否通过
    """
    try:
        # 读取JSON文件
        with open(data_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f'数据加载成功，共 {len(data)} 条记录')
        
        # 检查必需字段
        required_fields = ['username', 'user_id', 'content', 'publish_time', 'location', 'repost_count', 'comment_count', 'like_count', 'image_urls']
        
        print('\n=== 数据格式验证 ===')
        for i, record in enumerate(data[:5]):  # 检查前5条记录
            print(f'\n记录 {i+1}:')
            for field in required_fields:
                if field in record:
                    print(f'  [OK] {field}: {type(record[field]).__name__}')
                    if field == 'image_urls':
                        print(f'    值: {record[field]}')
                else:
                    print(f'  [ERROR] {field}: 缺失')
        
        # 统计信息
        print(f'\n=== 统计信息 ===')
        print(f'总记录数: {len(data)}')
        
        # 检查时间范围
        times = [record['publish_time'] for record in data]
        print(f'时间范围: {min(times)} 到 {max(times)}')
        
        # 检查地点分布
        locations = set(record['location'] for record in data)
        print(f'涉及地点数: {len(locations)}')
        print(f'地点列表: {sorted(list(locations))}')
        
        # 检查图片URL
        image_counts = [len(record['image_urls']) for record in data]
        print(f'图片URL统计: 最少{min(image_counts)}个，最多{max(image_counts)}个')
        
        # 检查数据类型
        print(f'\n=== 数据类型检查 ===')
        sample_record = data[0]
        for field in required_fields:
            value = sample_record[field]
            expected_type = {
                'username': str,
                'user_id': str, 
                'content': str,
                'publish_time': str,
                'location': str,
                'repost_count': int,
                'comment_count': int,
                'like_count': int,
                'image_urls': list
            }
            if isinstance(value, expected_type[field]):
                print(f'  [OK] {field}: 类型正确 ({expected_type[field].__name__})')
            else:
                print(f'  [ERROR] {field}: 类型错误，期望 {expected_type[field].__name__}，实际 {type(value).__name__}')
                return False
        
        # 检查所有记录的完整性
        print(f'\n=== 完整性检查 ===')
        missing_fields_count = 0
        for i, record in enumerate(data):
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                missing_fields_count += 1
                if missing_fields_count <= 5:  # 只显示前5个有问题的记录
                    print(f'  记录 {i+1} 缺失字段: {missing_fields}')
        
        if missing_fields_count > 0:
            print(f'  总共有 {missing_fields_count} 条记录存在字段缺失')
            return False
        else:
            print('  [OK] 所有记录字段完整')
        
        # 检查数值范围
        print(f'\n=== 数值范围检查 ===')
        repost_counts = [record['repost_count'] for record in data]
        comment_counts = [record['comment_count'] for record in data]
        like_counts = [record['like_count'] for record in data]
        
        print(f'转发数范围: {min(repost_counts)} - {max(repost_counts)}')
        print(f'评论数范围: {min(comment_counts)} - {max(comment_counts)}')
        print(f'点赞数范围: {min(like_counts)} - {max(like_counts)}')
        
        # 检查负数
        negative_counts = sum(1 for record in data if record['repost_count'] < 0 or record['comment_count'] < 0 or record['like_count'] < 0)
        if negative_counts > 0:
            print(f'  [ERROR] 发现 {negative_counts} 条记录包含负数')
            return False
        else:
            print('  [OK] 所有数值均为非负数')
        
        # 增强验证功能
        if not _validate_time_format(data):
            return False
        
        if not _validate_content_quality(data):
            return False
        
        if not _validate_business_logic(data):
            return False
        
        if not _validate_data_consistency(data):
            return False
        
        _generate_enhanced_report(data)
        
        print('\n[SUCCESS] 数据格式验证通过！')
        return True
        
    except FileNotFoundError:
        print(f'[ERROR] 文件不存在: {data_file_path}')
        return False
    except json.JSONDecodeError as e:
        print(f'[ERROR] JSON格式错误: {e}')
        return False
    except Exception as e:
        print(f'[ERROR] 验证失败: {e}')
        return False

def _validate_time_format(data: List[Dict]) -> bool:
    """验证时间格式和合理性"""
    print(f'\n=== 时间格式验证 ===')
    time_pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    valid_times = []
    time_errors = 0
    
    for i, record in enumerate(data):
        time_str = record.get('publish_time', '')
        if not time_pattern.match(time_str):
            time_errors += 1
            if time_errors <= 3:
                print(f'  [ERROR] 记录 {i+1} 时间格式错误: {time_str}')
            continue
        
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            valid_times.append(dt)
        except ValueError:
            time_errors += 1
            if time_errors <= 3:
                print(f'  [ERROR] 记录 {i+1} 时间解析失败: {time_str}')
    
    if time_errors > 0:
        print(f'  [ERROR] 发现 {time_errors} 个时间格式错误')
        return False
    
    if valid_times:
        min_time = min(valid_times)
        max_time = max(valid_times)
        time_span = max_time - min_time
        
        print(f'  时间范围: {min_time} 到 {max_time}')
        print(f'  时间跨度: {time_span}')
        
        if time_span.days > 365:
            print(f'  [WARNING] 时间跨度超过一年，可能不合理')
        elif time_span.days < 1:
            print(f'  [WARNING] 时间跨度小于一天，数据可能过于集中')
    
    print(f'  [OK] 时间格式验证通过')
    return True

def _validate_content_quality(data: List[Dict]) -> bool:
    """验证内容质量"""
    print(f'\n=== 内容质量验证 ===')
    content_issues = 0
    content_lengths = []
    duplicate_contents = {}
    
    for i, record in enumerate(data):
        content = record.get('content', '')
        
        if len(content) == 0:
            content_issues += 1
            if content_issues <= 3:
                print(f'  [ERROR] 记录 {i+1} content 为空')
        elif len(content) < 10:
            print(f'  [WARNING] 记录 {i+1} content 过短: {len(content)} 字符')
        elif len(content) > 1000:
            print(f'  [WARNING] 记录 {i+1} content 过长: {len(content)} 字符')
        
        content_lengths.append(len(content))
        
        # 检查重复内容
        if content in duplicate_contents:
            duplicate_contents[content].append(i)
        else:
            duplicate_contents[content] = [i]
    
    # 报告重复内容
    duplicates = {content: indices for content, indices in duplicate_contents.items() if len(indices) > 1}
    if duplicates:
        print(f'  [WARNING] 发现 {len(duplicates)} 条重复内容')
    
    if content_issues > 0:
        print(f'  [ERROR] 发现 {content_issues} 个内容质量问题')
        return False
    
    print(f'  内容长度范围: {min(content_lengths)} - {max(content_lengths)} 字符')
    print(f'  平均内容长度: {sum(content_lengths)/len(content_lengths):.1f} 字符')
    print(f'  [OK] 内容质量验证通过')
    return True

def _validate_business_logic(data: List[Dict]) -> bool:
    """验证业务逻辑"""
    print(f'\n=== 业务逻辑验证 ===')
    logic_errors = 0
    
    for i, record in enumerate(data):
        # 检查用户名和用户ID
        username = record.get('username', '')
        user_id = record.get('user_id', '')
        
        if not username or not user_id:
            logic_errors += 1
            if logic_errors <= 3:
                print(f'  [ERROR] 记录 {i+1} 用户名或用户ID为空')
        
        # 检查地理位置
        location = record.get('location', '')
        if not location or '北京' not in location:
            print(f'  [WARNING] 记录 {i+1} 地理位置可能不相关: {location}')
        
        # 检查图片URL格式
        image_urls = record.get('image_urls', [])
        if not isinstance(image_urls, list):
            logic_errors += 1
            if logic_errors <= 3:
                print(f'  [ERROR] 记录 {i+1} image_urls 不是列表格式')
        else:
            for j, url in enumerate(image_urls):
                if url and not (url.startswith('http://') or url.startswith('https://')):
                    print(f'  [WARNING] 记录 {i+1} 图片URL格式可能不正确: {url}')
    
    if logic_errors > 0:
        print(f'  [ERROR] 发现 {logic_errors} 个业务逻辑错误')
        return False
    
    print(f'  [OK] 业务逻辑验证通过')
    return True

def _validate_data_consistency(data: List[Dict]) -> bool:
    """验证数据一致性"""
    print(f'\n=== 数据一致性验证 ===')
    
    # 检查用户分布
    user_counts = {}
    for record in data:
        user_id = record.get('user_id', '')
        user_counts[user_id] = user_counts.get(user_id, 0) + 1
    
    # 检查是否有用户发布过多内容
    for user_id, count in user_counts.items():
        if count > 20:
            print(f'  [WARNING] 用户 {user_id} 发布内容过多: {count} 条')
    
    print(f'  唯一用户数: {len(user_counts)}')
    print(f'  平均每用户发布: {len(data)/len(user_counts):.1f} 条')
    print(f'  [OK] 数据一致性验证通过')
    return True

def _generate_enhanced_report(data: List[Dict]):
    """生成增强验证报告"""
    print(f'\n{"="*50}')
    print(f'增强数据验证报告')
    print(f'{"="*50}')
    
    # 数值统计分析
    repost_counts = [record['repost_count'] for record in data]
    comment_counts = [record['comment_count'] for record in data]
    like_counts = [record['like_count'] for record in data]
    
    print(f'\n数值统计分析:')
    for field_name, values in [('转发数', repost_counts), ('评论数', comment_counts), ('点赞数', like_counts)]:
        values_sorted = sorted(values)
        print(f'  {field_name}:')
        print(f'    范围: {min(values)} - {max(values)}')
        print(f'    平均: {sum(values)/len(values):.1f}')
        print(f'    中位数: {values_sorted[len(values)//2]}')
        print(f'    前10%: {values_sorted[int(len(values)*0.9)]}')
    
    # 地理分布统计
    locations = {}
    for record in data:
        location = record.get('location', '')
        locations[location] = locations.get(location, 0) + 1
    
    print(f'\n地理分布统计:')
    for location, count in sorted(locations.items(), key=lambda x: x[1], reverse=True)[:10]:
        percentage = (count / len(data)) * 100
        print(f'  {location}: {count} 条 ({percentage:.1f}%)')
    
    print(f'{"="*50}')

def test_beijing_rainstorm_data():
    """测试北京暴雨数据格式"""
    # 使用绝对路径，避免相对路径问题
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    data_file = os.path.join(project_root, 'data', 'beijing_rainstorm_posts.json')
    return validate_blog_data(data_file)

def main():
    """主函数，用于命令行调用"""
    success = test_beijing_rainstorm_data()
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
