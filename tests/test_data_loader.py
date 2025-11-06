import sys
import os
import json

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_loader import (
    load_blog_data,
    load_topics,
    load_sentiment_attributes,
    load_publisher_objects,
    save_enhanced_blog_data,
    load_enhanced_blog_data,
    load_all_reference_data
)

def test_load_blog_data():
    """测试博文数据加载"""
    print("\n=== 测试博文数据加载 ===")
    try:
        data = load_blog_data("data/beijing_rainstorm_posts.json")
        print(f"[OK] 成功加载 {len(data)} 条博文数据")
        
        # 检查数据结构
        if data and isinstance(data[0], dict):
            sample = data[0]
            required_fields = ['username', 'user_id', 'content', 'publish_time', 'location', 
                           'repost_count', 'comment_count', 'like_count', 'image_urls']
            
            print("字段检查:")
            for field in required_fields:
                if field in sample:
                    print(f"  [OK] {field}: {type(sample[field]).__name__}")
                else:
                    print(f"  [ERROR] {field}: 缺失")
                    return False
        
        return True
    except Exception as e:
        print(f"[ERROR] 博文数据加载失败: {e}")
        return False

def test_load_topics():
    """测试主题数据加载"""
    print("\n=== 测试主题数据加载 ===")
    try:
        topics = load_topics("data/topics.json")
        print(f"[OK] 成功加载 {len(topics)} 个主题")
        
        # 检查主题结构
        if topics and isinstance(topics[0], dict):
            sample = topics[0]
            if 'parent_topic' in sample and 'sub_topics' in sample:
                print(f"  [OK] 主题结构正确: {sample['parent_topic']}")
                print(f"  [OK] 子主题数量: {len(sample['sub_topics'])}")
                return True
            else:
                print("[ERROR] 主题结构不完整")
                return False
        
        return True
    except Exception as e:
        print(f"[ERROR] 主题数据加载失败: {e}")
        return False

def test_load_sentiment_attributes():
    """测试情感属性数据加载"""
    print("\n=== 测试情感属性数据加载 ===")
    try:
        attributes = load_sentiment_attributes("data/sentiment_attributes.json")
        print(f"[OK] 成功加载 {len(attributes)} 个情感属性")
        
        # 检查数据类型
        if attributes and isinstance(attributes[0], str):
            print(f"  [OK] 情感属性格式正确")
            print(f"  示例: {attributes[:5]}")
            return True
        else:
            print("[ERROR] 情感属性格式不正确")
            return False
        
    except Exception as e:
        print(f"[ERROR] 情感属性数据加载失败: {e}")
        return False

def test_load_publisher_objects():
    """测试发布者对象数据加载"""
    print("\n=== 测试发布者对象数据加载 ===")
    try:
        objects = load_publisher_objects("data/publisher_objects.json")
        print(f"[OK] 成功加载 {len(objects)} 个发布者对象")
        
        # 检查数据类型
        if objects and isinstance(objects[0], str):
            print(f"  [OK] 发布者对象格式正确")
            print(f"  示例: {objects[:5]}")
            return True
        else:
            print("[ERROR] 发布者对象格式不正确")
            return False
        
    except Exception as e:
        print(f"[ERROR] 发布者对象数据加载失败: {e}")
        return False

def test_save_enhanced_data():
    """测试增强数据保存"""
    print("\n=== 测试增强数据保存 ===")
    try:
        # 创建测试数据
        test_data = [
            {
                "username": "测试用户",
                "user_id": "test_user",
                "content": "测试内容",
                "publish_time": "2024-07-30 12:00:00",
                "location": "测试地点",
                "repost_count": 10,
                "comment_count": 5,
                "like_count": 20,
                "image_urls": [],
                "sentiment_polarity": 3,
                "sentiment_attribute": "担忧",
                "topics": "自然灾害",
                "publisher": "个人用户"
            }
        ]
        
        # 保存测试数据
        output_path = "data/test_enhanced_data.json"
        success = save_enhanced_blog_data(test_data, output_path)
        
        if success:
            print(f"[OK] 成功保存测试数据到 {output_path}")
            
            # 验证保存的数据
            loaded_data = load_enhanced_blog_data(output_path)
            if len(loaded_data) == len(test_data):
                print("[OK] 保存数据验证通过")
                
                # 清理测试文件
                if os.path.exists(output_path):
                    os.remove(output_path)
                    print("[OK] 清理测试文件")
                
                return True
            else:
                print("[ERROR] 保存数据验证失败")
                return False
        else:
            print("[ERROR] 数据保存失败")
            return False
        
    except Exception as e:
        print(f"[ERROR] 增强数据保存测试失败: {e}")
        return False

def test_load_all_reference_data():
    """测试加载所有参考数据"""
    print("\n=== 测试加载所有参考数据 ===")
    try:
        all_data = load_all_reference_data()
        
        expected_keys = ['blog_data', 'topics_hierarchy', 'sentiment_attributes', 'publisher_objects']
        
        print("数据检查:")
        for key in expected_keys:
            if key in all_data:
                data = all_data[key]
                if key == 'blog_data':
                    print(f"  [OK] {key}: {len(data)} 条博文")
                elif key == 'topics_hierarchy':
                    print(f"  [OK] {key}: {len(data)} 个主题")
                elif key == 'sentiment_attributes':
                    print(f"  [OK] {key}: {len(data)} 个情感属性")
                elif key == 'publisher_objects':
                    print(f"  [OK] {key}: {len(data)} 个发布者对象")
            else:
                print(f"  [ERROR] {key}: 缺失")
                return False
        
        print("[OK] 所有参考数据加载成功")
        return True
        
    except Exception as e:
        print(f"[ERROR] 参考数据加载失败: {e}")
        return False

def test_shared_dict_compatibility():
    """测试数据与shared字典的兼容性"""
    print("\n=== 测试shared字典兼容性 ===")
    try:
        # 模拟shared字典结构
        shared = {}
        
        # 加载所有参考数据
        all_data = load_all_reference_data()
        
        # 将数据添加到shared字典
        shared.update(all_data)
        
        # 验证shared字典结构
        expected_keys = ['blog_data', 'topics_hierarchy', 'sentiment_attributes', 'publisher_objects']
        
        print("shared字典检查:")
        for key in expected_keys:
            if key in shared:
                data = shared[key]
                print(f"  [OK] shared['{key}']: {type(data).__name__} (长度: {len(data)})")
            else:
                print(f"  [ERROR] shared['{key}']: 缺失")
                return False
        
        print("[OK] shared字典结构正确，便于nodes使用")
        return True
        
    except Exception as e:
        print(f"[ERROR] shared字典兼容性测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始测试data_loader函数...")
    
    test_results = []
    
    # 运行所有测试
    test_results.append(test_load_blog_data())
    test_results.append(test_load_topics())
    test_results.append(test_load_sentiment_attributes())
    test_results.append(test_load_publisher_objects())
    test_results.append(test_save_enhanced_data())
    test_results.append(test_load_all_reference_data())
    test_results.append(test_shared_dict_compatibility())
    
    # 统计结果
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n{'='*50}")
    print(f"测试结果: {passed}/{total} 通过")
    print(f"成功率: {passed/total*100:.1f}%")
    
    if passed == total:
        print("[SUCCESS] 所有测试通过！data_loader函数工作正常")
        return True
    else:
        print("[FAILED] 部分测试失败，请检查data_loader函数")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
