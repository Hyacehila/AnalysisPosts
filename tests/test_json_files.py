import json
import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def validate_json_files():
    """验证创建的JSON文件格式"""
    files_to_check = [
        'data/sentiment_attributes.json',
        'data/publisher_objects.json', 
        'data/topics.json'
    ]
    
    all_valid = True
    
    for file_path in files_to_check:
        print(f'\n=== 验证 {file_path} ===')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f'文件加载成功')
            print(f'数据类型: {type(data).__name__}')
            print(f'数据长度: {len(data)}')
            
            # 检查是否为列表格式
            if isinstance(data, list):
                print('[OK] 数据为列表格式，符合设计要求')
                
                if 'sentiment_attributes' in file_path:
                    print(f'情感属性数量: {len(data)}')
                    print(f'示例: {data[:5]}')
                    # 验证都是字符串
                    all_strings = all(isinstance(item, str) for item in data)
                    if all_strings:
                        print('[OK] 所有情感属性都是字符串格式')
                    else:
                        print('[ERROR] 存在非字符串格式的情感属性')
                        all_valid = False
                    
                elif 'publisher_objects' in file_path:
                    print(f'发布者对象数量: {len(data)}')
                    print(f'示例: {data[:5]}')
                    # 验证都是字符串
                    all_strings = all(isinstance(item, str) for item in data)
                    if all_strings:
                        print('[OK] 所有发布者对象都是字符串格式')
                    else:
                        print('[ERROR] 存在非字符串格式的发布者对象')
                        all_valid = False
                    
                elif 'topics' in file_path:
                    print(f'主题数量: {len(data)}')
                    # 检查两层嵌套结构
                    if data and isinstance(data[0], dict) and 'parent_topic' in data[0] and 'sub_topics' in data[0]:
                        print('[OK] 主题具有两层嵌套结构')
                        print(f'示例主题: {data[0]}')
                        total_sub_topics = sum(len(item.get('sub_topics', [])) for item in data)
                        print(f'子主题总数: {total_sub_topics}')
                        
                        # 验证每个主题的结构
                        for i, topic in enumerate(data):
                            if not isinstance(topic, dict):
                                print(f'[ERROR] 主题 {i} 不是字典格式')
                                all_valid = False
                                continue
                            
                            if 'parent_topic' not in topic or 'sub_topics' not in topic:
                                print(f'[ERROR] 主题 {i} 缺少必需字段')
                                all_valid = False
                                continue
                            
                            if not isinstance(topic['sub_topics'], list):
                                print(f'[ERROR] 主题 {i} 的sub_topics不是列表格式')
                                all_valid = False
                                continue
                                
                        print('[OK] 所有主题结构验证通过')
                    else:
                        print('[ERROR] 主题结构不符合两层嵌套要求')
                        all_valid = False
            else:
                print('[ERROR] 数据不是列表格式')
                all_valid = False
                
        except Exception as e:
            print(f'[ERROR] 验证失败: {e}')
            all_valid = False
    
    return all_valid

def test_data_loading():
    """测试数据加载功能"""
    print(f'\n{"="*50}')
    print(f'测试数据加载功能')
    print(f'{"="*50}')
    
    try:
        # 测试情感属性加载
        with open('data/sentiment_attributes.json', 'r', encoding='utf-8') as f:
            sentiment_attributes = json.load(f)
        print(f'[OK] 情感属性加载成功，共 {len(sentiment_attributes)} 个')
        
        # 测试发布者对象加载
        with open('data/publisher_objects.json', 'r', encoding='utf-8') as f:
            publisher_objects = json.load(f)
        print(f'[OK] 发布者对象加载成功，共 {len(publisher_objects)} 个')
        
        # 测试主题加载
        with open('data/topics.json', 'r', encoding='utf-8') as f:
            topics = json.load(f)
        print(f'[OK] 主题加载成功，共 {len(topics)} 个父主题')
        
        # 测试主题结构访问
        for topic in topics[:3]:  # 测试前3个主题
            parent = topic['parent_topic']
            sub_count = len(topic['sub_topics'])
            print(f'  主题 "{parent}" 包含 {sub_count} 个子主题')
        
        print('[OK] 所有数据加载测试通过')
        return True
        
    except Exception as e:
        print(f'[ERROR] 数据加载测试失败: {e}')
        return False

def main():
    """主函数"""
    print('开始验证JSON文件格式...')
    
    # 验证文件格式
    format_valid = validate_json_files()
    
    # 测试数据加载
    loading_valid = test_data_loading()
    
    if format_valid and loading_valid:
        print(f'\n[SUCCESS] 所有JSON文件验证通过！')
        return True
    else:
        print(f'\n[FAILED] JSON文件验证失败！')
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
