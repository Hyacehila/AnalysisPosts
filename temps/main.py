from flow import ThemeAnalyzerFlow, SentimentAnalyzerFlow
import json
from pprint import pprint

def initialize_shared_state(data_file='data.json', topic_file='topic.json'):
    """
    从指定的JSON文件中读取数据，并初始化工作流的共享状态字典。
    新版本适配了 data.json 为对象数组的情况。

    Args:
        data_file (str): 包含博文内容列表的JSON文件名。
        topic_file (str): 包含主题结构的JSON文件名。

    Returns:
        dict: 一个包含所有必要初始数据的共享状态字典。
              新结构如下:
              {
                  "posts": [ {post_1}, {post_2}, ... ], #posts 是一个对象数组/列表
                                                        每个元素是一个博文对象/字典,字典中包含名称保持了和 data.json 中相同的字段
                  "themes_optional": [...]              #themes_optional 是一个主题列表,包含了备选主题.
              }
    """
    shared = {}

    # --- 从 data.json 获取博文列表 ---
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            # json.load 现在会返回一个Python列表,列表中的每个元素都是一个博文对象/字典
                                              #因为 data.json 是一个对象数组.
            posts_list = json.load(f)
        
        # 将整个列表存入共享状态
        shared['posts'] = posts_list
        print(f"✅ 成功从 {data_file} 加载了 {len(posts_list)} 篇博文。")
    except FileNotFoundError:
        print(f"❌ 错误: 文件 {data_file} 未找到。将使用空博文列表。")
        shared['posts'] = []  # 默认为空列表，保持数据类型一致
    except json.JSONDecodeError:
        print(f"❌ 错误: 文件 {data_file} 不是有效的JSON格式。将使用博文空列表。")
        shared['posts'] = []

    # --- 从 topic.json 获取主题列表 ---
    try:
        with open(topic_file, 'r', encoding='utf-8') as f:
            # topic.json 使用对象数组结构,读取后就是列表,因此可以直接处理.
            topic_list = json.load(f)
        
        # 直接将加载的列表赋值给共享状态
        shared['themes_optional'] = topic_list
        print(f"✅ 成功从 {topic_file} 加载主题列表。")
    except FileNotFoundError:
        print(f"❌ 错误: 文件 {topic_file} 未找到。将使用空主题列表。")
        shared['themes_optional'] = []
    except json.JSONDecodeError:
        print(f"❌ 错误: 文件 {topic_file} 不是有效的JSON格式。将使用空主题列表。")
        shared['themes_optional'] = []
        
    return shared


# --- 示例执行 ---
if __name__ == "__main__":
    # 初始化整个预备数据字典shared,
    shared = initialize_shared_state()
    # 从类中实例化flow
    flow = SentimentAnalyzerFlow()
    # 执行flow 打印flow的结果
    print(flow.run(shared))




