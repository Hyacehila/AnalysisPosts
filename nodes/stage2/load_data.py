"""
Stage 2 data loading and summary nodes.
"""
import os
from collections import Counter
from datetime import datetime

from nodes.base import MonitoredNode

from utils.data_loader import load_enhanced_blog_data


class LoadEnhancedDataNode(MonitoredNode):
    """
    加载增强数据节点
    """

    def prep(self, shared):
        config = shared.get("config", {})
        enhanced_data_path = config.get("data_source", {}).get(
            "enhanced_data_path", "data/enhanced_blogs.json"
        )

        if not os.path.exists(enhanced_data_path):
            raise FileNotFoundError(
                f"阶段1输出文件不存在: {enhanced_data_path}\n"
                f"请先运行阶段1（增强处理）或确保文件路径正确"
            )

        return {"data_path": enhanced_data_path}

    def exec(self, prep_res):
        data_path = prep_res["data_path"]

        print(f"\n[LoadEnhancedData] 加载增强数据: {data_path}")
        blog_data = load_enhanced_blog_data(data_path)

        enhanced_fields = ["sentiment_polarity", "sentiment_attribute", "topics", "publisher"]
        valid_count = 0
        for post in blog_data:
            has_all_fields = all(post.get(field) is not None for field in enhanced_fields)
            if has_all_fields:
                valid_count += 1

        return {
            "blog_data": blog_data,
            "total_count": len(blog_data),
            "valid_count": valid_count,
            "enhancement_rate": round(valid_count / len(blog_data) * 100, 2) if blog_data else 0,
        }

    def post(self, shared, prep_res, exec_res):
        if "data" not in shared:
            shared["data"] = {}

        shared["data"]["blog_data"] = exec_res["blog_data"]

        print(f"[LoadEnhancedData] [√] 加载 {exec_res['total_count']} 条博文")
        print(f"[LoadEnhancedData] [√] 完整增强率: {exec_res['enhancement_rate']}%")

        return "default"


class DataSummaryNode(MonitoredNode):
    """
    数据概况生成节点
    """

    def prep(self, shared):
        return shared.get("data", {}).get("blog_data", [])

    def exec(self, prep_res):
        blog_data = prep_res

        if not blog_data:
            return {"summary": "无数据", "statistics": {}}

        total = len(blog_data)

        sentiment_dist = Counter(p.get("sentiment_polarity") for p in blog_data if p.get("sentiment_polarity"))
        publisher_dist = Counter(p.get("publisher") for p in blog_data if p.get("publisher"))

        parent_topics = Counter()
        for p in blog_data:
            topics = p.get("topics") or []
            if not isinstance(topics, list):
                continue
            for t in topics:
                if isinstance(t, dict) and t.get("parent_topic"):
                    parent_topics[t["parent_topic"]] += 1

        location_dist = Counter(p.get("location") for p in blog_data if p.get("location"))

        publish_times = []
        for p in blog_data:
            pt = p.get("publish_time")
            if pt:
                try:
                    publish_times.append(datetime.strptime(pt, "%Y-%m-%d %H:%M:%S"))
                except Exception:
                    pass

        time_range = None
        if publish_times:
            time_range = {
                "start": min(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "end": max(publish_times).strftime("%Y-%m-%d %H:%M:%S"),
                "span_hours": round((max(publish_times) - min(publish_times)).total_seconds() / 3600, 1),
            }

        total_reposts = sum(p.get("repost_count", 0) for p in blog_data)
        total_comments = sum(p.get("comment_count", 0) for p in blog_data)
        total_likes = sum(p.get("like_count", 0) for p in blog_data)

        summary_text = f"""数据概况:
- 总博文数: {total}
- 时间范围: {time_range['start'] if time_range else '未知'} 至 {time_range['end'] if time_range else '未知'}
- 情感分布: {dict(sentiment_dist.most_common(5))}
- 热门主题Top3: {[t[0] for t in parent_topics.most_common(3)]}
- 主要地区Top3: {[l[0] for l in location_dist.most_common(3)]}
- 发布者类型: {list(publisher_dist.keys())}
- 总互动量: 转发{total_reposts}, 评论{total_comments}, 点赞{total_likes}"""

        return {
            "summary": summary_text,
            "statistics": {
                "total_posts": total,
                "time_range": time_range,
                "sentiment_distribution": dict(sentiment_dist),
                "publisher_distribution": dict(publisher_dist),
                "topic_distribution": dict(parent_topics.most_common(10)),
                "location_distribution": dict(location_dist.most_common(10)),
                "engagement": {
                    "total_reposts": total_reposts,
                    "total_comments": total_comments,
                    "total_likes": total_likes,
                },
            },
        }

    def post(self, shared, prep_res, exec_res):
        if "agent" not in shared:
            shared["agent"] = {}

        shared["agent"]["data_summary"] = exec_res["summary"]
        shared["agent"]["data_statistics"] = exec_res["statistics"]

        print(f"\n[DataSummary] 数据概况已生成")
        print(exec_res["summary"])

        return "default"
