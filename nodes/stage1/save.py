"""
Stage 1 save node.
"""
from nodes.base import MonitoredNode

from utils.data_loader import save_enhanced_blog_data


class SaveEnhancedDataNode(MonitoredNode):
    """
    增强数据保存节点
    """

    def prep(self, shared):
        blog_data = shared.get("data", {}).get("blog_data", [])
        config = shared.get("config", {})
        output_path = config.get("data_source", {}).get(
            "enhanced_data_path", "data/enhanced_blogs.json"
        )
        return {
            "blog_data": blog_data,
            "output_path": output_path,
        }

    def exec(self, prep_res):
        blog_data = prep_res["blog_data"]
        output_path = prep_res["output_path"]
        success = save_enhanced_blog_data(blog_data, output_path)
        return {
            "success": success,
            "output_path": output_path,
            "data_count": len(blog_data),
        }

    def post(self, shared, prep_res, exec_res):
        if "stage1_results" not in shared:
            shared["stage1_results"] = {}

        if exec_res["success"]:
            print(f"[SaveData] [OK] 成功保存 {exec_res['data_count']} 条增强数据到: {exec_res['output_path']}")
            shared["stage1_results"]["data_save"] = {
                "saved": True,
                "output_path": exec_res["output_path"],
                "data_count": exec_res["data_count"],
            }
        else:
            print(f"[SaveData] [X] 保存增强数据失败: {exec_res['output_path']}")
            shared["stage1_results"]["data_save"] = {
                "saved": False,
                "output_path": exec_res["output_path"],
                "error": "保存失败",
            }
        return "default"
