"""
pipeline_state.py - 线性主链的阶段状态节点

包含 TerminalNode, Stage{1,2,3}CompletionNode
"""

from nodes.base import MonitoredNode


def _ensure_pipeline_state(shared):
    if "pipeline_state" not in shared:
        shared["pipeline_state"] = {
            "start_stage": 1,
            "current_stage": 0,
            "completed_stages": [],
        }
    return shared["pipeline_state"]


class TerminalNode(MonitoredNode):
    """终止节点 - 宣布流程结束并输出摘要。"""

    def prep(self, shared):
        pipeline_state = shared.get("pipeline_state", {})
        stage1_results = shared.get("stage1_results", {})
        return {
            "completed_stages": pipeline_state.get("completed_stages", []),
            "statistics": stage1_results.get("statistics", {}),
            "data_save": stage1_results.get("data_save", {}),
        }

    def exec(self, prep_res):
        completed_stages = prep_res["completed_stages"]
        statistics = prep_res["statistics"]
        data_save = prep_res["data_save"]
        return {
            "status": "completed",
            "completed_stages": completed_stages,
            "total_blogs_processed": statistics.get("total_blogs", 0),
            "data_saved": data_save.get("saved", False),
            "output_path": data_save.get("output_path", ""),
        }

    def post(self, shared, prep_res, exec_res):
        print("\n" + "=" * 60)
        print("舆情分析智能体 - 执行完成")
        print("=" * 60)
        print(f"状态: {exec_res['status']}")
        print(f"已完成阶段: {exec_res['completed_stages']}")
        print(f"处理博文数: {exec_res['total_blogs_processed']}")
        if exec_res["data_saved"]:
            print(f"数据已保存至: {exec_res['output_path']}")
        print("=" * 60 + "\n")
        shared["final_summary"] = exec_res
        return "default"


class Stage1CompletionNode(MonitoredNode):
    """阶段1完成节点。"""

    def prep(self, shared):
        pipeline_state = _ensure_pipeline_state(shared)
        return {
            "current_stage": pipeline_state.get("current_stage", 1),
            "completed_stages": pipeline_state.get("completed_stages", []),
        }

    def exec(self, prep_res):
        print("\n[Stage1] 阶段1处理完成")
        return {"stage": 1}

    def post(self, shared, prep_res, exec_res):
        stage = exec_res["stage"]
        pipeline_state = _ensure_pipeline_state(shared)
        completed_stages = pipeline_state.get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        pipeline_state["completed_stages"] = completed_stages
        pipeline_state["current_stage"] = stage
        print(f"[Stage1] 已完成阶段: {completed_stages}")
        return "default"


class Stage2CompletionNode(MonitoredNode):
    """阶段2完成节点。"""

    def prep(self, shared):
        pipeline_state = _ensure_pipeline_state(shared)
        return {
            "current_stage": pipeline_state.get("current_stage", 2),
            "completed_stages": pipeline_state.get("completed_stages", []),
        }

    def exec(self, prep_res):
        print("\n[Stage2] 阶段2分析执行完成")
        return {"stage": 2}

    def post(self, shared, prep_res, exec_res):
        stage = exec_res["stage"]
        pipeline_state = _ensure_pipeline_state(shared)
        completed_stages = pipeline_state.get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        pipeline_state["completed_stages"] = completed_stages
        pipeline_state["current_stage"] = stage
        print(f"[Stage2] 已完成阶段: {completed_stages}")
        return "default"


class Stage3CompletionNode(MonitoredNode):
    """阶段3完成节点。"""

    def prep(self, shared):
        return {
            "report_file": shared.get("stage3_results", {}).get("report_file", ""),
            "generation_mode": shared.get("stage3_results", {}).get("generation_mode", ""),
            "iterations": shared.get("report", {}).get("iteration", 0),
            "final_score": shared.get("report", {}).get("last_review", {}).get("total_score", 0),
        }

    def exec(self, prep_res):
        return True

    def post(self, shared, prep_res, exec_res):
        pipeline_state = _ensure_pipeline_state(shared)
        completed_stages = pipeline_state.get("completed_stages", [])
        if 3 not in completed_stages:
            completed_stages.append(3)
        pipeline_state["completed_stages"] = completed_stages
        pipeline_state["current_stage"] = 3

        print(f"\n[Stage3] 阶段3完成 - 报告生成模式: {prep_res['generation_mode']}")
        if prep_res["iterations"] > 0:
            print(f"[Stage3] 迭代次数: {prep_res['iterations']}")
        if prep_res["final_score"] > 0:
            print(f"[Stage3] 最终评分: {prep_res['final_score']}/100")

        return "default"
