"""
dispatcher.py - 中央调度节点与阶段完成节点

包含 DispatcherNode, TerminalNode, Stage{1,2,3}CompletionNode
"""

from nodes.base import MonitoredNode



class DispatcherNode(MonitoredNode):
    """
    综合调度节点 - 系统入口和中央控制器
    
    功能：
    1. 作为整个系统Flow的入口节点
    2. 根据shared["dispatcher"]配置决定执行哪个阶段
    3. 根据各阶段的config参数决定具体执行路径
    4. 每个阶段完成后返回此节点，决定下一步动作
    
    返回的Action类型：
    - stage1_async: 阶段1异步处理路径
    - stage2_agent: 阶段2 LLM自主分析
    - stage3_report: 阶段3统一报告流程
    - done: 所有阶段完成，跳转到TerminalNode
    """
    
    def prep(self, shared):
        """读取调度配置和当前状态"""
        # 初始化dispatcher配置（如果不存在）
        if "dispatcher" not in shared:
            shared["dispatcher"] = {
                "start_stage": 1,
                "run_stages": [1, 2, 3],
                "current_stage": 0,
                "completed_stages": [],
                "next_action": None
            }
        
        dispatcher = shared["dispatcher"]
        config = shared.get("config", {})
        
        return {
            "start_stage": dispatcher.get("start_stage", 1),
            "run_stages": dispatcher.get("run_stages", [1, 2, 3]),
            "current_stage": dispatcher.get("current_stage", 0),
            "completed_stages": dispatcher.get("completed_stages", []),
            "enhancement_mode": config.get("enhancement_mode", "async"),
        }
    
    def exec(self, prep_res):
        """计算下一步动作"""
        start_stage = prep_res["start_stage"]
        run_stages = prep_res["run_stages"]
        current_stage = prep_res["current_stage"]
        completed_stages = prep_res["completed_stages"]
        enhancement_mode = prep_res["enhancement_mode"]
        
        # 确定下一个需要执行的阶段
        if current_stage == 0:
            # 首次进入，从start_stage开始
            next_stage = start_stage
        else:
            # 找到下一个在run_stages中且未完成的阶段
            next_stage = None
            for stage in run_stages:
                if stage > current_stage and stage not in completed_stages:
                    next_stage = stage
                    break
        
        # 检查是否还有需要执行的阶段
        if next_stage is None or next_stage not in run_stages:
            return {"action": "done", "next_stage": None}
        
        # 根据阶段确定具体路径
        if next_stage == 1:
            action = f"stage1_{enhancement_mode}"
        elif next_stage == 2:
            action = "stage2_agent"
        elif next_stage == 3:
            action = "stage3_report"
        else:
            action = "done"
        
        return {"action": action, "next_stage": next_stage}
    
    def post(self, shared, prep_res, exec_res):
        """更新调度状态，返回Action"""
        action = exec_res["action"]
        next_stage = exec_res["next_stage"]
        
        # 更新当前阶段
        if next_stage is not None:
            shared["dispatcher"]["current_stage"] = next_stage
        
        shared["dispatcher"]["next_action"] = action
        
        print(f"[Dispatcher] 下一步动作: {action}")
        
        return action


class TerminalNode(MonitoredNode):
    """
    终止节点 - 宣布流程结束
    
    功能：
    1. 作为整个Flow的终点
    2. 输出执行摘要信息
    3. 清理临时状态（如需要）
    """
    
    def prep(self, shared):
        """读取执行结果摘要"""
        dispatcher = shared.get("dispatcher", {})
        stage1_results = shared.get("stage1_results", {})
        
        return {
            "completed_stages": dispatcher.get("completed_stages", []),
            "statistics": stage1_results.get("statistics", {}),
            "data_save": stage1_results.get("data_save", {})
        }
    
    def exec(self, prep_res):
        """生成执行摘要"""
        completed_stages = prep_res["completed_stages"]
        statistics = prep_res["statistics"]
        data_save = prep_res["data_save"]
        
        summary = {
            "status": "completed",
            "completed_stages": completed_stages,
            "total_blogs_processed": statistics.get("total_blogs", 0),
            "data_saved": data_save.get("saved", False),
            "output_path": data_save.get("output_path", "")
        }
        
        return summary
    
    def post(self, shared, prep_res, exec_res):
        """输出执行摘要，结束流程"""
        print("\n" + "=" * 60)
        print("舆情分析智能体 - 执行完成")
        print("=" * 60)
        print(f"状态: {exec_res['status']}")
        print(f"已完成阶段: {exec_res['completed_stages']}")
        print(f"处理博文数: {exec_res['total_blogs_processed']}")
        if exec_res['data_saved']:
            print(f"数据已保存至: {exec_res['output_path']}")
        print("=" * 60 + "\n")
        
        # 存储最终摘要
        shared["final_summary"] = exec_res
        
        return "default"


class Stage1CompletionNode(MonitoredNode):
    """
    阶段1完成节点
    
    功能：
    1. 标记阶段1完成
    2. 更新dispatcher状态
    3. 返回"dispatch" Action，跳转回DispatcherNode
    """
    
    def prep(self, shared):
        """读取当前状态"""
        return {
            "current_stage": shared.get("dispatcher", {}).get("current_stage", 1),
            "completed_stages": shared.get("dispatcher", {}).get("completed_stages", [])
        }
    
    def exec(self, prep_res):
        """确认阶段完成"""
        print(f"\n[Stage1] 阶段1处理完成")
        return {"stage": 1}
    
    def post(self, shared, prep_res, exec_res):
        """更新完成状态，返回dispatch"""
        stage = exec_res["stage"]

        # 确保dispatcher存在
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}

        # 更新已完成阶段列表
        completed_stages = shared["dispatcher"].get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        shared["dispatcher"]["completed_stages"] = completed_stages

        print(f"[Stage1] 已完成阶段: {completed_stages}")

        # 返回dispatch，跳转回调度器
        return "dispatch"


class Stage2CompletionNode(MonitoredNode):
    """
    阶段2完成节点
    
    功能：
    1. 标记阶段2完成
    2. 更新dispatcher状态
    3. 返回"dispatch" Action，跳转回DispatcherNode
    """
    
    def prep(self, shared):
        """读取当前状态"""
        return {
            "current_stage": shared.get("dispatcher", {}).get("current_stage", 2),
            "completed_stages": shared.get("dispatcher", {}).get("completed_stages", [])
        }
    
    def exec(self, prep_res):
        """确认阶段完成"""
        print(f"\n[Stage2] 阶段2分析执行完成")
        return {"stage": 2}
    
    def post(self, shared, prep_res, exec_res):
        """更新完成状态，返回dispatch"""
        stage = exec_res["stage"]
        
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}
        
        completed_stages = shared["dispatcher"].get("completed_stages", [])
        if stage not in completed_stages:
            completed_stages.append(stage)
        shared["dispatcher"]["completed_stages"] = completed_stages
        
        print(f"[Stage2] 已完成阶段: {completed_stages}")
        
        return "dispatch"



class Stage3CompletionNode(MonitoredNode):
    """
    阶段3完成节点

    功能：标记阶段3完成，返回调度器
    """

    def prep(self, shared):
        """读取当前阶段状态"""
        return {
            "report_file": shared.get("stage3_results", {}).get("report_file", ""),
            "generation_mode": shared.get("stage3_results", {}).get("generation_mode", ""),
            "iterations": shared.get("report", {}).get("iteration", 0),
            "final_score": shared.get("report", {}).get("last_review", {}).get("total_score", 0)
        }

    def exec(self, prep_res):
        """确认阶段完成"""
        return True

    def post(self, shared, prep_res, exec_res):
        """更新已完成阶段列表，返回调度器"""
        if "dispatcher" not in shared:
            shared["dispatcher"] = {}

        dispatcher = shared["dispatcher"]
        if "completed_stages" not in dispatcher:
            dispatcher["completed_stages"] = []

        if 3 not in dispatcher["completed_stages"]:
            dispatcher["completed_stages"].append(3)

        dispatcher["current_stage"] = 3

        print(f"\n[Stage3] 阶段3完成 - 报告生成模式: {prep_res['generation_mode']}")
        if prep_res["iterations"] > 0:
            print(f"[Stage3] 迭代次数: {prep_res['iterations']}")
        if prep_res["final_score"] > 0:
            print(f"[Stage3] 最终评分: {prep_res['final_score']}/100")

        # 返回dispatch让DispatcherNode决定下一步
        return "dispatch"

