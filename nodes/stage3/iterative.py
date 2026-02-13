"""
Stage 3 iterative report nodes.
"""
import json
import time

from nodes.base import MonitoredNode

from nodes._utils import normalize_path, _remap_report_images
from utils.call_llm import call_glm46


class InitReportStateNode(MonitoredNode):
    """
    初始化报告状态节点
    """

    def prep(self, shared):
        config = shared.get("config", {}).get("iterative_report_config", {})
        return {
            "max_iterations": config.get("max_iterations", 5),
            "satisfaction_threshold": config.get("satisfaction_threshold", 80),
            "enable_review": config.get("enable_review", True),
            "quality_check": config.get("quality_check", True),
        }

    def exec(self, prep_res):
        return {
            "iteration": 0,
            "current_iteration": 0,
            "current_draft": "",
            "revision_feedback": "",
            "review_history": [],
            "max_iterations": prep_res["max_iterations"],
            "satisfaction_threshold": prep_res.get("satisfaction_threshold", 80),
        }

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}
        shared["report"].update(exec_res)
        return "default"


class GenerateReportNode(MonitoredNode):
    """
    报告生成节点
    """

    def prep(self, shared):
        return {
            "stage3_data": shared.get("stage3_data", {}),
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "revision_feedback": shared.get("report", {}).get("revision_feedback", ""),
            "iteration": shared.get("report", {}).get("iteration", 0),
        }

    def exec(self, prep_res):
        stage3_data = prep_res["stage3_data"]
        current_draft = prep_res["current_draft"]
        revision_feedback = prep_res["revision_feedback"]
        iteration = prep_res["iteration"]

        charts = stage3_data.get("analysis_data", {}).get("charts", [])
        tables = stage3_data.get("analysis_data", {}).get("tables", [])
        chart_analyses = stage3_data.get("chart_analyses", [])
        insights = stage3_data.get("insights", {})
        sample_blogs = stage3_data.get("sample_blogs", [])
        images_dir = stage3_data.get("images_dir", "")

        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            file_path = normalize_path(chart.get("file_path", ""))
            if not file_path.startswith("./images/") and "images" in file_path:
                from pathlib import Path

                filename = Path(file_path).name
                file_path = f"./images/{filename}"
            available_charts[chart_id] = {
                "title": chart.get("title", ""),
                "file_path": file_path,
                "description": chart.get("description", ""),
                "type": chart.get("type", "unknown"),
            }

        available_tables = {}
        for table in tables:
            table_id = table["id"]
            available_tables[table_id] = {
                "title": table["title"],
                "data": table["data"],
            }

        if iteration == 0:
            prompt = f"""
你是一位专业的舆情分析师，需要基于以下数据生成一份图文并茂的舆情分析报告。

## 可用图表资源（必须使用）
{json.dumps(available_charts, ensure_ascii=False, indent=2)}

## 可用表格资源（必须使用）
{json.dumps(list(available_tables.keys()), ensure_ascii=False, indent=2)}

## 图表分析结果
{json.dumps(chart_analyses, ensure_ascii=False, indent=2)[:2000]}...

## 综合洞察
{json.dumps(insights, ensure_ascii=False, indent=2)}

## 报告要求
1. 必须包含图表引用（使用 markdown 图片语法）
2. 必须包含表格数据（使用 markdown 表格语法）
3. 每个章节都有数据支撑
4. 结构完整，逻辑清晰
5. 输出标准 Markdown

请生成完整报告：
"""
        else:
            prompt = f"""
你是一位专业的舆情分析师，需要根据评审意见修改图文并茂的舆情分析报告。

## 当前报告草稿
{current_draft}

## 评审修改意见
{revision_feedback}

## 可用资源（保持不变）
- 图表资源：{list(available_charts.keys())}
- 表格资源：{list(available_tables.keys())}

## 修改要求
1. **保持图文并茂**：确保修改后的报告仍包含所有必要的图片和表格
2. **数据准确性**：所有图表引用和表格数据必须准确无误
3. **改进数据支撑**：针对评审意见，增强薄弱环节的数据支撑
4. **视觉完整性**：确保每个分析章节都有对应的可视化元素
5. **解决指出问题**：明确说明如何解决了评审中提到的每个问题

请提供修改后的完整图文并茂报告：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        image_refs = response.count("![") + response.count(".png")
        table_rows = response.count("|---")
        data_citations = response.count("图表") + response.count("数据") + response.count("如图")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_rows,
            "data_citations": data_citations,
            "visual_completeness": "high" if image_refs >= 3 and table_rows >= 2 else "medium" if image_refs >= 1 else "low",
        }

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["current_draft"] = exec_res["content"]

        if "thinking" not in shared:
            shared["thinking"] = {}

        thinking_record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "planning_process": f"基于分析数据生成{'修改' if prep_res['iteration'] > 0 else '初始'}图文并茂报告",
            "organization_logic": "确保每个结论都有对应的图表或表格支撑，生成真正的图文并茂报告",
            "terminal_prompt": f"图文并茂报告{'修改' if prep_res['iteration'] > 0 else '生成'}完成：图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个，数据引用{exec_res['data_citations']}个，视觉完整度{exec_res['visual_completeness']}",
        }

        if "stage3_report_planning" not in shared["thinking"]:
            shared["thinking"]["stage3_report_planning"] = []

        shared["thinking"]["stage3_report_planning"].append(thinking_record)

        print(f"[Stage3] 图文并茂报告{'修改' if prep_res['iteration'] > 0 else '生成'}完成：")
        print(f"  - 图片引用：{exec_res['image_refs']}个")
        print(f"  - 表格引用：{exec_res['table_refs']}个")
        print(f"  - 数据引用：{exec_res['data_citations']}个")
        print(f"  - 视觉完整度：{exec_res['visual_completeness']}")

        return "default"


class ReviewReportNode(MonitoredNode):
    """
    报告评审节点
    """

    def prep(self, shared):
        config = shared.get("config", {}).get("iterative_report_config", {})
        return {
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "iteration": shared.get("report", {}).get("iteration", 0),
            "satisfaction_threshold": config.get("satisfaction_threshold", 80),
            "max_iterations": shared.get("report", {}).get("max_iterations", 5),
        }

    def exec(self, prep_res):
        current_draft = prep_res["current_draft"]
        stage3_data = prep_res["stage3_data"]
        iteration = prep_res["iteration"]
        satisfaction_threshold = prep_res["satisfaction_threshold"]
        max_iterations = prep_res["max_iterations"]

        prompt = f"""
你是一位资深舆情分析专家，需要对以下舆情分析报告进行质量评审。

## 报告内容
{current_draft}

## 评审标准（每项20分，总分100分）
1. **结构完整性** (20分): 报告结构是否完整，逻辑是否清晰
2. **数据支撑充分性** (20分): 每个结论是否有足够的数据支撑
3. **图表引用准确性** (20分): 图表引用是否准确，分析是否到位
4. **逻辑连贯性** (20分): 分析逻辑是否连贯，推理是否合理
5. **建议可行性** (20分): 提出的建议是否具有可行性

## 可用数据参考
{json.dumps(stage3_data.get('analysis_data', {}), ensure_ascii=False, indent=2)[:2000]}...

## 评审要求
1. 逐项评分，并给出具体理由
2. 识别所有缺乏数据支撑的结论
3. 指出图表引用中的问题
4. 提供具体的修改建议
5. 给出总体评分和是否需要修改的判断

## 输出格式
请按以下格式输出评审结果：

```json
{{
    "structure_score": 15,
    "structure_comment": "结构完整性的评价...",
    "data_support_score": 18,
    "data_support_comment": "数据支撑的评价...",
    "chart_reference_score": 16,
    "chart_reference_comment": "图表引用的评价...",
    "logic_score": 17,
    "logic_comment": "逻辑连贯性的评价...",
    "suggestion_score": 15,
    "suggestion_comment": "建议可行性的评价...",
    "total_score": 81,
    "unsupported_conclusions": [
        "缺乏数据支撑的结论1",
        "缺乏数据支撑的结论2"
    ],
    "chart_reference_issues": [
        "图表引用问题1",
        "图表引用问题2"
    ],
    "revision_feedback": "具体的修改建议...",
    "needs_revision": true,
    "overall_assessment": "总体评价..."
}}
```

请进行评审：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        try:
            json_start = response.find("{")
            json_end = response.rfind("}") + 1
            if json_start != -1 and json_end != -1:
                json_str = response[json_start:json_end]
                review_result = json.loads(json_str)
            else:
                raise ValueError("无法提取JSON评审结果")
        except Exception:
            review_result = {
                "structure_score": 15,
                "data_support_score": 15,
                "chart_reference_score": 15,
                "logic_score": 15,
                "suggestion_score": 15,
                "total_score": 75,
                "unsupported_conclusions": ["JSON解析失败，请人工检查"],
                "chart_reference_issues": ["JSON解析失败，请人工检查"],
                "revision_feedback": "JSON解析失败，建议人工检查报告内容",
                "needs_revision": True,
                "overall_assessment": "评审过程出现技术问题，建议人工检查",
            }

        return review_result

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}

        exec_res["satisfaction_threshold"] = prep_res["satisfaction_threshold"]
        exec_res["current_iteration"] = prep_res["iteration"]
        exec_res["max_iterations"] = prep_res["max_iterations"]

        shared["report"]["last_review"] = exec_res
        shared["report"]["review_history"].append(exec_res)

        print(f"[Stage3] 评审完成 - 数据支撑率：{exec_res['total_score']/100*100:.0f}%，发现问题：{len(exec_res.get('unsupported_conclusions', []))}个")

        total_score = exec_res.get("total_score", 0)
        current_iteration = prep_res["iteration"]
        max_iterations = prep_res["max_iterations"]
        satisfaction_threshold = prep_res["satisfaction_threshold"]

        if total_score >= satisfaction_threshold:
            print(f"[Stage3] 报告质量达标（{total_score} >= {satisfaction_threshold}），结束迭代")
            return "satisfied"
        if current_iteration >= max_iterations - 1:
            print(f"[Stage3] 达到最大迭代次数（{max_iterations}），强制结束迭代")
            return "satisfied"
        print(f"[Stage3] 报告需要继续改进（{total_score} < {satisfaction_threshold}），进入下一轮迭代")
        return "needs_revision"


class ApplyFeedbackNode(MonitoredNode):
    """
    应用修改意见节点
    """

    def prep(self, shared):
        return shared.get("report", {}).get("last_review", {})

    def exec(self, prep_res):
        revision_feedback = prep_res.get("revision_feedback", "")

        if not revision_feedback:
            revision_feedback = "无明显问题，报告质量良好。"

        feedback_details = []

        if prep_res.get("unsupported_conclusions"):
            feedback_details.append("需要补充数据支撑的结论：")
            for conclusion in prep_res["unsupported_conclusions"][:5]:
                feedback_details.append(f"- {conclusion}")

        if prep_res.get("chart_reference_issues"):
            feedback_details.append("图表引用问题：")
            for issue in prep_res["chart_reference_issues"][:5]:
                feedback_details.append(f"- {issue}")

        if feedback_details:
            revision_feedback += "\n\n具体问题：\n" + "\n".join(feedback_details)

        return revision_feedback

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["iteration"] = shared["report"].get("iteration", 0) + 1
        shared["report"]["revision_feedback"] = exec_res

        current_iteration = shared["report"]["iteration"]
        max_iterations = shared["report"].get("max_iterations", 5)

        if current_iteration >= max_iterations:
            print(f"[Stage3] 达到最大迭代次数 ({max_iterations})，结束迭代")
            return "max_iterations_reached"

        print(f"[Stage3] 开始第 {current_iteration + 1} 轮迭代")
        return "continue_iteration"
