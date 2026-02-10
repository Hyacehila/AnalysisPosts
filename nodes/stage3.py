"""
stage3.py - 阶段3节点：报告生成

包含分析结果加载/格式化/保存节点 + Template路径节点 + Iterative路径节点。
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from pocketflow import Node, AsyncNode

from nodes._utils import (
    normalize_path, _remap_report_images, _load_analysis_charts,
    _build_chart_path_index
)
from utils.call_llm import call_glm_45_air, call_glm46
from utils.data_loader import load_enhanced_blog_data


class LoadAnalysisResultsNode(Node):
    """
    加载分析结果节点

    功能：加载阶段2产生的分析结果，包括图表数据、洞察分析等
    前置检查：验证阶段2输出文件是否存在

    输入：shared["config"]["data_source"]["enhanced_data_path"] (用于读取少量博文样本)
    输出：将分析结果存储到shared["stage3_data"]
    """

    def prep(self, shared):
        """检查前置条件，准备文件路径"""
        # 优先检查 shared 中是否有 stage2_results（同一流程中 stage2 刚完成的情况）
        stage2_results = shared.get("stage2_results", {})
        completed_stages = shared.get("dispatcher", {}).get("completed_stages", [])
        insights = stage2_results.get("insights", {})
        if isinstance(insights, dict):
            has_insights = any(bool(value) for value in insights.values())
        else:
            has_insights = bool(insights)
        has_memory_data = (
            2 in completed_stages and
            bool(
                stage2_results.get("charts") or
                stage2_results.get("chart_analyses") or
                stage2_results.get("tables") or
                has_insights
            )
        )
        
        # 检查阶段2输出文件是否存在
        analysis_data_path = "report/analysis_data.json"
        chart_analyses_path = "report/chart_analyses.json"
        insights_path = "report/insights.json"
        images_dir = "report/images/"

        # 如果内存中有数据，文件检查可以放宽（images_dir 仍然需要）
        if not has_memory_data:
            missing_files = []
            for file_path in [analysis_data_path, chart_analyses_path, insights_path]:
                if not os.path.exists(file_path):
                    missing_files.append(file_path)

            if missing_files:
                raise FileNotFoundError(f"阶段2输出文件不存在: {missing_files}")

        if not os.path.exists(images_dir):
            raise FileNotFoundError(f"图表目录不存在: {images_dir}")

        return {
            "analysis_data_path": analysis_data_path,
            "chart_analyses_path": chart_analyses_path,
            "insights_path": insights_path,
            "images_dir": images_dir,
            "enhanced_data_path": shared.get("config", {}).get("data_source", {}).get("enhanced_data_path", ""),
            "has_memory_data": has_memory_data,
            "stage2_results": stage2_results
        }

    def exec(self, prep_res):
        """加载分析结果，优先从内存读取，否则从文件读取"""
        # 优先从 shared["stage2_results"] 读取（如果存在）
        if prep_res.get("has_memory_data"):
            stage2_results = prep_res["stage2_results"]
            print("[LoadAnalysisResults] 从内存中加载 stage2 结果")
            
            # 从内存数据构建 analysis_data
            analysis_data = {
                "charts": stage2_results.get("charts", []),
                "tables": stage2_results.get("tables", []),
                "execution_log": stage2_results.get("execution_log", {})
            }
            
            # 从内存数据获取 chart_analyses（可能是字典或列表）
            chart_analyses = stage2_results.get("chart_analyses", {})
            if isinstance(chart_analyses, list):
                # 如果是列表，转换为字典格式
                chart_analyses = {f"chart_{i}": item for i, item in enumerate(chart_analyses)}
            
            # 从内存数据获取 insights
            insights = stage2_results.get("insights", {})
        else:
            # 从文件读取
            print("[LoadAnalysisResults] 从文件加载 stage2 结果")
            with open(prep_res["analysis_data_path"], "r", encoding="utf-8") as f:
                analysis_data = json.load(f)

            with open(prep_res["chart_analyses_path"], "r", encoding="utf-8") as f:
                chart_analyses = json.load(f)

            with open(prep_res["insights_path"], "r", encoding="utf-8") as f:
                insights = json.load(f)

        # 读取少量博文样本用于典型案例引用
        sample_blogs = []
        if prep_res["enhanced_data_path"] and os.path.exists(prep_res["enhanced_data_path"]):
            with open(prep_res["enhanced_data_path"], "r", encoding="utf-8") as f:
                enhanced_data = json.load(f)
                # 随机选取10条博文作为样本
                import random
                if len(enhanced_data) > 0:
                    sample_blogs = random.sample(
                        enhanced_data,
                        min(10, len(enhanced_data))
                    )

        return {
            "analysis_data": analysis_data,
            "chart_analyses": chart_analyses,
            "insights": insights,
            "sample_blogs": sample_blogs,
            "images_dir": prep_res["images_dir"]
        }

    def post(self, shared, prep_res, exec_res):
        """存储分析结果到shared字典"""
        shared["stage3_data"] = exec_res
        return "default"


class FormatReportNode(Node):
    """
    报告格式化节点

    功能：格式化最终Markdown报告，处理图片路径、修复格式问题、添加目录
    """

    def prep(self, shared):
        """读取报告内容"""
        # 优先读取一次性生成的完整报告，否则读取当前草稿
        full_content = shared.get("report", {}).get("full_content", "")
        if full_content:
            return full_content
        return shared.get("stage3_results", {}).get("current_draft", "")

    def exec(self, prep_res):
        """格式化报告内容"""
        if not prep_res:
            return ""

        formatted_content = prep_res

        # 修复图片路径，确保跨平台兼容性
        # 处理各种可能的路径格式
        path_replacements = [
            ("report\\images\\", "./images/"),    # Windows格式
            ("report/images/", "./images/"),     # Unix格式
            ("./report/images/", "./images/"),   # 相对路径
            ("../report/images/", "./images/"),  # 上级目录相对路径
        ]

        for old_path, new_path in path_replacements:
            formatted_content = formatted_content.replace(old_path, new_path)

        analysis_charts = _load_analysis_charts()
        if analysis_charts:
            formatted_content = _remap_report_images(formatted_content, analysis_charts)

        # 添加目录（如果还没有）
        if "# 目录" not in formatted_content and "## 目录" not in formatted_content:
            # 提取所有标题
            import re
            headers = re.findall(r'^(#{1,6})\s+(.+)$', formatted_content, re.MULTILINE)
            if headers:
                toc_lines = ["## 目录\n"]
                for level, title in headers:
                    indent = "  " * (len(level) - 1)
                    toc_lines.append(f"{indent}- [{title}](#{title.replace(' ', '-').lower()})")
                toc = "\n".join(toc_lines) + "\n\n"
                formatted_content = toc + formatted_content

        # 确保结尾有换行
        if not formatted_content.endswith('\n'):
            formatted_content += '\n'

        return formatted_content

    def post(self, shared, prep_res, exec_res):
        """存储格式化后的报告"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["final_report_text"] = exec_res
        return "default"


class SaveReportNode(Node):
    """
    保存报告节点

    功能：将最终报告保存到文件
    """

    def prep(self, shared):
        """读取格式化后的报告"""
        return shared.get("stage3_results", {}).get("final_report_text", "")

    def exec(self, prep_res):
        """保存报告到文件"""
        report_path = "report/report.md"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(prep_res)

        return report_path

    def post(self, shared, prep_res, exec_res):
        """记录保存路径"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["report_file"] = exec_res
        print(f"\n[Stage3] 报告已保存到: {exec_res}")
        return "default"


class LoadTemplateNode(Node):
    """
    加载模板节点

    功能：加载预定义的报告模板
    """

    def prep(self, shared):
        """读取模板文件路径"""
        return "report/template.md"

    def exec(self, prep_res):
        """加载模板内容"""
        if not os.path.exists(prep_res):
            raise FileNotFoundError(f"模板文件不存在: {prep_res}")

        with open(prep_res, "r", encoding="utf-8") as f:
            template_content = f.read()

        return template_content

    def post(self, shared, prep_res, exec_res):
        """存储模板内容"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["template"] = exec_res
        return "default"


class FillSectionNode(Node):
    """
    章节填充节点

    功能：使用LLM填充单个章节内容，确保数据引用和减少幻觉
    """

    def __init__(self, section_name: str, section_title: str):
        super().__init__()
        self.section_name = section_name
        self.section_title = section_title

    def prep(self, shared):
        """准备章节填充所需数据"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "section_name": self.section_name,
            "section_title": self.section_title,
            "existing_sections": shared.get("report", {}).get("sections", {})
        }

    def exec(self, prep_res):
        """调用LLM填充章节内容，生成图文并茂的报告"""
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]
        section_name = prep_res["section_name"]
        section_title = prep_res["section_title"]

        # 提取图表和表格信息，构建可直接引用的内容
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])
        images_dir = stage3_data.get('images_dir', '')

        # 构建可用的图片和表格引用字典
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            # 标准化图片路径，确保相对路径可用
            file_path = normalize_path(chart.get('file_path', ''))
            if not file_path.startswith('./images/') and 'images' in file_path:
                filename = Path(file_path).name
                file_path = f'./images/{filename}'
            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        # 根据不同章节定制化生成内容
        section_prompts = self._get_section_specific_prompt(
            section_name, section_title, available_charts, available_tables,
            chart_analyses, insights, sample_blogs, images_dir
        )

        prompt = f"""
你是一位专业的舆情分析师，需要生成一个图文并茂的报告章节。

## 章节信息
- 章节名称: {section_name}
- 章节标题: {section_title}

## 可用资源
### 图表资源（可直接使用markdown图片引用）
{json.dumps(available_charts, ensure_ascii=False, indent=2)}

### 表格资源（可直接使用markdown表格）
{json.dumps(list(available_tables.keys()), ensure_ascii=False, indent=2)}

## 具体要求
1. **必须使用真实的图片引用**：使用格式 `![图表标题](图片路径)` 插入图表
2. **必须使用真实的表格数据**：将相关表格数据转换为markdown表格格式
3. **图文并茂**：每个分析点都要有对应的图表或表格支撑
4. **数据驱动**：所有结论必须基于可视化的数据
5. **markdown格式**：使用标准的markdown语法

## 输出格式示例
```markdown
## {section_title}

### 关键发现
![情感分布饼图](report/images/sentiment_pie_20251130_214725.png)

如图表所示，情感分布呈现明显的正面主导特征...

### 数据分析
| 情感类型 | 数量 | 占比 |
|---------|------|------|
| 乐观 | 13 | 76.47% |
| 悲观 | 3 | 17.65% |

表1：情感分布统计
```

{section_prompts}

现在请生成该章节的完整内容：
"""

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 统计图片和表格引用数量
        image_refs = response.count("![") + response.count(".png")
        table_refs = response.count("|") + response.count("表")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_refs,
            "total_visual_refs": image_refs + table_refs
        }

    def _get_section_specific_prompt(self, section_name, section_title, charts, tables, chart_analyses, insights, sample_blogs, images_dir):
        """根据不同章节生成特定的提示词"""

        if section_name == "summary":
            return f"""
### 报告摘要要点
- 使用情感分布饼图展示总体情感态势
- 使用主题热度排行图展示热点话题
- 使用地区分布图展示地理特征
- 关键数据要用表格形式呈现
"""

        elif section_name == "trend":
            return f"""
### 趋势分析要点
- 必须包含情感趋势/情绪桶/情绪属性趋势图，点出拐点与爆点
- 必须包含主题演化时序与焦点关键词趋势图
- 使用表格展示趋势统计数据（峰值、拐点、爆点）
- 分析转折点和异常值，注明发生时间
"""

        elif section_name == "spread":
            return f"""
### 传播分析要点
- 使用发布者类型/情绪桶/话题偏好图
- 使用地区分布/地区正负面对比/地区×时间热力图
- 使用交叉分析热力图或参与人数趋势
- 用表格展示传播数据统计与高峰时段
"""

        elif section_name == "focus":
            return f"""
### 焦点窗口分析要点
- 明确焦点窗口时间范围与选择依据（用表格或文字注明）
- 必须引用焦点窗口情感趋势、三分类趋势、拐点/异常说明
- 必须引用焦点窗口发布者类型趋势、主题/话题占比趋势图
- 结合表格概述窗口内的关键数据（峰值、主要发布者、核心话题）
- 给出窗口内的综合结论与预警
"""

        elif section_name == "content":
            return f"""
### 内容分析要点
- 使用主题关联网络图
- 使用主题排行榜
- 表格展示主题统计信息
- 分析话题关联性
"""
        elif section_name == "belief":
            return f"""
### 信念系统分析要点
- 使用信念系统网络图，展示子类共现关系
- 结合节点/边数据表说明核心信念与关联强度
- 指出主要信念类型之间的结构特征
"""

        else:
            return f"""
### 分析要点
- 根据章节主题选择最相关的图表
- 确保每个观点都有数据支撑
- 使用表格提供详细数据
- 保持分析的客观性和专业性
"""

    def post(self, shared, prep_res, exec_res):
        """存储章节内容"""
        if "report" not in shared:
            shared["report"] = {}
        if "sections" not in shared["report"]:
            shared["report"]["sections"] = {}

        shared["report"]["sections"][self.section_name] = exec_res["content"]

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}
        if "stage3_section_planning" not in shared["thinking"]:
            shared["thinking"]["stage3_section_planning"] = {}

        shared["thinking"]["stage3_section_planning"][self.section_name] = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "content_planning": f"基于分析数据生成{self.section_title}章节内容，使用图文并茂格式",
            "data_selection": f"引用了{exec_res['total_visual_refs']}个视觉元素（图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个）",
            "terminal_prompt": f"章节完成：{self.section_title} - 图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个"
        }

        print(f"[Stage3] 章节完成：{self.section_title}")
        print(f"  - 图片引用：{exec_res['image_refs']}个")
        print(f"  - 表格引用：{exec_res['table_refs']}个")
        print(f"  - 总视觉元素：{exec_res['total_visual_refs']}个")

        return "default"


class AssembleReportNode(Node):
    """
    报告组装节点

    功能：将各章节组装成完整报告
    """

    def prep(self, shared):
        """读取所有章节内容"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "sections": shared.get("report", {}).get("sections", {}),
            "stage3_data": shared.get("stage3_data", {})
        }

    def exec(self, prep_res):
        """组装完整报告"""
        template = prep_res["template"]
        sections = prep_res["sections"]
        stage3_data = prep_res["stage3_data"]

        # 生成报告头部
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        header = f"""# 舆情分析智能报告

**生成时间**: {current_time}
**分析工具**: 舆情分析智能体系统
**数据来源**: 社交媒体博文数据

---

## 报告摘要

本报告基于舆情分析智能体系统的自动分析结果，通过多维度数据分析和可视化图表，全面展现当前舆情态势。

"""

        # 组装各章节
        content_parts = [header]

        # 按模板顺序添加章节，如果没有对应的生成内容则跳过
        section_order = [
            ("summary", "报告摘要"),
            ("development", "舆情事件发展脉络"),
            ("trend", "舆情总体趋势分析"),
            ("focus", "焦点窗口专项分析"),
            ("spread", "传播场景分析"),
            ("content", "舆论内容结构分析"),
            ("belief", "信念系统分析"),
            ("region", "区域与空间认知差异"),
            ("risk", "舆情风险研判"),
            ("suggestion", "应对建议"),
            ("appendix", "附录")
        ]

        for section_key, section_title in section_order:
            if section_key in sections and sections[section_key]:
                content_parts.append(sections[section_key])
                content_parts.append("\n---\n")

        # 添加数据说明
        data_summary = f"""
## 数据说明

### 分析范围
- 总博文数: {stage3_data.get('analysis_data', {}).get('total_blogs', 'N/A')}
- 分析时段: {stage3_data.get('analysis_data', {}).get('time_range', 'N/A')}
- 图表数量: {len(stage3_data.get('analysis_data', {}).get('charts', []))}

### 智能体生成说明
本报告由舆情分析智能体系统自动生成，包括：
- 数据增强处理
- 多维度分析（情感趋势、主题演化、焦点窗口、地理分布、多维交互、信念体系）
- 可视化图表生成与深度分析
- 智能报告编排

---
*报告生成完成时间: {current_time}*
"""

        content_parts.append(data_summary)

        return "\n".join(content_parts)

    def post(self, shared, prep_res, exec_res):
        """存储组装后的报告"""
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["current_draft"] = exec_res
        shared["stage3_results"]["generation_mode"] = "template"

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}

        shared["thinking"]["stage3_report_planning"] = [{
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "planning_process": "基于模板结构组织报告章节，确保数据引用完整性",
            "organization_logic": "按舆情分析标准结构组织章节，包含摘要、发展脉络、趋势分析、传播分析、风险研判和建议等核心内容",
            "terminal_prompt": f"报告组装完成：模板模式 - 章节数：{len(shared.get('report', {}).get('sections', {}))}"
        }]

        print(f"[Stage3] 报告组装完成：模板模式 - 章节数：{len(shared.get('report', {}).get('sections', {}))}")

        return "default"


class InitReportStateNode(Node):
    """
    初始化报告状态节点

    功能：初始化迭代报告生成的状态
    """

    def prep(self, shared):
        """读取分析结果和配置"""
        return {
            "max_iterations": shared.get("config", {}).get("iterative_report_config", {}).get("max_iterations", 5),
            "stage3_data": shared.get("stage3_data", {})
        }

    def exec(self, prep_res):
        """初始化迭代状态"""
        return {
            "max_iterations": prep_res["max_iterations"],
            "current_iteration": 0,
            "review_history": [],
            "revision_feedback": "",
            "current_draft": ""
        }

    def post(self, shared, prep_res, exec_res):
        """设置报告状态"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"].update(exec_res)
        return "default"


class GenerateReportNode(Node):
    """
    报告生成节点

    功能：LLM生成或修改报告，确保数据引用和减少幻觉
    """

    def prep(self, shared):
        """准备报告生成所需数据"""
        return {
            "stage3_data": shared.get("stage3_data", {}),
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "revision_feedback": shared.get("report", {}).get("revision_feedback", ""),
            "iteration": shared.get("report", {}).get("iteration", 0)
        }

    def exec(self, prep_res):
        """调用LLM生成或修改报告，生成图文并茂的完整报告"""
        stage3_data = prep_res["stage3_data"]
        current_draft = prep_res["current_draft"]
        revision_feedback = prep_res["revision_feedback"]
        iteration = prep_res["iteration"]

        # 提取图表和表格信息
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])

        # 构建可用的图片和表格引用
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            file_path = normalize_path(chart.get('file_path', ''))
            if not file_path.startswith('./images/') and 'images' in file_path:
                filename = Path(file_path).name
                file_path = f'./images/{filename}'
            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        if iteration == 0:
            # 首次生成图文并茂报告
            prompt = f"""
你是一位专业的舆情分析师，需要基于完整的分析数据生成一份图文并茂的高质量舆情分析报告。

## 可用资源
### 图表资源（必须使用真实图片引用）
{json.dumps(available_charts, ensure_ascii=False, indent=2)}

### 表格资源（必须转换为markdown表格）
{json.dumps(available_tables, ensure_ascii=False, indent=2)}

### 洞察分析
{json.dumps(insights, ensure_ascii=False, indent=2)}

### 博文样本（用于典型案例）
{json.dumps(sample_blogs[:3], ensure_ascii=False, indent=2)}

## 核心要求
1. **图文并茂**：必须直接嵌入真实的图片和表格，使用markdown语法
2. **图片引用格式**：`![图表标题](图片路径)`
3. **表格转换**：将JSON表格数据转换为标准markdown表格格式
4. **数据驱动**：每个分析点都必须有对应的图表或表格支撑
5. **真实数据**：严禁使用虚构数据，所有图表和表格都必须来自提供的资源

## 报告结构（必须包含）
```markdown
# 舆情分析智能报告

## 报告摘要
![情感分布饼图](report/images/sentiment_pie_xxx.png)
![主题热度排行](report/images/topic_ranking_xxx.png)
简要分析数据...

## 舆情总体趋势分析
![情感趋势变化图](report/images/sentiment_trend_xxx.png)
![主题演化时序图](report/images/topic_evolution_xxx.png)
详细趋势分析...

## 焦点窗口专项分析
![焦点窗口情感趋势](report/images/sentiment_focus_window_xxx.png)
![焦点窗口发布者趋势](report/images/publisher_focus_distribution_xxx.png)
焦点窗口关键发现与预警...

## 传播场景分析
![发布者类型分布图](report/images/publisher_bar_xxx.png)
![地区分布图](report/images/geographic_bar_xxx.png)
传播特征分析...

## 舆论内容结构分析
![主题关联网络图](report/images/topic_network_xxx.png)
![交叉分析热力图](report/images/interaction_heatmap_xxx.png)
内容结构分析...

## 信念系统分析
信念节点激活与共现...

## 区域与空间认知差异
区域情感与归因差异...

## 舆情风险研判
基于数据的综合风险评估...

## 应对建议
基于分析结果的具体建议...

## 附录
数据范围与指标说明...
```

现在请生成完整的图文并茂报告：
"""
        else:
            # 基于反馈修改图文并茂报告
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

        # 统计图文并茂元素
        image_refs = response.count("![") + response.count(".png")
        table_rows = response.count("|---")
        data_citations = response.count("图表") + response.count("数据") + response.count("如图")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_rows,
            "data_citations": data_citations,
            "visual_completeness": "high" if image_refs >= 3 and table_rows >= 2 else "medium" if image_refs >= 1 else "low"
        }

    def post(self, shared, prep_res, exec_res):
        """存储生成的报告草稿"""
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["current_draft"] = exec_res["content"]

        # 记录编排思考
        if "thinking" not in shared:
            shared["thinking"] = {}

        thinking_record = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "planning_process": f"基于分析数据生成{'修改' if prep_res['iteration'] > 0 else '初始'}图文并茂报告",
            "organization_logic": "确保每个结论都有对应的图表或表格支撑，生成真正的图文并茂报告",
            "terminal_prompt": f"图文并茂报告{'修改' if prep_res['iteration'] > 0 else '生成'}完成：图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个，数据引用{exec_res['data_citations']}个，视觉完整度{exec_res['visual_completeness']}"
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


class ReviewReportNode(Node):
    """
    报告评审节点

    功能：LLM评审报告质量，重点核查数据支撑和减少幻觉
    """

    def prep(self, shared):
        """准备评审所需数据"""
        config = shared.get("config", {}).get("iterative_report_config", {})
        return {
            "current_draft": shared.get("report", {}).get("current_draft", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "iteration": shared.get("report", {}).get("iteration", 0),
            "satisfaction_threshold": config.get("satisfaction_threshold", 80),
            "max_iterations": shared.get("report", {}).get("max_iterations", 5)
        }

    def exec(self, prep_res):
        """调用LLM评审报告质量"""
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

        # 提取JSON结果
        try:
            json_start = response.find("{")
            json_end = response.rfind("}") + 1
            if json_start != -1 and json_end != -1:
                json_str = response[json_start:json_end]
                review_result = json.loads(json_str)
            else:
                raise ValueError("无法提取JSON评审结果")
        except Exception as e:
            # 如果JSON解析失败，使用默认评分
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
                "overall_assessment": "评审过程出现技术问题，建议人工检查"
            }

        # 计算数据支撑率
        data_support_rate = (review_result["data_support_score"] / 20) * 100

        return review_result

    def post(self, shared, prep_res, exec_res):
        """存储评审结果并决定下一步"""
        if "report" not in shared:
            shared["report"] = {}

        # 添加阈值和迭代次数到exec_res中
        exec_res["satisfaction_threshold"] = prep_res["satisfaction_threshold"]
        exec_res["current_iteration"] = prep_res["iteration"]
        exec_res["max_iterations"] = prep_res["max_iterations"]

        shared["report"]["last_review"] = exec_res
        shared["report"]["review_history"].append(exec_res)

        print(f"[Stage3] 评审完成 - 数据支撑率：{exec_res['total_score']/100*100:.0f}%，发现问题：{len(exec_res.get('unsupported_conclusions', []))}个")

        # 根据满意度阈值和最大迭代次数决定下一步
        total_score = exec_res.get("total_score", 0)
        current_iteration = prep_res["iteration"]
        max_iterations = prep_res["max_iterations"]
        satisfaction_threshold = prep_res["satisfaction_threshold"]

        # 如果达到满意度阈值或达到最大迭代次数，则结束迭代
        if total_score >= satisfaction_threshold:
            print(f"[Stage3] 报告质量达标（{total_score} >= {satisfaction_threshold}），结束迭代")
            return "satisfied"
        elif current_iteration >= max_iterations - 1:  # current_iteration从0开始
            print(f"[Stage3] 达到最大迭代次数（{max_iterations}），强制结束迭代")
            return "satisfied"
        else:
            print(f"[Stage3] 报告需要继续改进（{total_score} < {satisfaction_threshold}），进入下一轮迭代")
            return "needs_revision"


class ApplyFeedbackNode(Node):
    """
    应用修改意见节点

    功能：处理评审意见，准备下一轮迭代
    """

    def prep(self, shared):
        """读取评审意见"""
        return shared.get("report", {}).get("last_review", {})

    def exec(self, prep_res):
        """格式化修改意见"""
        revision_feedback = prep_res.get("revision_feedback", "")

        if not revision_feedback:
            revision_feedback = "无明显问题，报告质量良好。"

        # 添加具体的修改建议
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
        """存储修改意见并更新迭代计数"""
        if "report" not in shared:
            shared["report"] = {}

        # 增加迭代计数
        shared["report"]["iteration"] = shared["report"].get("iteration", 0) + 1
        shared["report"]["revision_feedback"] = exec_res

        # 检查是否达到最大迭代次数
        current_iteration = shared["report"]["iteration"]
        max_iterations = shared["report"].get("max_iterations", 5)

        if current_iteration >= max_iterations:
            print(f"[Stage3] 达到最大迭代次数 ({max_iterations})，结束迭代")
            return "max_iterations_reached"
        else:
            print(f"[Stage3] 开始第 {current_iteration + 1} 轮迭代")
            return "continue_iteration"




class GenerateFullReportNode(Node):
    """
    一次性完整报告生成节点

    功能：基于模板和分析结果，一次性生成完整的舆情分析报告
    替代原有的分章节生成模式，确保报告的一致性和数据引用的准确性
    """

    def prep(self, shared):
        """准备报告生成所需的所有数据"""
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "config": shared.get("config", {})
        }

    def exec(self, prep_res):
        """一次性生成完整的舆情分析报告"""
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]

        # 提取所有分析数据
        charts = stage3_data.get('analysis_data', {}).get('charts', [])
        tables = stage3_data.get('analysis_data', {}).get('tables', [])
        chart_analyses = stage3_data.get('chart_analyses', [])
        insights = stage3_data.get('insights', {})
        sample_blogs = stage3_data.get('sample_blogs', [])
        images_dir = stage3_data.get('images_dir', '')

        # 构建可用的图片和表格引用字典
        available_charts = {}
        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue
            # 标准化图片路径为跨平台兼容的相对路径
            file_path = normalize_path(chart.get('file_path', ''))
            # 确保图片路径使用统一的相对路径格式
            if not file_path.startswith('./images/') and 'images' in file_path:
                # 提取文件名并构造标准路径
                filename = Path(file_path).name
                file_path = f'./images/{filename}'

            available_charts[chart_id] = {
                'title': chart.get('title', ''),
                'file_path': file_path,
                'description': chart.get('description', ''),
                'type': chart.get('type', 'unknown')
            }

        available_tables = {}
        for table in tables:
            table_id = table['id']
            available_tables[table_id] = {
                'title': table['title'],
                'data': table['data']
            }

        # 构建详细的分析结果摘要
        detailed_analysis_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                chart_info = analysis.get("chart_info", {})
                micro_details = analysis.get("microscopic_details", {})
                macro_insights = analysis.get("macroscopic_insights", {})
                quality = analysis.get("quality_assessment", {})

                # 提取关键统计信息
                stats = micro_details.get("statistics", {})
                data_points_count = len(micro_details.get("data_points", []))

                # 提取关键发现
                key_findings = macro_insights.get("key_findings", [])
                trends = macro_insights.get("trend_analysis", {})

                analysis_summary = {
                    "chart_id": chart_id,
                    "title": title,
                    "chart_type": chart_info.get("chart_type", "unknown"),
                    "data_points_count": data_points_count,
                    "max_value": stats.get("max_value", {}),
                    "min_value": stats.get("min_value", {}),
                    "key_findings": [f.get("finding", "") for f in key_findings[:3]],
                    "primary_trend": trends.get("primary_trend", ""),
                    "quality_score": {
                        "density": quality.get("information_density", "unknown"),
                        "readability": quality.get("readability", "unknown")
                    }
                }
                detailed_analysis_summary.append(analysis_summary)

        # 构建增强的提示词
        prompt = f"""
你是一位资深的舆情分析专家，需要基于以下极其详细的数据分析结果，生成一份高质量的舆情分析报告。

## 核心要求
1. **充分利用详细分析**：充分利用GLM4.5V提供的微观细节和宏观洞察
2. **引用具体数据点**：在报告中引用具体的数据细节（如最高值、最低值、数据点数量）
3. **结合趋势分析**：利用图表分析中的趋势发现和转折点分析
4. **数据驱动结论**：每个结论都要有来自chart_analyses的具体支撑
5. **覆盖模板章节**：按模板包含摘要、发展脉络、总体趋势、焦点窗口、传播、内容结构、信念、区域、风险、建议、附录

## 详细分析数据摘要

### 增强版图表分析结果
{json.dumps(detailed_analysis_summary, ensure_ascii=False, indent=2)}

### 完整图表数据
{json.dumps(charts, ensure_ascii=False, indent=2)}

### 详细统计表格
{json.dumps(tables, ensure_ascii=False, indent=2)}

### 综合洞察描述
{json.dumps(insights, ensure_ascii=False, indent=2)}

## 报告生成指导

### 数据引用方式
- **微观细节引用**：根据chart_analyses中的统计分析，{detailed_analysis_summary[0]['title'] if detailed_analysis_summary else '某图表'}显示最高值为{detailed_analysis_summary[0]['max_value'].get('value', 'N/A') if detailed_analysis_summary else 'N/A'}
- **趋势分析引用**：基于GLM4.5V的趋势分析，主要趋势为{detailed_analysis_summary[0]['primary_trend'] if detailed_analysis_summary else 'N/A'}
- **关键发现引用**：图表分析发现：{'; '.join(detailed_analysis_summary[0]['key_findings'][:2]) if detailed_analysis_summary else 'N/A'}

### 报告章节和内容要求
1. **报告摘要** - 基于insights和关键图表，突出最重要的数据指标
2. **舆情事件发展脉络** - 利用时序趋势与转折点
3. **舆情总体趋势分析** - 情感/主题总体演化
4. **焦点窗口专项分析** - 独立窗口内的情感、发布者、主题对比与预警
5. **传播场景分析** - 地域与发布者分布、交互特征
6. **舆论内容结构分析** - 主题网络、共现、排行
7. **信念系统分析** - 信念节点激活与网络
8. **区域与空间认知差异** - 区域情感、归因差异
9. **舆情风险研判** - 结合情感/主题/传播指标
10. **应对建议** - 针对发现的问题提出措施
11. **附录** - 数据范围与指标说明

### 输出要求
- 使用标准Markdown格式
- 每个分析点都要引用具体的数据细节
- 图片引用使用相对路径：`![图表标题](./images/文件名.png)`
- 所有结论都要标注数据来源（如：根据chart_analyses中的统计显示...）
- 确保报告内容既有微观细节又有宏观洞察

现在请生成高质量的舆情分析报告：
"""

        # 调用LLM生成报告
        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        # 修正报告中的图片路径，确保跨平台兼容性
        path_replacements = [
            ("report\\images\\", "./images/"),    # Windows格式
            ("report/images/", "./images/"),     # Unix格式
            ("./report/images/", "./images/"),   # 相对路径
            ("../report/images/", "./images/"),  # 上级目录相对路径
        ]

        for old_path, new_path in path_replacements:
            response = response.replace(old_path, new_path)

        response = _remap_report_images(response, charts)

        return response

    def post(self, shared, prep_res, exec_res):
        """存储生成的完整报告"""
        if "report" not in shared:
            shared["report"] = {}

        # 存储完整报告内容
        shared["report"]["full_content"] = exec_res
        shared["report"]["generation_mode"] = "one_shot"

        # 记录生成信息
        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["generation_mode"] = "one_shot"
        shared["stage3_results"]["current_draft"] = exec_res

        print(f"[Stage3] 完整报告已生成（一次性模式）")
        print(f"[Stage3] 报告长度：{len(exec_res)} 字符")

        # 统计图片引用数量
        image_refs = exec_res.count('![')
        print(f"[Stage3] 图片引用：{image_refs} 个")

        return "default"

        return "dispatch"
