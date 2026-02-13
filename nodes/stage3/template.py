"""
Stage 3 template-based report nodes.
"""
import json
import os
import time
from pathlib import Path

from nodes.base import MonitoredNode

from nodes._utils import normalize_path, _remap_report_images
from utils.call_llm import call_glm46


class LoadTemplateNode(MonitoredNode):
    """
    加载模板节点
    """

    def prep(self, shared):
        return "data/report_template.md"

    def exec(self, prep_res):
        if not os.path.exists(prep_res):
            raise FileNotFoundError(f"模板文件不存在: {prep_res}")
        with open(prep_res, "r", encoding="utf-8") as f:
            template_content = f.read()
        return template_content

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}
        shared["report"]["template"] = exec_res
        return "default"


class FillSectionNode(MonitoredNode):
    """
    章节填充节点
    """

    def __init__(self, section_name: str, section_title: str):
        super().__init__()
        self.section_name = section_name
        self.section_title = section_title

    def prep(self, shared):
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "section_name": self.section_name,
            "section_title": self.section_title,
            "existing_sections": shared.get("report", {}).get("sections", {}),
        }

    def exec(self, prep_res):
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]
        section_name = prep_res["section_name"]
        section_title = prep_res["section_title"]

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

        section_prompts = self._get_section_specific_prompt(
            section_name,
            section_title,
            available_charts,
            available_tables,
            chart_analyses,
            insights,
            sample_blogs,
            images_dir,
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

        image_refs = response.count("![") + response.count(".png")
        table_refs = response.count("|") + response.count("表")

        return {
            "content": response,
            "image_refs": image_refs,
            "table_refs": table_refs,
            "total_visual_refs": image_refs + table_refs,
        }

    def _get_section_specific_prompt(self, section_name, section_title, charts, tables, chart_analyses, insights, sample_blogs, images_dir):
        if section_name == "summary":
            return """
### 报告摘要要点
- 使用情感分布饼图展示总体情感态势
- 使用主题热度排行图展示热点话题
- 使用地区分布图展示地理特征
- 关键数据要用表格形式呈现
"""
        if section_name == "trend":
            return """
### 趋势分析要点
- 必须包含情感趋势/情绪桶/情绪属性趋势图，点出拐点与爆点
- 必须包含主题演化时序与焦点关键词趋势图
- 使用表格展示趋势统计数据（峰值、拐点、爆点）
- 分析转折点和异常值，注明发生时间
"""
        if section_name == "spread":
            return """
### 传播分析要点
- 使用发布者类型/情绪桶/话题偏好图
- 使用地区分布/地区正负面对比/地区×时间热力图
- 使用交叉分析热力图或参与人数趋势
- 用表格展示传播数据统计与高峰时段
"""
        if section_name == "focus":
            return """
### 焦点窗口分析要点
- 明确焦点窗口时间范围与选择依据（用表格或文字注明）
- 必须引用焦点窗口情感趋势、三分类趋势、拐点/异常说明
- 必须引用焦点窗口发布者类型趋势、主题/话题占比趋势图
- 结合表格概述窗口内的关键数据（峰值、主要发布者、核心话题）
- 给出窗口内的综合结论与预警
"""
        if section_name == "content":
            return """
### 内容分析要点
- 使用主题关联网络图
- 使用主题排行榜
- 表格展示主题统计信息
- 分析话题关联性
"""
        if section_name == "belief":
            return """
### 信念系统分析要点
- 使用信念系统网络图，展示子类共现关系
- 结合节点/边数据表说明核心信念与关联强度
- 指出主要信念类型之间的结构特征
"""
        return """
### 分析要点
- 根据章节主题选择最相关的图表
- 确保每个观点都有数据支撑
- 使用表格提供详细数据
- 保持分析的客观性和专业性
"""

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}
        if "sections" not in shared["report"]:
            shared["report"]["sections"] = {}

        shared["report"]["sections"][self.section_name] = exec_res["content"]

        if "thinking" not in shared:
            shared["thinking"] = {}
        if "stage3_section_planning" not in shared["thinking"]:
            shared["thinking"]["stage3_section_planning"] = {}

        shared["thinking"]["stage3_section_planning"][self.section_name] = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "content_planning": f"基于分析数据生成{self.section_title}章节内容，使用图文并茂格式",
            "data_selection": f"引用了{exec_res['total_visual_refs']}个视觉元素（图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个）",
            "terminal_prompt": f"章节完成：{self.section_title} - 图片{exec_res['image_refs']}个，表格{exec_res['table_refs']}个",
        }

        print(f"[Stage3] 章节完成：{self.section_title}")
        print(f"  - 图片引用：{exec_res['image_refs']}个")
        print(f"  - 表格引用：{exec_res['table_refs']}个")
        print(f"  - 总视觉元素：{exec_res['total_visual_refs']}个")

        return "default"


class AssembleReportNode(MonitoredNode):
    """
    报告组装节点
    """

    def prep(self, shared):
        return {
            "template": shared.get("report", {}).get("template", ""),
            "sections": shared.get("report", {}).get("sections", {}),
            "stage3_data": shared.get("stage3_data", {}),
        }

    def exec(self, prep_res):
        template = prep_res["template"]
        sections = prep_res["sections"]
        stage3_data = prep_res["stage3_data"]

        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        header = f"""# 舆情分析智能报告

**生成时间**: {current_time}
**分析工具**: 舆情分析智能体系统
**数据来源**: 社交媒体博文数据

---

## 报告摘要

本报告基于舆情分析智能体系统的自动分析结果，通过多维度数据分析和可视化图表，全面展现当前舆情态势。

"""

        content_parts = [header]

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
            ("appendix", "附录"),
        ]

        for section_key, section_title in section_order:
            if section_key in sections:
                content_parts.append(sections[section_key])

        full_report = "\n\n".join(content_parts)
        return full_report

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}
        shared["report"]["full_content"] = exec_res
        return "default"


class GenerateFullReportNode(MonitoredNode):
    """
    一次性完整报告生成节点
    """

    def prep(self, shared):
        return {
            "template": shared.get("report", {}).get("template", ""),
            "stage3_data": shared.get("stage3_data", {}),
            "config": shared.get("config", {}),
        }

    def exec(self, prep_res):
        template = prep_res["template"]
        stage3_data = prep_res["stage3_data"]

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

        detailed_analysis_summary = []
        for chart_id, analysis in chart_analyses.items():
            if analysis.get("analysis_status") == "success":
                title = analysis.get("chart_title", chart_id)
                chart_info = analysis.get("chart_info", {})
                micro_details = analysis.get("microscopic_details", {})
                macro_insights = analysis.get("macroscopic_insights", {})
                quality = analysis.get("quality_assessment", {})

                stats = micro_details.get("statistics", {})
                data_points_count = len(micro_details.get("data_points", []))

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
                        "readability": quality.get("readability", "unknown"),
                    },
                }
                detailed_analysis_summary.append(analysis_summary)

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

        response = call_glm46(prompt, temperature=0.7, enable_reasoning=True)

        path_replacements = [
            ("report\\images\\", "./images/"),
            ("report/images/", "./images/"),
            ("./report/images/", "./images/"),
            ("../report/images/", "./images/"),
        ]
        for old_path, new_path in path_replacements:
            response = response.replace(old_path, new_path)

        response = _remap_report_images(response, charts)
        return response

    def post(self, shared, prep_res, exec_res):
        if "report" not in shared:
            shared["report"] = {}

        shared["report"]["full_content"] = exec_res
        shared["report"]["generation_mode"] = "template"

        if "stage3_results" not in shared:
            shared["stage3_results"] = {}

        shared["stage3_results"]["generation_mode"] = "template"
        shared["stage3_results"]["current_draft"] = exec_res

        print(f"[Stage3] 完整报告已生成（一次性模式）")
        print(f"[Stage3] 报告长度：{len(exec_res)} 字符")

        image_refs = exec_res.count("![")
        print(f"[Stage3] 图片引用：{image_refs} 个")

        return "default"
