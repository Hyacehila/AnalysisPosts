"""
IRRendererNode unit tests.
"""

from nodes.stage3.ir_renderer import IRRendererNode, render_blocks_to_markdown


def test_render_paragraph_with_marks():
    blocks = [
        {
            "type": "paragraph",
            "inlines": [
                {"text": "关键数据", "marks": [{"type": "bold"}]},
                {"text": "达到42% [E1]。", "marks": []},
            ],
        },
    ]
    md = render_blocks_to_markdown(blocks)
    assert "**关键数据**" in md
    assert "[E1]" in md


def test_render_swot_table():
    blocks = [
        {
            "type": "swotTable",
            "strengths": [{"point": "品牌", "detail": "知名度高"}],
            "weaknesses": [],
            "opportunities": [],
            "threats": [],
        },
    ]
    md = render_blocks_to_markdown(blocks)
    assert "优势" in md
    assert "品牌" in md


def test_ir_renderer_node_builds_reviewed_report_text():
    shared = {
        "stage3_results": {
            "outline": {"title": "测试报告"},
            "chapters": [
                {
                    "id": "ch01",
                    "title": "执行摘要",
                    "blocks": [
                        {"type": "paragraph", "inlines": [{"text": "这是正文[E1]。", "marks": []}]}
                    ],
                }
            ],
        }
    }
    node = IRRendererNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert shared["stage3_results"]["chapters"][0]["content"]
    assert "## 执行摘要" in shared["stage3_results"]["reviewed_report_text"]
