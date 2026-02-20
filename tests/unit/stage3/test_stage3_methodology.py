"""
MethodologyAppendixNode unit tests.
"""

from nodes import MethodologyAppendixNode


def test_methodology_appends_trace_summary():
    shared = {
        "stage3_results": {
            "report_text": "# 报告\n\n正文",
        },
        "trace": {
            "executions": [{"id": "e1"}, {"id": "e2"}],
            "forum_rounds": [{"round": 1}, {"round": 2}],
            "loop_status": {
                "stage2_search_reflection": {"current": 2, "max": 2, "termination_reason": "sufficient"},
                "stage3_chapter_review": {"current": 1, "max": 2, "termination_reason": "sufficient"},
            },
        },
    }

    node = MethodologyAppendixNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    text = shared["stage3_results"]["report_text"]
    assert "附录：分析方法论" in text
    assert "工具调用次数" in text
    assert "stage3_chapter_review" in text
