"""
InjectTraceNode unit tests.
"""

from nodes import InjectTraceNode


def test_inject_trace_appends_details_block():
    shared = {
        "stage3_results": {
            "reviewed_report_text": "# 报告\n\n## 结论\n文本",
        },
        "trace": {
            "insight_provenance": {
                "summary": [
                    {
                        "source": "DataAgent",
                        "evidence": "情感中性占比上升",
                        "confidence": 0.85,
                    }
                ]
            }
        },
    }

    node = InjectTraceNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert "<details>" in shared["stage3_results"]["report_text"]
    assert "DataAgent" in shared["stage3_results"]["report_text"]


def test_inject_trace_noop_on_empty_provenance():
    shared = {
        "stage3_results": {
            "reviewed_report_text": "# 报告\n\n文本",
        },
        "trace": {"insight_provenance": {}},
    }

    node = InjectTraceNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert shared["stage3_results"]["report_text"] == "# 报告\n\n文本"


def test_inject_trace_supports_dict_provenance_schema():
    shared = {
        "stage3_results": {
            "reviewed_report_text": "# 报告\n\n## 结论\n文本",
        },
        "trace": {
            "insight_provenance": {
                "insight_overall_summary": {
                    "text": "中性情感占主导",
                    "supporting_evidence": [
                        {
                            "tool": "sentiment_trend_chart",
                            "summary": "生成了情感趋势图",
                            "status": "success",
                        }
                    ],
                    "confidence": "medium",
                }
            }
        },
    }

    node = InjectTraceNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    report_text = shared["stage3_results"]["report_text"]
    assert "sentiment_trend_chart" in report_text
    assert "生成了情感趋势图" in report_text
