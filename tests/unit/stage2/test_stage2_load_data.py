"""
DataSummaryNode tests for time-range extraction and shared analysis context.
"""

from nodes.stage2.load_data import DataSummaryNode, LoadEnhancedDataNode


def test_data_summary_writes_analysis_context_with_publish_time_priority():
    shared = {
        "data": {
            "blog_data": [
                {
                    "content": "A",
                    "publish_time": "2024-08-16 10:00:00",
                    "created_at": "2024-08-15 09:00:00",
                    "sentiment_polarity": "中性",
                },
                {
                    "content": "B",
                    "publish_time": "2024-08-31 22:00:00",
                    "created_at": "2024-08-31 21:00:00",
                    "sentiment_polarity": "乐观",
                },
            ]
        },
        "analysis_context": {
            "user_analysis_instruction": "重点分析官方回应时效性",
            "time_range": None,
            "time_range_text": "",
        },
    }

    node = DataSummaryNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert shared["analysis_context"]["time_range"]["start"] == "2024-08-16 10:00:00"
    assert shared["analysis_context"]["time_range"]["end"] == "2024-08-31 22:00:00"
    assert "2024-08-16 10:00:00 至 2024-08-31 22:00:00" in shared["analysis_context"]["time_range_text"]
    assert shared["analysis_context"]["user_analysis_instruction"] == "重点分析官方回应时效性"


def test_data_summary_falls_back_to_created_at_when_publish_time_absent():
    shared = {
        "data": {
            "blog_data": [
                {"content": "A", "created_at": "2024-09-01 08:30:00"},
                {"content": "B", "created_at": "2024-09-02 19:15:00"},
            ]
        },
        "analysis_context": {
            "user_analysis_instruction": "",
            "time_range": None,
            "time_range_text": "",
        },
    }

    node = DataSummaryNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert shared["analysis_context"]["time_range"]["start"] == "2024-09-01 08:30:00"
    assert shared["analysis_context"]["time_range"]["end"] == "2024-09-02 19:15:00"
    assert shared["analysis_context"]["time_range"]["source_field"] == "created_at"


def test_load_enhanced_data_prep_cleans_legacy_chart_analyses(tmp_path):
    enhanced_path = tmp_path / "enhanced_blogs.json"
    enhanced_path.write_text("[]", encoding="utf-8")

    report_dir = tmp_path / "report"
    report_dir.mkdir()
    legacy_file = report_dir / "chart_analyses.json"
    legacy_file.write_text('{"legacy": true}', encoding="utf-8")

    shared = {
        "config": {
            "data_source": {"enhanced_data_path": str(enhanced_path)},
            "report": {"output_dir": str(report_dir)},
        }
    }

    node = LoadEnhancedDataNode()
    prep_res = node.prep(shared)

    assert prep_res["data_path"] == str(enhanced_path)
    assert not legacy_file.exists()
