"""
RenderHTMLNode unit tests.
"""

from nodes import RenderHTMLNode


def test_render_html_builds_interactive_markup():
    shared = {
        "stage3_results": {
            "final_report_text": "# 标题\n\n![图](./images/c1.png)\n\n<details><summary>证据</summary>内容</details>",
        }
    }

    node = RenderHTMLNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    html = shared["stage3_results"]["final_report_html"]
    assert "<html" in html.lower()
    assert "image-modal" in html
    assert "<details>" in html
    assert "img src=\"./images/c1.png\"" in html
