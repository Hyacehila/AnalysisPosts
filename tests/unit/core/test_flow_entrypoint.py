"""
test_flow_entrypoint.py — Main flow entrypoint selection tests.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from flow import create_main_flow


def _entry_node_name(flow):
    start = flow.start_node
    nested_start = getattr(start, "start_node", None)
    if nested_start is not None:
        return nested_start.__class__.__name__
    return start.__class__.__name__


def test_create_main_flow_uses_stage1_start_by_default():
    flow = create_main_flow()
    assert _entry_node_name(flow) == "DataLoadNode"


def test_create_main_flow_can_start_from_stage2():
    flow = create_main_flow(start_stage=2)
    assert _entry_node_name(flow) == "ClearReportDirNode"


def test_create_main_flow_can_start_from_stage3():
    flow = create_main_flow(start_stage=3)
    assert _entry_node_name(flow) == "ClearStage3OutputsNode"


def test_create_main_flow_rejects_invalid_start_stage():
    with pytest.raises(ValueError):
        create_main_flow(start_stage=4)
