"""
Tests for PathManager utilities.
"""
import os
from pathlib import Path

from utils.path_manager import PathManager, get_images_dir, get_report_dir


def test_get_images_dir_creates(tmp_path):
    img_dir = get_images_dir(str(tmp_path / "images"))
    assert os.path.isdir(img_dir)


def test_get_report_dir_creates(tmp_path):
    rep_dir = get_report_dir(str(tmp_path / "report"))
    assert os.path.isdir(rep_dir)


def test_path_manager_resolves_dirs(tmp_path):
    pm = PathManager(base_dir=str(tmp_path))
    report_dir = pm.ensure_dir(pm.report_dir())
    images_dir = pm.ensure_dir(pm.images_dir())
    assert report_dir.exists()
    assert images_dir.exists()
