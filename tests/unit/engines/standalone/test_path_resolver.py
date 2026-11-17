from pathlib import Path

import pytest

from sirocco.core.graph_items import GeneratedData
from sirocco.engines.standalone.path_resolver import PathResolver

DATA = GeneratedData(name="some_data", coordinates={})


def test_set_path_again_to_same_path(tmp_path):
    path_resolver = PathResolver(tmp_path)
    path_resolver.set_path(DATA, tmp_path / "a")
    path_resolver.set_path(DATA, tmp_path / "a")
    assert path_resolver.get_path(DATA) == tmp_path / "a"


def test_set_path_conflict(tmp_path):
    path_resolver = PathResolver(tmp_path)
    path_resolver.set_path(DATA, tmp_path / "a")
    with pytest.raises(ValueError, match="already resolved to"):
        path_resolver.set_path(DATA, tmp_path / "b")
    assert path_resolver.get_path(DATA) == tmp_path / "a"


def test_set_path_relative(tmp_path):
    with pytest.raises(ValueError, match="must be absolute"):
        PathResolver(tmp_path).set_path(DATA, Path("relative"))
