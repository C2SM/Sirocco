from pathlib import Path

import pytest

from sirocco.core import Workflow
from sirocco.pretty_print import PrettyPrinter
from sirocco.vizgraph import VizGraph


@pytest.fixture
def pprinter():
    return PrettyPrinter()


config_test_files = [
    "tests/cases/small/config/test_config_small.yml",
    "tests/cases/large/config/test_config_large.yml",
    "tests/cases/parameters/config/test_config_parameters.yml",
]


@pytest.fixture(params=config_test_files)
def config_paths(request):
    config_path = Path(request.param)
    return {
        "yml": config_path,
        "txt": (config_path.parent.parent / "data" / config_path.name).with_suffix(".txt"),
        "svg": (config_path.parent.parent / "svg" / config_path.name).with_suffix(".svg"),
    }


def test_parse_config_file(config_paths, pprinter):
    reference_str = config_paths["txt"].read_text()
    test_str = pprinter.format(Workflow.from_yaml(config_paths["yml"]))
    if test_str != reference_str:
        new_path = Path(config_paths["txt"]).with_suffix(".new.txt")
        new_path.write_text(test_str)
        msg = f"Workflow graph doesn't match serialized data. New graph string dumped to {new_path}."
        raise ValueError(msg)


@pytest.mark.skip(reason="don't run it each time, uncomment to regenerate serilaized data")
def test_serialize_workflow(config_paths, pprinter):
    config_paths["txt"].write_text(pprinter.format(Workflow.from_yaml(config_paths["yml"])))


def test_vizgraph(config_paths):
    VizGraph.from_yaml(config_paths["yml"]).draw(file_path=config_paths["svg"])
