import pathlib
import textwrap

import pydantic
import pytest

from sirocco.parsing import _yaml_data_models as models


@pytest.mark.parametrize("data_type", ["file", "dir"])
def test_base_data(data_type):
    testee = models.ConfigBaseData(name="name", type=data_type, src="foo.txt", format=None)

    assert testee.type == data_type


@pytest.mark.parametrize("data_type", [None, "invalid", 1.42])
def test_base_data_invalid_type(data_type):
    with pytest.raises(pydantic.ValidationError):
        _ = models.ConfigBaseData(name="name", src="foo", format="nml")

    with pytest.raises(pydantic.ValidationError):
        _ = models.ConfigBaseData(name="name", type=data_type, src="foo", format="nml")


def test_workflow_canonicalization():
    config = models.ConfigWorkflow(
        name="testee",
        cycles=[models.ConfigCycle(name="minimal", tasks=[models.ConfigCycleTask(name="a")])],
        tasks=[models.ConfigShellTask(name="some_task")],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(name="foo", type=models.DataType.FILE, src="foo.txt")],
            generated=[models.ConfigGeneratedData(name="bar", type=models.DataType.DIR, src="bar")],
        ),
    )

    testee = models.canonicalize_workflow(config, rootdir=pathlib.Path("foo"))
    assert testee.data_dict["foo"].name == "foo"
    assert testee.data_dict["bar"].name == "bar"
    assert testee.task_dict["some_task"].name == "some_task"


def test_load_workflow_config(tmp_path):
    minimal_config = textwrap.dedent(
        """
        cycles:
          - minimal:
              tasks:
                - a:
        tasks:
          - b:
              plugin: shell
        data:
          available:
            - c:
                type: "file"
                src: "c.txt"
          generated:
            - d:
                type: "dir"
                src: "d"
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    testee = models.load_workflow_config(str(minimal))
    assert testee.name == "minimal"
    assert testee.rootdir == tmp_path
