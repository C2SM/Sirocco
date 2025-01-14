import textwrap

from sirocco.parsing import _yaml_data_models as models


def test_workflow_test_internal_dicts():
    testee = models.ConfigWorkflow(
        cycles=[],
        tasks=[{"some_task": {"plugin": "shell"}}],
        data=models.ConfigData(
            available=[models.ConfigAvailableData(foo={})],
            generated=[models.ConfigGeneratedData(bar={})],
        ),
    )
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
          generated:
            - d:
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    testee = models.load_workflow_config(str(minimal))
    assert testee.name == "minimal"
    assert testee.rootdir == tmp_path
