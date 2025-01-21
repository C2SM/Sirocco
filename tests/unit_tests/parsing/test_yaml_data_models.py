import textwrap

from sirocco.parsing import _yaml_data_models as models


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
    testee = models.ConfigWorkflow.from_config_file(str(minimal))
    assert testee.name == "minimal"
    assert testee.rootdir == tmp_path
