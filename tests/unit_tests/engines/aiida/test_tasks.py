"""Unit tests for sirocco.engines.aiida.tasks module."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.tasks import (
    build_icon_task_spec,
    build_shell_task_spec,
)


class TestBuildShellTaskSpec:
    """Test building shell task specifications."""

    @pytest.fixture
    def mock_shell_task(self):
        """Create a mock ShellTask."""
        task = Mock(spec=core.ShellTask)
        task.name = "test_shell"
        task.coordinates = {}
        task.computer = "localhost"
        task.command = "echo hello"
        task.path = None
        task.walltime = "01:00:00"
        task.mem = None
        task.partition = None
        task.account = None
        task.nodes = None
        task.ntasks_per_node = None
        task.cpus_per_task = None
        task.uenv = None
        task.view = None
        task.cycle_point = Mock()
        task.cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.input_data_items.return_value = []
        task.output_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo hello"
        return task

    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.create_shell_code")
    @patch("aiida.orm.Computer.collection.get")
    def test_build_shell_spec_smoke_test(self, mock_get_computer, mock_create_shell_code, mock_shell_task):
        """Smoke test: verify build_shell_task_spec doesn't crash and returns valid spec."""
        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer

        # Mock the code creation
        mock_code = Mock()
        mock_code.pk = 123
        mock_create_shell_code.return_value = mock_code

        # Build spec
        spec = build_shell_task_spec(mock_shell_task)

        print("\n=== Shell task spec ===")
        pprint(spec)

        # Verify basic properties
        assert spec.label == "test_shell"
        assert spec.code_pk == 123

    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.build_graph_item_label")
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.create_shell_code")
    @patch("aiida.orm.Computer.collection.get")
    def test_build_shell_spec_with_inputs(
        self, mock_get_computer, mock_create_shell_code, mock_build_label, mock_shell_task
    ):
        """Test building shell spec with input data."""
        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer

        mock_code = Mock()
        mock_code.pk = 123
        mock_create_shell_code.return_value = mock_code

        # Add input data
        input_data = Mock(spec=core.AvailableData)
        input_data.name = "input1"
        input_data.coordinates = {}
        input_data.path = Path("/path/to/input")
        mock_shell_task.input_data_items.return_value = [("port1", input_data)]

        mock_build_label.side_effect = ["test_shell", "input1"]

        # Build spec
        spec = build_shell_task_spec(mock_shell_task)

        print("\n=== Input data info ===")
        pprint(spec.input_data_info)
        print("\n=== Filenames ===")
        pprint(spec.filenames)

        # Verify input info was captured
        assert len(spec.input_data_info) == 1
        assert spec.input_data_info[0]["name"] == "input1"
        assert spec.input_data_info[0]["port"] == "port1"

    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.build_graph_item_label")
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.create_shell_code")
    @patch("aiida.orm.Computer.collection.get")
    def test_build_shell_spec_with_outputs(
        self, mock_get_computer, mock_create_shell_code, mock_build_label, mock_shell_task
    ):
        """Test building shell spec with output data."""
        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer

        mock_code = Mock()
        mock_code.pk = 123
        mock_create_shell_code.return_value = mock_code

        # Add output data
        output_data = Mock(spec=core.GeneratedData)
        output_data.name = "output1"
        output_data.coordinates = {}
        output_data.path = Path("output.txt")
        mock_shell_task.output_data_items.return_value = [("output_port", output_data)]

        mock_build_label.side_effect = ["test_shell", "output1"]

        # Build spec
        spec = build_shell_task_spec(mock_shell_task)

        print("\n=== Output data info ===")
        pprint(spec.output_data_info)
        print("\n=== Output port mapping ===")
        pprint(spec.output_port_mapping)

        # Verify output info was captured
        assert len(spec.output_data_info) == 1
        assert spec.output_data_info[0]["name"] == "output1"
        assert spec.output_data_info[0]["port"] == "output_port"


class TestBuildIconTaskSpec:
    """Test building ICON task specifications."""

    @pytest.fixture
    def mock_icon_task(self):
        """Create a mock IconTask."""
        task = Mock(spec=core.IconTask)
        task.name = "test_icon"
        task.coordinates = {}
        task.computer = "localhost"
        task.bin = Path("/path/to/icon")
        task.walltime = "01:00:00"
        task.mem = None
        task.partition = None
        task.account = None
        task.nodes = 1
        task.ntasks_per_node = 12
        task.cpus_per_task = 1
        task.uenv = None
        task.view = None
        task.cycle_point = Mock()
        task.cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.wrapper_script = None
        task.outputs = {}

        # Mock namelists
        task.master_namelist = Mock()
        task.master_namelist.name = "master.namelist"
        task.master_namelist.namelist = Mock()
        task.master_namelist.namelist.write = Mock()

        task.model_namelists = {}
        task.update_icon_namelists_from_workflow = Mock()

        return task

    @patch("aiida.orm.Computer.collection.get")
    @patch("aiida.orm.load_code")
    @patch("aiida.orm.InstalledCode")
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_smoke_test(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        mock_get_computer,
        mock_icon_task,
    ):
        """Smoke test: verify build_icon_task_spec doesn't crash and returns valid spec."""
        from aiida.common.exceptions import NotExistent

        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer

        # Code not found, will create new
        mock_load_code.side_effect = NotExistent("Not found")

        mock_code_instance = Mock()
        mock_code_instance.pk = 123
        mock_code_instance.store.return_value = None
        mock_installed_code_class.return_value = mock_code_instance

        # Mock namelist creation
        mock_master_nml = Mock()
        mock_master_nml.pk = 456
        mock_create_nml.return_value = mock_master_nml

        # Build spec
        spec = build_icon_task_spec(mock_icon_task)

        print("\n=== ICON task spec ===")
        pprint(spec)

        # Verify basic properties
        assert spec.label == "test_icon"
        assert spec.code_pk == 123
        assert spec.master_namelist_pk == 456

    @patch("aiida.orm.Computer.collection.get")
    @patch("aiida.orm.load_code")
    @patch("aiida.orm.InstalledCode")
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.get_wrapper_script_data")
    def test_build_icon_spec_with_wrapper(
        self,
        mock_get_wrapper,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        mock_get_computer,
        mock_icon_task,
    ):
        """Test building ICON spec with wrapper script."""
        from aiida.common.exceptions import NotExistent

        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer
        mock_load_code.side_effect = NotExistent("Not found")

        mock_code = Mock(pk=123)
        mock_code.store.return_value = None
        mock_installed_code_class.return_value = mock_code

        mock_master_nml = Mock(pk=456)
        mock_create_nml.return_value = mock_master_nml

        # Mock wrapper script
        mock_wrapper = Mock()
        mock_wrapper.pk = 789
        mock_wrapper.store.return_value = None
        mock_get_wrapper.return_value = mock_wrapper

        # Set wrapper script path
        mock_icon_task.wrapper_script = Path("/path/to/wrapper.sh")

        # Build spec
        spec = build_icon_task_spec(mock_icon_task)

        print("\n=== ICON spec with wrapper ===")
        print(f"Wrapper script PK: {spec.wrapper_script_pk}")

        # Verify wrapper was stored
        assert spec.wrapper_script_pk == 789

    @patch("aiida.orm.Computer.collection.get")
    @patch("aiida.orm.load_code")
    @patch("aiida.orm.InstalledCode")
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_with_model_namelists(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        mock_get_computer,
        mock_icon_task,
    ):
        """Test building ICON spec with model namelists."""
        from aiida.common.exceptions import NotExistent

        # Setup mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_get_computer.return_value = mock_computer
        mock_load_code.side_effect = NotExistent("Not found")

        mock_code = Mock(pk=123)
        mock_code.store.return_value = None
        mock_installed_code_class.return_value = mock_code

        # Mock master and model namelists
        mock_master = Mock(pk=456)
        mock_atm = Mock(pk=789)
        mock_oce = Mock(pk=101)

        mock_create_nml.side_effect = [mock_master, mock_atm, mock_oce]

        # Add model namelists
        mock_icon_task.model_namelists = {
            "atm": Mock(name="atm.namelist", namelist=Mock()),
            "oce": Mock(name="oce.namelist", namelist=Mock()),
        }

        # Build spec
        spec = build_icon_task_spec(mock_icon_task)

        print("\n=== Model namelist PKs ===")
        pprint(spec.model_namelist_pks)

        # Verify model namelists were stored
        assert "atm" in spec.model_namelist_pks
        assert "oce" in spec.model_namelist_pks
        assert spec.model_namelist_pks["atm"] == 789
        assert spec.model_namelist_pks["oce"] == 101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
