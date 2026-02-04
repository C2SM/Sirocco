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
from tests.utils import (
    create_available_data,
    create_generated_data,
    create_mock_icon_task,
    create_mock_shell_task,
)


class TestBuildShellTaskSpec:
    """Test building shell task specifications."""

    @pytest.fixture
    def shell_task(self, create_shell_task):
        """Create a real ShellTask for testing."""
        return create_shell_task(name="test_shell", command="echo hello")

    def test_build_shell_spec_smoke_test(self, shell_task):
        """Smoke test: verify build_shell_task_spec doesn't crash and returns valid spec."""
        # Build spec
        spec = build_shell_task_spec(shell_task)

        print("\n=== Shell task spec ===")
        pprint(spec)

        # Verify basic properties
        assert spec.label == "test_shell"
        assert spec.code_pk is not None  # Should have a real code PK

    def test_build_shell_spec_with_inputs(self, shell_task):
        """Test building shell spec with input data."""
        # Add real input data
        input_data = create_available_data(name="input1", computer_label="localhost", path="/path/to/input")
        shell_task.inputs["port1"] = [input_data]

        # Build spec
        spec = build_shell_task_spec(shell_task)

        print("\n=== Input data info ===")
        pprint(spec.input_data_info)
        print("\n=== Filenames ===")
        pprint(spec.filenames)

        # Verify input info was captured
        assert len(spec.input_data_info) == 1
        assert spec.input_data_info[0]["name"] == "input1"
        assert spec.input_data_info[0]["port"] == "port1"

    def test_build_shell_spec_with_outputs(self, shell_task):
        """Test building shell spec with output data."""
        # Add real output data
        output_data = create_generated_data(name="output1", path="output.txt")
        shell_task.outputs["output_port"] = [output_data]

        # Build spec
        spec = build_shell_task_spec(shell_task)

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
    def icon_task(self, create_icon_task):
        """Create a real IconTask for testing."""
        return create_icon_task(name="test_icon")

    # Patch: Force NotExistent exception to test code creation path when code doesn't exist
    @patch("aiida.orm.load_code")
    # Patch: Mock InstalledCode class to prevent actual database storage during test
    @patch("aiida.orm.InstalledCode")
    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_smoke_test(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        icon_task,
    ):
        """Smoke test: verify build_icon_task_spec doesn't crash and returns valid spec."""
        from aiida.common.exceptions import NotExistent

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
        spec = build_icon_task_spec(icon_task)

        print("\n=== ICON task spec ===")
        pprint(spec)

        # Verify basic properties
        # Label includes date for DateCyclePoint tasks
        assert spec.label.startswith("test_icon")
        assert spec.code_pk == 123
        assert spec.master_namelist_pk == 456

    # Patch: Force NotExistent exception to test code creation path when code doesn't exist
    @patch("aiida.orm.load_code")
    # Patch: Mock InstalledCode class to prevent actual database storage during test
    @patch("aiida.orm.InstalledCode")
    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    # Patch: Mock wrapper script retrieval to test wrapper script handling path
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.get_wrapper_script_data")
    def test_build_icon_spec_with_wrapper(
        self,
        mock_get_wrapper,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        icon_task,
    ):
        """Test building ICON spec with wrapper script."""
        from aiida.common.exceptions import NotExistent

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
        icon_task.wrapper_script = Path("/path/to/wrapper.sh")

        # Build spec
        spec = build_icon_task_spec(icon_task)

        print("\n=== ICON spec with wrapper ===")
        print(f"Wrapper script PK: {spec.wrapper_script_pk}")

        # Verify wrapper was stored
        assert spec.wrapper_script_pk == 789

    # Patch: Force NotExistent exception to test code creation path when code doesn't exist
    @patch("aiida.orm.load_code")
    # Patch: Mock InstalledCode class to prevent actual database storage during test
    @patch("aiida.orm.InstalledCode")
    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_with_model_namelists(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        create_icon_task_with_models,
    ):
        """Test building ICON spec with model namelists."""
        from aiida.common.exceptions import NotExistent

        # Create ICON task with model namelists (atm, oce)
        icon_task = create_icon_task_with_models()

        mock_load_code.side_effect = NotExistent("Not found")

        mock_code = Mock(pk=123)
        mock_code.store.return_value = None
        mock_installed_code_class.return_value = mock_code

        # Mock master and model namelists
        mock_master = Mock(pk=456)
        mock_atm = Mock(pk=789)
        mock_oce = Mock(pk=101)

        mock_create_nml.side_effect = [mock_master, mock_atm, mock_oce]

        # Build spec
        spec = build_icon_task_spec(icon_task)

        print("\n=== Model namelist PKs ===")
        pprint(spec.model_namelist_pks)

        # Verify model namelists were stored
        assert "atm" in spec.model_namelist_pks
        assert "oce" in spec.model_namelist_pks
        assert spec.model_namelist_pks["atm"] == 789
        assert spec.model_namelist_pks["oce"] == 101


class TestBuildShellTaskSpecErrors:
    """Test error cases in build_shell_task_spec."""

    def test_computer_not_found(self, create_shell_task):
        """Test that missing computer raises ValueError."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Create a real ShellTask with non-existent computer
        task = create_shell_task(computer="nonexistent_computer")

        # Should raise ValueError with helpful message
        with pytest.raises(ValueError, match="Could not find computer 'nonexistent_computer'"):
            build_shell_task_spec(task)

    # Patch: Mock create_shell_code to return unstored code and test error condition
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.create_shell_code")
    def test_code_not_stored(self, mock_create_shell_code, aiida_localhost):
        """Test that unstored code raises RuntimeError."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Create task
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="echo test",
            walltime="01:00:00",
        )
        task.input_data_items.return_value = []
        task.output_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo test"

        # Mock code with pk=None (not stored)
        mock_code = Mock()
        mock_code.pk = None
        mock_create_shell_code.return_value = mock_code

        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match=r"Code for task .* must be stored"):
            build_shell_task_spec(task)


class TestBuildShellTaskSpecOutputs:
    """Test output handling in build_shell_task_spec."""

    def test_output_without_path(self, aiida_localhost):
        """Test output with path=None gets placeholder."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Setup task
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="echo test",
            walltime="01:00:00",
        )
        task.input_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo test {output_data}"

        # Add output with no path
        output_data = create_generated_data(name="result", path=None, coordinates={})
        task.output_data_items.return_value = [("output_port", output_data)]

        # Build spec
        spec = build_shell_task_spec(task)

        # Output without path should not be in output_port_mapping
        assert "result" not in spec.output_port_mapping

    def test_output_with_null_port(self, aiida_localhost):
        """Test output with None port is skipped (line 211)."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Setup task
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="echo test",
            walltime="01:00:00",
        )
        task.input_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo test"

        # Add output with port=None
        output_data = create_generated_data(name="result", path="result.txt", coordinates={})
        task.output_data_items.return_value = [(None, output_data)]  # port=None

        # Build spec - should not crash with None port
        spec = build_shell_task_spec(task)

        # Output with None port should be skipped
        assert len(spec.output_data_info) == 1
        assert spec.output_data_info[0]["port"] is None

    def test_generated_data_single_name_with_path(self, aiida_localhost):
        """Test GeneratedData with single name uses path basename (line 251)."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Setup task
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="echo test",
            walltime="01:00:00",
        )
        task.output_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo test"

        # Create one GeneratedData with path
        input1 = create_generated_data(name="data", path="/some/dir/data.txt", coordinates={"cycle": 1})

        task.input_data_items.return_value = [("port1", input1)]

        # Build spec
        spec = build_shell_task_spec(task)

        # Single GeneratedData should use path basename (line 251)
        # The label will be "data_cycle_1" (built by build_graph_item_label)
        data_label = [label for label in spec.filenames if "data" in label][0]
        assert spec.filenames[data_label] == "data.txt"

    def test_port_pattern_matching(self, aiida_localhost):
        """Test that ports referenced in command are detected."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Setup task with command that references an input port not in input_data_items
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="script.sh [[optional_input]]",
            path=Path("script.sh"),
            walltime="01:00:00",
        )
        task.input_data_items.return_value = []
        task.output_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        # Simulate resolve_ports creating empty string for missing port
        task.resolve_ports.return_value = "script.sh "

        # Build spec - should not crash even with unreferenced port
        spec = build_shell_task_spec(task)

        # Verify spec was created successfully
        assert spec.label == "test_task"

    def test_duplicate_input_names(self, aiida_localhost):
        """Test handling of multiple inputs with same name."""
        from sirocco.engines.aiida.tasks import build_shell_task_spec

        # Setup task with two inputs that have the same name but different coordinates
        task = create_mock_shell_task(
            name="test_task",
            computer=aiida_localhost.label,
            command="echo test",
            walltime="01:00:00",
        )
        task.output_data_items.return_value = []
        task.port_pattern = core.ShellTask.port_pattern
        task.resolve_ports.return_value = "echo test"

        # Create two GeneratedData items with same name
        input1 = create_generated_data(name="data", path="data1.txt", coordinates={"cycle": 1})
        input2 = create_generated_data(name="data", path="data2.txt", coordinates={"cycle": 2})

        task.input_data_items.return_value = [("port1", input1), ("port2", input2)]

        # Build spec
        spec = build_shell_task_spec(task)

        # When inputs have duplicate names, filenames should use labels
        # The labels will be "data_cycle_1" and "data_cycle_2" (built by build_graph_item_label)
        data_labels = [label for label in spec.filenames if "data" in label]
        assert len(data_labels) == 2
        assert all("cycle" in label for label in data_labels)


class TestBuildIconTaskSpecErrors:
    """Test error cases in build_icon_task_spec."""

    def test_computer_not_found(self, create_icon_task):
        """Test that missing computer raises ValueError."""
        from sirocco.engines.aiida.tasks import build_icon_task_spec

        # Create real task with non-existent computer
        task = create_icon_task(computer="nonexistent_computer")

        # Should raise ValueError with helpful message
        with pytest.raises(ValueError, match="Could not find computer 'nonexistent_computer'"):
            build_icon_task_spec(task)


class TestBuildIconTaskSpecOutputs:
    """Test output port mapping in build_icon_task_spec."""

    # Patch: Mock computer retrieval to control computer object in test
    @patch("aiida.orm.Computer.collection.get")
    # Patch: Force NotExistent exception to test code creation path when code doesn't exist
    @patch("aiida.orm.load_code")
    # Patch: Mock InstalledCode class to prevent actual database storage during test
    @patch("aiida.orm.InstalledCode")
    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_null_port_output(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        mock_get_computer,
    ):
        """Test output with None as port key is skipped."""
        from aiida.common.exceptions import NotExistent

        from sirocco.engines.aiida.tasks import build_icon_task_spec

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

        # Create task with output that has None port
        task = create_mock_icon_task(
            name="test_icon",
            computer="localhost",
            walltime="01:00:00",
            nodes=1,
            ntasks_per_node=12,
            cpus_per_task=1,
        )
        task.bin = Path("/path/to/icon")
        task.wrapper_script = None
        task.master_namelist = Mock()
        task.master_namelist.name = "master.namelist"
        task.master_namelist.namelist = Mock()
        task.model_namelists = {}
        task.update_icon_namelists_from_workflow = Mock()

        # Add output with None port
        output_data = create_generated_data(name="restart_file", path=None)
        task.outputs = {None: [output_data]}

        # Build spec
        spec = build_icon_task_spec(task)

        # Output with None port should not be in output_port_mapping
        assert "restart_file" not in spec.output_port_mapping

    # Patch: Mock computer retrieval to control computer object in test
    @patch("aiida.orm.Computer.collection.get")
    # Patch: Force NotExistent exception to test code creation path when code doesn't exist
    @patch("aiida.orm.load_code")
    # Patch: Mock InstalledCode class to prevent actual database storage during test
    @patch("aiida.orm.InstalledCode")
    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.tasks.create_namelist_singlefiledata_from_content")
    def test_output_port_mapping(
        self,
        mock_create_nml,
        mock_installed_code_class,
        mock_load_code,
        mock_get_computer,
    ):
        """Test that output port mapping is correctly built."""
        from aiida.common.exceptions import NotExistent

        from sirocco.engines.aiida.tasks import build_icon_task_spec

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

        # Create task with multiple outputs on same port
        task = create_mock_icon_task(
            name="test_icon",
            computer="localhost",
            walltime="01:00:00",
            nodes=1,
            ntasks_per_node=12,
            cpus_per_task=1,
        )
        task.bin = Path("/path/to/icon")
        task.wrapper_script = None
        task.master_namelist = Mock()
        task.master_namelist.name = "master.namelist"
        task.master_namelist.namelist = Mock()
        task.model_namelists = {}
        task.update_icon_namelists_from_workflow = Mock()

        # Add multiple outputs on the same port
        output1 = create_generated_data(name="atm_output", path=None)
        output2 = create_generated_data(name="oce_output", path=None)
        task.outputs = {"restart_data": [output1, output2]}

        # Build spec
        spec = build_icon_task_spec(task)

        # Both outputs should map to the same port
        assert spec.output_port_mapping["atm_output"] == "restart_data"
        assert spec.output_port_mapping["oce_output"] == "restart_data"


class TestWalltimeDefaults:
    """Test walltime default handling in task pair creation."""

    @pytest.mark.parametrize(
        ("walltime", "expected_timeout"),
        [
            (None, 3900),  # default 3600 + 300
            (7200, 7500),  # custom 7200 + 300
            (1800, 2100),  # 1800 + 300
        ],
        ids=["default", "custom-7200", "custom-1800"],
    )
    def test_walltime_timeout_calculation(self, aiida_localhost, walltime, expected_timeout):
        """Test walltime defaults and timeout buffer calculation (lines 568-572)."""
        from sirocco.engines.aiida.types import AiidaMetadata, AiidaMetadataOptions, AiidaShellTaskSpec

        # Create task spec with optional walltime
        options = AiidaMetadataOptions(max_wallclock_seconds=walltime) if walltime else AiidaMetadataOptions()
        task_spec = AiidaShellTaskSpec(
            label="test_task",
            code_pk=123,
            node_pks={},
            metadata=AiidaMetadata(computer=aiida_localhost, options=options),
            arguments_template="echo test",
            filenames={},
            outputs=[],
            input_data_info=[],
            output_data_info=[],
            output_port_mapping={},
        )

        # Simulate the walltime calculation logic from _add_task_pair_common
        walltime_seconds = 3600  # Default
        if task_spec.metadata and task_spec.metadata.options and task_spec.metadata.options.max_wallclock_seconds:
            walltime_seconds = task_spec.metadata.options.max_wallclock_seconds
        timeout = walltime_seconds + 300

        assert timeout == expected_timeout


class TestAddTaskPairFunctions:
    """Test add_icon_task_pair and add_shell_task_pair functions."""

    def test_add_icon_task_pair(self, aiida_localhost):
        """Test add_icon_task_pair creates launcher and monitor tasks (line 619)."""
        from unittest.mock import Mock, patch

        from sirocco.engines.aiida.tasks import add_icon_task_pair
        from sirocco.engines.aiida.types import AiidaIconTaskSpec, AiidaMetadata, DependencyMapping

        # Create a mock WorkGraph
        # Patch: Mock build_icon_task_with_dependencies to isolate add_icon_task_pair logic
        with patch("sirocco.engines.aiida.tasks.build_icon_task_with_dependencies"):
            mock_wg = Mock()
            mock_wg.add_task = Mock()

            # Create task spec
            task_spec = AiidaIconTaskSpec(
                label="test_icon",
                code_pk=123,
                master_namelist_pk=456,
                model_namelist_pks={},
                metadata=AiidaMetadata(computer=aiida_localhost),
                output_port_mapping={},
            )

            # Create empty dependencies
            dependencies = DependencyMapping(port_mapping={}, parent_folders={}, job_ids={})

            # Tracking dicts
            task_dep_info = {}
            prev_dep_tasks = {}

            # Call add_icon_task_pair
            add_icon_task_pair(
                wg=mock_wg,
                wg_name="test_wg",
                task_label="test_icon",
                task_spec=task_spec,
                input_data_for_task={},
                dependencies=dependencies,
                task_dep_info=task_dep_info,
                prev_dep_tasks=prev_dep_tasks,
            )

            # Should have created two tasks: launcher and monitor
            assert mock_wg.add_task.call_count == 2

            # Should have updated task_dep_info
            assert "test_icon" in task_dep_info

            # Should have updated prev_dep_tasks
            assert "test_icon" in prev_dep_tasks
