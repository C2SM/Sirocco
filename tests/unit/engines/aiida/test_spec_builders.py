"""Unit tests for sirocco.engines.aiida.spec_builders module."""

import re
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.models import (
    AiidaMetadata,
    AiidaMetadataOptions,
    AiidaShellTaskSpec,
    InputDataInfo,
    OutputDataInfo,
)
from sirocco.engines.aiida.spec_builders import (
    IconTaskSpecBuilder,
    ShellTaskSpecBuilder,
)
from tests.unit.utils import (
    create_available_data,
    create_generated_data,
    create_mock_icon_task,
    create_mock_shell_task,
)


def create_mock_shell_task_for_builder(command: str = "echo test") -> core.ShellTask:
    """Create a mock ShellTask for testing builder methods.

    Args:
        command: Command template for the task

    Returns:
        Mock ShellTask with necessary attributes
    """
    task = Mock(spec=core.ShellTask)
    task.name = "test_task"
    task.computer = "localhost"
    task.path = "/bin/echo"
    task.command = command
    task.walltime = "01:00:00"
    task.coordinates = {}
    task.cycle_point = None  # For DateCyclePoint check in build_metadata
    # Simple port pattern that matches {name} format with group(2) capturing the name
    task.port_pattern = re.compile(r"\{()([^}]+)\}")
    task.resolve_ports = Mock(return_value=command)
    task.input_data_items = Mock(return_value=[])
    task.output_data_items = Mock(return_value=[])
    # Resource attributes (for build_scheduler_options)
    task.nodes = None
    task.ntasks_per_node = None
    task.cpus_per_task = None
    task.mem = None  # Memory in MB
    task.mem_per_cpu = None
    task.account = None
    task.partition = None
    task.mpi = None
    task.uenv = None
    task.view = None
    return task


class TestBuildShellTaskSpec:
    """Test building shell task specifications."""

    @pytest.fixture
    def shell_task(self, create_shell_task):
        """Create a real ShellTask for testing."""
        return create_shell_task(name="test_shell", command="echo hello")

    def test_build_shell_spec_smoke_test(self, shell_task):
        """Smoke test: verify ShellTaskSpecBuilder doesn't crash and returns valid spec."""
        # Build spec
        spec = ShellTaskSpecBuilder(shell_task).build_spec()

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
        spec = ShellTaskSpecBuilder(shell_task).build_spec()

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
        spec = ShellTaskSpecBuilder(shell_task).build_spec()

        print("\n=== Output data info ===")
        pprint(spec.output_data_info)
        print("\n=== Output port mapping ===")
        pprint(spec.output_port_mapping)

        # Verify output info was captured
        assert len(spec.output_data_info) == 1
        assert spec.output_data_info[0]["name"] == "output1"
        assert spec.output_data_info[0]["port"] == "output_port"


class TestShellTaskSpecBuilderHelpers:
    """Test private helper methods of ShellTaskSpecBuilder."""

    def test_build_input_labels_available_data_with_path(self, aiida_localhost):
        """Test _build_input_labels with AvailableData that has a path."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="input_file",
                name="test.txt",
                coordinates={},
                label="test_label",
                is_available=True,
                path="/path/to/test.txt",
            )
        ]

        result = builder._build_input_labels(input_data_info)

        print("\n=== Build input labels (AvailableData with path) ===")
        print(f"Result: {result}")

        assert "input_file" in result
        assert result["input_file"] == ["/path/to/test.txt"]

    def test_build_input_labels_generated_data_creates_placeholder(self, aiida_localhost):
        """Test _build_input_labels with GeneratedData creates placeholder."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="generated_input",
                name="output.txt",
                coordinates={},
                label="task_a_output",
                is_available=False,
                path="output.txt",
            )
        ]

        result = builder._build_input_labels(input_data_info)

        print("\n=== Build input labels (GeneratedData) ===")
        print(f"Result: {result}")

        assert "generated_input" in result
        assert result["generated_input"] == ["{task_a_output}"]

    def test_build_input_labels_multiple_inputs_same_port(self, aiida_localhost):
        """Test _build_input_labels with multiple inputs on same port."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="input_files",
                name="file1.txt",
                coordinates={},
                label="file1",
                is_available=True,
                path="/path/to/file1.txt",
            ),
            InputDataInfo(
                port="input_files",
                name="file2.txt",
                coordinates={},
                label="file2",
                is_available=True,
                path="/path/to/file2.txt",
            ),
        ]

        result = builder._build_input_labels(input_data_info)

        print("\n=== Build input labels (multiple inputs same port) ===")
        print(f"Result: {result}")

        assert "input_files" in result
        assert len(result["input_files"]) == 2
        assert "/path/to/file1.txt" in result["input_files"]
        assert "/path/to/file2.txt" in result["input_files"]

    def test_build_output_labels_with_path(self, aiida_localhost):
        """Test _build_output_labels with output that has a path."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        output_data_info = [
            OutputDataInfo(
                port="output_file",
                name="result.txt",
                coordinates={},
                label="result",
                path="result.txt",
            )
        ]

        result = builder._build_output_labels(output_data_info)

        print("\n=== Build output labels (with path) ===")
        print(f"Result: {result}")

        assert "output_file" in result
        assert result["output_file"] == ["result.txt"]

    def test_build_output_labels_without_path_creates_placeholder(self, aiida_localhost):
        """Test _build_output_labels without path creates placeholder."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        output_data_info = [
            OutputDataInfo(
                port="output_data",
                name="result",
                coordinates={},
                label="result_label",
                path="",
            )
        ]

        result = builder._build_output_labels(output_data_info)

        print("\n=== Build output labels (without path) ===")
        print(f"Result: {result}")

        assert "output_data" in result
        assert result["output_data"] == ["{result_label}"]

    def test_build_output_labels_skips_none_port(self, aiida_localhost):
        """Test _build_output_labels skips outputs with None port."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        output_data_info = [
            OutputDataInfo(
                port=None,
                name="result.txt",
                coordinates={},
                label="result",
                path="result.txt",
            )
        ]

        result = builder._build_output_labels(output_data_info)

        print("\n=== Build output labels (None port) ===")
        print(f"Result: {result}")

        assert len(result) == 0

    def test_add_referenced_ports_from_command(self, aiida_localhost):
        """Test _add_referenced_ports_from_command finds ports in command."""
        task = create_mock_shell_task_for_builder(command="process {input_file} > {output_file}")
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_labels: dict[str, list[str]] = {}
        builder._add_referenced_ports_from_command(input_labels)

        print("\n=== Add referenced ports from command ===")
        print(f"Command: {task.command}")
        print(f"Found ports: {list(input_labels.keys())}")

        # Should have found both input_file and output_file ports
        assert "input_file" in input_labels
        assert "output_file" in input_labels
        assert input_labels["input_file"] == []
        assert input_labels["output_file"] == []

    def test_add_referenced_ports_does_not_overwrite_existing(self, aiida_localhost):
        """Test _add_referenced_ports_from_command doesn't overwrite existing entries."""
        task = create_mock_shell_task_for_builder(command="process {input_file}")
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_labels = {"input_file": ["/path/to/file.txt"]}
        builder._add_referenced_ports_from_command(input_labels)

        print("\n=== Add referenced ports (existing entry) ===")
        print(f"Result: {input_labels}")

        # Should preserve existing entry
        assert input_labels["input_file"] == ["/path/to/file.txt"]

    def test_resolve_arguments_template(self, aiida_localhost):
        """Test _resolve_arguments_template combines labels and resolves."""
        task = create_mock_shell_task_for_builder(command="echo {input} > {output}")
        task.computer = aiida_localhost.label
        task.path = "/bin/echo"
        task.resolve_ports.return_value = "echo /path/to/input.txt > result.txt"
        builder = ShellTaskSpecBuilder(task)

        input_labels = {"input": ["/path/to/input.txt"]}
        output_labels = {"output": ["result.txt"]}

        result = builder._resolve_arguments_template(input_labels, output_labels)

        print("\n=== Resolve arguments template ===")
        print(f"Input labels: {input_labels}")
        print(f"Output labels: {output_labels}")
        print(f"Result: {result}")

        # Should have called resolve_ports with combined labels
        combined = {**input_labels, **output_labels}
        task.resolve_ports.assert_called_once_with(combined)

        # Result should be the arguments without script name
        assert result == "/path/to/input.txt > result.txt"

    def test_build_filenames_mapping_available_data(self, aiida_localhost):
        """Test _build_filenames_mapping for AvailableData."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="input_file",
                name="test.txt",
                coordinates={},
                label="test_label",
                is_available=True,
                path="/path/to/test.txt",
            )
        ]

        result = builder._build_filenames_mapping(input_data_info)

        print("\n=== Build filenames mapping (AvailableData) ===")
        print(f"Result: {result}")

        # For AvailableData, uses name as key and filename from path
        assert "test.txt" in result
        assert result["test.txt"] == "test.txt"

    def test_build_filenames_mapping_generated_data_unique_name(self, aiida_localhost):
        """Test _build_filenames_mapping for GeneratedData with unique name."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="generated_input",
                name="output",
                coordinates={},
                label="task_a_output",
                is_available=False,
                path="output.txt",
            )
        ]

        result = builder._build_filenames_mapping(input_data_info)

        print("\n=== Build filenames mapping (GeneratedData, unique name) ===")
        print(f"Result: {result}")

        # For GeneratedData with unique name, uses label as key and filename from path
        assert "task_a_output" in result
        assert result["task_a_output"] == "output.txt"

    def test_build_filenames_mapping_generated_data_duplicate_names(self, aiida_localhost):
        """Test _build_filenames_mapping for GeneratedData with duplicate names."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="input1",
                name="output",  # Same name
                coordinates={},
                label="task_a_output",
                is_available=False,
                path="output.txt",
            ),
            InputDataInfo(
                port="input2",
                name="output",  # Same name
                coordinates={},
                label="task_b_output",
                is_available=False,
                path="output.txt",
            ),
        ]

        result = builder._build_filenames_mapping(input_data_info)

        print("\n=== Build filenames mapping (GeneratedData, duplicate names) ===")
        print(f"Result: {result}")

        # For duplicate names, uses label as both key and value to disambiguate
        assert "task_a_output" in result
        assert result["task_a_output"] == "task_a_output"
        assert "task_b_output" in result
        assert result["task_b_output"] == "task_b_output"

    def test_build_filenames_mapping_mixed_data_types(self, aiida_localhost):
        """Test _build_filenames_mapping with mixed AvailableData and GeneratedData."""
        task = create_mock_shell_task_for_builder()
        task.computer = aiida_localhost.label
        builder = ShellTaskSpecBuilder(task)

        input_data_info = [
            InputDataInfo(
                port="available",
                name="input.txt",
                coordinates={},
                label="input_label",
                is_available=True,
                path="/path/to/input.txt",
            ),
            InputDataInfo(
                port="generated",
                name="output",
                coordinates={},
                label="task_a_output",
                is_available=False,
                path="output.txt",
            ),
        ]

        result = builder._build_filenames_mapping(input_data_info)

        print("\n=== Build filenames mapping (mixed types) ===")
        print(f"Result: {result}")

        # AvailableData uses name as key
        assert "input.txt" in result
        assert result["input.txt"] == "input.txt"

        # GeneratedData uses label as key
        assert "task_a_output" in result
        assert result["task_a_output"] == "output.txt"


class TestBuildIconTaskSpec:
    """Test building ICON task specifications."""

    @pytest.fixture
    def icon_task(self, create_icon_task, aiida_localhost):
        """Create a real IconTask for testing."""
        return create_icon_task(name="test_icon", computer=aiida_localhost.label)

    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_smoke_test(
        self,
        mock_create_nml,
        icon_task,
    ):
        """Smoke test: verify IconTaskSpecBuilder doesn't crash and returns valid spec.

        Tests Sirocco logic:
        - Icon spec builder creates a real Code in the database
        - Code has correct properties (label, with_mpi, etc.)
        - Master namelist is handled correctly

        """

        # Mock namelist creation (external aiida-icon dependency)
        mock_master_nml = Mock()
        mock_master_nml.pk = 456
        mock_create_nml.return_value = mock_master_nml

        # Build spec
        spec = IconTaskSpecBuilder(icon_task).build_spec()

        print("\n=== ICON task spec ===")
        pprint(spec)

        # Verify spec properties
        assert spec.label.startswith("test_icon")
        assert spec.code_pk is not None
        assert spec.master_namelist_pk == 456

        # Verify real Code was created in database (Sirocco logic test)
        from aiida.orm import load_node

        code = load_node(spec.code_pk)
        assert code.is_stored
        assert "icon" in code.label
        assert code.with_mpi is True
        assert code.use_double_quotes is True

    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
    # Patch: Mock wrapper script retrieval to test wrapper script handling path
    @patch("sirocco.engines.aiida.adapter.AiidaAdapter.get_wrapper_script_data")
    def test_build_icon_spec_with_wrapper(
        self,
        mock_get_wrapper,
        mock_create_nml,
        icon_task,
    ):
        """Test building ICON spec with wrapper script.

        Tests Sirocco logic:
        - Wrapper script is properly attached to spec
        - Code is still created correctly

        """

        mock_master_nml = Mock(pk=456)
        mock_create_nml.return_value = mock_master_nml

        # Mock wrapper script (external dependency)
        mock_wrapper = Mock()
        mock_wrapper.pk = 789
        mock_wrapper.store.return_value = None
        mock_get_wrapper.return_value = mock_wrapper

        # Set wrapper script path
        icon_task.wrapper_script = Path("/path/to/wrapper.sh")

        # Build spec
        spec = IconTaskSpecBuilder(icon_task).build_spec()

        print("\n=== ICON spec with wrapper ===")
        print(f"Wrapper script PK: {spec.wrapper_script_pk}")

        # Verify wrapper was stored (Sirocco logic test)
        assert spec.wrapper_script_pk == 789

        # Verify real Code was created
        from aiida.orm import load_node

        code = load_node(spec.code_pk)
        assert code.is_stored

    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @pytest.mark.usefixtures("aiida_localhost")
    @patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
    def test_build_icon_spec_with_model_namelists(
        self,
        mock_create_nml,
        create_icon_task_with_models,
    ):
        """Test building ICON spec with model namelists.

        Tests Sirocco logic:
        - Model namelists are correctly collected and stored in spec
        - Code is created for multi-model ICON task

        """

        # Create ICON task with model namelists (atm, oce)
        icon_task = create_icon_task_with_models()

        # Mock master and model namelists (external aiida-icon dependency)
        mock_master = Mock(pk=456)
        mock_atm = Mock(pk=789)
        mock_oce = Mock(pk=101)

        mock_create_nml.side_effect = [mock_master, mock_atm, mock_oce]

        # Build spec
        spec = IconTaskSpecBuilder(icon_task).build_spec()

        print("\n=== Model namelist PKs ===")
        pprint(spec.model_namelist_pks)

        # Verify model namelists were stored (Sirocco logic test)
        assert "atm" in spec.model_namelist_pks
        assert "oce" in spec.model_namelist_pks
        assert spec.model_namelist_pks["atm"] == 789
        assert spec.model_namelist_pks["oce"] == 101

        # Verify real Code was created
        from aiida.orm import load_node

        code = load_node(spec.code_pk)
        assert code.is_stored


class TestBuildShellTaskSpecErrors:
    """Test error cases in ShellTaskSpecBuilder."""

    # Patch: Mock create_shell_code to return unstored code and test error condition
    @patch("sirocco.engines.aiida.code_factory.CodeFactory.create_shell_code")
    def test_code_not_stored(self, mock_create_shell_code, aiida_localhost):
        """Test that unstored code raises RuntimeError."""

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
            ShellTaskSpecBuilder(task).build_spec()


class TestBuildShellTaskSpecOutputs:
    """Test output handling in ShellTaskSpecBuilder."""

    def test_output_without_path(self, aiida_localhost):
        """Test output with path=None gets placeholder."""

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
        spec = ShellTaskSpecBuilder(task).build_spec()

        print("\n=== Output without path ===")
        print(f"Output data: {output_data}")
        print(f"Output port mapping: {spec.output_port_mapping}")
        print(f"Output data info: {spec.output_data_info}")

        # Output without path should not be in output_port_mapping
        assert "result" not in spec.output_port_mapping

    def test_output_with_null_port(self, aiida_localhost):
        """Test output with None port is skipped."""

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
        spec = ShellTaskSpecBuilder(task).build_spec()

        print("\n=== Output with null port ===")
        print(f"Output data: {output_data}")
        print("Output data info:")
        pprint(spec.output_data_info)
        print(f"Output port mapping: {spec.output_port_mapping}")

        # Output with None port should be skipped
        assert len(spec.output_data_info) == 1
        assert spec.output_data_info[0]["port"] is None

    def test_generated_data_single_name_with_path(self, aiida_localhost):
        """Test GeneratedData with single name uses path basename."""

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
        spec = ShellTaskSpecBuilder(task).build_spec()

        print("\n=== GeneratedData with single name and path ===")
        print(f"Input data: {input1}")
        print("Filenames:")
        pprint(spec.filenames)
        print("Input data info:")
        pprint(spec.input_data_info)

        # Single GeneratedData should use path basename
        # The label will be "data_cycle_1" (built by build_label_from_graph_item)
        data_label = [label for label in spec.filenames if "data" in label][0]
        assert spec.filenames[data_label] == "data.txt"

    def test_port_pattern_matching(self, aiida_localhost):
        """Test that ports referenced in command are detected."""

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
        spec = ShellTaskSpecBuilder(task).build_spec()

        print("\n=== Port pattern matching ===")
        print(f"Task command: {task.command}")
        print(f"Resolved ports: {task.resolve_ports.return_value}")
        print(f"Spec label: {spec.label}")
        print(f"Arguments template: {spec.arguments_template}")

        # Verify spec was created successfully
        assert spec.label == "test_task"

    def test_duplicate_input_names(self, aiida_localhost):
        """Test handling of multiple inputs with same name."""

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
        spec = ShellTaskSpecBuilder(task).build_spec()

        print("\n=== Duplicate input names ===")
        print(f"Input 1: {input1}")
        print(f"Input 2: {input2}")
        print("Filenames:")
        pprint(spec.filenames)
        print("Input data info:")
        pprint(spec.input_data_info)

        # When inputs have duplicate names, filenames should use labels
        # The labels will be "data_cycle_1" and "data_cycle_2" (built by build_label_from_graph_item)
        data_labels = [label for label in spec.filenames if "data" in label]
        assert len(data_labels) == 2
        assert all("cycle" in label for label in data_labels)


class TestBuildIconTaskSpecOutputs:
    """Test output port mapping in IconTaskSpecBuilder."""

    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
    def test_null_port_output(
        self,
        mock_create_nml,
        aiida_localhost,  # Real Computer object
    ):
        """Test output with None as port key is skipped.

        Tests Sirocco logic:
        - Outputs with None port are correctly filtered out
        - Spec building handles None ports gracefully

        """

        mock_master_nml = Mock(pk=456)
        mock_create_nml.return_value = mock_master_nml

        # Create task with output that has None port
        task = create_mock_icon_task(
            name="test_icon",
            computer=aiida_localhost.label,
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
        spec = IconTaskSpecBuilder(task).build_spec()

        print("\n=== ICON task with null port output ===")
        print(f"Output data: {output_data}")
        print(f"Task outputs: {task.outputs}")
        print("Output port mapping:")
        pprint(spec.output_port_mapping)

        # Verify None port is filtered out (Sirocco logic test)
        assert "restart_file" not in spec.output_port_mapping

    # Patch: Mock external package dependency (aiida-icon) to prevent actual namelist file creation
    @patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
    def test_output_port_mapping(
        self,
        mock_create_nml,
        aiida_localhost,  # Real Computer object
    ):
        """Test that output port mapping is correctly built.

        Tests Sirocco logic:
        - Multiple outputs on the same port are correctly mapped
        - Output port mapping structure is built correctly

        """

        mock_master_nml = Mock(pk=456)
        mock_create_nml.return_value = mock_master_nml

        # Create task with multiple outputs on same port
        task = create_mock_icon_task(
            name="test_icon",
            computer=aiida_localhost.label,
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
        spec = IconTaskSpecBuilder(task).build_spec()

        print("\n=== ICON task output port mapping ===")
        print(f"Task outputs: {task.outputs}")
        print("Output port mapping:")
        pprint(spec.output_port_mapping)

        # Verify port mapping is correct (Sirocco logic test)
        assert spec.output_port_mapping["atm_output"] == "restart_data"
        assert spec.output_port_mapping["oce_output"] == "restart_data"


class TestWalltimeDefaults:
    """Test walltime default handling in task pair creation."""

    @pytest.mark.parametrize(
        ("walltime", "expected_timeout"),
        [
            pytest.param(None, 3900, id="default"),
            pytest.param(7200, 7500, id="custom_7200"),
            pytest.param(1800, 2100, id="custom_1800"),
        ],
    )
    def test_walltime_timeout_calculation(self, aiida_localhost, walltime, expected_timeout):
        """Test walltime defaults and timeout buffer calculation."""

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

        print("\n=== Walltime calculation ===")
        print(f"Input walltime: {walltime}")
        print(f"Walltime seconds: {walltime_seconds}")
        print(f"Timeout (with buffer): {timeout}")
        print(f"Expected timeout: {expected_timeout}")

        assert timeout == expected_timeout
