"""Unit tests for sirocco.engines.aiida.dependency_resolvers module."""

import textwrap
from unittest.mock import Mock, patch

import aiida.orm
import f90nml
import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.dependency_resolvers import (
    _resolve_icon_dependency,
    _resolve_icon_output_stream_paths,
    _resolve_icon_restart_file,
    build_dependency_mapping,
    resolve_available_data_inputs,
    resolve_icon_dependency_mapping,
    resolve_shell_dependency_mappings,
)
from sirocco.engines.aiida.models import DependencyInfo
from tests.unit.utils import create_available_data, create_generated_data


class TestCollectAvailableDataInputs:
    """Test collecting AvailableData inputs for a task."""

    def test_collect_single_input(self, tmp_path):
        """Test collecting a single AvailableData input."""
        # Create real AvailableData
        input_data = create_available_data(
            name="input1",
            computer_label="localhost",
            path=tmp_path / "input1.txt",
        )

        # Create mock task with one AvailableData input
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = [("port1", input_data)]

        # Create mock data nodes mapping

        with patch.object(AiidaAdapter, "build_label_from_graph_item", return_value="input1"):
            aiida_nodes = {"input1": Mock()}

            result = resolve_available_data_inputs(task, aiida_nodes)

        print("\n=== Available data inputs ===")
        pprint(result)

        assert "port1" in result
        assert result["port1"] == aiida_nodes["input1"]

    def test_collect_multiple_inputs(self, tmp_path):
        """Test collecting multiple AvailableData inputs."""
        # Create real AvailableData objects
        input1 = create_available_data(
            name="input1",
            computer_label="localhost",
            path=tmp_path / "input1.txt",
        )
        input2 = create_available_data(
            name="input2",
            computer_label="localhost",
            path=tmp_path / "input2.txt",
        )

        task = Mock(spec=core.Task)
        task.input_data_items.return_value = [
            ("port1", input1),
            ("port2", input2),
        ]

        print("\n=== Test collect_multiple_inputs ===")
        print("Input data items: [('port1', input1), ('port2', input2)]")

        with patch.object(AiidaAdapter, "build_label_from_graph_item", side_effect=["input1", "input2"]):
            aiida_nodes = {"input1": Mock(), "input2": Mock()}

            result = resolve_available_data_inputs(task, aiida_nodes)

        print(f"Result: {list(result.keys())}")
        print(f"Number of inputs collected: {len(result)}")

        assert len(result) == 2
        assert "port1" in result
        assert "port2" in result

    def test_collect_no_inputs(self):
        """Test collecting when task has no AvailableData inputs."""
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = []

        print("\n=== Test collect_no_inputs ===")
        print("Input data items: []")

        result = resolve_available_data_inputs(task, {})

        print(f"Result: {result}")

        assert result == {}

    def test_collect_skip_generated_data(self, tmp_path):
        """Test that GeneratedData is skipped."""
        # Create real AvailableData and GeneratedData
        available = create_available_data(
            name="available",
            computer_label="localhost",
            path=tmp_path / "available.txt",
        )
        generated = create_generated_data(
            name="generated",
            path="generated.txt",
        )

        task = Mock(spec=core.Task)
        task.input_data_items.return_value = [
            ("port1", available),
            ("port2", generated),
        ]

        print("\n=== Test collect_skip_generated_data ===")
        print("Input data items: [('port1', AvailableData), ('port2', GeneratedData)]")

        with patch.object(
            AiidaAdapter,
            "build_label_from_graph_item",
            side_effect=["available", "generated"],
        ):
            aiida_nodes = {"available": Mock(), "generated": Mock()}

            result = resolve_available_data_inputs(task, aiida_nodes)

        print(f"Result ports: {list(result.keys())}")
        print("Expected: only port1 (GeneratedData skipped)")

        # Should only include AvailableData
        assert len(result) == 1
        assert "port1" in result
        assert "port2" not in result


class TestBuildDependencyMapping:
    """Test building dependency mappings for tasks."""

    def test_build_mapping_no_dependencies(self):
        """Test building mapping for task with no dependencies."""
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = []

        workflow = Mock(spec=core.Workflow)
        workflow.tasks = []

        print("\n=== Test build_mapping_no_dependencies ===")
        print("Task has no input data items")

        mapping = build_dependency_mapping(task, workflow, {})

        print("\n=== Dependency mapping result ===")
        pprint(
            {
                "port_mapping": mapping.port_mapping,
                "parent_folders": mapping.task_folders,
                "job_ids": mapping.task_job_ids,
            }
        )

        assert mapping.port_mapping == {}
        assert mapping.task_folders == {}
        assert mapping.task_job_ids == {}

    def test_build_mapping_available_data_only(self, tmp_path):
        """Test building mapping for task with only AvailableData inputs."""
        # Create real AvailableData
        available = create_available_data(
            name="input",
            computer_label="localhost",
            path=tmp_path / "input.txt",
        )

        task = Mock(spec=core.Task)
        task.input_data_items.return_value = [("port1", available)]

        workflow = Mock(spec=core.Workflow)
        workflow.tasks = []

        print("\n=== Test build_mapping_available_data_only ===")
        print("Task has one AvailableData input on port1")

        with patch.object(AiidaAdapter, "build_label_from_graph_item", return_value="input"):
            mapping = build_dependency_mapping(task, workflow, {})

        print("\n=== Dependency mapping result ===")
        pprint(
            {
                "port_mapping": mapping.port_mapping,
                "parent_folders": mapping.task_folders,
                "job_ids": mapping.task_job_ids,
            }
        )

        # AvailableData should not create dependencies
        assert mapping.port_mapping == {}
        assert mapping.task_folders == {}
        assert mapping.task_job_ids == {}

    def test_build_mapping_with_generated_data(self):
        """Test building mapping for task with GeneratedData inputs."""
        # Create real GeneratedData
        output_data = create_generated_data(
            name="output",
            path=None,  # No specific path
        )

        # Create producer task
        producer = Mock(spec=core.Task)
        producer.name = "producer"
        producer.coordinates = {}
        producer.output_data_items.return_value = [("output_port", output_data)]

        # Create consumer task
        consumer = Mock(spec=core.Task)
        consumer.input_data_items.return_value = [("input_port", output_data)]

        # Create workflow
        workflow = Mock(spec=core.Workflow)
        workflow.tasks = [producer]

        # Mock task outputs (producer has completed)

        with patch.object(
            AiidaAdapter,
            "build_label_from_graph_item",
            side_effect=["producer", "output", "output", "producer"],  # Added extra "producer" for generator check
        ):
            task_outputs = {
                "producer": Mock(
                    remote_folder=Mock(value=123),
                    job_id=Mock(value=456),
                )
            }

            mapping = build_dependency_mapping(consumer, workflow, task_outputs)

        print("\n=== Dependency mapping ===")
        pprint(
            {
                "port_mapping": mapping.port_mapping,
                "parent_folders": mapping.task_folders,
                "job_ids": mapping.task_job_ids,
            }
        )

        # Should create dependency
        assert "input_port" in mapping.port_mapping
        assert len(mapping.port_mapping["input_port"]) == 1
        assert mapping.port_mapping["input_port"][0].dep_label == "producer"

        assert "producer" in mapping.task_folders
        assert mapping.task_folders["producer"].value == 123

        assert "producer" in mapping.task_job_ids
        assert mapping.task_job_ids["producer"].value == 456

    def test_build_mapping_with_missing_producer(self):
        """Test building mapping when producer doesn't exist."""
        # Create real GeneratedData that references a non-existent producer
        generated_data = create_generated_data(
            name="output",
            path="output.txt",
        )

        task = Mock(spec=core.Task)
        task.input_data_items.return_value = [("input_port", generated_data)]

        # Empty workflow - no producer tasks
        workflow = Mock(spec=core.Workflow)
        workflow.tasks = []

        print("\n=== Test build_mapping_with_missing_producer ===")
        print("Task has GeneratedData input but no producer in workflow")

        with patch.object(AiidaAdapter, "build_label_from_graph_item", return_value="output"):
            mapping = build_dependency_mapping(task, workflow, {})

        print("\n=== Dependency mapping result (should be empty) ===")
        pprint(
            {
                "port_mapping": mapping.port_mapping,
                "parent_folders": mapping.task_folders,
                "job_ids": mapping.task_job_ids,
            }
        )

        # Should skip this input (continue)
        assert mapping.port_mapping == {}
        assert mapping.task_folders == {}
        assert mapping.task_job_ids == {}

    def test_build_mapping_producer_not_in_outputs(self):
        """Test building mapping when producer exists but not in task_output_mapping."""
        # Create real GeneratedData
        output_data = create_generated_data(
            name="output",
            path=None,
        )

        # Create producer task
        producer = Mock(spec=core.Task)
        producer.name = "producer"
        producer.coordinates = {}
        producer.output_data_items.return_value = [("output_port", output_data)]

        # Create consumer task
        consumer = Mock(spec=core.Task)
        consumer.input_data_items.return_value = [("input_port", output_data)]

        # Create workflow with producer
        workflow = Mock(spec=core.Workflow)
        workflow.tasks = [producer]

        print("\n=== Test build_mapping_producer_not_in_outputs ===")
        print("Producer task exists but not in task_output_mapping (not completed)")

        with patch.object(
            AiidaAdapter,
            "build_label_from_graph_item",
            side_effect=["producer", "output", "output", "producer"],  # Added extra "producer" for generator check
        ):
            # Empty task_output_mapping - producer hasn't completed yet
            mapping = build_dependency_mapping(consumer, workflow, {})

        print("\n=== Dependency mapping result (should be empty) ===")
        pprint(
            {
                "port_mapping": mapping.port_mapping,
                "parent_folders": mapping.task_folders,
                "job_ids": mapping.task_job_ids,
            }
        )

        # Should skip this dependency
        assert "input_port" not in mapping.port_mapping
        assert mapping.task_folders == {}
        assert mapping.task_job_ids == {}


def test_resolve_icon_restart_file_single_model(aiida_localhost, tmp_path):
    """Test resolving restart file for single model."""

    # Create a real namelist file
    nml_file = tmp_path / "model_atm.nml"
    nml_file.write_text("&run_nml\n  restart_filename = 'restart_atm.nc'\n/")

    # Create and store real SinglefileData node
    nml_node = aiida.orm.SinglefileData(file=str(nml_file))
    nml_node.store()

    # Create RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/dir")
    workdir_remote.store()

    print("\n=== Test resolve_icon_restart_file_single_model ===")
    print("Workdir path: /work/dir")
    print("Model name: atm")
    print(f"Namelist PK: {nml_node.pk}")

    # Patch aiida-icon's utility function to avoid complex namelist parsing dependencies.
    # This function reads ICON namelists to find restart file names, which would require
    # setting up complex ICON namelist structures. We mock it to focus on testing
    # our dependency resolution logic rather than aiida-icon's internal namelist parsing.
    with patch(
        "aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name",
        return_value="restart_atm_latest.nc",
    ):
        result = _resolve_icon_restart_file(
            workdir_path="/work/dir",
            model_name="atm",
            model_namelist_pks={"atm": nml_node.pk},
            workdir_remote_data=workdir_remote,
        )

    print(f"Result type: {type(result).__name__}")
    print(f"Result remote path: {result.get_remote_path()}")

    # Should create RemoteData pointing to restart file
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result.get_remote_path()


def test_resolve_icon_restart_file_coupled_models(aiida_localhost, tmp_path):
    """Test resolving restart file for coupled simulation with multiple models."""

    # Create real namelist files for multiple models
    nml_atm = tmp_path / "model_atm.nml"
    nml_atm.write_text("&run_nml\n  model = 'atm'\n/")

    nml_oce = tmp_path / "model_oce.nml"
    nml_oce.write_text("&run_nml\n  model = 'oce'\n/")

    # Create and store real nodes
    nml_node_atm = aiida.orm.SinglefileData(file=str(nml_atm))
    nml_node_atm.store()

    nml_node_oce = aiida.orm.SinglefileData(file=str(nml_oce))
    nml_node_oce.store()

    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/dir")
    workdir_remote.store()

    print("\n=== Test resolve_icon_restart_file_coupled_models ===")
    print("Workdir path: /work/dir")
    print("Model name: oce")
    print(f"Model namelist PKs: {{'atm': {nml_node_atm.pk}, 'oce': {nml_node_oce.pk}}}")

    # Patch aiida-icon's utility function to avoid complex namelist parsing dependencies.
    # For coupled models, the function would need to parse multiple model namelists
    # to determine the correct restart file. We mock it to test our logic for
    # handling multiple model namelists without requiring full ICON namelist setup.
    with patch(
        "aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name",
        return_value="restart_oce_latest.nc",
    ):
        result = _resolve_icon_restart_file(
            workdir_path="/work/dir",
            model_name="oce",
            model_namelist_pks={"atm": nml_node_atm.pk, "oce": nml_node_oce.pk},
            workdir_remote_data=workdir_remote,
        )

    print(f"Result type: {type(result).__name__}")
    print(f"Result remote path: {result.get_remote_path()}")

    # Should create RemoteData with combined namelist info
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_oce_latest.nc" in result.get_remote_path()


def test_resolve_icon_dependency_with_filename(aiida_localhost):
    """Test resolving ICON dependency when filename is known."""

    # Create dependency with known filename
    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename="output.nc")

    # Create real RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    print("\n=== Test resolve_icon_dependency_with_filename ===")
    print(f"Dependency info: dep_label={dep_info.dep_label}, filename={dep_info.filename}")
    print(f"Workdir remote path: {workdir_remote.get_remote_path()}")

    result = _resolve_icon_dependency(dep_info, workdir_remote, model_name="atm", model_namelist_pks={})

    print(f"Result type: {type(result).__name__}")
    print(f"Result remote path: {result.get_remote_path()}")

    # Should create RemoteData pointing to specific file
    assert isinstance(result, aiida.orm.RemoteData)
    assert "output.nc" in result.get_remote_path()


# Patch: Mock aiida-icon namelist utility to avoid complex ICON-specific dependencies
@patch("aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name")
def test_resolve_icon_dependency_via_namelist(mock_read_restart, aiida_localhost, tmp_path):
    """Test resolving ICON dependency via namelist when no filename is given."""

    # Create dependency without filename (requires namelist resolution)
    dep_info = DependencyInfo(dep_label="task_a", data_label="restart", filename=None)

    # Create real RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    # Create real namelist node
    nml_file = tmp_path / "model_atm.nml"
    nml_file.write_text("&run_nml\n  restart_filename = 'restart_atm.nc'\n/")
    nml_node = aiida.orm.SinglefileData(file=str(nml_file))
    nml_node.store()

    # Mock the aiida-icon utility to return restart file name
    mock_read_restart.return_value = "restart_atm_latest.nc"

    print("\n=== Resolve ICON dependency via namelist ===")
    print(
        f"Dependency info: dep_label={dep_info.dep_label!r}, data_label={dep_info.data_label!r}, filename={dep_info.filename}"
    )
    print(f"Workdir remote path: {workdir_remote.get_remote_path()}")
    print(f"Namelist file content: {nml_file.read_text()!r}")
    print("Model name: 'atm'")
    print(f"Mock read_restart return value: {mock_read_restart.return_value!r}")

    result = _resolve_icon_dependency(
        dep_info,
        workdir_remote,
        model_name="atm",
        model_namelist_pks={"atm": nml_node.pk},
    )

    print(f"Result type: {type(result)}")
    print(f"Result remote path: {result.get_remote_path()}")
    print(f"Mock was called: {mock_read_restart.called}")

    # Should call the aiida-icon function and return RemoteData with restart file
    assert mock_read_restart.called
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result.get_remote_path()


def test_resolve_icon_dependency_fallback(aiida_localhost):
    """Test resolving ICON dependency fallback when no filename and no namelists."""

    # Create dependency without filename and no model_namelist_pks
    dep_info = DependencyInfo(dep_label="task_a", data_label="restart", filename=None)

    # Create real RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    print("\n=== Test resolve_icon_dependency_fallback ===")
    print("Dependency info: filename=None, no model_namelist_pks")
    print("Expected: return workdir_remote as fallback")

    # Call with empty model_namelist_pks - should return workdir_remote as fallback
    result = _resolve_icon_dependency(dep_info, workdir_remote, model_name="atm", model_namelist_pks={})

    print(f"Result is same as workdir_remote: {result == workdir_remote}")

    # Should return workdir_remote itself as fallback
    assert result == workdir_remote


# Patch: Mock aiida-icon namelist utility to avoid complex multi-model parsing dependencies
@patch("aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name")
def test_resolve_icon_dependency_mapping_with_models(mock_read_restart, aiida_localhost, tmp_path):
    """Test resolve_icon_dependency_mapping with multiple models."""

    # Create parent folder RemoteData
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    # Create parent_folders mapping
    parent_folders = {"task_a": Mock(value=workdir_remote.pk)}

    # Create port dependency mapping (restart file without filename)
    port_dependency_mapping = {
        "restart_file": [DependencyInfo(dep_label="task_a", data_label="restart", filename=None)]
    }

    # Create master namelist with multiple models
    master_nml_file = tmp_path / "icon_master.namelist"
    master_nml_content = textwrap.dedent("""\
        &master_nml
         lrestart = .true.
        /
        &master_model_nml
         model_name="atm"
         model_namelist_filename="atm.namelist"
         model_type=1
        /
        &master_model_nml
         model_name="oce"
         model_namelist_filename="oce.namelist"
         model_type=1
        /
        """)
    master_nml_file.write_text(master_nml_content)
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    # Create model namelists
    atm_nml_file = tmp_path / "atm.namelist"
    atm_nml_file.write_text("&run_nml\n num_lev = 90\n/")
    atm_nml = aiida.orm.SinglefileData(file=str(atm_nml_file))
    atm_nml.store()

    oce_nml_file = tmp_path / "oce.namelist"
    oce_nml_file.write_text("&run_nml\n num_lev = 40\n/")
    oce_nml = aiida.orm.SinglefileData(file=str(oce_nml_file))
    oce_nml.store()

    model_namelist_pks = {"atm": atm_nml.pk, "oce": oce_nml.pk}

    print("\n=== Test resolve_icon_dependency_mapping_with_models ===")
    print("Models: atm, oce")
    print("Port dependency mapping: restart_file (no filename)")
    print("Expected: model-specific port mappings (restart_file.atm, restart_file.oce)")

    # Mock restart file resolution - return different names for different models
    mock_read_restart.side_effect = lambda model_name, **_kwargs: f"restart_{model_name}_latest.nc"

    # Call resolve_icon_dependency_mapping
    result = resolve_icon_dependency_mapping(parent_folders, port_dependency_mapping, master_nml.pk, model_namelist_pks)

    print("\n=== Result port mappings ===")
    pprint(list(result.keys()))
    for key, value in result.items():
        if hasattr(value, "get_remote_path"):
            print(f"{key}: {value.get_remote_path()}")

    # Should create model-specific port mappings
    assert "restart_file.atm" in result
    assert "restart_file.oce" in result
    assert isinstance(result["restart_file.atm"], aiida.orm.RemoteData)
    assert isinstance(result["restart_file.oce"], aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result["restart_file.atm"].get_remote_path()
    assert "restart_oce_latest.nc" in result["restart_file.oce"].get_remote_path()


@pytest.mark.usefixtures("aiida_localhost")
def test_resolve_icon_dependency_mapping_no_parent_folders(tmp_path):
    """Test resolve_icon_dependency_mapping with no parent folders."""

    # Create minimal namelists
    master_nml_file = tmp_path / "icon_master.namelist"
    master_nml_file.write_text("&master_nml\n lrestart = .false.\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    print("\n=== Test resolve_icon_dependency_mapping_no_parent_folders ===")
    print("Parent folders: None")
    print("Expected: empty dict")

    # Call with None parent_folders
    result = resolve_icon_dependency_mapping(None, {}, master_nml.pk, {})

    print(f"Result: {result}")

    # Should return empty dict
    assert result == {}


@pytest.mark.usefixtures("aiida_localhost")
def test_resolve_shell_dependency_mappings_type_error(tmp_path):
    """Test that non-RemoteData parent folders raise TypeError."""

    # Create real SinglefileData (wrong type - should be RemoteData)
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    single_file = aiida.orm.SinglefileData(file=str(test_file))
    single_file.store()

    # Use Mock for the value wrapper to match the expected structure
    parent_folders = {"task_a": Mock(value=single_file.pk)}
    port_dependency_mapping = {}
    original_filenames = {}

    print("\n=== Test resolve_shell_dependency_mappings_type_error ===")
    print(f"Parent folder PK: {single_file.pk} (SinglefileData, should be RemoteData)")
    print("Expected: TypeError with 'Expected RemoteData'")

    with pytest.raises(TypeError, match="Expected RemoteData"):
        resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)


def test_resolve_shell_dependency_mappings_missing_parent(aiida_localhost):
    """Test resolve_shell_dependency_mappings with missing parent folder."""

    # Create parent folder for task_a
    workdir_a = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_a.store()

    parent_folders = {"task_a": Mock(value=workdir_a.pk)}

    # Port dependency references task_b which is NOT in parent_folders
    port_dependency_mapping = {
        "input_port": [
            DependencyInfo(dep_label="task_a", data_label="output_a", filename="output.txt"),
            DependencyInfo(dep_label="task_b", data_label="output_b", filename="result.txt"),
        ]
    }

    original_filenames = {"output_a": "output.txt", "output_b": "result.txt"}

    print("\n=== Test resolve_shell_dependency_mappings_missing_parent ===")
    print("Dependencies: task_a (exists), task_b (missing)")
    print("Expected: process task_a, skip task_b")

    # Call function - should skip task_b
    mappings = resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)

    print("\n=== Result ===")
    print(f"All nodes keys: {list(mappings.nodes.keys())}")
    print(f"Placeholder mapping: {mappings.placeholders}")

    # Should only process task_a, skip task_b
    assert "task_a_output_a_remote" in mappings.nodes  # Updated to include data_label
    assert "output_a" in mappings.placeholders
    assert mappings.placeholders["output_a"] == "task_a_output_a_remote"  # Updated to include data_label

    # task_b should be skipped
    assert "output_b" not in mappings.placeholders


def test_resolve_shell_dependency_mappings_with_filenames(aiida_localhost):
    """Test resolve_shell_dependency_mappings builds filename mapping."""

    # Create parent folder
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    parent_folders = {"task_a": Mock(value=workdir.pk)}

    # Port dependency with filename
    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename="result.txt")
    port_dependency_mapping = {"input_port": [dep_info]}

    # Original filenames mapping
    original_filenames = {"output": "custom_name.txt"}

    print("\n=== Test resolve_shell_dependency_mappings_with_filenames ===")
    print(f"Dependency: {dep_info.dep_label}/{dep_info.data_label} (filename={dep_info.filename})")
    print(f"Original filename mapping: {original_filenames}")
    print("Expected: task_a_remote should map to custom_name.txt (from original mapping)")

    # Call function
    mappings = resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)

    print("\n=== Result filenames ===")
    pprint(mappings.filenames)

    # Should build filename mapping
    assert "task_a_output_remote" in mappings.filenames  # Updated to include data_label
    assert mappings.filenames["task_a_output_remote"] == "custom_name.txt"

    # Should create RemoteData pointing to specific file
    assert "task_a_output_remote" in mappings.nodes
    remote_data = mappings.nodes["task_a_output_remote"]
    assert isinstance(remote_data, aiida.orm.RemoteData)
    assert "result.txt" in remote_data.get_remote_path()
    assert remote_data.is_stored


def test_resolve_shell_dependency_mappings_workdir_fallback(aiida_localhost):
    """Test resolve_shell_dependency_mappings uses workdir when no filename specified."""

    # Create parent folder
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    parent_folders = {"task_a": Mock(value=workdir.pk)}

    # Port dependency WITHOUT filename (should use workdir as fallback)
    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename=None)
    port_dependency_mapping = {"input_port": [dep_info]}

    original_filenames = {}

    print("\n=== Test resolve_shell_dependency_mappings_workdir_fallback ===")
    print(f"Dependency: {dep_info.dep_label}/{dep_info.data_label} (filename=None)")
    print("Expected: Should use workdir itself as RemoteData (no new node created)")

    # Call function
    mappings = resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)

    print("\n=== Result ===")
    print(f"All nodes keys: {list(mappings.nodes.keys())}")
    print(f"Remote data is workdir: {mappings.nodes.get('task_a_output_remote') == workdir}")

    # Should use workdir itself (not create new RemoteData)
    assert "task_a_output_remote" in mappings.nodes
    assert mappings.nodes["task_a_output_remote"] == workdir

    # Should have placeholder mapping
    assert "output" in mappings.placeholders
    assert mappings.placeholders["output"] == "task_a_output_remote"

    # No filename mapping since filename was None
    assert "task_a_output_remote" not in mappings.filenames


def test_resolve_shell_dependency_mappings_multiple_outputs_from_same_task(aiida_localhost):
    """Regression test: Multiple outputs from same task should not overwrite each other.

    This tests the bug fix where multiple outputs from the same producer task
    (e.g., ICON output streams: atm_2d, atm_3d_pl) were being mapped to the same
    unique_key, causing one to overwrite the other.

    The fix ensures unique_key includes the data_label to distinguish outputs.
    """

    # Create parent folder for the producer task
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/icon_task")
    workdir.store()

    parent_folders = {"icon_task": Mock(value=workdir.pk)}

    # Two outputs from the same producer task (like ICON output streams)
    port_dependency_mapping = {
        "input_port": [
            DependencyInfo(dep_label="icon_task", data_label="atm_2d", filename="atm_2d"),
            DependencyInfo(dep_label="icon_task", data_label="atm_3d_pl", filename="atm_3d_pl"),
        ]
    }

    original_filenames = {"atm_2d": "atm_2d", "atm_3d_pl": "atm_3d_pl"}

    print("\n=== Regression test: Multiple outputs from same task ===")
    print("Producer: icon_task")
    print("Outputs: atm_2d, atm_3d_pl (both from same task)")
    print("Expected: Both should be preserved with unique keys")

    # Call function
    mappings = resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)

    print("\n=== Result ===")
    print(f"All nodes keys: {list(mappings.nodes.keys())}")
    print(f"Placeholder mapping: {mappings.placeholders}")
    print(f"Filenames: {mappings.filenames}")

    # CRITICAL: Both outputs should be present with distinct keys
    assert "icon_task_atm_2d_remote" in mappings.nodes
    assert "icon_task_atm_3d_pl_remote" in mappings.nodes

    # Both should have placeholder mappings
    assert "atm_2d" in mappings.placeholders
    assert "atm_3d_pl" in mappings.placeholders

    # Each placeholder should map to its unique key
    assert mappings.placeholders["atm_2d"] == "icon_task_atm_2d_remote"
    assert mappings.placeholders["atm_3d_pl"] == "icon_task_atm_3d_pl_remote"

    # Both should have different RemoteData nodes with different paths
    atm_2d_node = mappings.nodes["icon_task_atm_2d_remote"]
    atm_3d_pl_node = mappings.nodes["icon_task_atm_3d_pl_remote"]
    assert atm_2d_node != atm_3d_pl_node
    assert "atm_2d" in atm_2d_node.get_remote_path()
    assert "atm_3d_pl" in atm_3d_pl_node.get_remote_path()

    # Verify we have exactly 2 nodes (not 1 due to overwriting)
    assert len(mappings.nodes) == 2

    print("\n✓ Both outputs preserved with unique keys (bug fixed!)")


def test_resolve_icon_output_stream_paths():
    """Regression test: Extract output stream paths from ICON namelist.

    This tests the _resolve_icon_output_stream_paths() function which extracts
    output stream directory names from ICON namelists using aiida-icon's
    read_output_stream_infos() utility.

    This is needed when output streams are defined with empty config (atm_2d: {})
    and the actual directory names come from the ICON namelist.
    """

    # Create mock ICON task with output streams
    icon_task = Mock(spec=core.IconTask)

    # Create output data objects
    atm_2d_output = create_generated_data(name="atm_2d", path=None)
    atm_3d_pl_output = create_generated_data(name="atm_3d_pl", path=None)

    icon_task.outputs = {"output_streams": [atm_2d_output, atm_3d_pl_output]}

    # Create a realistic ICON namelist with output_nml sections
    # Note: filename_format includes the directory path (e.g., ./atm_2d/...)
    namelist_content = textwrap.dedent("""
        &output_nml
         output_filename  = 'exclaim_ape_R02B04'
         filename_format  = './atm_2d/<output_filename>_atm_2d_<datetime2>'
         output_grid      = .true.
        /
        &output_nml
         output_filename  = 'exclaim_ape_R02B04'
         filename_format  = './atm_3d_pl/<output_filename>_atm_3d_pl_<datetime2>'
         output_grid      = .false.
        /
    """)

    nml_data = f90nml.reads(namelist_content)

    # Mock the model_namelists dict
    icon_task.model_namelists = {"atm": Mock(namelist=nml_data)}

    print("\n=== Regression test: Extract ICON output stream paths ===")
    print("Output streams in task: atm_2d, atm_3d_pl")
    print("Namelist defines: atm_2d, atm_3d_pl directories")

    # Call the function
    stream_paths = _resolve_icon_output_stream_paths(icon_task)

    print("\n=== Result ===")
    print(f"Stream paths: {stream_paths}")

    # Should extract both output stream paths
    assert "atm_2d" in stream_paths
    assert "atm_3d_pl" in stream_paths

    # Path values should match the directory names (derived from filename_format)
    assert stream_paths["atm_2d"] == "atm_2d"
    assert stream_paths["atm_3d_pl"] == "atm_3d_pl"

    print("\n✓ Output stream paths correctly extracted from namelist!")
