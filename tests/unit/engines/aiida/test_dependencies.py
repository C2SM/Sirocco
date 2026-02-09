"""Unit tests for sirocco.engines.aiida.dependencies module."""

import textwrap
from unittest.mock import Mock, patch

import aiida.orm
import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.adapter import AiidaAdapter
from sirocco.engines.aiida.dependencies import (
    _get_icon_output_stream_paths,
    _resolve_icon_dependency,
    _resolve_remote_data_for_dependency,
    add_slurm_dependencies_to_metadata,
    build_dependency_mapping,
    build_icon_calcjob_inputs,
    build_slurm_dependency_directive,
    collect_available_data_inputs,
    resolve_icon_dependency_mapping,
    resolve_icon_restart_file,
    resolve_shell_dependency_mappings,
)
from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaMetadataOptions,
    DependencyInfo,
)
from tests.unit.utils import create_available_data, create_generated_data


class TestBuildSlurmDependencyDirective:
    """Test SLURM dependency directive construction."""

    def test_single_job_id(self):
        """Test directive with single job ID."""
        job_ids = {"task1": Mock(value=12345)}

        directive = build_slurm_dependency_directive(job_ids)

        print("\n=== SLURM dependency directive (single job) ===")
        print(directive)

        assert directive == "#SBATCH --dependency=afterok:12345 --kill-on-invalid-dep=yes"

    def test_multiple_job_ids(self):
        """Test directive with multiple job IDs."""
        job_ids = {
            "task1": Mock(value=12345),
            "task2": Mock(value=67890),
            "task3": Mock(value=11111),
        }

        directive = build_slurm_dependency_directive(job_ids)

        print("\n=== SLURM dependency directive (multiple jobs) ===")
        print(directive)

        # Dict maintains insertion order in Python 3.7+, so order is deterministic
        assert directive == "#SBATCH --dependency=afterok:12345:67890:11111 --kill-on-invalid-dep=yes"


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

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="input1"):
            aiida_nodes = {"input1": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

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

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["input1", "input2"]):
            aiida_nodes = {"input1": Mock(), "input2": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

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

        result = collect_available_data_inputs(task, {})

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
            "build_graph_item_label",
            side_effect=["available", "generated"],
        ):
            aiida_nodes = {"available": Mock(), "generated": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

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

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="input"):
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
            "build_graph_item_label",
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

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="output"):
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
            "build_graph_item_label",
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
        result = resolve_icon_restart_file(
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
        result = resolve_icon_restart_file(
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


def test_build_icon_calcjob_inputs_no_wrapper(aiida_localhost, tmp_path):
    """Test building ICON CalcJob inputs without wrapper script."""

    # Create real code
    code = aiida.orm.InstalledCode(computer=aiida_localhost, filepath_executable="/bin/bash")
    code.label = "test_code"
    code.store()

    # Create real namelist files
    master_nml_file = tmp_path / "master.nml"
    master_nml_file.write_text("&master_nml\n  nproma = 8\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    model_nml_file = tmp_path / "model_atm.nml"
    model_nml_file.write_text("&run_nml\n  model = 'atm'\n/")
    model_nml = aiida.orm.SinglefileData(file=str(model_nml_file))
    model_nml.store()

    # Create metadata and task spec without wrapper
    metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())
    task_spec = AiidaIconTaskSpec(
        label="test_icon_task",
        code_pk=code.pk,
        master_namelist_pk=master_nml.pk,
        model_namelist_pks={"atm": model_nml.pk},
        wrapper_script_pk=None,
        metadata=metadata,
        output_port_mapping={},
    )

    input_data_nodes = {}

    print("\n=== Test build_icon_calcjob_inputs_no_wrapper ===")
    print(f"Task spec label: {task_spec.label}")
    print(f"Wrapper script PK: {task_spec.wrapper_script_pk}")
    print("Expected: inputs should have code, namelists, but no wrapper_script")

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    print("\n=== Built inputs ===")
    print(f"Input keys: {list(inputs.keys())}")
    print(f"Has wrapper_script: {'wrapper_script' in inputs}")

    # Should have code and namelists but no wrapper_script
    assert "code" in inputs
    assert inputs["code"].pk == code.pk
    assert "master_namelist" in inputs
    assert inputs["master_namelist"].pk == master_nml.pk
    assert "models" in inputs
    assert "atm" in inputs["models"]
    assert inputs["models"]["atm"].pk == model_nml.pk
    assert "wrapper_script" not in inputs
    assert "metadata" in inputs


def test_build_icon_calcjob_inputs_with_wrapper(aiida_localhost, tmp_path):
    """Test building ICON CalcJob inputs with wrapper script."""

    # Create real code
    code = aiida.orm.InstalledCode(computer=aiida_localhost, filepath_executable="/bin/bash")
    code.label = "test_code"
    code.store()

    # Create real namelist files
    master_nml_file = tmp_path / "master.nml"
    master_nml_file.write_text("&master_nml\n  nproma = 8\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    # Create wrapper script
    wrapper_file = tmp_path / "wrapper.sh"
    wrapper_file.write_text("#!/bin/bash\necho 'wrapper'")
    wrapper_script = aiida.orm.SinglefileData(file=str(wrapper_file))
    wrapper_script.store()

    # Create metadata and task spec with wrapper
    metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())
    task_spec = AiidaIconTaskSpec(
        label="test_icon_task_wrapper",
        code_pk=code.pk,
        master_namelist_pk=master_nml.pk,
        model_namelist_pks={},
        wrapper_script_pk=wrapper_script.pk,
        metadata=metadata,
        output_port_mapping={},
    )

    input_data_nodes = {}

    print("\n=== Test build_icon_calcjob_inputs_with_wrapper ===")
    print(f"Task spec label: {task_spec.label}")
    print(f"Wrapper script PK: {task_spec.wrapper_script_pk}")
    print("Expected: inputs should include wrapper_script")

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    print("\n=== Built inputs ===")
    print(f"Input keys: {list(inputs.keys())}")
    print(f"Has wrapper_script: {'wrapper_script' in inputs}")

    # Should have wrapper_script
    assert "wrapper_script" in inputs
    assert inputs["wrapper_script"].pk == wrapper_script.pk


# Patch: Mock aiida-icon IconCalculation to test port wrapping logic without full ICON dependencies
@patch("aiida_icon.calculations.IconCalculation")
def test_build_icon_calcjob_inputs_namespace_port(mock_icon_calc, aiida_localhost, tmp_path):
    """Test that namespace ports are wrapped in dict."""
    from unittest.mock import Mock

    from aiida.engine.processes.ports import PortNamespace

    # Create real code and namelist
    code = aiida.orm.InstalledCode(computer=aiida_localhost, filepath_executable="/bin/bash")
    code.label = "test_code"
    code.store()

    master_nml_file = tmp_path / "master.nml"
    master_nml_file.write_text("&master_nml\n  nproma = 8\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    # Create metadata and task spec
    metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())
    task_spec = AiidaIconTaskSpec(
        label="test_namespace",
        code_pk=code.pk,
        master_namelist_pk=master_nml.pk,
        model_namelist_pks={},
        metadata=metadata,
        output_port_mapping={},
    )

    # Create real RemoteData node that should be wrapped in namespace
    remote_data = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/data")
    remote_data.label = "test_label"
    remote_data.store()
    input_data_nodes = {"input_files": remote_data}

    # Mock IconCalculation spec to have a namespace port
    mock_spec = Mock()
    mock_namespace_port = Mock(spec=PortNamespace)
    mock_spec.inputs = {"input_files": mock_namespace_port}
    mock_icon_calc.spec.return_value = mock_spec

    print("\n=== Test build_icon_calcjob_inputs_namespace_port ===")
    print("Input data nodes: input_files -> RemoteData")
    print("Port type: PortNamespace")
    print("Expected: input should be wrapped in dict with label as key")

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    print("\n=== Built inputs ===")
    print(f"input_files type: {type(inputs.get('input_files'))}")
    if isinstance(inputs.get("input_files"), dict):
        print(f"input_files keys: {list(inputs['input_files'].keys())}")

    # Should wrap namespace port in dict
    assert "input_files" in inputs
    assert isinstance(inputs["input_files"], dict)
    assert "test_label" in inputs["input_files"]
    assert inputs["input_files"]["test_label"] == remote_data


# Patch: Mock aiida-icon IconCalculation to test non-namespace port logic without full ICON dependencies
@patch("aiida_icon.calculations.IconCalculation")
def test_build_icon_calcjob_inputs_non_namespace_port(mock_icon_calc, aiida_localhost, tmp_path):
    """Test that non-namespace ports are added directly."""
    from unittest.mock import Mock

    # Create real code and namelist
    code = aiida.orm.InstalledCode(computer=aiida_localhost, filepath_executable="/bin/bash")
    code.label = "test_code"
    code.store()

    master_nml_file = tmp_path / "master.nml"
    master_nml_file.write_text("&master_nml\n  nproma = 8\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    # Create metadata and task spec
    metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())
    task_spec = AiidaIconTaskSpec(
        label="test_non_namespace",
        code_pk=code.pk,
        master_namelist_pk=master_nml.pk,
        model_namelist_pks={},
        metadata=metadata,
        output_port_mapping={},
    )

    # Create real RemoteData node that should NOT be wrapped
    remote_data = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/data")
    remote_data.label = "test_label"
    remote_data.store()
    input_data_nodes = {"restart_file": remote_data}

    # Mock IconCalculation spec to have a regular (non-namespace) port
    # Don't use Port spec - just use a Mock that's not a PortNamespace
    mock_spec = Mock()
    mock_regular_port = Mock()
    # Ensure it's NOT a PortNamespace by not setting spec
    mock_spec.inputs = {"restart_file": mock_regular_port}
    mock_icon_calc.spec.return_value = mock_spec

    print("\n=== Test build_icon_calcjob_inputs_non_namespace_port ===")
    print("Input data nodes: restart_file -> RemoteData")
    print("Port type: regular (non-namespace)")
    print("Expected: input should be added directly without wrapping")

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    print("\n=== Built inputs ===")
    print(f"restart_file type: {type(inputs.get('restart_file'))}")
    print(f"restart_file is dict: {isinstance(inputs.get('restart_file'), dict)}")

    # Should add port directly without wrapping
    assert "restart_file" in inputs
    assert inputs["restart_file"] == remote_data
    assert not isinstance(inputs["restart_file"], dict)


def test_resolve_remote_data_for_dependency_with_filename(aiida_localhost):
    """Test creating RemoteData for dependency with specific filename."""

    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename="result.txt")

    # Create real RemoteData for workdir
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    print("\n=== Test resolve_remote_data_for_dependency_with_filename ===")
    print(f"Dependency: {dep_info.dep_label}, filename={dep_info.filename}")
    print(f"Workdir path: {workdir.get_remote_path()}")

    key, result = _resolve_remote_data_for_dependency(dep_info, workdir)

    print(f"Result key: {key}")
    print(f"Result remote path: {result.get_remote_path()}")

    # Should create RemoteData with specific file path
    assert key == "task_a_output_remote"  # Updated to include data_label
    assert isinstance(result, aiida.orm.RemoteData)
    assert "result.txt" in result.get_remote_path()
    assert result.is_stored


def test_resolve_remote_data_for_dependency_workdir_fallback(aiida_localhost):
    """Test using workdir when no filename specified."""

    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename=None)

    # Create real RemoteData for workdir
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    print("\n=== Test resolve_remote_data_for_dependency_workdir_fallback ===")
    print(f"Dependency: {dep_info.dep_label}, filename={dep_info.filename}")
    print("Expected: return workdir itself as fallback")

    key, result = _resolve_remote_data_for_dependency(dep_info, workdir)

    print(f"Result key: {key}")
    print(f"Result is workdir: {result == workdir}")

    # Should return the workdir itself
    assert key == "task_a_output_remote"  # Updated to include data_label
    assert result == workdir


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
    all_nodes, placeholder_mapping, _filenames = resolve_shell_dependency_mappings(
        parent_folders, port_dependency_mapping, original_filenames
    )

    print("\n=== Result ===")
    print(f"All nodes keys: {list(all_nodes.keys())}")
    print(f"Placeholder mapping: {placeholder_mapping}")

    # Should only process task_a, skip task_b
    assert "task_a_output_a_remote" in all_nodes  # Updated to include data_label
    assert "output_a" in placeholder_mapping
    assert placeholder_mapping["output_a"] == "task_a_output_a_remote"  # Updated to include data_label

    # task_b should be skipped
    assert "output_b" not in placeholder_mapping


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
    _all_nodes, _placeholder_mapping, filenames = resolve_shell_dependency_mappings(
        parent_folders, port_dependency_mapping, original_filenames
    )

    print("\n=== Result filenames ===")
    pprint(filenames)

    # Should build filename mapping
    assert "task_a_output_remote" in filenames  # Updated to include data_label
    assert filenames["task_a_output_remote"] == "custom_name.txt"


@pytest.mark.parametrize(
    (
        "has_job_ids",
        "label",
        "expected_has_commands",
        "expected_job_ids_in_commands",
        "expected_call_link_label",
    ),
    [
        pytest.param(
            False,
            None,
            False,
            [],
            None,
            id="no_job_ids",
        ),
        pytest.param(
            True,
            None,
            True,
            ["12345", "67890"],
            None,
            id="with_job_ids",
        ),
        pytest.param(
            False,
            "icon_task",
            False,
            [],
            "icon_task",
            id="with_call_link_label",
        ),
    ],
)
def test_add_slurm_dependencies_to_metadata(
    has_job_ids,
    label,
    expected_has_commands,
    expected_job_ids_in_commands,
    expected_call_link_label,
    aiida_localhost,
):
    """Test adding SLURM dependencies to metadata.

    Covers:
    - No job_ids provided (custom_scheduler_commands should be None)
    - With job_ids (should add --dependency=afterok directive)
    - With call_link_label for ICON tasks
    """

    # Create base metadata
    base_metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())

    # Setup job_ids based on test case
    job_ids = None
    if has_job_ids:
        job_ids = {"task_a": Mock(value=12345), "task_b": Mock(value=67890)}

    print("\n=== Test add_slurm_dependencies_to_metadata ===")
    print(f"has_job_ids: {has_job_ids}")
    print(f"label: {label}")
    print(f"expected_has_commands: {expected_has_commands}")

    # Call the function
    result = add_slurm_dependencies_to_metadata(base_metadata, job_ids=job_ids, computer=aiida_localhost, label=label)

    print("\n=== Result ===")
    print(f"custom_scheduler_commands: {result.options.custom_scheduler_commands}")
    print(f"call_link_label: {getattr(result, 'call_link_label', None)}")

    # Verify custom_scheduler_commands
    if expected_has_commands:
        assert result.options.custom_scheduler_commands is not None
        assert "--dependency=afterok:" in result.options.custom_scheduler_commands
        for job_id in expected_job_ids_in_commands:
            assert job_id in result.options.custom_scheduler_commands
    else:
        assert result.options.custom_scheduler_commands is None

    # Verify call_link_label
    if expected_call_link_label:
        assert result.call_link_label == expected_call_link_label
    else:
        # call_link_label might be None or not set
        assert getattr(result, "call_link_label", None) != "icon_task" or expected_call_link_label is None


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
    all_nodes, placeholder_mapping, filenames = resolve_shell_dependency_mappings(
        parent_folders, port_dependency_mapping, original_filenames
    )

    print("\n=== Result ===")
    print(f"All nodes keys: {list(all_nodes.keys())}")
    print(f"Placeholder mapping: {placeholder_mapping}")
    print(f"Filenames: {filenames}")

    # CRITICAL: Both outputs should be present with distinct keys
    assert "icon_task_atm_2d_remote" in all_nodes
    assert "icon_task_atm_3d_pl_remote" in all_nodes

    # Both should have placeholder mappings
    assert "atm_2d" in placeholder_mapping
    assert "atm_3d_pl" in placeholder_mapping

    # Each placeholder should map to its unique key
    assert placeholder_mapping["atm_2d"] == "icon_task_atm_2d_remote"
    assert placeholder_mapping["atm_3d_pl"] == "icon_task_atm_3d_pl_remote"

    # Both should have different RemoteData nodes with different paths
    atm_2d_node = all_nodes["icon_task_atm_2d_remote"]
    atm_3d_pl_node = all_nodes["icon_task_atm_3d_pl_remote"]
    assert atm_2d_node != atm_3d_pl_node
    assert "atm_2d" in atm_2d_node.get_remote_path()
    assert "atm_3d_pl" in atm_3d_pl_node.get_remote_path()

    # Verify we have exactly 2 nodes (not 1 due to overwriting)
    assert len(all_nodes) == 2

    print("\n✓ Both outputs preserved with unique keys (bug fixed!)")


def test_get_icon_output_stream_paths():
    """Regression test: Extract output stream paths from ICON namelist.

    This tests the _get_icon_output_stream_paths() function which extracts
    output stream directory names from ICON namelists using aiida-icon's
    read_output_stream_infos() utility.

    This is needed when output streams are defined with empty config (atm_2d: {})
    and the actual directory names come from the ICON namelist.
    """
    import f90nml

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
    stream_paths = _get_icon_output_stream_paths(icon_task)

    print("\n=== Result ===")
    print(f"Stream paths: {stream_paths}")

    # Should extract both output stream paths
    assert "atm_2d" in stream_paths
    assert "atm_3d_pl" in stream_paths

    # Path values should match the directory names (derived from filename_format)
    assert stream_paths["atm_2d"] == "atm_2d"
    assert stream_paths["atm_3d_pl"] == "atm_3d_pl"

    print("\n✓ Output stream paths correctly extracted from namelist!")
