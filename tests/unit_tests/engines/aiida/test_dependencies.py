"""Unit tests for sirocco.engines.aiida.dependencies module."""

from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.dependencies import (
    build_dependency_mapping,
    build_slurm_dependency_directive,
    collect_available_data_inputs,
)
from tests.utils import create_available_data, create_generated_data


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

        assert directive.startswith("#SBATCH --dependency=afterok:")
        assert "12345" in directive
        assert "67890" in directive
        assert "11111" in directive
        assert "--kill-on-invalid-dep=yes" in directive


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
        from sirocco.engines.aiida.adapter import AiidaAdapter

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

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["input1", "input2"]):
            aiida_nodes = {"input1": Mock(), "input2": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

        assert len(result) == 2
        assert "port1" in result
        assert "port2" in result

    def test_collect_no_inputs(self):
        """Test collecting when task has no AvailableData inputs."""
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = []

        result = collect_available_data_inputs(task, {})

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

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["available", "generated"]):
            aiida_nodes = {"available": Mock(), "generated": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

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

        mapping = build_dependency_mapping(task, workflow, {})

        assert mapping.port_mapping == {}
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}

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

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="input"):
            mapping = build_dependency_mapping(task, workflow, {})

        # AvailableData should not create dependencies
        assert mapping.port_mapping == {}
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}

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
        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["producer", "output", "output"]):
            task_outputs = {
                "producer": Mock(
                    remote_folder=Mock(value=123),
                    job_id=Mock(value=456),
                )
            }

            mapping = build_dependency_mapping(consumer, workflow, task_outputs)

        print("\n=== Dependency mapping ===")
        pprint(
            {"port_mapping": mapping.port_mapping, "parent_folders": mapping.parent_folders, "job_ids": mapping.job_ids}
        )

        # Should create dependency
        assert "input_port" in mapping.port_mapping
        assert len(mapping.port_mapping["input_port"]) == 1
        assert mapping.port_mapping["input_port"][0].dep_label == "producer"

        assert "producer" in mapping.parent_folders
        assert mapping.parent_folders["producer"].value == 123

        assert "producer" in mapping.job_ids
        assert mapping.job_ids["producer"].value == 456

    def test_build_mapping_with_missing_producer(self):
        """Test building mapping when producer doesn't exist (lines 459-461)."""
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

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="output"):
            mapping = build_dependency_mapping(task, workflow, {})

        # Should skip this input (line 461: continue)
        assert mapping.port_mapping == {}
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}

    def test_build_mapping_producer_not_in_outputs(self):
        """Test building mapping when producer exists but not in task_output_mapping (lines 470-471)."""
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

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["producer", "output", "output"]):
            # Empty task_output_mapping - producer hasn't completed yet
            mapping = build_dependency_mapping(consumer, workflow, {})

        # Should skip this dependency (line 471: continue)
        assert "input_port" not in mapping.port_mapping
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}


def test_resolve_icon_restart_file_single_model(aiida_localhost, tmp_path):
    """Test resolving restart file for single model."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_icon_restart_file

    # Create a real namelist file
    nml_file = tmp_path / "model_atm.nml"
    nml_file.write_text("&run_nml\n  restart_filename = 'restart_atm.nc'\n/")

    # Create and store real SinglefileData node
    nml_node = aiida.orm.SinglefileData(file=str(nml_file))
    nml_node.store()

    # Create RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/dir")
    workdir_remote.store()

    # Patch aiida-icon's utility function to avoid complex namelist parsing dependencies.
    # This function reads ICON namelists to find restart file names, which would require
    # setting up complex ICON namelist structures. We mock it to focus on testing
    # our dependency resolution logic rather than aiida-icon's internal namelist parsing.
    with patch(
        "aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name", return_value="restart_atm_latest.nc"
    ):
        result = resolve_icon_restart_file(
            workdir_path="/work/dir",
            model_name="atm",
            model_namelist_pks={"atm": nml_node.pk},
            workdir_remote_data=workdir_remote,
        )

    # Should create RemoteData pointing to restart file
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result.get_remote_path()


def test_resolve_icon_restart_file_coupled_models(aiida_localhost, tmp_path):
    """Test resolving restart file for coupled simulation with multiple models."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_icon_restart_file

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

    # Should create RemoteData with combined namelist info
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_oce_latest.nc" in result.get_remote_path()


def test_resolve_icon_dependency_with_filename(aiida_localhost):
    """Test resolving ICON dependency when filename is known."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import _resolve_icon_dependency
    from sirocco.engines.aiida.types import DependencyInfo

    # Create dependency with known filename
    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename="output.nc")

    # Create real RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    result = _resolve_icon_dependency(dep_info, workdir_remote, model_name="atm", model_namelist_pks={})

    # Should create RemoteData pointing to specific file
    assert isinstance(result, aiida.orm.RemoteData)
    assert "output.nc" in result.get_remote_path()


# Patch: Mock aiida-icon namelist utility to avoid complex ICON-specific dependencies
@patch("aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name")
def test_resolve_icon_dependency_via_namelist(mock_read_restart, aiida_localhost, tmp_path):
    """Test resolving ICON dependency via namelist when no filename."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import _resolve_icon_dependency
    from sirocco.engines.aiida.types import DependencyInfo

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

    result = _resolve_icon_dependency(
        dep_info, workdir_remote, model_name="atm", model_namelist_pks={"atm": nml_node.pk}
    )

    # Should call the aiida-icon function and return RemoteData with restart file
    assert mock_read_restart.called
    assert isinstance(result, aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result.get_remote_path()


def test_resolve_icon_dependency_fallback(aiida_localhost):
    """Test resolving ICON dependency fallback when no filename and no namelists (line 110)."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import _resolve_icon_dependency
    from sirocco.engines.aiida.types import DependencyInfo

    # Create dependency without filename and no model_namelist_pks
    dep_info = DependencyInfo(dep_label="task_a", data_label="restart", filename=None)

    # Create real RemoteData for workdir
    workdir_remote = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir_remote.store()

    # Call with empty model_namelist_pks - should return workdir_remote as fallback
    result = _resolve_icon_dependency(dep_info, workdir_remote, model_name="atm", model_namelist_pks={})

    # Should return workdir_remote itself as fallback (line 110)
    assert result == workdir_remote


# Patch: Mock aiida-icon namelist utility to avoid complex multi-model parsing dependencies
@patch("aiida_icon.iconutils.modelnml.read_latest_restart_file_link_name")
def test_resolve_icon_dependency_mapping_with_models(mock_read_restart, aiida_localhost, tmp_path):
    """Test resolve_icon_dependency_mapping with multiple models (lines 140-188)."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_icon_dependency_mapping
    from sirocco.engines.aiida.types import DependencyInfo

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
    master_nml_content = """&master_nml
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
"""
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

    # Mock restart file resolution - return different names for different models
    mock_read_restart.side_effect = lambda model_name, _model_nml=None: f"restart_{model_name}_latest.nc"

    # Call resolve_icon_dependency_mapping
    result = resolve_icon_dependency_mapping(parent_folders, port_dependency_mapping, master_nml.pk, model_namelist_pks)

    # Should create model-specific port mappings (lines 176-186)
    assert "restart_file.atm" in result
    assert "restart_file.oce" in result
    assert isinstance(result["restart_file.atm"], aiida.orm.RemoteData)
    assert isinstance(result["restart_file.oce"], aiida.orm.RemoteData)
    assert "restart_atm_latest.nc" in result["restart_file.atm"].get_remote_path()
    assert "restart_oce_latest.nc" in result["restart_file.oce"].get_remote_path()


@pytest.mark.usefixtures("aiida_localhost")
def test_resolve_icon_dependency_mapping_no_parent_folders(tmp_path):
    """Test resolve_icon_dependency_mapping with no parent folders (lines 144-145)."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_icon_dependency_mapping

    # Create minimal namelists
    master_nml_file = tmp_path / "icon_master.namelist"
    master_nml_file.write_text("&master_nml\n lrestart = .false.\n/")
    master_nml = aiida.orm.SinglefileData(file=str(master_nml_file))
    master_nml.store()

    # Call with None parent_folders
    result = resolve_icon_dependency_mapping(None, {}, master_nml.pk, {})

    # Should return empty dict
    assert result == {}


def test_build_icon_calcjob_inputs_no_wrapper(aiida_localhost, tmp_path):
    """Test building ICON CalcJob inputs without wrapper script."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import build_icon_calcjob_inputs
    from sirocco.engines.aiida.types import AiidaIconTaskSpec, AiidaMetadata, AiidaMetadataOptions

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

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

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
    import aiida.orm

    from sirocco.engines.aiida.dependencies import build_icon_calcjob_inputs
    from sirocco.engines.aiida.types import AiidaIconTaskSpec, AiidaMetadata, AiidaMetadataOptions

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

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    # Should have wrapper_script
    assert "wrapper_script" in inputs
    assert inputs["wrapper_script"].pk == wrapper_script.pk


# Patch: Mock aiida-icon IconCalculation to test port wrapping logic without full ICON dependencies
@patch("aiida_icon.calculations.IconCalculation")
def test_build_icon_calcjob_inputs_namespace_port(mock_icon_calc, aiida_localhost, tmp_path):
    """Test that namespace ports are wrapped in dict."""
    from unittest.mock import Mock

    import aiida.orm
    from aiida.engine.processes.ports import PortNamespace

    from sirocco.engines.aiida.dependencies import build_icon_calcjob_inputs
    from sirocco.engines.aiida.types import AiidaIconTaskSpec, AiidaMetadata, AiidaMetadataOptions

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

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    # Should wrap namespace port in dict
    assert "input_files" in inputs
    assert isinstance(inputs["input_files"], dict)
    assert "test_label" in inputs["input_files"]
    assert inputs["input_files"]["test_label"] == remote_data


# Patch: Mock aiida-icon IconCalculation to test non-namespace port logic without full ICON dependencies
@patch("aiida_icon.calculations.IconCalculation")
def test_build_icon_calcjob_inputs_non_namespace_port(mock_icon_calc, aiida_localhost, tmp_path):
    """Test that non-namespace ports are added directly (line 250)."""
    from unittest.mock import Mock

    import aiida.orm

    from sirocco.engines.aiida.dependencies import build_icon_calcjob_inputs
    from sirocco.engines.aiida.types import AiidaIconTaskSpec, AiidaMetadata, AiidaMetadataOptions

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

    inputs = build_icon_calcjob_inputs(task_spec, input_data_nodes, metadata)

    # Should add port directly without wrapping (line 250)
    assert "restart_file" in inputs
    assert inputs["restart_file"] == remote_data
    assert not isinstance(inputs["restart_file"], dict)


def test_resolve_remote_data_for_dependency_with_filename(aiida_localhost):
    """Test creating RemoteData for dependency with specific filename."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import _resolve_remote_data_for_dependency
    from sirocco.engines.aiida.types import DependencyInfo

    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename="result.txt")

    # Create real RemoteData for workdir
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    key, result = _resolve_remote_data_for_dependency(dep_info, workdir)

    # Should create RemoteData with specific file path
    assert key == "task_a_remote"
    assert isinstance(result, aiida.orm.RemoteData)
    assert "result.txt" in result.get_remote_path()
    assert result.is_stored


def test_resolve_remote_data_for_dependency_workdir_fallback(aiida_localhost):
    """Test using workdir when no filename specified."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import _resolve_remote_data_for_dependency
    from sirocco.engines.aiida.types import DependencyInfo

    dep_info = DependencyInfo(dep_label="task_a", data_label="output", filename=None)

    # Create real RemoteData for workdir
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    key, result = _resolve_remote_data_for_dependency(dep_info, workdir)

    # Should return the workdir itself
    assert key == "task_a_remote"
    assert result == workdir


@pytest.mark.usefixtures("aiida_localhost")
def test_resolve_shell_dependency_mappings_type_error(tmp_path):
    """Test that non-RemoteData parent folders raise TypeError."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_shell_dependency_mappings

    # Create real SinglefileData (wrong type - should be RemoteData)
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    single_file = aiida.orm.SinglefileData(file=str(test_file))
    single_file.store()

    # Use Mock for the value wrapper to match the expected structure
    parent_folders = {"task_a": Mock(value=single_file.pk)}
    port_dependency_mapping = {}
    original_filenames = {}

    with pytest.raises(TypeError, match="Expected RemoteData"):
        resolve_shell_dependency_mappings(parent_folders, port_dependency_mapping, original_filenames)


def test_resolve_shell_dependency_mappings_missing_parent(aiida_localhost):
    """Test resolve_shell_dependency_mappings with missing parent folder (lines 342-343)."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_shell_dependency_mappings
    from sirocco.engines.aiida.types import DependencyInfo

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

    # Call function - should skip task_b (line 343: continue)
    all_nodes, placeholder_mapping, _filenames = resolve_shell_dependency_mappings(
        parent_folders, port_dependency_mapping, original_filenames
    )

    # Should only process task_a, skip task_b
    assert "task_a_remote" in all_nodes
    assert "output_a" in placeholder_mapping
    assert placeholder_mapping["output_a"] == "task_a_remote"

    # task_b should be skipped
    assert "output_b" not in placeholder_mapping


def test_resolve_shell_dependency_mappings_with_filenames(aiida_localhost):
    """Test resolve_shell_dependency_mappings builds filename mapping (lines 354-356)."""
    import aiida.orm

    from sirocco.engines.aiida.dependencies import resolve_shell_dependency_mappings
    from sirocco.engines.aiida.types import DependencyInfo

    # Create parent folder
    workdir = aiida.orm.RemoteData(computer=aiida_localhost, remote_path="/work/task_a")
    workdir.store()

    parent_folders = {"task_a": Mock(value=workdir.pk)}

    # Port dependency with filename
    port_dependency_mapping = {
        "input_port": [DependencyInfo(dep_label="task_a", data_label="output", filename="result.txt")]
    }

    # Original filenames mapping
    original_filenames = {"output": "custom_name.txt"}

    # Call function
    _all_nodes, _placeholder_mapping, filenames = resolve_shell_dependency_mappings(
        parent_folders, port_dependency_mapping, original_filenames
    )

    # Should build filename mapping (lines 354-356)
    assert "task_a_remote" in filenames
    assert filenames["task_a_remote"] == "custom_name.txt"


@pytest.mark.parametrize(
    ("has_job_ids", "label", "expected_has_commands", "expected_job_ids_in_commands", "expected_call_link_label"),
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
    from sirocco.engines.aiida.dependencies import add_slurm_dependencies_to_metadata
    from sirocco.engines.aiida.types import AiidaMetadata, AiidaMetadataOptions

    # Create base metadata
    base_metadata = AiidaMetadata(computer=aiida_localhost, options=AiidaMetadataOptions())

    # Setup job_ids based on test case
    job_ids = None
    if has_job_ids:
        job_ids = {"task_a": Mock(value=12345), "task_b": Mock(value=67890)}

    # Call the function
    result = add_slurm_dependencies_to_metadata(base_metadata, job_ids=job_ids, computer=aiida_localhost, label=label)

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
