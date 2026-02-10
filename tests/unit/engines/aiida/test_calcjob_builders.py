"""Unit tests for sirocco.engines.aiida.calcjob_builders module."""

from unittest.mock import Mock, patch

import aiida.orm
import pytest

from sirocco.engines.aiida.calcjob_builders import (
    _build_slurm_dependency_directive,
    add_slurm_dependencies_to_metadata,
    build_icon_calcjob_inputs,
)
from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaMetadataOptions,
)


class TestBuildSlurmDependencyDirective:
    """Test SLURM dependency directive construction."""

    def test_single_job_id(self):
        """Test directive with single job ID."""
        job_ids = {"task1": Mock(value=12345)}

        directive = _build_slurm_dependency_directive(job_ids)

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

        directive = _build_slurm_dependency_directive(job_ids)

        print("\n=== SLURM dependency directive (multiple jobs) ===")
        print(directive)

        # Dict maintains insertion order in Python 3.7+, so order is deterministic
        assert directive == "#SBATCH --dependency=afterok:12345:67890:11111 --kill-on-invalid-dep=yes"


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
