"""Shared test utilities for creating real Sirocco objects.

These helper functions create real task and data objects via workflow parsing,
which ensures they are properly initialized through the same path as production code.
This avoids the need to repeat complex setup code across test files.
"""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, Mock

from sirocco import core
from sirocco.core import workflow


# NOTE: See if this can be merged with the function from `utils.py`
def create_mock_shell_task(**overrides):
    """Create a mock ShellTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock ShellTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.ShellTask)

    # Set defaults for common attributes
    defaults = {
        "name": "test_task",
        "coordinates": {},
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


def create_mock_icon_task(**overrides):
    """Create a mock IconTask with default attributes.

    Args:
        **overrides: Attribute overrides for the task

    Returns:
        Mock IconTask with default attributes and any overrides applied
    """
    task = Mock(spec=core.IconTask)

    # Set defaults for common attributes
    defaults = {
        "name": "test_task",
        "coordinates": {},
        "computer": "test_computer",
        "walltime": None,
        "mem": None,
        "partition": None,
        "account": None,
        "nodes": None,
        "ntasks_per_node": None,
        "cpus_per_task": None,
        "uenv": None,
        "view": None,
        "path": None,
        "command": None,
    }

    # Apply overrides
    defaults.update(overrides)

    # Set attributes on mock
    for key, value in defaults.items():
        setattr(task, key, value)

    # Add default cycle_point if not in overrides
    if "cycle_point" not in overrides:
        cycle_point = Mock()
        cycle_point.__class__.__name__ = "IntegerCyclePoint"
        task.cycle_point = cycle_point

    return task


def create_available_data(name, computer_label, path, coordinates=None, file_format=None):
    """Create a real AvailableData instance.

    Args:
        name: Data name
        computer_label: Computer label
        path: Path to data (can be str or Path)
        coordinates: Optional coordinates dict (defaults to empty dict)
        format: Optional format string

    Returns:
        Real AvailableData instance
    """
    return core.AvailableData(
        name=name,
        computer=computer_label,
        path=Path(path) if not isinstance(path, Path) else path,
        coordinates=coordinates if coordinates is not None else {},
        format=file_format,
    )


def create_transport(path_exists=True, isfile=True):  # noqa FBT002: Boolean default positional argument in function definition
    """Create a mock transport configured as a context manager.

    Note: We still mock transports for tests that need to simulate
    remote file checks without actual remote connections.

    Args:
        path_exists: Whether path_exists() should return True
        isfile: Whether isfile() should return True

    Returns:
        MagicMock configured as transport context manager
    """
    mock_transport = MagicMock()
    mock_transport.path_exists.return_value = path_exists
    mock_transport.isfile.return_value = isfile
    mock_transport.__enter__.return_value = mock_transport
    mock_transport.__exit__.return_value = None
    return mock_transport


def create_authinfo(transport=None):
    """Create or mock authinfo with transport.

    For tests simulating remote scenarios, we mock the authinfo.
    For local scenarios, we could use real authinfo.

    Args:
        computer: AiiDA Computer instance
        transport: Optional mock transport (creates mock if None)

    Returns:
        MagicMock configured as authinfo with transport
    """
    if transport is None:
        transport = create_transport()

    mock_authinfo = MagicMock()
    mock_authinfo.get_transport.return_value = transport
    return mock_authinfo


def create_shell_task_from_workflow(
    tmp_path,
    name="test_task",
    command="echo hello",
    computer="localhost",
    walltime="01:00:00",
    **task_config,
):
    """Create a real ShellTask by parsing a minimal workflow.

    Args:
        tmp_path: Temporary directory for workflow rootdir
        name: Task name
        command: Shell command
        computer: Computer label
        walltime: Wall time limit
        **task_config: Additional task config (nodes, partition, etc.)

    Returns:
        ShellTask: A real ShellTask extracted from the workflow
    """
    # Build additional task config lines with proper indentation
    extra_config = ""
    if task_config:
        extra_config = "\n" + "\n".join(f"              {k}: {v}" for k, v in task_config.items())

    config_yaml = textwrap.dedent(
        f"""
        name: test_workflow
        scheduler: slurm
        cycles:
          - single:
              tasks:
                - {name}: {{}}
        tasks:
          - {name}:
              plugin: shell
              computer: {computer}
              command: {command}
              walltime: {walltime}{extra_config}
        data:
          available: []
          generated: []
        """
    )
    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    # Return the first task from the workflow by iterating
    return next(iter(wf.tasks))


def create_icon_task_from_workflow(
    tmp_path,
    name="test_icon",
    computer="localhost",
    bin_path=None,
    walltime="01:00:00",
    nodes=1,
    ntasks_per_node=12,
    **task_config,
):
    """Create a real IconTask by parsing a workflow with namelist files.

    Args:
        tmp_path: Temporary directory for workflow rootdir and namelists
        name: Task name
        computer: Computer label
        bin_path: Path to ICON binary (defaults to tmp_path/icon)
        walltime: Wall time limit
        nodes: Number of nodes
        ntasks_per_node: Tasks per node
        **task_config: Additional task config

    Returns:
        IconTask: A real IconTask extracted from the workflow
    """
    # Create minimal namelist files
    icon_dir = tmp_path / "ICON"
    icon_dir.mkdir(exist_ok=True)

    master_nml = icon_dir / "icon_master.namelist"
    master_nml.write_text(
        textwrap.dedent("""
        &master_nml
         lrestart = .false.
        /
    """)
    )

    model_nml = icon_dir / "model.namelist"
    model_nml.write_text(
        textwrap.dedent("""
        &run_nml
         num_lev = 90
        /
    """)
    )

    bin_path = bin_path or tmp_path / "icon"
    bin_path.touch(exist_ok=True)

    # Build additional task config lines with proper indentation
    extra_config = ""
    if task_config:
        extra_config = "\n" + "\n".join(f"              {k}: {v}" for k, v in task_config.items())

    # Use date cycling for IconTask (required by update_icon_namelists_from_workflow)
    config_yaml = textwrap.dedent(
        f"""
        name: test_workflow
        scheduler: slurm
        cycles:
          - single:
              cycling:
                start_date: 2026-01-01T00:00
                stop_date: 2026-01-02T00:00
                period: P1D
              tasks:
                - {name}: {{}}
        tasks:
          - {name}:
              plugin: icon
              computer: {computer}
              bin: {bin_path}
              namelists:
                - ./ICON/icon_master.namelist
                - ./ICON/model.namelist
              walltime: {walltime}
              nodes: {nodes}
              ntasks_per_node: {ntasks_per_node}{extra_config}
        data:
          available: []
          generated: []
        """
    )
    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    # Return the first task from the workflow by iterating
    return next(iter(wf.tasks))


def create_icon_task_with_model_namelists(
    tmp_path,
    name="test_icon",
    computer="localhost",
    models=None,
):
    """Create an ICON task with model namelists (atm, oce, etc.).

    Args:
        tmp_path: Temporary directory for workflow rootdir and namelists
        name: Task name
        computer: Computer label
        models: List of model names (defaults to ["atm", "oce"])

    Returns:
        IconTask: An ICON task with master and model namelists
    """
    models = models or ["atm", "oce"]

    # Create namelist directory
    icon_dir = tmp_path / "ICON"
    icon_dir.mkdir(exist_ok=True)

    # Create master namelist referencing model namelists
    master_content = "&master_nml\n lrestart = .false.\n/\n"
    for model in models:
        master_content += f"""&master_model_nml
 model_name="{model}"
 model_namelist_filename="{model}.namelist"
 model_type=1
/
"""

    master_nml = icon_dir / "icon_master.namelist"
    master_nml.write_text(master_content)

    # Create model namelist files
    namelist_paths = ["./ICON/icon_master.namelist"]
    for model in models:
        model_nml = icon_dir / f"{model}.namelist"
        model_nml.write_text("&run_nml\n num_lev = 90\n/\n")
        namelist_paths.append(f"./ICON/{model}.namelist")

    # Create bin path
    bin_path = tmp_path / "icon"
    bin_path.touch()

    # Hardcoded YAML config with model namelists
    namelist_yaml = "\n".join(f"                - {path}" for path in namelist_paths)

    config_yaml = f"""name: test_workflow
scheduler: slurm
cycles:
  - single:
      cycling:
        start_date: 2026-01-01T00:00
        stop_date: 2026-01-02T00:00
        period: P1D
      tasks:
        - {name}: {{}}
tasks:
  - {name}:
      plugin: icon
      computer: {computer}
      bin: {bin_path}
      namelists:
{namelist_yaml}
      walltime: 01:00:00
      nodes: 1
      ntasks_per_node: 12
data:
  available: []
  generated: []
"""

    wf = workflow.Workflow.from_config_str(config_yaml, rootdir=tmp_path)
    return next(iter(wf.tasks))


def create_generated_data(
    name="output_data",
    path="output.txt",
    coordinates=None,
):
    """Create a real GeneratedData object directly.

    Args:
        name: Data name
        path: Path to output file (can be None)
        coordinates: Cycle coordinates (defaults to empty dict)

    Returns:
        GeneratedData: A real GeneratedData instance
    """
    from sirocco.core import GeneratedData

    return GeneratedData(
        name=name,
        path=Path(path) if path else None,
        coordinates=coordinates or {},
    )
