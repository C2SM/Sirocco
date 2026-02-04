"""Unit tests for sirocco.engines.aiida.builder module."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import aiida.orm
import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.builder import WorkGraphBuilder, build_sirocco_workgraph


def create_builder_mock_task(name="valid_task", input_names=None, output_names=None):
    """Create a mock task for builder validation tests.

    Args:
        name: Task name
        input_names: List of input data node names (None for empty list)
        output_names: List of output data node names (None for empty list)

    Returns:
        Mock task with input_data_nodes() and output_data_nodes() methods
    """
    task = Mock(spec=core.Task)
    task.name = name

    # Create input data mocks with name attributes
    if input_names:
        input_mocks = []
        for n in input_names:
            mock_input = Mock()
            mock_input.name = n
            input_mocks.append(mock_input)
        task.input_data_nodes.return_value = input_mocks
    else:
        task.input_data_nodes.return_value = []

    # Create output data mocks with name attributes
    if output_names:
        output_mocks = []
        for n in output_names:
            mock_output = Mock()
            mock_output.name = n
            output_mocks.append(mock_output)
        task.output_data_nodes.return_value = output_mocks
    else:
        task.output_data_nodes.return_value = []

    return task


class TestWorkGraphBuilder:
    """Tests for WorkGraphBuilder class."""

    @pytest.fixture
    def builder(self, minimal_workflow):
        """Create a WorkGraphBuilder instance with a real workflow."""
        return WorkGraphBuilder(minimal_workflow)

    def test_initialization(self, minimal_workflow):
        """Test WorkGraphBuilder initialization."""
        builder = WorkGraphBuilder(minimal_workflow)

        print("\n=== Workflow ===")
        pprint(minimal_workflow)
        print("\n=== Builder ===")
        pprint({"workflow_name": builder.workflow.name, "resolved_config_path": builder.resolved_config_path})

        assert builder.workflow == minimal_workflow
        # resolved_config_path is read from the workflow
        assert builder.resolved_config_path == minimal_workflow.resolved_config_path

    @pytest.mark.parametrize(
        ("task_name", "input_names", "should_raise"),
        [
            ("valid_task_name", None, False),  # Valid - should not raise
            ("invalid/task/name", None, True),  # Invalid task name (slashes not allowed)
            ("valid_task", ["invalid:input"], True),  # Invalid input name (colons not allowed)
        ],
    )
    def test_validate_labels(self, builder, task_name, input_names, should_raise):
        """Test label validation with valid and invalid labels."""
        task = create_builder_mock_task(name=task_name, input_names=input_names)
        builder.workflow.tasks = [task]

        print(f"\n=== Validating task: {task_name=}, {input_names=} ===")

        if should_raise:
            with pytest.raises(ValueError, match="invalid"):
                builder._validate_labels()
        else:
            builder._validate_labels()  # Should not raise

    @pytest.mark.parametrize(
        ("workflow_name", "expected_in_name"),
        [
            ("test_workflow", "test_workflow"),
            (None, "SIROCCO_WF"),
        ],
    )
    def test_generate_workgraph_name(self, builder, workflow_name, expected_in_name):
        """Test WorkGraph name generation with and without workflow name."""
        builder.workflow.name = workflow_name

        if workflow_name:
            # Patch: Mock datetime to control timestamp in generated WorkGraph name for deterministic testing
            with patch("sirocco.engines.aiida.builder.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2026, 1, 15, 10, 30, tzinfo=UTC)
                name = builder._generate_workgraph_name()
            assert "2026" in name
        else:
            name = builder._generate_workgraph_name()

        print(f"\n=== Generated name: {name} (from workflow_name={workflow_name}) ===")

        assert expected_in_name in name

    @pytest.mark.parametrize(
        ("front_depth", "should_raise"),
        [
            (0, True),  # Invalid: front_depth < 1
            (2, False),  # Valid
        ],
    )
    def test_store_window_config(self, builder, front_depth, should_raise):
        """Test storing window config with valid and invalid front_depth."""
        wg = Mock()
        wg.extras = {}
        builder.launcher_deps = {"task1": ["task0"]}

        if should_raise:
            with pytest.raises(ValueError, match="front_depth must be >= 1"):
                builder._store_window_config(wg, front_depth=front_depth)
        else:
            builder._store_window_config(wg, front_depth=front_depth)
            print(f"\n=== Window config (front_depth={front_depth}) ===")
            pprint(wg.extras["window_config"])
            assert "window_config" in wg.extras
            assert wg.extras["window_config"]["enabled"] is True
            assert wg.extras["window_config"]["front_depth"] == front_depth
            assert wg.extras["window_config"]["task_dependencies"] == {"task1": ["task0"]}

    # Patch: Mock load_code to prevent database lookups and allow test isolation
    @patch("aiida.orm.load_code")
    @pytest.mark.parametrize("front_depth", [1, 2, 3])
    def test_build_smoke_test(self, mock_load_code, minimal_workflow, aiida_localhost, front_depth):
        """Smoke test: verify build() doesn't crash and returns a WorkGraph."""
        from aiida_workgraph import WorkGraph

        # Use real computer from fixture
        # Update the workflow to use the real computer
        for task in minimal_workflow.tasks:
            task.computer = aiida_localhost.label

        # Mock code loading - return a mock code
        mock_code = Mock()
        mock_code.pk = 123
        mock_load_code.return_value = mock_code

        # Build with real workflow
        builder = WorkGraphBuilder(minimal_workflow)
        wg = builder.build(front_depth=front_depth)

        print(f"\n=== WorkGraph built (front_depth={front_depth}) ===")
        print(f"WorkGraph name: {wg.name}")
        print(f"Number of tasks: {len(wg.tasks._get_keys())}")
        print("\n=== Window config ===")
        pprint(wg.extras.get("window_config"))

        # Verify we got a WorkGraph
        assert isinstance(wg, WorkGraph)

        # Verify window config was stored with correct front_depth
        assert "window_config" in wg.extras
        assert wg.extras["window_config"]["front_depth"] == front_depth
        assert wg.extras["window_config"]["enabled"] is True

        # Verify the workflow was processed
        assert builder.workflow == minimal_workflow

    # Patch: Mock load_code to prevent database lookups and allow test isolation
    @patch("aiida.orm.load_code")
    def test_build_with_dependencies(self, mock_load_code, workflow_with_dependencies, aiida_localhost):
        """Test build() correctly processes workflows with task dependencies."""
        from aiida_workgraph import WorkGraph

        # Use real computer from fixture
        for task in workflow_with_dependencies.tasks:
            task.computer = aiida_localhost.label

        mock_code = Mock()
        mock_code.pk = 123
        mock_load_code.return_value = mock_code

        # Build the workflow
        builder = WorkGraphBuilder(workflow_with_dependencies)
        wg = builder.build(front_depth=1)

        # Verify we got a WorkGraph with tasks
        assert isinstance(wg, WorkGraph)

        # Verify all workflow tasks were added to the WorkGraph
        # Tasks are added as pairs (launcher + get_job_data), so we expect:
        # - 3 workflow tasks x 2 (launcher + get_job_data) = 6 tasks
        # - Plus graph control tasks (graph_inputs, graph_outputs, graph_ctx)
        task_names = wg.tasks._get_keys()
        assert len(task_names) >= 6  # At least the 3 task pairs

        # Verify our tasks were created (check for launcher tasks)
        launcher_tasks = [name for name in task_names if name.startswith("launch_")]
        assert len(launcher_tasks) == 3  # task_a, task_b, task_c
        assert any("task_a" in name for name in launcher_tasks)
        assert any("task_b" in name for name in launcher_tasks)
        assert any("task_c" in name for name in launcher_tasks)

        print("\n=== Launcher tasks ===")
        pprint(launcher_tasks)

        # Verify the actual dependency structure is correct
        # Expected: task_a -> task_b -> task_c (linear dependency chain)

        # launcher_deps maps launcher_name -> [parent_launcher_names]
        # Find the launcher names for our tasks
        launcher_names = {name: name for name in task_names if name.startswith("launch_")}
        task_a_launcher = [n for n in launcher_names if "task_a" in n][0]
        task_b_launcher = [n for n in launcher_names if "task_b" in n][0]
        task_c_launcher = [n for n in launcher_names if "task_c" in n][0]

        print("\n=== Builder launcher_deps ===")
        pprint(builder.launcher_deps)
        print("\n=== Builder dependency_outputs ===")
        pprint(builder.dependency_outputs)

        # Verify dependency relationships
        # task_a should have no dependencies
        assert task_a_launcher not in builder.launcher_deps or len(builder.launcher_deps[task_a_launcher]) == 0

        # task_b should depend on task_a
        assert task_b_launcher in builder.launcher_deps
        assert task_a_launcher in builder.launcher_deps[task_b_launcher]

        # task_c should depend on task_b
        assert task_c_launcher in builder.launcher_deps
        assert task_b_launcher in builder.launcher_deps[task_c_launcher]

        # Verify window config includes the dependency information
        assert "window_config" in wg.extras
        assert "task_dependencies" in wg.extras["window_config"]
        window_deps = wg.extras["window_config"]["task_dependencies"]

        print("\n=== Window config task_dependencies ===")
        pprint(window_deps)

        # Verify the dependency chain is preserved in window config
        assert task_b_launcher in window_deps
        assert task_a_launcher in window_deps[task_b_launcher]
        assert task_c_launcher in window_deps
        assert task_b_launcher in window_deps[task_c_launcher]


@pytest.mark.parametrize(
    ("front_depth", "expected_build_arg"),
    [
        (None, 1),  # Default front_depth=1
        (3, 3),  # Custom front_depth
    ],
)
# Patch: Mock load_code to prevent database lookups and allow test isolation
@patch("aiida.orm.load_code")
def test_build_sirocco_workgraph(
    mock_load_code,
    minimal_workflow,
    aiida_localhost,
    front_depth,
    expected_build_arg,
):
    """Test building WorkGraph with various front_depth values using real WorkGraphBuilder."""
    from aiida_workgraph import WorkGraph

    # Update the workflow to use the real computer
    for task in minimal_workflow.tasks:
        task.computer = aiida_localhost.label

    # Mock code loading - return a mock code
    mock_code = Mock()
    mock_code.pk = 123
    mock_load_code.return_value = mock_code

    # Build kwargs for the function call
    kwargs = {}
    if front_depth is not None:
        kwargs["front_depth"] = front_depth

    # Use real WorkGraphBuilder through the function
    wg = build_sirocco_workgraph(minimal_workflow, **kwargs)

    # Verify we got a real WorkGraph
    assert isinstance(wg, WorkGraph)

    # Verify window config was stored with correct front_depth
    assert "window_config" in wg.extras
    assert wg.extras["window_config"]["front_depth"] == expected_build_arg
    assert wg.extras["window_config"]["enabled"] is True

    # Verify the workflow was processed (check for tasks)
    task_names = wg.tasks._get_keys()
    assert len(task_names) > 0  # Should have at least one task


def test_validate_labels_invalid_output_name(minimal_workflow):
    """Test that invalid output names raise ValueError."""
    builder = WorkGraphBuilder(minimal_workflow)

    # Create task with invalid output name
    task = create_builder_mock_task(name="test_task", output_names=["invalid:output"])
    builder.workflow.tasks = [task]

    with pytest.raises(ValueError, match="validating output name 'invalid:output'"):
        builder._validate_labels()


def test_validate_labels_output_error_message_format(minimal_workflow):
    """Test that output validation errors have proper message format."""
    builder = WorkGraphBuilder(minimal_workflow)

    # Create task with output containing multiple invalid chars
    task = create_builder_mock_task(name="task_a", output_names=["output/data"])
    builder.workflow.tasks = [task]

    with pytest.raises(ValueError, match="output name") as excinfo:
        builder._validate_labels()

    # Verify error message mentions both the output name and the validation issue
    error_msg = str(excinfo.value)
    assert "output/data" in error_msg
    assert "validating output name" in error_msg


@pytest.mark.parametrize(
    ("data_name", "task_type", "include_generated_data", "expected_used_by_icon", "expected_call_count"),
    [
        pytest.param(
            "input_file",
            None,
            True,
            False,
            1,
            id="filters_available_data",
        ),
        pytest.param(
            "icon_input",
            "IconTask",
            False,
            True,
            1,
            id="used_by_icon_task",
        ),
        pytest.param(
            "shell_input",
            "ShellTask",
            False,
            False,
            1,
            id="not_used_by_icon",
        ),
        pytest.param(
            "shared_input",
            "both",
            False,
            True,
            1,
            id="multiple_icon_tasks_same_data",
        ),
    ],
)
def test_prepare_data_nodes_used_by_icon_logic(
    minimal_workflow,
    aiida_localhost,
    tmp_path,
    data_name,
    task_type,
    include_generated_data,
    expected_used_by_icon,
    expected_call_count,
):
    """Test data node preparation and used_by_icon flag logic.

    Covers:
    - Filtering AvailableData vs GeneratedData
    - Setting used_by_icon=True when used by IconTask
    - Setting used_by_icon=False when used by ShellTask only
    - Setting used_by_icon=True when used by any IconTask (even if also used by ShellTask)
    """
    from tests.utils import create_available_data, create_generated_data

    builder = WorkGraphBuilder(minimal_workflow)

    # Create real available data with a temp file
    data_file = tmp_path / f"{data_name}.txt"
    data_file.write_text("test data")
    available_data = create_available_data(
        name=data_name,
        computer_label=aiida_localhost.label,
        path=data_file,
        coordinates={},
    )

    # Build workflow data list
    workflow_data = [available_data]
    if include_generated_data:
        generated_data = create_generated_data(
            name="output_file",
            path="output.txt",
            coordinates={},
        )
        workflow_data.append(generated_data)

    builder.workflow.data = workflow_data

    # Create tasks based on test case
    tasks = []
    if task_type == "IconTask":
        icon_task = Mock(spec=core.IconTask)
        icon_task.input_data_nodes.return_value = [available_data]
        tasks.append(icon_task)
    elif task_type == "ShellTask":
        shell_task = Mock(spec=core.ShellTask)
        shell_task.input_data_nodes.return_value = [available_data]
        tasks.append(shell_task)
    elif task_type == "both":
        shell_task = Mock(spec=core.ShellTask)
        shell_task.input_data_nodes.return_value = [available_data]
        icon_task = Mock(spec=core.IconTask)
        icon_task.input_data_nodes.return_value = [available_data]
        tasks.extend([shell_task, icon_task])

    builder.workflow.tasks = tasks

    # Call the real method to create real data nodes
    builder._prepare_data_nodes()

    # Verify nodes were created (should only create nodes for AvailableData)
    assert len(builder.data_nodes) == expected_call_count

    if expected_call_count > 0:
        # Get the created node
        data_label = f"{data_name}"
        assert data_label in builder.data_nodes
        created_node = builder.data_nodes[data_label]

        # Verify the node type based on expected_used_by_icon
        if expected_used_by_icon:
            # Should be RemoteData for ICON tasks
            assert isinstance(created_node, aiida.orm.RemoteData)
        else:
            # Should be SinglefileData or FolderData for non-ICON tasks on local transport
            assert isinstance(created_node, (aiida.orm.SinglefileData, aiida.orm.FolderData))


# Patch: Mock create_input_data_node to test label generation without actual AiiDA node creation
@patch("sirocco.engines.aiida.builder.AiidaAdapter.create_input_data_node")
# Patch: Mock build_graph_item_label to control generated labels for data nodes
@patch("sirocco.engines.aiida.builder.AiidaAdapter.build_graph_item_label")
def test_prepare_data_nodes_label_generation(mock_label, mock_create_node, minimal_workflow):
    """Test that correct labels are generated for data nodes."""
    builder = WorkGraphBuilder(minimal_workflow)

    # Create available data
    available_data = Mock(spec=core.AvailableData)
    available_data.name = "test_data"
    available_data.coordinates = {"member": 0}

    builder.workflow.data = [available_data]
    builder.workflow.tasks = []

    mock_label.return_value = "test_data_member_0"
    mock_node = Mock()
    mock_create_node.return_value = mock_node

    builder._prepare_data_nodes()

    # Should use the label as key in data_nodes dict
    assert "test_data_member_0" in builder.data_nodes
    assert builder.data_nodes["test_data_member_0"] == mock_node


@pytest.mark.parametrize(
    (
        "num_shell_tasks",
        "num_icon_tasks",
        "expected_shell_specs",
        "expected_icon_specs",
        "expected_shell_calls",
        "expected_icon_calls",
    ),
    [
        pytest.param(
            0,
            1,
            0,
            1,
            0,
            1,
            id="icon_task_only",
        ),
        pytest.param(
            1,
            1,
            1,
            1,
            1,
            1,
            id="mixed_shell_and_icon",
        ),
        pytest.param(
            0,
            2,
            0,
            2,
            0,
            2,
            id="icon_only_workflow",
        ),
    ],
)
# Patch: Mock build_icon_task_spec to isolate task routing logic from spec building complexity
@patch("sirocco.engines.aiida.builder.build_icon_task_spec")
# Patch: Mock build_shell_task_spec to isolate task routing logic from spec building complexity
@patch("sirocco.engines.aiida.builder.build_shell_task_spec")
def test_build_task_specs_task_type_routing(
    mock_shell_spec,
    mock_icon_spec,
    minimal_workflow,
    create_shell_task,
    create_icon_task,
    num_shell_tasks,
    num_icon_tasks,
    expected_shell_specs,
    expected_icon_specs,
    expected_shell_calls,
    expected_icon_calls,
):
    """Test that IconTask and ShellTask are routed to correct spec builders.

    Uses real task objects created via fixtures, but keeps spec builder mocks
    to isolate routing logic from spec building complexity.

    Covers:
    - IconTask triggers build_icon_task_spec only
    - Mixed workflows handle both task types correctly
    - Multiple IconTasks are all processed
    """
    builder = WorkGraphBuilder(minimal_workflow)

    tasks = []

    # Create real ShellTasks using the fixture
    for i in range(num_shell_tasks):
        task_name = f"shell_task_{i}" if num_shell_tasks > 1 else "shell_task"
        shell_task = create_shell_task(name=task_name, command="echo test")
        tasks.append(shell_task)

    # Create real IconTasks using the fixture
    for i in range(num_icon_tasks):
        task_name = f"icon_{i + 1}" if num_icon_tasks > 1 else "icon_task"
        icon_task = create_icon_task(name=task_name)
        tasks.append(icon_task)

    builder.workflow.tasks = tasks

    mock_shell_spec.return_value = {"spec": "shell_spec"}
    mock_icon_spec.return_value = {"spec": "icon_spec"}

    builder._build_task_specs()

    # Verify call counts
    assert mock_shell_spec.call_count == expected_shell_calls
    assert mock_icon_spec.call_count == expected_icon_calls

    # Verify spec storage
    assert len(builder.shell_specs) == expected_shell_specs
    assert len(builder.icon_specs) == expected_icon_specs

    # Verify that real task objects were passed to the spec builders
    if expected_shell_calls > 0:
        for call_args in mock_shell_spec.call_args_list:
            task_arg = call_args[0][0]
            assert isinstance(task_arg, core.ShellTask)
            assert hasattr(task_arg, "command")

    if expected_icon_calls > 0:
        for call_args in mock_icon_spec.call_args_list:
            task_arg = call_args[0][0]
            assert isinstance(task_arg, core.IconTask)
            assert hasattr(task_arg, "namelists")


# Patch: Mock build_icon_task_spec to isolate label key generation logic from spec building
@patch("sirocco.engines.aiida.builder.build_icon_task_spec")
# Patch: Mock build_graph_item_label to control generated task labels for testing dictionary keying
@patch("sirocco.engines.aiida.builder.AiidaAdapter.build_graph_item_label")
def test_build_task_specs_icon_label_as_key(mock_label, mock_icon_spec, minimal_workflow):
    """Test that icon specs are keyed by the graph item label."""
    builder = WorkGraphBuilder(minimal_workflow)

    # Create IconTask with coordinates
    icon_task = Mock(spec=core.IconTask)
    icon_task.name = "icon_task"
    icon_task.coordinates = {"member": 5}

    builder.workflow.tasks = [icon_task]

    mock_label.return_value = "icon_task_member_5"
    mock_icon_spec.return_value = {"spec": "icon_spec"}

    builder._build_task_specs()

    # Should use label as key
    assert "icon_task_member_5" in builder.icon_specs


@pytest.mark.parametrize(
    ("config_path_type", "workflow_name", "node_pk", "should_store_pk", "should_log"),
    [
        pytest.param(
            None,
            "test_workflow",
            None,
            False,
            False,
            id="no_resolved_config",
        ),
        pytest.param(
            "nonexistent",
            "test_workflow",
            None,
            False,
            False,
            id="resolved_config_missing_file",
        ),
        pytest.param(
            "exists",
            "test_workflow",
            12345,
            True,
            True,
            id="resolved_config_stored",
        ),
        pytest.param(
            "exists",
            "test_workflow",
            None,
            False,
            False,
            id="resolved_config_pk_none",
        ),
        pytest.param(
            "exists",
            "my_workflow",
            99999,
            True,
            True,
            id="resolved_config_labels",
        ),
    ],
)
# Patch: Mock LOGGER to verify logging behavior when resolved config is stored
@patch("sirocco.engines.aiida.builder.LOGGER")
# Patch: Mock SinglefileData to prevent actual AiiDA node creation when storing resolved config
@patch("sirocco.engines.aiida.builder.aiida.orm.SinglefileData")
def test_store_window_config_resolved_config_handling(
    mock_singlefile,
    mock_logger,
    minimal_workflow,
    tmp_path,
    config_path_type,
    workflow_name,
    node_pk,
    should_store_pk,
    should_log,
):
    """Test window config storage with various resolved config scenarios.

    Covers:
    - No resolved config path (None)
    - Config file doesn't exist
    - Config file exists and is stored as SinglefileData
    - Node pk is None (not stored in extras)
    - Proper labels and logging for stored config
    """
    builder = WorkGraphBuilder(minimal_workflow)
    builder.workflow.name = workflow_name
    builder.launcher_deps = {}

    # Setup resolved_config_path based on test case
    if config_path_type is None:
        builder.resolved_config_path = None
    elif config_path_type == "nonexistent":
        builder.resolved_config_path = tmp_path / "nonexistent.yml"
    elif config_path_type == "exists":
        config_file = tmp_path / "resolved_config.yml"
        config_file.write_text(f"name: {workflow_name}\nscheduler: slurm")
        builder.resolved_config_path = config_file

        # Mock SinglefileData
        mock_node = Mock()
        mock_node.pk = node_pk
        mock_node.store.return_value = None
        mock_singlefile.return_value = mock_node

    wg = Mock()
    wg.extras = {}

    builder._store_window_config(wg, front_depth=1)

    # All cases should have window_config
    assert "window_config" in wg.extras

    # Check resolved_config_pk in extras
    if should_store_pk:
        assert "resolved_config_pk" in wg.extras
        assert wg.extras["resolved_config_pk"] == node_pk

        # Verify SinglefileData creation
        assert mock_singlefile.called
        call_kwargs = mock_singlefile.call_args[1]
        assert call_kwargs["file"] == builder.resolved_config_path

        # Verify labels
        assert workflow_name in mock_node.label
        assert workflow_name in mock_node.description
        assert "Resolved configuration" in mock_node.description

        # Verify node was stored
        assert mock_node.store.called
    else:
        assert "resolved_config_pk" not in wg.extras

    # Check logging
    if should_log:
        assert mock_logger.info.called
        log_call = mock_logger.info.call_args[0]
        assert str(node_pk) in str(log_call)
    # Logger might be called for other reasons, just verify pk not in message if not storing
    elif mock_logger.info.called and node_pk:
        log_call = mock_logger.info.call_args[0]
        # If pk is None, it shouldn't be logged
