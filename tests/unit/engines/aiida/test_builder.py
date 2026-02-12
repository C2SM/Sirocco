"""Unit tests for sirocco.engines.aiida.builder module."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import aiida.orm
import pytest
from aiida_workgraph import WorkGraph
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.builder import WorkGraphBuilder, build_sirocco_workgraph
from tests.unit.utils import create_available_data, create_generated_data


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
    task.computer = None  # Prevent computer validation from failing

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
        pprint({"workflow_name": builder.workflow.name})

        assert builder.workflow == minimal_workflow

    @pytest.mark.parametrize(
        ("task_name", "input_names", "should_raise"),
        [
            pytest.param("valid_task_name", None, False, id="valid_task"),
            pytest.param("invalid/task/name", None, True, id="invalid_task_name"),
            pytest.param("valid_task", ["invalid:input"], True, id="invalid_input_name"),
        ],
    )
    def test_validate_labels(self, builder, task_name, input_names, should_raise):
        """Test label validation with valid and invalid labels (now in adapter)."""
        from sirocco.engines.aiida.adapter import AiidaAdapter

        task = create_builder_mock_task(name=task_name, input_names=input_names)
        builder.workflow.tasks = [task]

        print(f"\n=== Validating task: {task_name=}, {input_names=} ===")

        if should_raise:
            with pytest.raises(ValueError, match="invalid"):
                AiidaAdapter.validate_workflow(builder.workflow)
        else:
            AiidaAdapter.validate_workflow(builder.workflow)  # Should not raise

    @pytest.mark.parametrize(
        ("workflow_name", "expected_name"),
        [
            pytest.param("test_workflow", "test_workflow_2026_01_15_10_30", id="with_workflow_name"),
            pytest.param(None, "SIROCCO_WF_2026_01_15_10_30", id="default_name"),
        ],
    )
    def test_generate_workgraph_name(self, builder, workflow_name, expected_name):
        """Test WorkGraph name generation with and without workflow name."""
        builder.workflow.name = workflow_name

        # Patch: Mock datetime to control timestamp in generated WorkGraph name for deterministic testing
        with patch("sirocco.engines.aiida.builder.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2026, 1, 15, 10, 30, tzinfo=UTC)
            name = builder._generate_workgraph_name()

        print(f"\n=== Generated name: {name} (from workflow_name={workflow_name}) ===")

        assert name == expected_name

    @pytest.mark.parametrize(
        ("front_depth", "should_raise"),
        [
            pytest.param(0, True, id="invalid_front_depth"),
            pytest.param(2, False, id="valid_front_depth"),
        ],
    )
    def test_store_window_config(self, builder, front_depth, should_raise):
        """Test storing window config with valid and invalid front_depth."""
        wg = Mock()
        wg.extras = {}
        builder.launcher_parents = {"task1": ["task0"]}

        print(f"\n=== Test store_window_config (front_depth={front_depth}, should_raise={should_raise}) ===")
        print(f"launcher_parents: {builder.launcher_parents}")

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

    def test_build_with_dependencies(self, workflow_with_dependencies, aiida_localhost, stored_shell_code):
        """Test build() correctly processes workflows with task dependencies."""
        # Use real computer and code from fixtures
        for task in workflow_with_dependencies.tasks:
            task.computer = aiida_localhost.label
            task.code = stored_shell_code.pk

        # Set front_depth on workflow
        workflow_with_dependencies.front_depth = 1

        # Build the workflow
        builder = WorkGraphBuilder(workflow_with_dependencies)
        wg = builder.build()

        # Verify we got a WorkGraph with tasks
        assert isinstance(wg, WorkGraph)

        # Verify all workflow tasks were added to the WorkGraph
        # Tasks are added as pairs (launcher + get_job_data), so we expect:
        # - 3 workflow tasks x 2 (launcher + get_job_data) = 6 tasks
        # - Plus graph control tasks (graph_inputs, graph_outputs, graph_ctx) = 3
        # Total: 9 tasks
        task_names = wg.tasks._get_keys()
        assert len(task_names) == 9

        # Verify our tasks were created (check for launcher tasks)
        launcher_tasks = [name for name in task_names if name.startswith("launch_")]
        assert len(launcher_tasks) == 3  # task_a, task_b, task_c

        # Verify all three tasks are present - check that names end with expected task names
        # (can't check exact equality because workflow name and timestamp vary)
        launcher_task_set = set(launcher_tasks)
        assert any(name.endswith("_task_a") for name in launcher_task_set), "task_a not found"
        assert any(name.endswith("_task_b") for name in launcher_task_set), "task_b not found"
        assert any(name.endswith("_task_c") for name in launcher_task_set), "task_c not found"

        print("\n=== Launcher tasks ===")
        pprint(launcher_tasks)

        # Verify the actual dependency structure is correct
        # Expected: task_a -> task_b -> task_c (linear dependency chain)

        # launcher_parents maps launcher_name -> [parent_launcher_names]
        # Find the launcher names for our tasks
        launcher_names = {name: name for name in task_names if name.startswith("launch_")}
        task_a_launcher = [n for n in launcher_names if "task_a" in n][0]
        task_b_launcher = [n for n in launcher_names if "task_b" in n][0]
        task_c_launcher = [n for n in launcher_names if "task_c" in n][0]

        print("\n=== Builder launcher_parents ===")
        pprint(builder.launcher_parents)

        # Verify dependency relationships
        # task_a should have no dependencies
        assert task_a_launcher not in builder.launcher_parents or len(builder.launcher_parents[task_a_launcher]) == 0

        # task_b should depend on task_a
        assert task_b_launcher in builder.launcher_parents
        assert task_a_launcher in builder.launcher_parents[task_b_launcher]

        # task_c should depend on task_b
        assert task_c_launcher in builder.launcher_parents
        assert task_b_launcher in builder.launcher_parents[task_c_launcher]

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
        pytest.param(1, 1, id="default_front_depth"),
        pytest.param(3, 3, id="custom_front_depth"),
    ],
)
def test_build_sirocco_workgraph(
    minimal_workflow,
    aiida_localhost,
    stored_shell_code,
    front_depth,
    expected_build_arg,
):
    """Test building WorkGraph with various front_depth values.

    Tests workflow building with different front_depth configurations.
    """
    for task in minimal_workflow.tasks:
        task.computer = aiida_localhost.label

    minimal_workflow.front_depth = front_depth

    print(f"\n=== Test build_sirocco_workgraph (front_depth={front_depth}) ===")
    print(f"Expected build arg: {expected_build_arg}")
    print(f"Code PK: {stored_shell_code.pk}")

    wg = build_sirocco_workgraph(minimal_workflow)

    print(f"WorkGraph name: {wg.name}")
    print(f"Number of tasks: {len(wg.tasks._get_keys())}")
    print("\n=== Window config ===")
    pprint(wg.extras.get("window_config"))

    # Verify we got a real WorkGraph
    assert isinstance(wg, WorkGraph)

    # Verify window config was stored with correct front_depth
    assert "window_config" in wg.extras
    assert wg.extras["window_config"]["front_depth"] == expected_build_arg
    assert wg.extras["window_config"]["enabled"] is True

    # Verify the workflow was processed (check for tasks)
    task_names = wg.tasks._get_keys()
    # Should have: 1 launcher + 1 get_job_data + 3 control tasks (inputs, outputs, ctx) = 5
    assert len(task_names) == 5


def test_validate_labels_invalid_output_name(minimal_workflow):
    """Test that invalid output names raise ValueError (now in adapter)."""
    from sirocco.engines.aiida.adapter import AiidaAdapter

    # Create task with invalid output name
    task = create_builder_mock_task(name="test_task", output_names=["invalid:output"])
    minimal_workflow.tasks = [task]

    print("\n=== Test validate_labels with invalid output name ===")
    print("Task name: test_task")
    print("Output names: ['invalid:output']")

    with pytest.raises(ValueError, match="validating output name 'invalid:output'"):
        AiidaAdapter.validate_workflow(minimal_workflow)


def test_validate_labels_output_error_message_format(minimal_workflow):
    """Test that output validation errors have proper message format (now in adapter)."""
    from sirocco.engines.aiida.adapter import AiidaAdapter

    # Create task with output containing multiple invalid chars
    task = create_builder_mock_task(name="task_a", output_names=["output/data"])
    minimal_workflow.tasks = [task]

    print("\n=== Test validate_labels output error message format ===")
    print("Task name: task_a")
    print("Output names: ['output/data']")
    print("Expected: ValueError with 'output name' and 'output/data'")

    with pytest.raises(ValueError, match="output name") as excinfo:
        AiidaAdapter.validate_workflow(minimal_workflow)

    # Verify error message mentions both the output name and the validation issue
    error_msg = str(excinfo.value)
    print(f"Error message: {error_msg}")
    assert "output/data" in error_msg
    assert "validating output name" in error_msg


@pytest.mark.parametrize(
    (
        "data_name",
        "task_type",
        "include_generated_data",
        "expected_used_by_icon",
    ),
    [
        pytest.param(
            "input_file",
            None,
            True,
            False,
            id="filters_available_data",
        ),
        pytest.param(
            "icon_input",
            "IconTask",
            False,
            True,
            id="used_by_icon_task",
        ),
        pytest.param(
            "shell_input",
            "ShellTask",
            False,
            False,
            id="not_used_by_icon",
        ),
        pytest.param(
            "shared_input",
            "both",
            False,
            True,
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
):
    """Test data node preparation and used_by_icon flag logic.

    Covers:
    - Filtering AvailableData vs GeneratedData
    - Setting used_by_icon=True when used by IconTask
    - Setting used_by_icon=False when used by ShellTask only
    - Setting used_by_icon=True when used by any IconTask (even if also used by ShellTask)
    """

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

    print(f"\n=== Test prepare_data_nodes (data_name={data_name}, task_type={task_type}) ===")
    print(f"Include generated data: {include_generated_data}")
    print(f"Expected used_by_icon: {expected_used_by_icon}")

    # Call the real method to create real data nodes
    builder._prepare_data_nodes()

    print(f"Created data nodes: {len(builder.data_nodes)}")
    if builder.data_nodes:
        print("Data node labels:")
        pprint(list(builder.data_nodes.keys()))

    # Verify exactly one node was created (should only create nodes for AvailableData)
    assert len(builder.data_nodes) == 1

    # Get the created node
    data_label = f"{data_name}"
    assert data_label in builder.data_nodes
    created_node = builder.data_nodes[data_label]

    print(f"Created node type: {type(created_node).__name__}")

    # Verify the node type based on expected_used_by_icon
    if expected_used_by_icon:
        # Should be RemoteData for ICON tasks
        assert isinstance(created_node, aiida.orm.RemoteData)
    else:
        # Should be SinglefileData or FolderData for non-ICON tasks on local transport
        assert isinstance(created_node, (aiida.orm.SinglefileData, aiida.orm.FolderData))


# Patch: Mock create_input_data_node to test label generation without actual AiiDA node creation
@patch("sirocco.engines.aiida.builder.AiidaAdapter.create_input_data_node")
def test_prepare_data_nodes_label_generation(mock_create_node, minimal_workflow):
    """Test that correct labels are generated for data nodes.

    Only mocks node creation to avoid database operations.
    """
    from sirocco.engines.aiida.adapter import AiidaAdapter

    builder = WorkGraphBuilder(minimal_workflow)

    available_data = Mock(spec=core.AvailableData)
    available_data.name = "test_data"
    available_data.coordinates = {"member": 0}

    builder.workflow.data = [available_data]
    builder.workflow.tasks = []

    expected_label = AiidaAdapter.build_label_from_graph_item(available_data)

    mock_node = Mock()
    mock_create_node.return_value = mock_node

    print("\n=== Test prepare_data_nodes label generation ===")
    print(f"Data name: {available_data.name}")
    print(f"Coordinates: {available_data.coordinates}")
    print(f"Expected label: {expected_label}")

    builder._prepare_data_nodes()

    print(f"Generated data nodes: {list(builder.data_nodes.keys())}")

    # Should use the real generated label as key in data_nodes dict
    assert expected_label in builder.data_nodes
    assert builder.data_nodes[expected_label] == mock_node


@pytest.mark.parametrize(
    (
        "num_shell_tasks",
        "num_icon_tasks",
        "expected_shell_specs",
        "expected_icon_specs",
    ),
    [
        pytest.param(0, 1, 0, 1, id="icon_task_only"),
        pytest.param(1, 1, 1, 1, id="mixed_shell_and_icon"),
        pytest.param(0, 2, 0, 2, id="icon_only_workflow"),
    ],
)
@patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
def test_build_task_specs_task_type_routing(
    mock_create_nml,
    minimal_workflow,
    create_shell_task,
    create_icon_task,
    aiida_localhost,
    num_shell_tasks,
    num_icon_tasks,
    expected_shell_specs,
    expected_icon_specs,
):
    """Test that IconTask and ShellTask are routed to correct spec builders.

    Covers:
    - IconTask triggers IconTaskSpecBuilder
    - ShellTask triggers ShellTaskSpecBuilder
    - Mixed workflows handle both task types correctly
    - Multiple IconTasks are all processed
    """
    builder = WorkGraphBuilder(minimal_workflow)

    mock_nml = Mock()
    mock_nml.pk = 999
    mock_create_nml.return_value = mock_nml

    tasks = []

    for i in range(num_shell_tasks):
        task_name = f"shell_task_{i}" if num_shell_tasks > 1 else "shell_task"
        shell_task = create_shell_task(name=task_name, command="echo test", computer=aiida_localhost.label)
        tasks.append(shell_task)

    for i in range(num_icon_tasks):
        task_name = f"icon_{i + 1}" if num_icon_tasks > 1 else "icon_task"
        icon_task = create_icon_task(name=task_name, computer=aiida_localhost.label)
        tasks.append(icon_task)

    builder.workflow.tasks = tasks

    print(f"\n=== Test build_task_specs routing (shell={num_shell_tasks}, icon={num_icon_tasks}) ===")
    print(f"Expected shell specs: {expected_shell_specs}")
    print(f"Expected icon specs: {expected_icon_specs}")

    builder._build_task_specs()

    print(f"Shell specs created: {len(builder.shell_specs)}")
    print(f"Icon specs created: {len(builder.icon_specs)}")

    assert len(builder.shell_specs) == expected_shell_specs
    assert len(builder.icon_specs) == expected_icon_specs

    for spec in builder.shell_specs.values():
        assert spec.code_pk is not None
        assert spec.label is not None

    for spec in builder.icon_specs.values():
        assert spec.code_pk is not None
        assert spec.master_namelist_pk is not None
        assert spec.label is not None

    total_specs = len(builder.shell_specs) + len(builder.icon_specs)
    total_tasks = num_shell_tasks + num_icon_tasks
    assert total_specs == total_tasks


@patch("sirocco.engines.aiida.spec_builders.create_namelist_singlefiledata_from_content")
def test_build_task_specs_icon_label_as_key(mock_create_nml, minimal_workflow, create_icon_task, aiida_localhost):
    """Test that icon specs are keyed by the graph item label."""
    from sirocco.engines.aiida.adapter import AiidaAdapter

    builder = WorkGraphBuilder(minimal_workflow)

    mock_nml = Mock()
    mock_nml.pk = 999
    mock_create_nml.return_value = mock_nml

    icon_task = create_icon_task(name="icon_task", computer=aiida_localhost.label)
    icon_task.coordinates = {"member": 5}

    builder.workflow.tasks = [icon_task]

    expected_label = AiidaAdapter.build_label_from_graph_item(icon_task)

    print("\n=== Test build_task_specs icon label as key ===")
    print(f"Task name: {icon_task.name}")
    print(f"Coordinates: {icon_task.coordinates}")
    print(f"Expected label key: {expected_label}")

    builder._build_task_specs()

    print(f"Icon spec keys: {list(builder.icon_specs.keys())}")

    assert expected_label in builder.icon_specs

    spec = builder.icon_specs[expected_label]
    assert spec.code_pk is not None
    assert spec.master_namelist_pk is not None


@pytest.mark.parametrize(
    ("config_path_type", "workflow_name", "should_store_pk", "should_log"),
    [
        pytest.param(None, "test_workflow", False, False, id="no_resolved_config"),
        pytest.param(
            "nonexistent",
            "test_workflow",
            False,
            False,
            id="resolved_config_missing_file",
        ),
        pytest.param("exists", "test_workflow", True, True, id="resolved_config_stored"),
        pytest.param("exists", "my_workflow", True, True, id="resolved_config_labels"),
    ],
)
# Patch: Mock logger to verify logging behavior when resolved config is stored
@patch("sirocco.engines.aiida.builder.logger")
def test_store_window_config_resolved_config_handling(
    mock_logger,
    minimal_workflow,
    tmp_path,
    config_path_type,
    workflow_name,
    should_store_pk,
    should_log,
):
    """Test window config storage with various resolved config scenarios.

    Covers:
    - No resolved config path (None)
    - Config file doesn't exist
    - Config file exists and is stored as SinglefileData
    - Proper labels and logging for stored config
    """
    builder = WorkGraphBuilder(minimal_workflow)
    builder.workflow.name = workflow_name
    builder.launcher_parents = {}

    print("\n=== Test store_window_config resolved config handling ===")
    print(f"Config path type: {config_path_type}")
    print(f"Workflow name: {workflow_name}")
    print(f"Should store PK: {should_store_pk}")
    print(f"Should log: {should_log}")

    # Setup resolved_config_path based on test case
    if config_path_type is None:
        builder.resolved_config_path = None
    elif config_path_type == "nonexistent":
        builder.resolved_config_path = tmp_path / "nonexistent.yml"
    elif config_path_type == "exists":
        config_file = tmp_path / "resolved_config.yml"
        config_file.write_text(f"name: {workflow_name}\nscheduler: slurm")
        builder.resolved_config_path = config_file

    wg = Mock()
    wg.extras = {}

    builder._store_window_config(wg, front_depth=1)

    print("\n=== Window config extras ===")
    pprint(wg.extras)

    # All cases should have window_config
    assert "window_config" in wg.extras

    # Check resolved_config_pk in extras
    if should_store_pk:
        assert "resolved_config_pk" in wg.extras
        node_pk = wg.extras["resolved_config_pk"]

        # Load the real node and verify its properties
        import aiida.orm

        node = aiida.orm.load_node(node_pk)
        assert isinstance(node, aiida.orm.SinglefileData)
        assert node.is_stored

        # Verify labels contain workflow name
        assert workflow_name in node.label
        assert workflow_name in node.description
        assert "Resolved configuration" in node.description
    else:
        assert "resolved_config_pk" not in wg.extras

    # Check logging
    if should_log:
        assert mock_logger.info.called
        log_call = mock_logger.info.call_args[0]
        # Verify the pk was logged
        assert wg.extras["resolved_config_pk"] in log_call or str(wg.extras["resolved_config_pk"]) in str(log_call)
