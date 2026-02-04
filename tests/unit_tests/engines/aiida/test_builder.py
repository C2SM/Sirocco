"""Unit tests for sirocco.engines.aiida.builder module."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

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

    @patch("aiida.orm.load_code")
    @patch("aiida.orm.Computer.collection.get")
    @pytest.mark.parametrize("front_depth", [1, 2, 3])
    def test_build_smoke_test(self, mock_get_computer, mock_load_code, minimal_workflow, front_depth):
        """Smoke test: verify build() doesn't crash and returns a WorkGraph."""
        from aiida_workgraph import WorkGraph

        # Setup AiiDA mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_computer.get_transport_class.return_value = Mock()
        mock_get_computer.return_value = mock_computer

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

    @patch("aiida.orm.load_code")
    @patch("aiida.orm.Computer.collection.get")
    def test_build_with_dependencies(self, mock_get_computer, mock_load_code, workflow_with_dependencies):
        """Test build() correctly processes workflows with task dependencies."""
        from aiida_workgraph import WorkGraph

        # Setup AiiDA mocks
        mock_computer = Mock()
        mock_computer.label = "localhost"
        mock_computer.get_transport_class.return_value = Mock()
        mock_get_computer.return_value = mock_computer

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
@patch("sirocco.engines.aiida.builder.WorkGraphBuilder")
def test_build_sirocco_workgraph(
    mock_builder_class,
    minimal_workflow,
    front_depth,
    expected_build_arg,
):
    """Test building WorkGraph with various front_depth values."""
    mock_builder = Mock()
    mock_wg = Mock()
    mock_builder.build.return_value = mock_wg
    mock_builder_class.return_value = mock_builder

    # Build kwargs for the function call
    kwargs = {}
    if front_depth is not None:
        kwargs["front_depth"] = front_depth

    wg = build_sirocco_workgraph(minimal_workflow, **kwargs)

    # Verify builder initialization - no resolved_config_path parameter
    mock_builder_class.assert_called_once_with(minimal_workflow)

    # Verify build call
    mock_builder.build.assert_called_once_with(expected_build_arg)
    assert wg == mock_wg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
