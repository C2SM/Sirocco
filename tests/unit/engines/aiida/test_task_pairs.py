"""Unit tests for sirocco.engines.aiida.task_pairs module."""

from unittest.mock import Mock, patch

from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    DependencyMapping,
)
from sirocco.engines.aiida.task_pairs import (
    TaskPairContext,
)


class TestTaskPairContext:
    """Test TaskPairContext class."""

    def test_context_encapsulates_state(self):
        """Test that TaskPairContext properly encapsulates state."""

        mock_wg = Mock()
        ctx = TaskPairContext(mock_wg, "test_workflow")

        # Initially empty
        assert ctx.get_dependency_outputs() == {}
        assert ctx.get_dependency_tasks() == {}

    def test_context_add_icon_task_updates_state(self, aiida_localhost):
        """Test that adding ICON task via context updates internal state."""

        from sirocco.engines.aiida.task_pairs import TaskPairContext

        # Mock WorkGraph
        mock_wg = Mock()
        mock_wg.add_task = Mock()

        # Create context
        ctx = TaskPairContext(mock_wg, "test_workflow_123")

        # Create task spec
        task_spec = AiidaIconTaskSpec(
            label="test_icon",
            code_pk=123,
            master_namelist_pk=456,
            model_namelist_pks={},
            metadata=AiidaMetadata(computer=aiida_localhost),
            output_port_mapping={},
        )

        # Mock dependencies
        dependencies = DependencyMapping(port_mapping={}, task_folders={}, task_job_ids={})

        # Patch the build function
        with patch("sirocco.engines.aiida.spec_resolvers.build_icon_task_with_dependencies"):
            # Add task pair
            ctx.add_icon_task_pair("test_icon", task_spec, {}, dependencies)

        # Verify WorkGraph was updated
        assert mock_wg.add_task.call_count == 2  # Launcher + monitor

        # Verify state was updated
        outputs = ctx.get_dependency_outputs()
        tasks = ctx.get_dependency_tasks()

        assert "test_icon" in outputs
        assert "test_icon" in tasks

    def test_context_cleaner_api_than_functions(self):
        """Demonstrate that TaskPairContext provides cleaner API."""

        # Old way: 9 parameters, 2 mutated
        # add_icon_task_pair(wg, wg_name, label, spec, input_data, deps, task_dep_info, prev_dep_tasks)

        # New way: Context encapsulates state
        mock_wg = Mock()
        ctx = TaskPairContext(mock_wg, "workflow")

        # Clear what's being tracked
        assert isinstance(ctx, TaskPairContext)
        assert hasattr(ctx, "add_icon_task_pair")
        assert hasattr(ctx, "add_shell_task_pair")
        assert hasattr(ctx, "get_dependency_outputs")
        assert hasattr(ctx, "get_dependency_tasks")
