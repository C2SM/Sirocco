"""Unit tests for sirocco.engines.aiida.task_pairs module."""

from unittest.mock import Mock, patch

from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    DependencyMapping,
)
from sirocco.engines.aiida.task_pairs import (
    add_icon_task_pair,
)


class TestAddTaskPairFunctions:
    """Test add_icon_task_pair and add_shell_task_pair functions."""

    def test_add_icon_task_pair(self, aiida_localhost):
        """Test add_icon_task_pair creates launcher and monitor tasks."""

        # Create a mock WorkGraph
        # Patch: Mock build_icon_task_with_dependencies to isolate add_icon_task_pair logic
        with patch("sirocco.engines.aiida.spec_resolvers.build_icon_task_with_dependencies"):
            mock_wg = Mock()
            mock_wg.add_task = Mock()

            # Create task spec
            task_spec = AiidaIconTaskSpec(
                label="test_icon",
                code_pk=123,
                master_namelist_pk=456,
                model_namelist_pks={},
                metadata=AiidaMetadata(computer=aiida_localhost),
                output_port_mapping={},
            )

            # Create empty dependencies
            dependencies = DependencyMapping(port_mapping={}, task_folders={}, task_job_ids={})

            # Tracking dicts
            task_dep_info = {}
            prev_dep_tasks = {}

            # Call add_icon_task_pair
            add_icon_task_pair(
                wg=mock_wg,
                wg_name="test_wg",
                task_label="test_icon",
                task_spec=task_spec,
                input_data_for_task={},
                dependencies=dependencies,
                task_monitor_outputs=task_dep_info,
                dependency_tasks=prev_dep_tasks,
            )

            print("\n=== Add ICON task pair ===")
            print(f"WorkGraph add_task call count: {mock_wg.add_task.call_count}")
            print(f"Task dep info: {task_dep_info}")
            print(f"Prev dep tasks: {prev_dep_tasks}")

            # Should have created two tasks: launcher and monitor
            assert mock_wg.add_task.call_count == 2

            # Should have updated task_dep_info
            assert "test_icon" in task_dep_info

            # Should have updated prev_dep_tasks
            assert "test_icon" in prev_dep_tasks


class TestTaskPairContext:
    """Test TaskPairContext class."""

    def test_context_encapsulates_state(self):
        """Test that TaskPairContext properly encapsulates state."""
        from unittest.mock import Mock

        from sirocco.engines.aiida.task_pairs import TaskPairContext

        mock_wg = Mock()
        ctx = TaskPairContext(mock_wg, "test_workflow")

        # Initially empty
        assert ctx.get_dependency_outputs() == {}
        assert ctx.get_dependency_tasks() == {}

    def test_context_add_icon_task_updates_state(self, aiida_localhost):
        """Test that adding ICON task via context updates internal state."""
        from unittest.mock import Mock, patch

        from sirocco.engines.aiida.models import (
            AiidaIconTaskSpec,
            AiidaMetadata,
            DependencyMapping,
        )
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
        from unittest.mock import Mock

        from sirocco.engines.aiida.task_pairs import TaskPairContext

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
