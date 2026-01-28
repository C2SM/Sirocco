"""Unit tests for dynamic level computation and front-depth logic.

These tests verify the algorithms for:
- Topological level computation
- Dynamic level recomputation as tasks complete
- Window-size submission logic
- Cross-dependency handling
"""

import pytest

from sirocco.engines.aiida.topology import compute_topological_levels


class TestTopologicalLevels:
    """Test basic topological level computation."""

    def test_simple_linear_chain(self):
        """Test a simple linear dependency chain."""
        task_deps = {
            "task_a": [],
            "task_b": ["task_a"],
            "task_c": ["task_b"],
        }
        levels = compute_topological_levels(task_deps)

        assert levels["task_a"] == 0
        assert levels["task_b"] == 1
        assert levels["task_c"] == 2

    def test_parallel_branches(self):
        """Test two parallel branches from a root task."""
        task_deps = {
            "root": [],
            "fast_1": ["root"],
            "fast_2": ["fast_1"],
            "slow_1": ["root"],
            "slow_2": ["slow_1"],
        }
        levels = compute_topological_levels(task_deps)

        assert levels["root"] == 0
        # Both branches at same topological levels (but will diverge with dynamic levels!)
        assert levels["fast_1"] == 1
        assert levels["slow_1"] == 1
        assert levels["fast_2"] == 2
        assert levels["slow_2"] == 2

    def test_diamond_dependency(self):
        """Test diamond-shaped dependency graph."""
        task_deps = {
            "root": [],
            "left": ["root"],
            "right": ["root"],
            "merge": ["left", "right"],  # Depends on both
        }
        levels = compute_topological_levels(task_deps)

        assert levels["root"] == 0
        assert levels["left"] == 1
        assert levels["right"] == 1
        assert levels["merge"] == 2  # Max(left, right) + 1

    def test_cross_branch_dependency(self):
        """Test cross-dependency between branches."""
        task_deps = {
            "fast_1": [],
            "fast_2": ["fast_1"],
            "medium_1": [],
            "medium_2": ["medium_1", "fast_2"],  # Cross-dependency!
        }
        levels = compute_topological_levels(task_deps)

        assert levels["fast_1"] == 0
        assert levels["medium_1"] == 0
        assert levels["fast_2"] == 1
        assert levels["medium_2"] == 2  # Max(medium_1=0, fast_2=1) + 1

    def test_complex_graph(self):
        """Test more complex dependency graph."""
        task_deps = {
            "a": [],
            "b": ["a"],
            "c": ["a"],
            "d": ["b", "c"],
            "e": ["c"],
            "f": ["d", "e"],
        }
        levels = compute_topological_levels(task_deps)

        assert levels["a"] == 0
        assert levels["b"] == 1
        assert levels["c"] == 1
        assert levels["d"] == 2  # Max(b=1, c=1) + 1
        assert levels["e"] == 2  # c + 1
        assert levels["f"] == 3  # Max(d=2, e=2) + 1


class TestDynamicLevels:
    """Test dynamic level recomputation as tasks complete."""

    def test_levels_update_after_root_completes(self):
        """Test that levels are recomputed after root task completes."""
        # Initial state: all tasks present
        all_deps = {
            "root": [],
            "fast_1": ["root"],
            "fast_2": ["fast_1"],
            "slow_1": ["root"],
        }

        # Static levels
        static_levels = compute_topological_levels(all_deps)
        assert static_levels["fast_1"] == 1
        assert static_levels["slow_1"] == 1

        # After root completes: remove it and recompute
        completed = {"root"}
        remaining_deps = {
            task: [dep for dep in deps if dep not in completed]
            for task, deps in all_deps.items()
            if task not in completed
        }
        dynamic_levels = compute_topological_levels(remaining_deps)

        # Now fast_1 and slow_1 have no dependencies -> level 0
        assert dynamic_levels["fast_1"] == 0
        assert dynamic_levels["slow_1"] == 0
        assert dynamic_levels["fast_2"] == 1  # Still depends on fast_1

    def test_fast_branch_advances_independently(self):
        """Test that fast branch can advance while slow branch is running."""
        all_deps = {
            "root": [],
            "fast_1": ["root"],
            "fast_2": ["fast_1"],
            "fast_3": ["fast_2"],
            "slow_1": ["root"],
            "slow_2": ["slow_1"],
            "slow_3": ["slow_2"],
        }

        # After root and fast_1 complete
        completed = {"root", "fast_1"}
        remaining_deps = {
            task: [dep for dep in deps if dep not in completed]
            for task, deps in all_deps.items()
            if task not in completed
        }
        dynamic_levels = compute_topological_levels(remaining_deps)

        # Fast branch advances
        assert dynamic_levels["fast_2"] == 0  # No dependencies left!
        assert dynamic_levels["fast_3"] == 1  # Depends on fast_2

        # Slow branch still has dependencies
        assert dynamic_levels["slow_1"] == 0  # root is done
        assert dynamic_levels["slow_2"] == 1  # Depends on slow_1
        assert dynamic_levels["slow_3"] == 2  # Depends on slow_2

    def test_cross_dependency_blocks_advancement(self):
        """Test that cross-dependencies properly block task advancement."""
        all_deps = {
            "fast_1": [],
            "fast_2": ["fast_1"],
            "medium_1": [],
            "medium_2": ["medium_1", "fast_2"],  # Cross-dep!
        }

        # After fast_1 completes (but medium_1 still running)
        completed = {"fast_1"}
        remaining_deps = {
            task: [dep for dep in deps if dep not in completed]
            for task, deps in all_deps.items()
            if task not in completed
        }
        dynamic_levels = compute_topological_levels(remaining_deps)

        assert dynamic_levels["fast_2"] == 0  # No dependencies left
        assert dynamic_levels["medium_1"] == 0  # No dependencies
        assert dynamic_levels["medium_2"] == 1  # Still depends on medium_1 (fast_2 is done)

        # After both fast_1 and medium_1 complete
        completed = {"fast_1", "medium_1"}
        remaining_deps = {
            task: [dep for dep in deps if dep not in completed]
            for task, deps in all_deps.items()
            if task not in completed
        }
        dynamic_levels = compute_topological_levels(remaining_deps)

        assert dynamic_levels["fast_2"] == 0
        assert dynamic_levels["medium_2"] == 1  # Max(fast_2) + 1


class TestWindowSizeLogic:
    """Test front-depth submission logic."""

    def test_front_depth_zero_sequential(self):
        """Test that front_depth=0 means sequential execution."""
        # With front_depth=0, can only submit tasks at current level
        current_max_level = 0
        front_depth = 0

        task_levels = {"task_a": 0, "task_b": 1, "task_c": 2}

        # Can only submit level 0 tasks
        submittable = {task for task, level in task_levels.items() if level <= current_max_level + front_depth}

        assert "task_a" in submittable
        assert "task_b" not in submittable
        assert "task_c" not in submittable

    def test_front_depth_one_ahead(self):
        """Test that front_depth=1 allows submitting 1 level ahead."""
        current_max_level = 0
        front_depth = 1

        task_levels = {"task_a": 0, "task_b": 1, "task_c": 2}

        # Can submit levels 0 and 1
        submittable = {task for task, level in task_levels.items() if level <= current_max_level + front_depth}

        assert "task_a" in submittable
        assert "task_b" in submittable  # Pre-submission!
        assert "task_c" not in submittable

    def test_front_depth_two_aggressive(self):
        """Test that front_depth=2 allows submitting 2 levels ahead."""
        current_max_level = 0
        front_depth = 2

        task_levels = {"task_a": 0, "task_b": 1, "task_c": 2, "task_d": 3}

        # Can submit levels 0, 1, and 2
        submittable = {task for task, level in task_levels.items() if level <= current_max_level + front_depth}

        assert "task_a" in submittable
        assert "task_b" in submittable
        assert "task_c" in submittable  # Very aggressive pre-submission!
        assert "task_d" not in submittable

    def test_window_advances_with_max_level(self):
        """Test that window advances as max running level increases."""
        front_depth = 1
        task_levels = {"task_a": 0, "task_b": 1, "task_c": 2, "task_d": 3}

        # Initially, max level is 0
        current_max_level = 0
        submittable = {task for task, level in task_levels.items() if level <= current_max_level + front_depth}
        assert submittable == {"task_a", "task_b"}

        # After task_a starts, max level is still 0
        # But after task_b starts, max level becomes 1
        current_max_level = 1
        submittable = {task for task, level in task_levels.items() if level <= current_max_level + front_depth}
        assert submittable == {"task_a", "task_b", "task_c"}


class TestComplexScenarios:
    """Test complex scenarios combining multiple features."""

    def test_branch_independence_simulation(self):
        """Simulate the dynamic-simple test case."""
        # Initial workflow
        all_deps = {
            "root": [],
            "fast_1": ["root"],
            "fast_2": ["fast_1"],
            "fast_3": ["fast_2"],
            "slow_1": ["root"],
            "slow_2": ["slow_1"],
            "slow_3": ["slow_2"],
        }

        # Simulate execution sequence
        completed_tasks = set()
        execution_sequence = []

        # Root completes
        completed_tasks.add("root")
        remaining = {
            t: [d for d in deps if d not in completed_tasks] for t, deps in all_deps.items() if t not in completed_tasks
        }
        levels = compute_topological_levels(remaining)
        execution_sequence.append(("root_done", dict(levels)))

        # Fast_1 completes (slow_1 still running)
        completed_tasks.add("fast_1")
        remaining = {
            t: [d for d in deps if d not in completed_tasks] for t, deps in all_deps.items() if t not in completed_tasks
        }
        levels = compute_topological_levels(remaining)
        execution_sequence.append(("fast_1_done", dict(levels)))

        # Verify: fast_2 should be level 0, while slow_2 is level 1
        assert levels["fast_2"] == 0  # Fast branch advances!
        assert levels["slow_2"] == 1  # Slow branch still waiting

        # Fast_2 completes
        completed_tasks.add("fast_2")
        remaining = {
            t: [d for d in deps if d not in completed_tasks] for t, deps in all_deps.items() if t not in completed_tasks
        }
        levels = compute_topological_levels(remaining)

        # Verify: fast_3 should be level 0
        assert levels["fast_3"] == 0  # Fast branch continues advancing!

    def test_complex_workflow_with_cross_deps(self):
        """Simulate the complex workflow (3 branches + cross-deps)."""
        all_deps = {
            "setup": [],
            "fast_1": ["setup"],
            "fast_2": ["fast_1"],
            "fast_3": ["fast_2"],
            "medium_1": ["setup"],
            "medium_2": ["medium_1", "fast_2"],  # Cross-dep!
            "medium_3": ["medium_2"],
            "slow_1": ["setup"],
            "slow_2": ["slow_1", "medium_2"],  # Cross-dep!
            "slow_3": ["slow_2"],
            "finalize": ["fast_3", "medium_3", "slow_3"],  # Convergence!
        }

        # After setup and fast_1 complete
        completed = {"setup", "fast_1"}
        remaining = {t: [d for d in deps if d not in completed] for t, deps in all_deps.items() if t not in completed}
        levels = compute_topological_levels(remaining)

        # Fast branch advances
        assert levels["fast_2"] == 0

        # Medium branch can't start medium_2 yet (needs fast_2)
        assert levels["medium_1"] == 0
        assert levels["medium_2"] == 1  # Still waits for fast_2

        # Slow branch independent for now
        assert levels["slow_1"] == 0

        # After fast_2 also completes
        completed.add("fast_2")
        remaining = {t: [d for d in deps if d not in completed] for t, deps in all_deps.items() if t not in completed}
        levels = compute_topological_levels(remaining)

        # Now medium_2 can potentially start (if medium_1 is done)
        assert levels["fast_3"] == 0
        # medium_2 level depends on whether medium_1 is done


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
