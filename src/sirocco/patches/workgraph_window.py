"""Patch aiida-workgraph to add rolling window functionality.

This patch adds a dynamic task windowing system to WorkGraph's TaskManager,
which allows controlling the number of concurrent task submissions based on
topological levels. This is particularly useful for managing resource usage
in large workflows.

Key features:
- Dynamic level computation: Task levels are recomputed as tasks complete,
  allowing faster branches to advance independently
- Configurable window size: Control how many levels can be active simultaneously
- Optional max_queued_jobs limit: Cap total concurrent submissions
- Transparent: When disabled, behaves identically to unpatched WorkGraph

This patch is intended for use with Sirocco workflows that may have hundreds
or thousands of tasks and need fine-grained submission control.
"""

import logging

logger = logging.getLogger(__name__)


def patch_workgraph_window():
    """Apply rolling window functionality to aiida-workgraph TaskManager."""
    try:
        from aiida_workgraph.engine.task_manager import TaskManager
        from aiida_workgraph.workgraph import WorkGraph
    except ImportError:
        logger.debug("aiida-workgraph not installed, skipping window patch")
        return

    # Store original methods
    original_task_manager_init = TaskManager.__init__
    original_task_manager_continue_workgraph = TaskManager.continue_workgraph
    original_workgraph_init = WorkGraph.__init__
    original_workgraph_to_dict = WorkGraph.to_dict
    original_workgraph_from_dict = WorkGraph.from_dict

    # ========================================================================
    # Patched WorkGraph methods
    # ========================================================================

    def patched_workgraph_init(self, name=None, **kwargs):
        """Initialize WorkGraph with extras dict for window config storage."""
        original_workgraph_init(self, name=name, **kwargs)
        self.extras = {}  # Initialize extras dict for custom metadata

    def patched_workgraph_to_dict(self, include_sockets=False, should_serialize=False):
        """Serialize WorkGraph including extras dict."""
        result = original_workgraph_to_dict(self, include_sockets=include_sockets, should_serialize=should_serialize)
        result['extras'] = getattr(self, 'extras', {})  # Serialize extras dict
        return result

    @classmethod
    def patched_workgraph_from_dict(cls, data, *args, **kwargs):
        """Deserialize WorkGraph and restore extras dict."""
        wg = original_workgraph_from_dict(data, *args, **kwargs)
        # Restore extras dict (for window_config)
        if 'extras' in data:
            wg.extras = data['extras']
        return wg

    # ========================================================================
    # Patched TaskManager methods
    # ========================================================================

    def patched_task_manager_init(self, ctx_manager, logger, runner, process, awaitable_manager):
        """Initialize TaskManager with window state management."""
        original_task_manager_init(self, ctx_manager, logger, runner, process, awaitable_manager)

        # Initialize window state with defaults (will be loaded from WorkGraph context later)
        self.window_config = {
            'enabled': False,
            'window_size': float('inf'),
            'task_dependencies': {},
        }
        self.window_state = {
            'min_active_level': 0,
            'max_allowed_level': float('inf'),
            'dynamic_task_levels': {},
        }
        self._window_initialized = False

    def _init_window_state(self):
        """Initialize window state from WorkGraph context."""
        # Check if WorkGraph is available yet
        if not hasattr(self.process, 'wg') or self.process.wg is None:
            self.logger.debug("WorkGraph not available yet for window initialization")
            return  # WorkGraph not loaded yet, use defaults

        if self._window_initialized:
            return  # Already initialized

        # Load window config from WorkGraph extras (persisted with the WorkGraph)
        window_config = getattr(self.process.wg, 'extras', {}).get('window_config', {})
        self.logger.debug(f"Initializing window state, config: {window_config}")

        self.window_config = {
            'enabled': window_config.get('enabled', False),
            'window_size': window_config.get('window_size', float('inf')),
            'max_queued_jobs': window_config.get('max_queued_jobs', None),
            'task_dependencies': window_config.get('task_dependencies', {}),
        }

        # Initialize window state
        if self.window_config['enabled']:
            self.window_state = {
                'min_active_level': 0,
                'max_allowed_level': self.window_config['window_size'],
                'dynamic_task_levels': self._compute_dynamic_levels(),
            }
        else:
            self.window_state = {
                'min_active_level': 0,
                'max_allowed_level': float('inf'),
                'dynamic_task_levels': {},
            }

        self._window_initialized = True

    def _compute_dynamic_levels(self):
        """Compute task levels based on current unfinished tasks only.

        Key idea: Exclude FINISHED/FAILED/SKIPPED tasks from dependency graph,
        then run BFS to compute levels. This allows faster branches to collapse
        to lower levels as their dependencies complete.

        Returns:
            Dict mapping task_name -> current dynamic level
        """
        from collections import deque

        if not self.window_config['enabled']:
            return {}

        task_deps = self.window_config['task_dependencies']

        # Step 1: Filter to only unfinished tasks
        unfinished_tasks = set()
        for task_name in task_deps.keys():
            state = self.state_manager.get_task_runtime_info(task_name, 'state')
            if state not in ['FINISHED', 'FAILED', 'SKIPPED']:
                unfinished_tasks.add(task_name)

        # Step 2: Build filtered dependency graph (only unfinished tasks)
        filtered_deps = {}
        for task_name in unfinished_tasks:
            unfinished_parents = [
                p for p in task_deps[task_name]
                if p in unfinished_tasks
            ]
            filtered_deps[task_name] = unfinished_parents

        # Step 3: Compute levels using BFS (same algorithm as compute_topological_levels)
        levels = {}
        in_degree = {task: len(parents) for task, parents in filtered_deps.items()}

        # Find all tasks with no unfinished dependencies (level 0)
        queue = deque([task for task, degree in in_degree.items() if degree == 0])
        for task_name in queue:
            levels[task_name] = 0

        # Build reverse dependency graph
        children = {task: [] for task in filtered_deps}
        for task_name, parents in filtered_deps.items():
            for parent in parents:
                if parent not in children:
                    children[parent] = []
                children[parent].append(task_name)

        # Process tasks in topological order
        processed = set()
        while queue:
            current = queue.popleft()
            processed.add(current)

            for child in children.get(current, []):
                parents = filtered_deps[child]
                if all(p in processed for p in parents):
                    parent_levels = [levels[p] for p in parents]
                    levels[child] = max(parent_levels) + 1 if parent_levels else 0
                    queue.append(child)

        return levels

    def _update_window(self):
        """Update the active window based on task completion.

        Recomputes dynamic levels after each task completion to allow
        faster branches to advance independently.
        """
        if not self.window_config['enabled']:
            return

        # RECOMPUTE DYNAMIC LEVELS based on current task states
        self.window_state['dynamic_task_levels'] = self._compute_dynamic_levels()

        # Find minimum level of active (CREATED/RUNNING) launcher tasks
        active_levels = []
        for task_name, level in self.window_state['dynamic_task_levels'].items():
            state = self.state_manager.get_task_runtime_info(task_name, 'state')
            if state in ['CREATED', 'RUNNING']:
                active_levels.append(level)

        if not active_levels:
            # No active tasks - advance window to next pending level
            old_min = self.window_state['min_active_level']
            # Find next level with pending tasks
            if self.window_state['dynamic_task_levels']:
                max_level = max(self.window_state['dynamic_task_levels'].values())
                for level in range(old_min, max_level + 1):
                    tasks_at_level = [
                        name for name, lvl in self.window_state['dynamic_task_levels'].items()
                        if lvl == level
                    ]
                    if tasks_at_level:
                        # Check if any task at this level is not finished
                        has_pending = any(
                            self.state_manager.get_task_runtime_info(name, 'state')
                            not in ['FINISHED', 'FAILED', 'SKIPPED']
                            for name in tasks_at_level
                        )
                        if has_pending:
                            self.window_state['min_active_level'] = level
                            break
                else:
                    # All tasks finished, keep current min
                    self.window_state['min_active_level'] = old_min
            else:
                # No tasks in dynamic levels (all finished), keep current min
                self.window_state['min_active_level'] = old_min
        else:
            # Set min_active_level to minimum of active tasks
            self.window_state['min_active_level'] = min(active_levels)

        # Update max_allowed_level
        window_size = self.window_config['window_size']
        self.window_state['max_allowed_level'] = (
            self.window_state['min_active_level'] + window_size
        )

    def _is_task_in_window(self, task_name):
        """Check if task is within the active submission window."""
        if not self.window_config['enabled']:
            return True  # No windowing, all tasks allowed

        # get_job_data tasks and other non-launcher tasks are always allowed
        if not task_name.startswith('launch_'):
            return True

        # Check dynamic topological level
        task_level = self.window_state['dynamic_task_levels'].get(task_name)
        if task_level is None:
            # Task not in level mapping - allow it
            return True

        if task_level > self.window_state['max_allowed_level']:
            return False  # Outside window

        # Check max_queued_jobs threshold if configured
        if self.window_config.get('max_queued_jobs'):
            active_count = self._count_active_jobs()
            if active_count >= self.window_config['max_queued_jobs']:
                return False  # Too many jobs already

        return True

    def _count_active_jobs(self):
        """Count tasks in CREATED or RUNNING state."""
        count = 0
        for task in self.process.wg.tasks:
            state = self.state_manager.get_task_runtime_info(task.name, 'state')
            if state in ['CREATED', 'RUNNING']:
                count += 1
        return count

    def patched_continue_workgraph(self):
        """Resume the WorkGraph with rolling window management.

        This wraps the original continue_workgraph to add:
        1. Window state initialization (lazy)
        2. Window updates before each task submission cycle
        3. Window-aware task filtering (only submit tasks within window)
        4. Reporting of window state and skipped tasks
        """
        # Initialize window state if not already done (lazy initialization)
        self._init_window_state()

        # Update window state if rolling window is enabled
        if self.window_config.get('enabled'):
            self._update_window()
            # Report window state
            if self.window_state['dynamic_task_levels']:
                active_count = self._count_active_jobs()
                max_level = max(self.window_state['dynamic_task_levels'].values()) if self.window_state['dynamic_task_levels'] else 0
                self.process.report(
                    f"Window: levels {self.window_state['min_active_level']}-"
                    f"{self.window_state['max_allowed_level']} (max dynamic level: {max_level}), "
                    f"active jobs: {active_count}"
                )

        # Collect tasks ready to run, filtering by window
        task_to_run = []
        skipped_by_window = []
        for task in self.process.wg.tasks:
            # Skip tasks that are already in progress, finished, or already executed
            if (
                self.state_manager.get_task_runtime_info(task.name, 'state')
                in [
                    'CREATED',
                    'RUNNING',
                    'FINISHED',
                    'FAILED',
                    'SKIPPED',
                    'MAPPED',
                ]
                or task.name in self.ctx._executed_tasks
            ):
                continue
            ready, _ = self.state_manager.is_task_ready_to_run(task.name)
            if ready:
                # Check if task is within active window
                if self._is_task_in_window(task.name):
                    task_to_run.append(task.name)
                else:
                    skipped_by_window.append(task.name)

        # Report tasks
        self.process.report('tasks ready to run: {}'.format(','.join(task_to_run)))
        if skipped_by_window:
            self.process.report('tasks skipped (outside window): {}'.format(','.join(skipped_by_window)))

        # Run the tasks
        self.run_tasks(task_to_run)

    # ========================================================================
    # Apply all patches
    # ========================================================================

    # Patch WorkGraph
    WorkGraph.__init__ = patched_workgraph_init
    WorkGraph.to_dict = patched_workgraph_to_dict
    WorkGraph.from_dict = classmethod(patched_workgraph_from_dict.__func__)

    # Patch TaskManager
    TaskManager.__init__ = patched_task_manager_init
    TaskManager.continue_workgraph = patched_continue_workgraph
    TaskManager._init_window_state = _init_window_state
    TaskManager._compute_dynamic_levels = _compute_dynamic_levels
    TaskManager._update_window = _update_window
    TaskManager._is_task_in_window = _is_task_in_window
    TaskManager._count_active_jobs = _count_active_jobs

    logger.info("Applied aiida-workgraph rolling window patches (TaskManager, WorkGraph)")
