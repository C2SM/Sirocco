"""Standalone engine: orchestrates a core workflow with the machine scheduler only.

Importing this package registers the standalone handlers of all built-in task
plugins (see `handlers.py` and `tasks/`).
"""

from .engine import StandaloneEngine, WorkflowStatus
from .tasks import SiroccoContinueTask  # importing `tasks` registers the built-in plugin handlers

__all__ = ["SiroccoContinueTask", "StandaloneEngine", "WorkflowStatus"]
