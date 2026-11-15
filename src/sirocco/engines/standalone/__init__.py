"""Standalone engine: orchestrates a core workflow with the machine scheduler only.

Importing this package registers the standalone handlers of all built-in task
plugins (see ``handlers.py`` and ``tasks/``).
"""

from . import tasks  # noqa: F401  # registers built-in plugin handlers
from .engine import StandaloneEngine, WorkflowStatus
from .tasks import SiroccoContinueTask

__all__ = ["SiroccoContinueTask", "StandaloneEngine", "WorkflowStatus"]
