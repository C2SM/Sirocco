"""Standalone engine: orchestrates a core workflow with the machine scheduler only.

Importing this package registers the handlers of all built-in task plugins (see
`plugin_handlers/`).
"""

from .engine import StandaloneEngine, WorkflowStatus
from .plugin_handlers import SiroccoContinueTask  # importing `plugin_handlers` registers the built-in handlers

__all__ = ["SiroccoContinueTask", "StandaloneEngine", "WorkflowStatus"]
