"""Standalone engine: orchestrates a core workflow with the machine scheduler only.

Importing this package registers the handlers of all built-in task plugins (see
`plugin_handlers/`).
"""

from .engine import StandaloneEngine, WorkflowStatus
from .plugin_handlers import (  # importing `plugin_handlers` registers the built-in handlers
    SiroccoContinueTask,
    SiroccoStartTask,
    SiroccoTask,
)

__all__ = ["SiroccoContinueTask", "SiroccoStartTask", "SiroccoTask", "StandaloneEngine", "WorkflowStatus"]
