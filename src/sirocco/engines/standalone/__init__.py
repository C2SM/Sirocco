"""Standalone engine: orchestrates a core workflow with the machine scheduler only.

Importing this package registers the handlers of all built-in task plugins (see
`plugin_handlers/`).
"""

from .engine import StandaloneEngine, clean_up, stop
from .plugin_handlers import SiroccoContinueTask  # importing `plugin_handlers` registers the built-in handlers
from .state import WorkflowState, WorkflowStatus
from .transaction import CommandOutcome, Outcome, Transaction

__all__ = [
    "CommandOutcome",
    "Outcome",
    "SiroccoContinueTask",
    "StandaloneEngine",
    "Transaction",
    "WorkflowState",
    "WorkflowStatus",
    "clean_up",
    "stop",
]
