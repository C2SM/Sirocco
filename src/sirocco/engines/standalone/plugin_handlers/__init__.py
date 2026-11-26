"""Task handlers of the standalone engine: what it does with each kind of task.

`plugin_handler.py` defines the `PluginHandler` base class. Importing a module below
defines its `PluginHandler` subclass, which registers it for its task type: this is what
makes the engine able to handle that task type. A third party plugin would do the same
from its own package.
"""

from .icon_handler import IconHandler
from .plugin_handler import PluginHandler
from .shell_handler import ShellHandler
from .sirocco_task_handler import SiroccoContinueTask, SiroccoTaskHandler

__all__ = ["IconHandler", "PluginHandler", "ShellHandler", "SiroccoContinueTask", "SiroccoTaskHandler"]
