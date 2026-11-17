"""Built-in plugin support for the standalone engine.

Importing a module below defines its `PluginHandler` subclass, which registers it for
its task type: this is what makes the engine able to handle that task type. A third
party plugin would do the same from its own package.
"""

from .icon import IconHandler
from .shell import ShellHandler
from .sirocco_task import SiroccoContinueTask, SiroccoTaskHandler

__all__ = ["IconHandler", "ShellHandler", "SiroccoContinueTask", "SiroccoTaskHandler"]
