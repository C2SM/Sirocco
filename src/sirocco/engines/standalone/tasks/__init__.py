"""Built-in plugin support for the standalone engine.

Importing a module below runs its `@<handler>.register` decorators, which is
what makes the engine able to handle that task type. A third party plugin
would do the same from its own package.
"""

from . import icon, shell
from .sirocco_task import SiroccoContinueTask

__all__ = ["SiroccoContinueTask", "icon", "shell"]
