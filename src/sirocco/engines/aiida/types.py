"""Type definitions for Sirocco's AiiDA engine module.

This module contains only pure type definitions (type aliases and Protocols).
Data Transfer Objects are in dto.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import aiida.orm

if TYPE_CHECKING:
    from sirocco.engines.aiida.models import DependencyInfo


class WgSocketValue(Protocol):
    """Protocol for WorkGraph sockets."""

    @property
    def value(self) -> Any:
        """The wrapped value (e.g., RemoteData PK or job_id)."""
        ...


class WgTaskProtocol(Protocol):
    """Protocol for WorkGraph task node.

    WorkGraph tasks are runtime objects that support chaining via >> operator.
    """

    outputs: WgMonitorOutputs

    def __rshift__(self, other: WgTaskProtocol) -> WgTaskProtocol:
        """Chain this task to run before another task.

        Returns the other task to allow chaining: task1 >> task2 >> task3
        """
        ...


class WgMonitorOutputs(Protocol):
    """Protocol for our WorkGraph task monitor outputs."""

    remote_folder: WgSocketValue  # WgSocketValue wrapping RemoteData PK
    job_id: WgSocketValue  # WgSocketValue wrapping int job_id


type FileNode = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
"""Union of AiiDA file/data node types."""

type PortDataMapping = dict[str, FileNode]
"""Maps port/label names to data nodes."""

type TaskMonitorOutputsMapping = dict[str, WgMonitorOutputs]
"""Maps task_label -> WorkGraph monitor outputs namespace with remote_folder and job_id."""

type TaskDependencyMapping = dict[str, WgTaskProtocol]
"""Maps task_label -> task object for chaining execution order."""

type PortDependencyMapping = dict[str, list[DependencyInfo]]
"""Maps port name -> list of dependencies for that port."""

type TaskFolderMapping = dict[str, WgSocketValue]
"""Maps task_label -> WgSocketValue containing RemoteData PK."""

type TaskJobIdMapping = dict[str, WgSocketValue]
"""Maps task_label -> WgSocketValue containing SLURM job_id (int)."""

type LauncherParentsMapping = dict[str, list[str]]
"""Maps launcher_name -> list of parent launcher names."""
