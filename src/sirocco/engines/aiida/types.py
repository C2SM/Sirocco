"""Type definitions for Sirocco's AiiDA engine module.

This module contains only pure type definitions (type aliases and Protocols).
Data Transfer Objects are in dto.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypedDict

import aiida.orm

if TYPE_CHECKING:
    from sirocco.engines.aiida.models import DependencyInfo

__all__ = [
    "FileNode",
    "LauncherParentsMapping",
    "PortDataMapping",
    "PortDependencyMapping",
    "SerializedDependencyInfo",
    "SerializedInputDataInfo",
    "SerializedOutputDataInfo",
    "TaskDependencyMapping",
    "TaskFolderMapping",
    "TaskJobIdMapping",
    "TaskMonitorOutputsMapping",
    "WgMonitorOutputs",
    "WgSocketValue",
    "WgTaskProtocol",
]

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


# Serialized data structures (TypedDicts for .model_dump() output)
class SerializedInputDataInfo(TypedDict):
    """Serialized InputDataInfo after .model_dump().

    This is the dict representation of InputDataInfo that gets stored in task specs
    and passed through WorkGraph serialization.
    """

    port: str
    name: str
    coordinates: dict[str, Any]
    label: str
    is_available: bool
    path: str


class SerializedOutputDataInfo(TypedDict):
    """Serialized OutputDataInfo after .model_dump().

    This is the dict representation of OutputDataInfo that gets stored in task specs
    and passed through WorkGraph serialization.
    """

    port: str | None
    name: str
    coordinates: dict[str, Any]
    label: str
    path: str


class SerializedDependencyInfo(TypedDict):
    """Serialized DependencyInfo after .model_dump().

    This is the dict representation of DependencyInfo that gets added to task specs
    at runtime by the launcher.
    """

    dep_label: str
    filename: str | None
    data_label: str
