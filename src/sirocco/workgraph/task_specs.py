"""Task specification dataclasses and serialization utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    pass


# =============================================================================
# Data Structures
# =============================================================================


@dataclass(frozen=True)
class DependencyInfo:
    """Information about a task dependency.

    Attributes:
        dep_label: Label of the task that produces this dependency
        filename: Optional filename within the remote folder (None = use whole folder)
        data_label: Label of the data item being consumed
    """

    dep_label: str
    filename: str | None
    data_label: str

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "dep_label": self.dep_label,
            "filename": self.filename,
            "data_label": self.data_label,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            dep_label=data["dep_label"],
            filename=data["filename"],
            data_label=data["data_label"],
        )


@dataclass(frozen=True)
class BaseDataInfo:
    name: str
    coordinates: dict
    label: str
    path: str


@dataclass(frozen=True)
class InputDataInfo(BaseDataInfo):
    port: str
    is_available: bool

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "port": self.port,
            "name": self.name,
            "coordinates": self.coordinates,
            "label": self.label,
            "is_available": self.is_available,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            port=data["port"],
            name=data["name"],
            coordinates=data["coordinates"],
            label=data["label"],
            is_available=data["is_available"],
            path=data["path"],
        )


@dataclass(frozen=True)
class OutputDataInfo(BaseDataInfo):
    port: str | None

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "name": self.name,
            "coordinates": self.coordinates,
            "label": self.label,
            "path": self.path,
            "port": self.port,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """Create from dict."""
        return cls(
            name=data["name"],
            coordinates=data["coordinates"],
            label=data["label"],
            path=data["path"],
            port=data["port"],
        )


# Type Aliases for Complex Mappings
type PortToDependencies = dict[str, list[DependencyInfo]]
type ParentFolders = dict[str, Any]  # {dep_label: TaggedValue with .value = int PK}
type JobIds = dict[str, Any]  # {dep_label: TaggedValue with .value = int job_id}
type TaskDepInfo = dict[str, Any]  # {task_label: namespace with .remote_folder, .job_id}
type LauncherDependencies = dict[str, list[str]]  # {launcher_name: [parent_launcher_names]}


# =============================================================================
# Helper Functions - Dataclass Serialization
# =============================================================================


def port_to_dependencies_to_dict(
    port_to_dep: PortToDependencies,
) -> dict[str, list[dict]]:
    """Convert PortToDependencies to JSON-serializable dict.

    Args:
        port_to_dep: PortToDependencies mapping

    Returns:
        Dict with list of dict values
    """
    return {port: [dep.to_dict() for dep in deps] for port, deps in port_to_dep.items()}


def port_to_dependencies_from_dict(data: dict[str, list[dict]]) -> PortToDependencies:
    """Convert dict to PortToDependencies.

    Args:
        data: Dict representation

    Returns:
        PortToDependencies mapping
    """
    return {port: [DependencyInfo.from_dict(dep) for dep in deps] for port, deps in data.items()}
