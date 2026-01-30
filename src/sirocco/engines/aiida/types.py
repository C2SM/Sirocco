"""Centralized type definitions for the AiiDA engine."""

from __future__ import annotations

from enum import Enum, auto
from typing import Any, NamedTuple, Protocol

import aiida.orm
from pydantic import BaseModel, ConfigDict, field_validator


class AiidaFileNodeType(Enum):
    """Type of AiiDA file/data node to create."""

    REMOTE = auto()  # RemoteData - for remote execution
    SINGLE_FILE = auto()  # SinglefileData - for local files
    FOLDER = auto()  # FolderData - for local directories


class TaggedValue(Protocol):
    """Protocol for WorkGraph TaggedValue wrapper.

    TaggedValue objects wrap output values from completed tasks,
    containing both the value and metadata about the output socket.
    """

    @property
    def value(self) -> Any:
        """The wrapped value (e.g., RemoteData PK or job_id)."""
        ...


class WorkGraphTask(Protocol):
    """Protocol for WorkGraph task node.

    WorkGraph tasks are runtime objects that support chaining via >> operator.
    """

    outputs: WorkGraphOutputs

    def __rshift__(self, other: WorkGraphTask) -> None:
        """Chain this task to run before another task."""
        ...


class WorkGraphOutputs(Protocol):
    """Protocol for WorkGraph task output namespace.

    Output namespaces contain TaggedValue objects that reference
    remote folders and job IDs from completed tasks.
    """

    remote_folder: TaggedValue  # TaggedValue wrapping RemoteData PK
    job_id: TaggedValue  # TaggedValue wrapping int job_id


type AiidaFileNode = aiida.orm.RemoteData | aiida.orm.SinglefileData | aiida.orm.FolderData
"""Union of AiiDA file/data node types."""

type AiidaDataNodeMapping = dict[str, AiidaFileNode]
"""Maps port/label names to AiiDA data nodes."""

type DependencyOutputs = dict[str, WorkGraphOutputs]
"""Maps task_label -> outputs namespace for connecting dependencies."""

type DependencyTasks = dict[str, WorkGraphTask]
"""Maps task_label -> task object for chaining execution order."""

type PortToDependencies = dict[str, list["DependencyInfo"]]
"""Maps port name -> list of dependencies for that port."""

type ParentFolders = dict[str, TaggedValue]
"""Maps dep_label -> TaggedValue containing RemoteData PK."""

type JobIds = dict[str, TaggedValue]
"""Maps dep_label -> TaggedValue containing SLURM job_id (int)."""

type TaskDepInfo = dict[str, WorkGraphOutputs]
"""Maps task_label -> WorkGraph outputs namespace with remote_folder and job_id."""

type LauncherDependencies = dict[str, list[str]]
"""Maps launcher_name -> list of parent launcher names."""


class DependencyMapping(NamedTuple):
    """Result of dependency mapping for a task.

    Contains all dependency information needed to connect a task to its upstream dependencies.
    """

    port_to_dep: PortToDependencies
    """Maps input port names to list of dependency info for that port."""

    parent_folders: ParentFolders
    """Maps dependency task labels to their remote folder outputs."""

    job_ids: JobIds
    """Maps dependency task labels to their SLURM job IDs."""


class DependencyInfo(BaseModel):
    """Information about a task dependency.

    Attributes:
        dep_label: Label of the task that produces this dependency
        filename: Optional filename within the remote folder (None = use whole folder)
        data_label: Label of the data item being consumed
    """

    model_config = ConfigDict(extra="forbid")

    dep_label: str
    filename: str | None
    data_label: str


class BaseDataInfo(BaseModel):
    """Base information for input/output data."""

    model_config = ConfigDict(extra="forbid")

    name: str
    coordinates: dict[str, Any]
    label: str
    path: str


class InputDataInfo(BaseDataInfo):
    """Input data information with port mapping."""

    port: str
    is_available: bool


class OutputDataInfo(BaseDataInfo):
    """Output data information with optional port."""

    port: str | None


class AiidaResources(BaseModel):
    """AiiDA scheduler resources specification.

    Maps to SLURM resource directives.
    """

    model_config = ConfigDict(extra="forbid")

    num_machines: int | None = None  # Number of nodes (--nodes)
    num_mpiprocs_per_machine: int | None = None  # MPI tasks per node (--ntasks-per-node)
    num_cores_per_mpiproc: int | None = None  # CPUs per task (--cpus-per-task)


class AiidaMetadataOptions(BaseModel):
    """AiiDA CalcJob metadata options.

    See aiida-core documentation for complete list of available options.
    """

    model_config = ConfigDict(extra="forbid")

    account: str | None = None
    prepend_text: str | None = None
    custom_scheduler_commands: str | None = None
    use_symlinks: bool | None = None
    resources: AiidaResources | None = None
    max_wallclock_seconds: int | None = None
    max_memory_kb: int | None = None
    queue_name: str | None = None
    withmpi: bool | None = None
    additional_retrieve_list: list[str] | None = None


class AiidaMetadata(BaseModel):
    """AiiDA CalcJob metadata structure."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    options: AiidaMetadataOptions | None = None
    computer: aiida.orm.Computer | None = None
    computer_label: str | None = None
    call_link_label: str | None = None


class AiidaShellTaskSpec(BaseModel):
    """Specification for ShellTask launcher creation.

    Contains all parameters needed to create a shell task at runtime.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    label: str
    code_pk: int
    node_pks: dict[str, int]
    metadata: AiidaMetadata
    arguments_template: str
    filenames: dict[str, str]
    outputs: list[str]
    input_data_info: list[dict[str, Any]]
    output_data_info: list[dict[str, Any]]
    output_port_mapping: dict[str, str]
    port_to_dep_mapping: dict[str, list[dict[str, Any]]] | None = None  # Added by launcher

    @field_validator("code_pk")
    @classmethod
    def validate_code_pk_not_none(cls, v: int | None) -> int:
        if v is None:
            msg = "code_pk cannot be None - code must be stored"
            raise ValueError(msg)
        return v


class AiidaIconTaskSpec(BaseModel):
    """Specification for IconTask launcher creation.

    Contains all parameters needed to create an ICON task at runtime.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    label: str
    code_pk: int
    master_namelist_pk: int
    model_namelist_pks: dict[str, int]
    wrapper_script_pk: int | None = None
    metadata: AiidaMetadata
    output_port_mapping: dict[str, str]
    port_to_dep_mapping: dict[str, list[dict[str, Any]]] | None = None  # Added by launcher

    @field_validator("code_pk", "master_namelist_pk")
    @classmethod
    def validate_pk_not_none(cls, v: int | None) -> int:
        if v is None:
            msg = "PKs cannot be None - nodes must be stored"
            raise ValueError(msg)
        return v

    @field_validator("model_namelist_pks")
    @classmethod
    def validate_model_pks_not_none(cls, v: dict[str, int | None]) -> dict[str, int]:
        for model_name, pk in v.items():
            if pk is None:
                msg = f"Model namelist PK for {model_name} cannot be None"
                raise ValueError(msg)
        return v  # type: ignore[return-value]
