"""Pydantic models for Sirocco's AiiDA engine module.

Models carry data between different layers of the application (core → adapter → builder → tasks).
They contain validation logic but no business logic.
"""

from __future__ import annotations

__all__ = [
    "AiidaIconTaskSpec",
    "AiidaMetadata",
    "AiidaMetadataOptions",
    "AiidaResources",
    "AiidaShellTaskSpec",
    "BaseDataInfo",
    "DependencyInfo",
    "DependencyMapping",
    "InputDataInfo",
    "OutputDataInfo",
    "ShellDependencyMappings",
]

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple

import aiida.orm  # noqa TC002:
from pydantic import BaseModel, ConfigDict, field_validator

# Import TypedDicts outside TYPE_CHECKING for Pydantic runtime validation
from sirocco.engines.aiida.types import (
    SerializedDependencyInfo,  # noqa TC001
    SerializedInputDataInfo,  # noqa TC001
    SerializedOutputDataInfo,  # noqa TC001
)

if TYPE_CHECKING:
    from sirocco.engines.aiida.types import (
        PortDependencyMapping,
        TaskFolderMapping,
        TaskJobIdMapping,
    )


class DependencyMapping(NamedTuple):
    """Result of dependency mapping for a task.

    Contains all dependency information needed to connect a task to its upstream dependencies.
    """

    port_mapping: PortDependencyMapping
    """Maps input port names to list of dependency info for that port."""

    task_folders: TaskFolderMapping
    """Maps dependency task labels to their remote folder outputs."""

    task_job_ids: TaskJobIdMapping
    """Maps dependency task labels to their SLURM job IDs."""


@dataclass
class ShellDependencyMappings:
    """Result of resolving shell task dependencies.

    Groups all three mappings needed for shell task execution:
    - nodes: The actual RemoteData nodes to pass as inputs
    - placeholders: Maps data labels to node keys for argument substitution
    - filenames: Maps node keys to filenames for file staging
    """

    nodes: dict[str, aiida.orm.RemoteData]
    """RemoteData nodes for each dependency, keyed by unique node key."""

    placeholders: dict[str, str]
    """Maps data labels to node keys for argument placeholder substitution."""

    filenames: dict[str, str]
    """Maps node keys to filenames for file staging."""


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

    # NOTE: This will become non-optional

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
    # NOTE: Do we need both computer and computer_label?
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
    input_data_info: list[SerializedInputDataInfo]
    output_data_info: list[SerializedOutputDataInfo]
    output_port_mapping: dict[str, str]
    port_dependency_mapping: dict[str, list[SerializedDependencyInfo]] | None = None  # Added by launcher

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
    port_dependency_mapping: dict[str, list[dict[str, Any]]] | None = None  # Added by launcher

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
