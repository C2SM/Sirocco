"""CalcJob input and metadata builders.

This module handles the construction of complete CalcJob inputs and metadata
from resolved dependencies and task specifications.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import aiida.orm

from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaMetadataOptions,
)

if TYPE_CHECKING:
    from sirocco.engines.aiida.types import (
        TaskJobIdMapping,
    )

__all__ = [
    "SlurmDirectiveBuilder",
    "add_slurm_dependencies_to_metadata",
    "build_icon_calcjob_inputs",
]

logger = logging.getLogger(__name__)


class SlurmDirectiveBuilder:
    """Fluent builder for SLURM scheduler directives.

    Provides a clean API for building SBATCH directives with automatic
    newline handling and validation.

    Example::

        builder = SlurmDirectiveBuilder()
        directives = (
            builder.add_uenv("icon-wcp/v1:rc4")
            .add_view("icon")
            .add_dependency_afterok([12345, 67890])
            .build()
        )
        # Result: "#SBATCH --uenv=icon-wcp/v1:rc4\\n#SBATCH --view=icon\\n..."
    """

    def __init__(self) -> None:
        """Initialize empty directive list."""
        self._directives: list[str] = []

    def add_directive(self, directive: str) -> SlurmDirectiveBuilder:
        """Add a raw SBATCH directive.

        Args:
            directive: Full directive string (e.g., "#SBATCH --nodes=4")

        Returns:
            Self for chaining
        """
        if directive:
            self._directives.append(directive)
        return self

    def add_uenv(self, uenv: str | None) -> SlurmDirectiveBuilder:
        """Add --uenv directive.

        Args:
            uenv: User environment specification

        Returns:
            Self for chaining
        """
        if uenv is not None:
            self._directives.append(f"#SBATCH --uenv={uenv}")
        return self

    def add_view(self, view: str | None) -> SlurmDirectiveBuilder:
        """Add --view directive.

        Args:
            view: View specification

        Returns:
            Self for chaining
        """
        if view is not None:
            self._directives.append(f"#SBATCH --view={view}")
        return self

    def add_dependency_afterok(
        self,
        job_ids: list[int] | TaskJobIdMapping,
        *,
        kill_on_invalid: bool = True,
    ) -> SlurmDirectiveBuilder:
        """Add --dependency=afterok directive.

        Args:
            job_ids: List of job IDs or TaskJobIdMapping dict
            kill_on_invalid: Whether to add --kill-on-invalid-dep=yes (keyword-only)

        Returns:
            Self for chaining
        """
        if not job_ids:
            return self

        # Handle both list[int] and TaskJobIdMapping
        if isinstance(job_ids, dict):
            dep_str = ":".join(str(jid.value) for jid in job_ids.values())
        else:
            dep_str = ":".join(str(jid) for jid in job_ids)

        directive = f"#SBATCH --dependency=afterok:{dep_str}"
        if kill_on_invalid:
            directive += " --kill-on-invalid-dep=yes"

        self._directives.append(directive)
        return self

    def extend_from_string(self, commands: str | None) -> SlurmDirectiveBuilder:
        """Extend directives from a newline-separated string.

        Args:
            commands: String containing multiple directives separated by newlines

        Returns:
            Self for chaining
        """
        if commands:
            self._directives.extend(line.strip() for line in commands.split("\n") if line.strip())
        return self

    def build(self) -> str | None:
        """Build final directive string.

        Returns:
            Newline-joined directives, or None if no directives were added
        """
        if not self._directives:
            return None
        return "\n".join(self._directives)


def build_icon_calcjob_inputs(
    task_spec: AiidaIconTaskSpec,
    input_data_nodes: dict,
    aiida_metadata: AiidaMetadata,
) -> dict:
    """Build complete inputs dict for IconCalculation.

    Assembles all required inputs for an ICON CalcJob from multiple sources:
    - Loads code and namelists from stored PKs
    - Assembles model namelists into a namespace dict
    - Adds wrapper script if present
    - Adds input data nodes, handling namespace vs regular ports
    - Converts and adds AiiDA metadata

    Args:
        task_spec: AiidaIconTaskSpec model with task specifications
        input_data_nodes: Dict of input data nodes (both AvailableData and RemoteData)
        aiida_metadata: AiidaMetadata model with computer and options

    Returns:
        Complete inputs dict ready for IconCalculation submission
    """
    from aiida.engine.processes.ports import PortNamespace
    from aiida_icon.calculations import IconCalculation

    # Get IconCalculation spec to determine which ports are namespaces
    icon_spec = IconCalculation.spec()

    # Start with code and namelists
    inputs: dict[str, Any] = {
        "code": aiida.orm.load_node(task_spec.code_pk),
        "master_namelist": aiida.orm.load_node(task_spec.master_namelist_pk),
    }

    # Add model namelists as a dict (namespace input)
    models = {}
    for model_name, model_pk in task_spec.model_namelist_pks.items():
        models[model_name] = aiida.orm.load_node(model_pk)
    if models:
        inputs["models"] = models

    # Add wrapper script if present
    if task_spec.wrapper_script_pk is not None:
        inputs["wrapper_script"] = aiida.orm.load_node(task_spec.wrapper_script_pk)

    # Add ALL input data nodes (both AvailableData and RemoteData for GeneratedData)
    for port_name, data_node in input_data_nodes.items():
        # Check if this port is a namespace by inspecting the spec
        is_namespace = False
        if port_name in icon_spec.inputs:
            port = icon_spec.inputs[port_name]
            is_namespace = isinstance(port, PortNamespace)

        # Wrap namespace ports in a dict with node label as key
        if is_namespace:
            # Use the node's label or a generic key for the namespace
            node_label = data_node.label if data_node.label else "item"
            inputs[port_name] = {node_label: data_node}
        else:
            inputs[port_name] = data_node

    # Add metadata (convert Pydantic model to dict for AiiDA)
    inputs["metadata"] = aiida_metadata.model_dump(mode="python", exclude_none=True)

    logger.debug("ICON inputs=%s", inputs)

    return inputs


def add_slurm_dependencies_to_metadata(
    base_metadata: AiidaMetadata,
    job_ids: TaskJobIdMapping | None,
    computer: aiida.orm.Computer | None,
    label: str | None = None,
) -> AiidaMetadata:
    """Add SLURM job dependencies to metadata.

    Args:
        base_metadata: Base metadata from task spec
        job_ids: Dict of {dep_label: job_id_tagged_value} or None
        computer: AiiDA computer object
        label: Optional task label for call_link_label (ICON tasks only)

    Returns:
        AiidaMetadata with computer and optional SLURM dependencies
    """
    # Get options or create empty if None
    options = base_metadata.options or AiidaMetadataOptions()

    # Add SLURM dependency directive if needed
    if job_ids:
        builder = SlurmDirectiveBuilder()
        builder.extend_from_string(options.custom_scheduler_commands).add_dependency_afterok(job_ids)
        new_cmds = builder.build()
        options = options.model_copy(update={"custom_scheduler_commands": new_cmds})

    if label is not None:
        return AiidaMetadata(computer=computer, options=options, call_link_label=label)
    return AiidaMetadata(computer=computer, options=options)
