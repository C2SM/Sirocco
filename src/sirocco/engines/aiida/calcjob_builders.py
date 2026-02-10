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

LOGGER = logging.getLogger(__name__)


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

    LOGGER.debug("ICON inputs=%s", inputs)

    return inputs


def _build_slurm_dependency_directive(job_ids: TaskJobIdMapping) -> str:
    """Build SLURM --dependency directive from job IDs.

    Args:
        job_ids: Dict of {dep_label: job_id_tagged_value}

    Returns:
        SLURM directive string like "#SBATCH --dependency=afterok:123:456"
    """
    dep_str = ":".join(str(jid.value) for jid in job_ids.values())
    return f"#SBATCH --dependency=afterok:{dep_str} --kill-on-invalid-dep=yes"


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
        custom_cmd = _build_slurm_dependency_directive(job_ids)
        current_cmds = options.custom_scheduler_commands or ""
        new_cmds = f"{current_cmds}\n{custom_cmd}" if current_cmds else custom_cmd
        options = options.model_copy(update={"custom_scheduler_commands": new_cmds})

    if label is not None:
        return AiidaMetadata(computer=computer, options=options, call_link_label=label)
    return AiidaMetadata(computer=computer, options=options)
