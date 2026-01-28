"""Public API for Sirocco AiiDA workflow orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sirocco.engines.aiida.builder import WorkGraphBuilder

if TYPE_CHECKING:
    import aiida.orm
    from aiida_workgraph import WorkGraph

    from sirocco import core

__all__ = [
    "build_sirocco_workgraph",
    "run_sirocco_workgraph",
    "submit_sirocco_workgraph",
]


def build_sirocco_workgraph(
    core_workflow: core.Workflow,
    front_depth: int = 1,
    resolved_config_path: str | None = None,
) -> WorkGraph:
    """Build a Sirocco WorkGraph from a core workflow.

    This is the main entry point for building Sirocco workflows.

    Args:
        core_workflow: The core workflow to convert
        front_depth: Number of topological levels to keep active (default: 1, must be >= 1)
                    1 = sequential (no pre-submission - wait for level N to finish before submitting N+1)
                    2 = one level ahead
                    higher values = more aggressive streaming submission
        resolved_config_path: Optional path to the resolved config file (with Jinja2 variables replaced)

    Returns:
        A WorkGraph ready for submission

    Example::

        from sirocco import core
        from sirocco.engines.aiida import build_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Build the WorkGraph with front_depth=2
        wg = build_sirocco_workgraph(wf, front_depth=2)

        # Submit to AiiDA daemon
        wg.submit()
    """
    builder = WorkGraphBuilder(core_workflow, resolved_config_path=resolved_config_path)
    return builder.build(front_depth)


def submit_sirocco_workgraph(
    core_workflow: core.Workflow,
    *,
    inputs: None | dict[str, Any] = None,
    wait: bool = False,
    timeout: int = 60,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and submit a Sirocco workflow to the AiiDA daemon.

    Args:
        core_workflow: The core workflow to convert and submit
        inputs: Optional inputs to pass to the workgraph
        wait: Whether to wait for completion
        timeout: Timeout in seconds if wait=True
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If submission fails

    Example::

        from sirocco import core
        from sirocco.engines.aiida import submit_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Submit the workflow
        node = submit_sirocco_workgraph(wf)
        print(f"Submitted as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.submit(inputs=inputs, wait=wait, timeout=timeout, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when submitting workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node


def run_sirocco_workgraph(
    core_workflow: core.Workflow,
    inputs: None | dict[str, Any] = None,
    metadata: None | dict[str, Any] = None,
) -> aiida.orm.Node:
    """Build and run a Sirocco workflow in a blocking fashion.

    Args:
        core_workflow: The core workflow to convert and run
        inputs: Optional inputs to pass to the workgraph
        metadata: Optional metadata for the workgraph

    Returns:
        The AiiDA process node

    Raises:
        RuntimeError: If execution fails

    Example::

        from sirocco import core
        from sirocco.engines.aiida import run_sirocco_workgraph

        # Build your core workflow
        wf = core.Workflow.from_config_file("workflow.yml")

        # Run the workflow (blocking)
        node = run_sirocco_workgraph(wf)
        print(f"Completed as PK={node.pk}")
    """
    wg = build_sirocco_workgraph(core_workflow)

    wg.run(inputs=inputs, metadata=metadata)

    if (output_node := wg.process) is None:
        msg = "Something went wrong when running workgraph. Please contact a developer."
        raise RuntimeError(msg)

    return output_node
