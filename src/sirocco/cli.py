from pathlib import Path
from typing import Annotated

import typer
from aiida.manage.configuration import load_profile
from rich.console import Console
from rich.traceback import install as install_rich_traceback

from sirocco import core, parsing, pretty_print, vizgraph
from sirocco.workgraph import build_sirocco_workgraph

# --- Typer App and Rich Console Setup ---
# Print tracebacks with syntax highlighting and rich formatting
install_rich_traceback(show_locals=False)

# Create the Typer app instance
app = typer.Typer(
    help="Sirocco Weather and Climate Workflow Management Tool.",
    add_completion=True,
)

# Create a Rich console instance for printing
console = Console()


def _create_aiida_workflow(workflow_file: Path) -> tuple[core.Workflow, "WorkGraph"]:
    """Load workflow file and build WorkGraph.

    Returns:
        Tuple of (core_workflow, aiida_workgraph)
    """
    load_profile()
    config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))
    core_wf = core.Workflow.from_config_workflow(config_workflow)
    wg = build_sirocco_workgraph(core_wf)
    return core_wf, wg


def create_aiida_workflow(workflow_file: Path) -> tuple[core.Workflow, "WorkGraph"]:
    """Helper to prepare WorkGraph from workflow file.

    Returns:
        Tuple of (core_workflow, aiida_workgraph)
    """

    from aiida.common import ProfileConfigurationError

    try:
        core_wf, wg = _create_aiida_workflow(workflow_file=workflow_file)
        console.print(
            f"⚙️ Workflow [magenta]'{wg.name}'[/magenta] prepared for AiiDA execution."
        )
        return core_wf, wg  # noqa: TRY300 | try-consider-else -> shouldn't move this to `else` block
    except ProfileConfigurationError as e:
        console.print(f"[bold red]❌ No AiiDA profile set up: {e}[/bold red]")
        console.print(
            "[bold green]You can create one using `verdi presto`[/bold green]"
        )
        console.print_exception()
        raise typer.Exit(code=1) from e
    except Exception as e:
        console.print(f"[bold red]❌ Failed to prepare AiiDA workflow: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


# --- CLI Commands ---


@app.command()
def verify(
    workflow_file: Annotated[
        Path,
        typer.Argument(
            ...,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to the workflow definition YAML file.",
        ),
    ],
):
    """
    Validate the workflow definition file for syntax and basic consistency.
    """
    console.print(f"🔍 Verifying workflow file: [cyan]{workflow_file!s}[/cyan]")
    try:
        # Attempt to load and validate the configuration
        parsing.ConfigWorkflow.from_config_file(str(workflow_file))
        console.print("[green]✅ Workflow definition is valid.[/green]")
    except Exception as e:
        console.print("[bold red]❌ Workflow validation failed:[/bold red]")
        # Rich traceback handles printing the exception nicely
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command()
def visualize(
    workflow_file: Annotated[
        Path,
        typer.Argument(
            ...,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to the workflow definition YAML file.",
        ),
    ],
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            writable=True,
            file_okay=True,
            dir_okay=False,
            help="Optional path to save the output SVG file.",
        ),
    ] = None,
):
    """
    Generate an interactive SVG visualization of the unrolled workflow.
    """
    console.print(f"📊 Visualizing workflow from: [cyan]{workflow_file!s}[/cyan]")
    try:
        # Load configuration
        config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))

        # Create the core workflow representation (unrolls parameters/cycles)
        core_workflow = core.Workflow.from_config_workflow(config_workflow)

        # Create the visualization graph
        viz_graph = vizgraph.VizGraph.from_core_workflow(core_workflow)

        # Determine output path
        output_path = (
            workflow_file.parent / f"{core_workflow.name}.svg"
            if output_file is None
            else output_file
        )

        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Draw the graph
        viz_graph.draw(file_path=output_path)

        console.print(
            f"[green]✅ Visualization saved to:[/green] [cyan]{output_path.resolve()}[/cyan]"
        )

    except Exception as e:
        console.print("[bold red]❌ Failed to generate visualization:[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command()
def represent(
    workflow_file: Annotated[
        Path,
        typer.Argument(
            ...,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to the workflow definition YAML file.",
        ),
    ],
):
    """
    Display the text representation of the unrolled workflow graph.
    """
    console.print(f"📄 Representing workflow from: [cyan]{workflow_file}[/cyan]")
    try:
        config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))
        core_workflow = core.Workflow.from_config_workflow(config_workflow)

        printer = pretty_print.PrettyPrinter(colors=False)
        output_from_printer = printer.format(core_workflow)

        console.print(output_from_printer)

    except Exception as e:
        console.print("[bold red]❌ Failed to represent workflow:[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command(help="Run the workflow in a blocking fashion.")
def run(
    workflow_file: Annotated[
        Path,
        typer.Argument(
            ...,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to the workflow definition YAML file.",
        ),
    ],
):
    core_wf, wg = create_aiida_workflow(workflow_file)
    console.print(
        f"▶️ Running workflow [magenta]'{core_wf.name}'[/magenta] directly (blocking)..."
    )
    try:
        _ = wg.run(inputs=None)
        console.print("[green]✅ Workflow execution finished.[/green]")
    except Exception as e:
        console.print(
            f"[bold red]❌ Workflow execution failed during run: {e}[/bold red]"
        )
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command(help="Submit the workflow to the AiiDA daemon.")
def submit(
    workflow_file: Annotated[
        Path,
        typer.Argument(
            ...,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to the workflow definition YAML file.",
        ),
    ],
):
    """Submit the workflow to the AiiDA daemon."""

    core_wf, wg = create_aiida_workflow(workflow_file)
    try:
        console.print(
            f"🚀 Submitting workflow [magenta]'{core_wf.name}'[/magenta] to AiiDA daemon..."
        )
        wg.submit(inputs=None)

        if (results_node := wg.process) is None:
            raise RuntimeError("Something went wrong when submitting workgraph")

        console.print(f"[green]✅ Workflow submitted. PK: {results_node.pk}[/green]")

    except Exception as e:
        console.print(f"[bold red]❌ Workflow submission failed: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command()
def create_symlink_tree(
    pk: Annotated[
        int,
        typer.Argument(
            ...,
            help="PK (Primary Key) of the submitted workflow node.",
        ),
    ],
    base_directory: Annotated[
        str | None,
        typer.Option(
            "--base-dir",
            "-b",
            help="Base directory on the HPC where the symlink tree will be created. Defaults to the Computer's work directory.",
        ),
    ] = None,
    output_dirname: Annotated[
        str | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Name of the output directory. Defaults to 'workflow-name-timestamp'.",
        ),
    ] = None,
):
    """
    Create a human-readable directory tree with symlinks to CalcJob remote working directories.

    This command queries a submitted workflow by its PK and creates symlinks on the HPC
    to the remote working directories of all CalcJobNodes. The symlinks are organized
    with human-readable names based on the workgraph task names.

    The command is incremental: existing symlinks are skipped, and new ones are added
    as the workflow progresses.
    """
    from aiida.orm import CalcJobNode, WorkflowNode, load_node
    try:
        load_profile()
        import re

        # Load the workflow node
        console.print(f"🔍 Loading workflow node with PK: [cyan]{pk}[/cyan]")
        try:
            node = load_node(pk)
        except Exception as e:
            console.print(
                f"[bold red]❌ Failed to load node with PK {pk}: {e}[/bold red]"
            )
            raise typer.Exit(code=1) from e

        if not isinstance(node, WorkflowNode):
            msg = f"Node with pk {pk} not a WorkflowNode but of type `{type(node)}`. Not supported."
            raise ValueError(msg)

        # Get workflow name
        workflow_name = node.process_label or node.label or f"workflow_{pk}"
        workflow_name = re.sub(r"<[^>]*>", "", workflow_name)

        # Query all CalcJobNodes that are descendants of this workflow
        calcjob_nodes = [n for n in node.called_descendants if isinstance(n, CalcJobNode)]

        if not calcjob_nodes:
            console.print(
                "[yellow]⚠️  No CalcJobNodes found for this workflow yet.[/yellow]"
            )
            return

        console.print(f"Found [green]{len(calcjob_nodes)}[/green] CalcJobNode(s)")

        # Get the computer and create transport
        # We assume all CalcJobs run on the same computer
        if calcjob_nodes:
            for calcjob_node in calcjob_nodes:
                computer = calcjob_node.computer
                if computer:
                    break

            # Use Computer's work directory as default if base_directory not specified
            if base_directory is None:
                base_directory = computer.get_workdir()
                console.print(
                    f"📂 Using Computer's work directory: [cyan]{base_directory}[/cyan]"
                )

            # Determine output directory name
            if output_dirname is None:
                output_dirname = f"{workflow_name}-{node.label}-{pk}"

            full_output_path = f"{base_directory}/workflows/{output_dirname}"
            console.print(
                f"📁 Creating symlink tree in: [cyan]{full_output_path}[/cyan]"
            )

            transport = computer.get_transport()

            with transport:
                # Create base directory if it doesn't exist
                if not transport.path_exists(full_output_path):
                    transport.makedirs(full_output_path)
                    console.print(
                        f"✅ Created directory: [cyan]{full_output_path}[/cyan]"
                    )

                # Create symlinks for each CalcJobNode
                created_count = 0
                skipped_count = 0

                for calcjob in calcjob_nodes:
                    # Get the remote working directory
                    try:
                        remote_workdir = calcjob.get_remote_workdir()
                    except Exception:
                        # console.print(
                        #     f"[yellow]⚠️  Could not get remote workdir for {calcjob.process_label} (PK: {calcjob.pk}): {e}[/yellow]"
                        # )
                        continue

                    if remote_workdir is None:
                        console.print(
                            f"[yellow]⚠️  No remote workdir for {calcjob.process_label} (PK: {calcjob.pk})[/yellow]"
                        )
                        continue

                    # Create a human-readable name
                    # Use the process label and add the PK for uniqueness
                    # task_label = calcjob.process_label or f"task_{calcjob.pk}"
                    # symlink_name = f"{task_label}_pk{calcjob.pk}"
                    # TODO: Easier way to access that? maybe add to calcjob extras on WG construction?
                    symlink_name = calcjob.base.attributes.get("metadata_inputs")[
                        "metadata"
                    ]["call_link_label"]

                    symlink_path = f"{full_output_path}/{symlink_name}"

                    # Check if symlink already exists
                    if transport.path_exists(symlink_path):
                        skipped_count += 1
                        continue

                    # Create the symlink
                    try:
                        transport.symlink(remote_workdir, symlink_path)
                        created_count += 1
                        console.print(
                            f"  🔗 Created: [green]{symlink_name}[/green] -> {remote_workdir}"
                        )

                        # Create a back-reference symlink in the actual CalcJob directory
                        # pointing to the workflow root, so users can navigate back easily
                        back_symlink_name = "workflow_root"
                        back_symlink_path = f"{remote_workdir}/{back_symlink_name}"

                        # Only create if it doesn't exist already
                        if not transport.path_exists(back_symlink_path):
                            try:
                                import os
                                rel_path = os.path.relpath(full_output_path, remote_workdir)
                                transport.symlink(rel_path, back_symlink_path)
                            except Exception:
                                pass
                                # console.print(
                                #     f"[yellow]⚠️  Could not create back-reference symlink in {symlink_name}: {e}[/yellow]"
                                # )
                    except Exception as e:
                        console.print(
                            f"[bold red]❌ Failed to create symlink {symlink_name}: {e}[/bold red]"
                        )

                console.print(
                    f"\n✅ Done! Created [green]{created_count}[/green] new symlink(s), "
                    f"skipped [yellow]{skipped_count}[/yellow] existing."
                )

    except Exception as e:
        console.print(f"[bold red]❌ Command failed: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


# --- Main entry point for the script ---
if __name__ == "__main__":
    app()
