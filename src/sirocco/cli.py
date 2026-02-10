import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

# Apply patches for third-party libraries before any AiiDA operations
from sirocco.engines.aiida.patches import (
    patch_firecrest_symlink,
    patch_slurm_dependency_handling,
    patch_workgraph_window,
)

patch_firecrest_symlink()
patch_slurm_dependency_handling()
patch_workgraph_window()

# Imports below require patches to be applied first
if TYPE_CHECKING:
    from aiida_workgraph import WorkGraph

from aiida.manage.configuration import load_profile  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.traceback import install as install_rich_traceback  # noqa: E402

from sirocco import core, parsing, pretty_print, vizgraph  # noqa: E402
from sirocco.core._tasks.sirocco_task import SiroccoContinueTask  # noqa: E402
from sirocco.engines.aiida import build_sirocco_workgraph  # noqa: E402

# --- Typer App and Rich Console Setup ---
# Print tracebacks with syntax highlighting and rich formatting
install_rich_traceback(show_locals=False)

# Create the Typer app instance
app = typer.Typer(
    help="Sirocco Climate and Weather Workflow Management Tool.",
    add_completion=True,
)

# Create a Rich console instance for printing
console = Console()

# Create logger
logger = logging.getLogger(__name__)


def _create_aiida_workflow(
    workflow_file: Path,
    jinja_vars_file: Path | None = None,
) -> tuple[core.Workflow, "WorkGraph"]:
    """Load workflow file and build WorkGraph.

    Uses configuration from config.yml (single source of truth).

    Args:
        workflow_file: Path to workflow configuration file
        jinja_vars_file: Optional path to variables file for Jinja2 templating

    Returns:
        Tuple of (core_workflow, aiida_workgraph)
    """
    load_profile()
    config_workflow = parsing.ConfigWorkflow.from_config_file(
        str(workflow_file),
        jinja_vars_file_path=str(jinja_vars_file) if jinja_vars_file else None,
    )

    core_wf = core.Workflow.from_config_workflow(config_workflow)
    wg = build_sirocco_workgraph(core_wf)
    return core_wf, wg


def create_aiida_workflow(
    workflow_file: Path,
    jinja_vars_file: Path | None = None,
) -> tuple[core.Workflow, "WorkGraph"]:
    """Helper to prepare WorkGraph from workflow file.

    Uses configuration from config.yml (single source of truth).

    Args:
        workflow_file: Path to workflow configuration file
        jinja_vars_file: Optional path to variables file for Jinja2 templating

    Returns:
        Tuple of (core_workflow, aiida_workgraph)
    """

    from aiida.common import ProfileConfigurationError

    try:
        core_wf, wg = _create_aiida_workflow(workflow_file=workflow_file, jinja_vars_file=jinja_vars_file)
        console.print(f"⚙️ Workflow [magenta]'{wg.name}'[/magenta] prepared for AiiDA execution.")
        return core_wf, wg  # noqa: TRY300 | try-consider-else -> shouldn't move this to `else` block
    except ProfileConfigurationError as e:
        console.print(f"[bold red]❌ No AiiDA profile set up: {e}[/bold red]")
        console.print("[bold green]You can create one using `verdi presto`[/bold green]")
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

    Note: This validates the template syntax without variable substitution.
    Use 'sirocco resolve' to render templates with variables.
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
def resolve(
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
    jinja_vars_file: Annotated[
        Path | None,
        typer.Option(
            "--jinja-vars-file",
            "-v",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to variables file for Jinja2 templating. If not specified, auto-detects vars.yml/vars.yaml.",
        ),
    ] = None,
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            writable=True,
            file_okay=True,
            dir_okay=False,
            help="Output file path. If not specified, prints to stdout.",
        ),
    ] = None,
):
    """
    Render Jinja2 template variables in workflow config file.

    This command resolves all Jinja2 template variables ({{ var }}) in the workflow
    configuration file and outputs the fully rendered YAML.

    Variables are loaded from:
    1. Explicitly specified --jinja-vars-file, or
    2. Auto-detected vars.yml/vars.yaml in the same directory

    Examples:
        # Render to stdout
        sirocco resolve config.yml

        # Use custom variables file
        sirocco resolve config.yml --jinja-vars-file custom_vars.yml

        # Save to file
        sirocco resolve config.yml -o config.resolved.yml
    """
    from pathlib import Path

    from sirocco.parsing.yaml_data_models import JinjaResolver

    console.print(f"🔧 Resolving template in: [cyan]{workflow_file!s}[/cyan]")
    if jinja_vars_file:
        console.print(f"   Using variables from: [cyan]{jinja_vars_file!s}[/cyan]")

    # Validate input file
    config_resolved_path = Path(workflow_file).resolve()
    if not config_resolved_path.exists():
        console.print(f"[bold red]❌ File not found: {config_resolved_path}[/bold red]")
        raise typer.Exit(code=1)

    content = config_resolved_path.read_text()
    if content == "":
        console.print(f"[bold red]❌ File is empty: {config_resolved_path}[/bold red]")
        raise typer.Exit(code=1)

    try:
        # Render Jinja2 template
        resolver = JinjaResolver()

        # Load variables from file (if any)
        context = resolver.load_variables_from_file(
            config_resolved_path, Path(jinja_vars_file) if jinja_vars_file else None
        )

        # Render the template
        rendered_content = resolver.render(content, context)

        # Output to file or stdout
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered_content)
            console.print(f"[green]✅ Resolved config written to:[/green] [cyan]{output_path.resolve()}[/cyan]")
        else:
            # Print to stdout
            console.print("\n[bold]Resolved configuration:[/bold]")
            console.print(rendered_content)

    except Exception as e:
        console.print("[bold red]❌ Template resolution failed:[/bold red]")
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

    Note: Uses auto-detected vars.yml/vars.yaml if present.
    Use 'sirocco resolve' first if you need custom variable substitution.
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
        output_path = workflow_file.parent / f"{core_workflow.name}.svg" if output_file is None else output_file

        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Draw the graph
        viz_graph.draw(file_path=output_path)

        console.print(f"[green]✅ Visualization saved to:[/green] [cyan]{output_path.resolve()}[/cyan]")

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

    Note: Uses auto-detected vars.yml/vars.yaml if present.
    Use 'sirocco resolve' first if you need custom variable substitution.
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


@app.command(help="Run the workflow in a blocking fashion. [AiiDA]")
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
    jinja_vars_file: Annotated[
        Path | None,
        typer.Option(
            "--jinja-vars-file",
            "-v",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to variables file for Jinja2 templating. If not specified, auto-detects vars.yml/vars.yaml.",
        ),
    ] = None,
):
    # Load config and use values from config.yml (single source of truth)
    config_workflow = parsing.ConfigWorkflow.from_config_file(
        str(workflow_file),
        jinja_vars_file_path=str(jinja_vars_file) if jinja_vars_file else None,
    )
    front_depth = config_workflow.front_depth

    core_wf, wg = create_aiida_workflow(workflow_file, jinja_vars_file)
    console.print(f"▶️ Running workflow [magenta]'{core_wf.name}'[/magenta] directly (blocking)...")
    if front_depth == 1:
        console.print("   Without pre-submission (front_depth=1)")
    else:
        console.print(f"   Front depth: {front_depth} levels")
    try:
        _ = wg.run(inputs=None)
        console.print("[green]✅ Workflow execution finished.[/green]")
    except Exception as e:
        console.print(f"[bold red]❌ Workflow execution failed during run: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command(help="Submit the workflow to the AiiDA daemon. [AiiDA]")
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
    jinja_vars_file: Annotated[
        Path | None,
        typer.Option(
            "--jinja-vars-file",
            "-v",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to variables file for Jinja2 templating. If not specified, auto-detects vars.yml/vars.yaml.",
        ),
    ] = None,
):
    """Submit the workflow to the AiiDA daemon."""

    # Load config and use values from config.yml (single source of truth)
    config_workflow = parsing.ConfigWorkflow.from_config_file(
        str(workflow_file),
        jinja_vars_file_path=str(jinja_vars_file) if jinja_vars_file else None,
    )
    front_depth = config_workflow.front_depth

    core_wf, wg = create_aiida_workflow(workflow_file, jinja_vars_file)
    try:
        console.print(f"🚀 Submitting workflow [magenta]'{core_wf.name}'[/magenta] to AiiDA daemon...")
        if front_depth == 1:
            console.print("   No pre-submission (front_depth=1)")
        else:
            console.print(f"   Front depth: {front_depth} levels")

        wg.submit(inputs=None)

        if (results_node := wg.process) is None:
            msg = "Something went wrong when submitting workgraph"
            raise RuntimeError(msg)  # noqa: TRY301

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
        import os
        import re

        # Load the workflow node
        console.print(f"🔍 Loading workflow node with PK: [cyan]{pk}[/cyan]")
        try:
            node = load_node(pk)
        except Exception as e:
            console.print(f"[bold red]❌ Failed to load node with PK {pk}: {e}[/bold red]")
            raise typer.Exit(code=1) from e

        if not isinstance(node, WorkflowNode):
            msg = f"Node with pk {pk} not a WorkflowNode but of type `{type(node)}`. Not supported."
            raise TypeError(msg)  # noqa: TRY301

        # Get workflow name
        workflow_name = node.process_label or node.label or f"workflow_{pk}"
        workflow_name = re.sub(r"<[^>]*>", "", workflow_name)

        # Query all CalcJobNodes that are descendants of this workflow
        calcjob_nodes = [n for n in node.called_descendants if isinstance(n, CalcJobNode)]

        if not calcjob_nodes:
            console.print("[yellow]⚠️  No CalcJobNodes found for this workflow yet.[/yellow]")
            return

        console.print(f"Found [green]{len(calcjob_nodes)}[/green] CalcJobNode(s)")

        # Get the computer from the first CalcJob that has one
        computer = None
        for calcjob_node in calcjob_nodes:
            if calcjob_node.computer:
                computer = calcjob_node.computer
                break

        if computer is None:
            console.print("[bold red]❌ No computer found for any CalcJobNode[/bold red]")
            raise typer.Exit(code=1)  # noqa: TRY301

        # Use Computer's work directory as default if base_directory not specified
        if base_directory is None:
            base_directory = computer.get_workdir()
            console.print(f"📂 Using Computer's work directory: [cyan]{base_directory}[/cyan]")

        # Determine output directory name
        if output_dirname is None:
            output_dirname = f"{workflow_name}-{node.label}-{pk}"

        full_output_path = f"{base_directory}/workflows/{output_dirname}"
        console.print(f"📁 Creating symlink tree in: [cyan]{full_output_path}[/cyan]")

        transport = computer.get_transport()

        with transport:
            # Create base directory if it doesn't exist
            if not transport.path_exists(full_output_path):
                transport.makedirs(full_output_path)
                console.print(f"✅ Created directory: [cyan]{full_output_path}[/cyan]")

            # Create symlinks for each CalcJobNode
            created_count = 0
            skipped_count = 0

            for calcjob in calcjob_nodes:
                # Get the remote working directory
                try:
                    remote_workdir = calcjob.get_remote_workdir()
                except Exception as e:  # noqa: BLE001
                    logger.debug(
                        "Could not get remote workdir for %s (PK: %s): %s",
                        calcjob.process_label,
                        calcjob.pk,
                        e,
                    )
                    continue

                if remote_workdir is None:
                    console.print(
                        f"[yellow]⚠️  No remote workdir for {calcjob.process_label} (PK: {calcjob.pk})[/yellow]"
                    )
                    continue

                # Create a human-readable name from metadata
                symlink_name = calcjob.base.attributes.get("metadata_inputs")["metadata"]["call_link_label"]
                symlink_path = f"{full_output_path}/{symlink_name}"

                # Skip if symlink already exists
                if transport.path_exists(symlink_path):
                    skipped_count += 1
                    continue

                # Create the symlink
                try:
                    transport.symlink(remote_workdir, symlink_path)
                    created_count += 1
                    console.print(f"  🔗 Created: [green]{symlink_name}[/green] -> {remote_workdir}")

                    # Create a back-reference symlink in the actual CalcJob directory
                    back_symlink_name = "workflow_root"
                    back_symlink_path = f"{remote_workdir}/{back_symlink_name}"

                    # Only create if it doesn't exist already
                    if not transport.path_exists(back_symlink_path):
                        try:
                            rel_path = os.path.relpath(full_output_path, remote_workdir)
                            transport.symlink(rel_path, back_symlink_path)
                        except Exception as e:  # noqa: BLE001
                            logger.debug(
                                "Could not create back-reference symlink in %s: %s",
                                symlink_name,
                                e,
                            )
                except Exception as e:  # noqa: BLE001
                    console.print(f"[bold red]❌ Failed to create symlink {symlink_name}: {e}[/bold red]")

            console.print(
                f"\n✅ Done! Created [green]{created_count}[/green] new symlink(s), "
                f"skipped [yellow]{skipped_count}[/yellow] existing."
            )

    except Exception as e:
        console.print(f"[bold red]❌ Command failed: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command(help="Start a workflow. [standalone]")
def start(
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
    cleanup: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--cleanup",
            help="clean up before starting",
        ),
    ] = False,
):
    console.print(add_now())
    wf = core.Workflow.from_config_file(workflow_file)
    if cleanup:
        console.print(f"▶️ Cleaning up workflow at {wf.config_rootdir} ...")
        if (run_dir := wf.config_rootdir / wf.RUN_ROOT).exists():
            shutil.rmtree(run_dir)
        (wf.config_rootdir / SiroccoContinueTask.SUBMIT_FILENAME).unlink(missing_ok=True)
        (wf.config_rootdir / SiroccoContinueTask.STDOUTERR_FILENAME).unlink(missing_ok=True)
    if (wf.config_rootdir / wf.RUN_ROOT).exists():
        msg = "Workflow already exists, cannot start. Use --cleanup to clean up before starting."
        raise ValueError(msg)
    console.print(f"▶️ Starting workflow at {wf.config_rootdir} ...")
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))
    try:
        wf.start()
        if wf.status == core.workflow.WorkflowStatus.CONTINUE:
            console.print("✅ Workflow started successfully.")
        elif wf.status == core.workflow.WorkflowStatus.FAILED:
            console.print("❌ Workflow start failed")
    except Exception as e:
        console.print(f"❌ Workflow start failed: {e}")
        console.print_exception()
        raise typer.Exit(code=1) from e
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))


@app.command(help="Restart a stopped workflow. [standalone]")
def restart(
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
    console.print(add_now())
    wf = core.Workflow.from_config_file(workflow_file)
    console.print(f"▶️ Restarting workflow at {wf.config_rootdir} ...")
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))
    try:
        wf.restart()
        if wf.status == core.workflow.WorkflowStatus.CONTINUE:
            console.print("✅ Workflow restarted successfully.")
        elif wf.status == core.workflow.WorkflowStatus.COMPLETED:
            console.print("✅ Workflow completed!")
        elif wf.status == core.workflow.WorkflowStatus.RESTART_FAILED:
            console.print("❌ Workflow restart failed")
    except Exception as e:
        console.print(f"❌ Workflow restart failed: {e}")
        console.print_exception()
        raise typer.Exit(code=1) from e
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))


@app.command(help="Stop a workflow. [standalone]")
def stop(
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
    cool_down: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--cool-down",
            help="Do not cancel currently running tasks",
        ),
    ] = False,
):
    console.print(add_now())
    wf = core.Workflow.from_config_file(workflow_file)
    msg = f"▶️ Stopping workflow at {wf.config_rootdir}"
    if cool_down:
        msg += " in cool down mode"
    msg += " ..."
    console.print(msg)
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))
    try:
        wf.stop(mode="cool-down" if cool_down else "cancel")
        if wf.status == core.workflow.WorkflowStatus.STOPPED:
            console.print("✅ Workflow stopped successfully.")
        else:
            console.print("❌ Workflow stop failed")
    except Exception as e:
        console.print(f"❌ Workflow stop failed: {e}")
        console.print_exception()
        raise typer.Exit(code=1) from e
    with (wf.config_rootdir / core.SiroccoContinueTask.STDOUTERR_FILENAME).open("a") as logfile:
        logfile.write(console.export_text(clear=True))


@app.command(name="continue", hidden=True)
def continue_wf(
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
    from_wf: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--from_wf",
            help="Specify command is executed from a running worflow (as opposed to interactively)",
        ),
    ] = False,
):
    console.print(add_now())
    if not from_wf:
        msg = "Do not use interactively, the continue command is reserved for internal use"
        raise ValueError(msg)
    wf = core.Workflow.from_config_file(workflow_file)
    console.print("▶️ Continue workflow ...")
    try:
        wf.continue_wf()
        if wf.status == core.workflow.WorkflowStatus.CONTINUE:
            console.print("✅ Workflow continuation submitted successfully.")
        elif wf.status == core.workflow.WorkflowStatus.COMPLETED:
            console.print("✅ Workflow completed!")
        elif wf.status == core.workflow.WorkflowStatus.FAILED:
            console.print("❌ Workflow failed")
    except Exception as e:
        console.print(f"❌ Workflow continuation failed: {e}")
        console.print_exception()
        raise typer.Exit(code=1) from e


@app.command(help="Visualize workflow status. [standalone]")
def stviz(
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
    console.print(f"📊 Visualizing workflow status from: [cyan]{workflow_file!s}[/cyan]")
    try:
        wf = core.Workflow.from_config_file(workflow_file)
        wf.load_state()
        viz_graph = vizgraph.VizGraph.from_status_workflow(wf)
        viz_graph.draw(file_path=Path("./status.svg"))
        console.print("[green]✅ Status visualization saved to:[/green] [cyan]./status.svg[/cyan]")

    except Exception as e:
        console.print("[bold red]❌ Failed to generate status visualization:[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


def add_now(width: int = 25) -> str:
    rule = width * "─"
    space = width * " "
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # noqa: DTZ005
    date_rule = (len(date_str) + 2) * "─"
    return "\n".join([f"{space}╭{date_rule}╮", f"{rule}┤ {date_str} ├{rule}", f"{space}╰{date_rule}╯"])


# --- Main entry point for the script ---
if __name__ == "__main__":
    app()
