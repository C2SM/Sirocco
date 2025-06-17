from pathlib import Path
from typing import Annotated

import typer
from aiida.manage.configuration import load_profile
from rich.console import Console
from rich.traceback import install as install_rich_traceback

from sirocco import core, parsing, pretty_print, vizgraph
from sirocco.workgraph import AiidaWorkGraph

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


def _create_aiida_workflow(workflow_file: Path) -> AiidaWorkGraph:
    load_profile()
    config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))
    core_wf = core.Workflow.from_config_workflow(config_workflow)
    return AiidaWorkGraph(core_wf)


def create_aiida_workflow(workflow_file: Path) -> AiidaWorkGraph:
    """Helper to prepare AiidaWorkGraph from workflow file."""

    from aiida.common import ProfileConfigurationError

    try:
        aiida_wg = _create_aiida_workflow(workflow_file=workflow_file)
        console.print(f"⚙️ Workflow [magenta]'{aiida_wg._workgraph.name}'[/magenta] prepared for AiiDA execution.")  # noqa: SLF001 | private-member-access
        return aiida_wg  # noqa: TRY300 | try-consider-else -> shouldn't move this to `else` block
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
    aiida_wg = create_aiida_workflow(workflow_file)
    console.print(
        f"▶️ Running workflow [magenta]'{aiida_wg._core_workflow.name}'[/magenta] directly (blocking)..."  # noqa: SLF001 | private-member-access
    )
    try:
        _ = aiida_wg.run(inputs=None)
        console.print("[green]✅ Workflow execution finished.[/green]")
    except Exception as e:
        console.print(f"[bold red]❌ Workflow execution failed during run: {e}[/bold red]")
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

    aiida_wg = create_aiida_workflow(workflow_file)
    try:
        console.print(
            f"🚀 Submitting workflow [magenta]'{aiida_wg._core_workflow.name}'[/magenta] to AiiDA daemon..."  # noqa: SLF001 | private-member-access
        )
        results_node = aiida_wg.submit(inputs=None)

        console.print(f"[green]✅ Workflow submitted. PK: {results_node.pk}[/green]")

    except Exception as e:
        console.print(f"[bold red]❌ Workflow submission failed: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1) from e


# --- Main entry point for the script ---
if __name__ == "__main__":
    app()
