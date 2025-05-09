import typer
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.traceback import install as install_rich_traceback

from sirocco import parsing
from sirocco import core
from sirocco import vizgraph
from sirocco import pretty_print
from sirocco.workgraph import AiidaWorkGraph
import aiida


# --- Typer App and Rich Console Setup ---
# Install rich tracebacks for beautiful error reporting
install_rich_traceback(show_locals=False)

# Create the Typer app instance
app = typer.Typer(
    help="Sirocco Weather and Climate Workflow Management Tool.",
    add_completion=True,  # Optional: disable shell completion installation prompts
)

# Create a Rich console instance for printing
console = Console()


# --- Helper functions ---
def load_aiida_profile(profile: Optional[str] = None):
    try:
        aiida.load_profile(profile=profile, allow_switch=True)  # Allow switch for flexibility
        # console.print(f"ℹ️ AiiDA profile [green]'{aiida.get_profile().name}'[/green] loaded.")
    except Exception as e:
        console.print(f"[bold red]Failed to load AiiDA profile '{profile if profile else 'default'}': {e}[/bold red]")
        console.print("Ensure an AiiDA profile exists and the AiiDA daemon is configured if submitting.")
        raise typer.Exit(code=1)


def _prepare_aiida_workgraph(workflow_file_str: str, aiida_profile_name: Optional[str]) -> AiidaWorkGraph:
    """Helper to load profile, config, and prepare AiidaWorkGraph."""
    load_aiida_profile(aiida_profile_name)
    try:
        config_workflow = parsing.ConfigWorkflow.from_config_file(workflow_file_str)
        core_wf = core.Workflow.from_config_workflow(config_workflow)
        aiida_wg = AiidaWorkGraph(core_wf)
        console.print(f"⚙️ Workflow [magenta]'{core_wf.name}'[/magenta] prepared for AiiDA execution.")
        return aiida_wg
    except Exception as e:
        console.print(f"[bold red]❌ Failed to prepare workflow for AiiDA: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1)


# --- CLI Commands ---


@app.command()
def verify(
    workflow_file: Path = typer.Argument(
        ...,  # Ellipsis indicates a required argument
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the workflow definition YAML file.",
    ),
):
    """
    Validate the workflow definition file for syntax and basic consistency.
    """
    console.print(f"🔍 Verifying workflow file: [cyan]{workflow_file}[/cyan]")
    try:
        # Attempt to load and validate the configuration
        parsing.ConfigWorkflow.from_config_file(str(workflow_file))
        console.print("[green]✅ Workflow definition is valid.[/green]")
    except Exception:
        console.print("[bold red]❌ Workflow validation failed:[/bold red]")
        # Rich traceback handles printing the exception nicely
        console.print_exception()
        raise typer.Exit(code=1)


@app.command()
def visualize(
    workflow_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the workflow definition YAML file.",
    ),
    output_file: Optional[Path] = typer.Option(
        None,  # Default value is None, making it optional
        "--output",
        "-o",
        writable=True,  # Check if the path (or its parent dir) is writable
        file_okay=True,
        dir_okay=False,
        help="Optional path to save the output SVG file.",
    ),
):
    """
    Generate an interactive SVG visualization of the unrolled workflow.
    """
    console.print(f"📊 Visualizing workflow from: [cyan]{workflow_file}[/cyan]")
    try:
        # 1. Load configuration
        config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))

        # 2. Create the core workflow representation (unrolls parameters/cycles)
        core_workflow = core.Workflow.from_config_workflow(config_workflow)

        # 3. Create the visualization graph
        viz_graph = vizgraph.VizGraph.from_core_workflow(core_workflow)

        # 4. Determine output path
        if output_file is None:
            # Default output name based on workflow name in the same directory
            output_path = workflow_file.parent / f"{core_workflow.name}.svg"
        else:
            output_path = output_file

        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 5. Draw the graph
        viz_graph.draw(file_path=output_path)

        console.print(f"[green]✅ Visualization saved to:[/green] [cyan]{output_path.resolve()}[/cyan]")

    except Exception:
        console.print("[bold red]❌ Failed to generate visualization:[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1)


@app.command()
def represent(
    workflow_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the workflow definition YAML file.",
    ),
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

    except Exception:
        console.print("[bold red]❌ Failed to represent workflow:[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1)


@app.command(help="Run the workflow in a blocking fashion.")
def run(
    workflow_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the workflow definition YAML file.",
    ),
    aiida_profile: Optional[str] = typer.Option(
        None, "--aiida-profile", "-P", help="AiiDA profile to use (defaults to current active)."
    ),
):
    aiida_wg = _prepare_aiida_workgraph(str(workflow_file), aiida_profile)
    try:
        console.print(f"▶️ Running workflow [magenta]'{aiida_wg._core_workflow.name}'[/magenta] directly (blocking)...")
        results = aiida_wg.run(inputs=None)  # No metadata
        console.print("[green]✅ Workflow execution finished.[/green]")
        console.print("Results:")
        if isinstance(results, dict):
            for k, v in results.items():
                console.print(f"  [bold blue]{k}[/bold blue]: {v}")
        else:
            console.print(f"  {results}")
    except Exception as e:
        console.print(f"[bold red]❌ Workflow execution failed during run: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1)


@app.command(help="Submit the workflow to the AiiDA daemon.")
def submit(
    workflow_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the workflow definition YAML file.",
    ),
    aiida_profile: Optional[str] = typer.Option(
        None, "--aiida-profile", "-P", help="AiiDA profile to use (defaults to current active)."
    ),
    wait: bool = typer.Option(False, "--wait", "-w", help="Wait for the workflow to complete after submission."),
    timeout: int = typer.Option(  # Default AiiDA timeout for wait is often very long or infinite
        3600, "--timeout", "-t", help="Timeout in seconds when waiting (if --wait is used)."
    ),
):
    """Submit the workflow to the AiiDA daemon."""

    aiida_wg = _prepare_aiida_workgraph(str(workflow_file), aiida_profile)
    try:
        console.print(f"🚀 Submitting workflow [magenta]'{aiida_wg._core_workflow.name}'[/magenta] to AiiDA daemon...")
        # No metadata passed to submit
        results_node = aiida_wg.submit(inputs=None, wait=wait, timeout=timeout if wait else None)

        if isinstance(results_node, aiida.orm.WorkChainNode):
            console.print(f"[green]✅ Workflow submitted. PK: {results_node.pk}[/green]")
            if wait:
                console.print(
                    f"🏁 Workflow completed. Final state: [bold { 'green' if results_node.is_finished_ok else 'red' }]{results_node.process_state.value.upper()}[/bold { 'green' if results_node.is_finished_ok else 'red' }]"
                )
                if not results_node.is_finished_ok:
                    console.print(
                        "[yellow]Inspect the workchain for more details (e.g., `verdi process report PK`).[/yellow]"
                    )
        else:  # Should typically be a WorkChainNode
            console.print(f"[green]✅ Submission initiated. Result: {results_node}[/green]")

    except Exception as e:
        console.print(f"[bold red]❌ Workflow submission failed: {e}[/bold red]")
        console.print_exception()
        raise typer.Exit(code=1)


# --- Main entry point for the script ---
if __name__ == "__main__":
    app()
