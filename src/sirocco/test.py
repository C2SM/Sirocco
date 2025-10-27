from pathlib import Path
from typing import Annotated

import typer
from aiida.manage.configuration import load_profile
from rich.console import Console
from rich.traceback import install as install_rich_traceback

from sirocco import core, parsing, pretty_print, vizgraph
from sirocco.workgraph import AiidaWorkGraph

load_profile()

# Use the uploaded file path
workflow_file = '/home/geiger_j/aiida_projects/swiss-twins/git-repos/Sirocco/tests/cases/APE_R02B04/config/config.yml'

# Parse the configuration file
config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))

# Build the core workflow
core_wf = core.Workflow.from_config_workflow(config_workflow)

# Convert to AiidaWorkGraph
aiida_wg = AiidaWorkGraph(core_wf)

# Build the workgraph
workgraph = aiida_wg.build()

# Submit the workflow
# breakpoint()
submit_result = workgraph.run()

print(f"Workflow submitted with PK: {submit_result.pk}")

