
from aiida.manage.configuration import load_profile

from sirocco import core, parsing
from sirocco.workgraph import build_sirocco_workgraph

load_profile()

# Use the uploaded file path
workflow_file = '/home/geiger_j/aiida_projects/swiss-twins/git-repos/Sirocco/tests/cases/APE_R02B04/config/config.yml'

# Parse the configuration file
config_workflow = parsing.ConfigWorkflow.from_config_file(str(workflow_file))

# Build the core workflow
core_wf = core.Workflow.from_config_workflow(config_workflow)

# Build and run the workgraph using the functional API
workgraph = build_sirocco_workgraph(core_wf)

# Submit the workflow
submit_result = workgraph.run()

