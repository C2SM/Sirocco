import logging
import pathlib
import shutil
import subprocess

import pytest
import requests
from aiida.orm import Computer

from sirocco import pretty_print
from sirocco.core import _tasks as core_tasks
from sirocco.core import workflow
from sirocco.parsing import yaml_data_models as models

pytest_plugins = ["aiida.tools.pytest_fixtures"]

LOGGER = logging.getLogger(__name__)


class DownloadError(RuntimeError):
    def __init__(self, url: str, response: requests.Response):
        super().__init__(f"Failed downloading file {url} , exited with response {response}")


def download_file(url: str, file_path: pathlib.Path):
    response = requests.get(url)
    if not response.ok:
        raise DownloadError(url, response)

    file_path.write_bytes(response.content)


@pytest.fixture(scope="session")
def icon_grid_path(pytestconfig):
    url = "https://github.com/agoscinski/icon-testfiles/raw/refs/heads/main/icon_grid_0013_R02B04_R.nc"
    filename = "icon_grid_0013_R02B04_R.nc"
    cache_dir = pytestconfig.cache.mkdir("downloaded_files")
    icon_grid_path = cache_dir / filename

    # Check if the file is already cached
    if icon_grid_path.exists():
        LOGGER.info("Found icon grid in cache, reusing it.")
    else:
        # File is not cached, download and save it
        LOGGER.info("Downloading and caching icon grid.")
        download_file(url, icon_grid_path)

    return icon_grid_path


@pytest.fixture
@pytest.mark.requires_icon
def icon_filepath_executable() -> str:
    which_icon = subprocess.run(["which", "icon"], capture_output=True, check=False)
    if which_icon.returncode:
        msg = "Could not find icon executable."
        raise FileNotFoundError(msg)

    return which_icon.stdout.decode().strip()


@pytest.fixture(scope="session")
def minimal_config() -> models.ConfigWorkflow:
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[models.ConfigCycle(name="minimal", tasks=[models.ConfigCycleTask(name="some_task")])],
        tasks=[models.ConfigShellTask(name="some_task", command="some_command", computer="localhost")],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(name="available", computer="localhost", src=pathlib.Path("/foo.txt"))
            ],
            generated=[models.ConfigGeneratedData(name="bar", src=pathlib.Path("bar"))],
        ),
        parameters={},
    )


@pytest.fixture(scope="session")
def minimal_invert_task_io_config() -> models.ConfigWorkflow:
    return models.ConfigWorkflow(
        name="minimal",
        rootdir=pathlib.Path("minimal"),
        cycles=[
            models.ConfigCycle(
                name="minimal",
                tasks=[
                    models.ConfigCycleTask(
                        name="task_b",
                        inputs=[models.ConfigCycleTaskInput(name="output_a", port="None")],
                        outputs=[models.ConfigCycleTaskOutput(name="output_b")],
                    ),
                    models.ConfigCycleTask(
                        name="task_a",
                        inputs=[models.ConfigCycleTaskInput(name="available", port="None")],
                        outputs=[models.ConfigCycleTaskOutput(name="output_a")],
                    ),
                ],
            ),
        ],
        tasks=[
            models.ConfigShellTask(name="task_a", computer="localhost", command="command_a"),
            models.ConfigShellTask(name="task_b", computer="localhost", command="command_b"),
        ],
        data=models.ConfigData(
            available=[
                models.ConfigAvailableData(name="available", computer="localhost", src=pathlib.Path("/foo.txt"))
            ],
            generated=[
                models.ConfigGeneratedData(name="output_a", src=pathlib.Path("bar")),
                models.ConfigGeneratedData(name="output_b", src=pathlib.Path("bar")),
            ],
        ),
        parameters={},
    )


# configs that are tested for parsing
ALL_CONFIG_CASES = ["small-shell", "small-icon", "parameters", "large"]


@pytest.fixture(params=ALL_CONFIG_CASES)
def config_case(request) -> str:
    return request.param


@pytest.fixture
def pprinter() -> pretty_print.PrettyPrinter:
    return pretty_print.PrettyPrinter()


def generate_config_paths(test_case: str):
    return {
        "yml": pathlib.Path(f"tests/cases/{test_case}/config/config.yml"),
        "txt": pathlib.Path(f"tests/cases/{test_case}/data/config.txt"),
        "svg": pathlib.Path(f"tests/cases/{test_case}/svg/config.svg"),
    }


@pytest.fixture
def config_paths(config_case, icon_grid_path, tmp_path, test_rootdir) -> dict[str, pathlib.Path]:
    config = generate_config_paths(config_case)
    # Copy test directory to tmp path and adapt config
    shutil.copytree(test_rootdir / f"tests/cases/{config_case}", tmp_path / f"tests/cases/{config_case}")
    for key, value in config.items():
        config[key] = tmp_path / value

    # Expand /TESTS_ROOTDIR to directory where config is located
    for key in ["yml", "txt"]:
        config[key].write_text(config[key].read_text().replace("/TESTS_ROOTDIR", str(tmp_path)))

    if config_case == "small-icon":
        config_rootdir = config["yml"].parent
        # We link the icon grid as specified in the model.namelist
        config_icon_grid_path = pathlib.Path(config_rootdir / "./ICON/icon_grid_simple.nc")
        if not config_icon_grid_path.exists():
            config_icon_grid_path.symlink_to(icon_grid_path)
    return config


def pytest_addoption(parser):
    parser.addoption("--reserialize", action="store_true", default=False)


def serialize_worklfow(config_paths: dict[str, pathlib.Path], workflow: workflow.Workflow) -> None:
    config_paths["txt"].write_text(pretty_print.PrettyPrinter().format(workflow))


def serialize_nml(config_paths: dict[str, pathlib.Path], workflow: workflow.Workflow) -> None:
    nml_refdir = config_paths["txt"].parent / "ICON_namelists"
    for task in workflow.tasks:
        if isinstance(task, core_tasks.icon_task.IconTask):
            task.dump_namelists(directory=nml_refdir)


def pytest_configure(config):
    if config.getoption("reserialize"):
        LOGGER.info("Regenerating serialized references")
        for config_case in ALL_CONFIG_CASES:
            config_paths = generate_config_paths(config_case)
            for key, value in config_paths.items():
                config_paths[key] = pathlib.Path(config.rootdir) / value
            wf = workflow.Workflow.from_config_file(str(config_paths["yml"]))
            serialize_worklfow(config_paths=config_paths, workflow=wf)
            serialize_nml(config_paths=config_paths, workflow=wf)


@pytest.fixture(scope="session")
def test_rootdir(pytestconfig):
    """The directory of the project independent from where the tests are started"""
    return pathlib.Path(pytestconfig.rootdir)
    # aiida_localhost.set_mpirun_command(['mpirun', '-np', '1'])


@pytest.fixture
def aiida_localhost_slurm(aiida_computer_ssh) -> Computer:
    """Return a :class:`aiida.orm.computers.Computer` instance representing local SLURM."""
    from aiida.common.exceptions import NotExistent
    from aiida.orm import Computer
    import os

    # Try to get the local SLURM computer from CI setup
    try:
        return Computer.collection.get(label="slurm-ssh")
    except NotExistent:
        # Use the aiida_computer_ssh factory but customize for SLURM
        comp = aiida_computer_ssh(label="slurm-ssh", configure=False)

        # Update the scheduler to SLURM (the factory creates it with core.direct by default)
        comp.scheduler_type = "core.slurm"
        comp.description = "slurm container"

        comp.set_workdir("/home/xenon/workdir")

        # Set minimum job poll interval and default MPI procs per machine
        comp.set_minimum_job_poll_interval(0)
        comp.set_default_mpiprocs_per_machine(1)

        # Configure with custom SSH settings for the SLURM container
        ssh_key_path = os.path.expanduser("~/.ssh/slurm_rsa")
        comp.configure(
            username="xenon",
            port=5001,
            look_for_keys=True,
            key_filename=ssh_key_path,
            key_policy="AutoAddPolicy",
            safe_interval=0,
        )

        return comp

def setup_hpc_data_environment(config_rootdir, computer_label="slurm-ssh"):
    """Setup HPC-like data environment for testing."""
    import subprocess
    import os

    ssh_key = os.path.expanduser("~/.ssh/slurm_rsa")
    hpc_data_path = "/scratch/icon_data"

    # Copy data to container
    subprocess.run([
        "ssh", "-p", "5001", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
        "xenon@localhost", f"mkdir -p {hpc_data_path}"
    ], check=True)

    local_icon_data = config_rootdir / "ICON"
    subprocess.run([
        "scp", "-r", "-P", "5001", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
        str(local_icon_data) + "/",
        f"xenon@localhost:{hpc_data_path}/"
    ], check=True)

    return hpc_data_path

def update_workflow_data_paths(workflow, hpc_data_path):
    """Update workflow data paths to point to HPC locations."""
    for data in workflow.data.available:
        if str(data.src).startswith("/TESTS_ROOTDIR"):
            filename = pathlib.Path(data.src).name
            data.src = pathlib.Path(f"{hpc_data_path}/{filename}")
    return workflow