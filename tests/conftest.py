import logging
import os
import pathlib
import subprocess

import pytest
import requests

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
            available=[models.ConfigAvailableData(name="available", computer="localhost", src=pathlib.Path("foo.txt"))],
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
            available=[models.ConfigAvailableData(name="available", computer="localhost", src=pathlib.Path("foo.txt"))],
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
def config_paths(config_case, icon_grid_path) -> dict[str, pathlib.Path]:
    config = generate_config_paths(config_case)
    if config_case == "small-icon":
        config_rootdir = config["yml"].parent.absolute()
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
    os.environ["TEST_ROOTDIR"] = f"{config.rootdir}"
    print(f"Running tests from: {config.rootdir}")  # noqa: T201

    if config.getoption("reserialize"):
        LOGGER.info("Regenerating serialized references")
        for config_case in ALL_CONFIG_CASES:
            config_paths = generate_config_paths(config_case)
            wf = workflow.Workflow.from_config_file(str(config_paths["yml"]))
            serialize_worklfow(config_paths=config_paths, workflow=wf)
            serialize_nml(config_paths=config_paths, workflow=wf)


@pytest.fixture(scope="session")
def test_rootdir(pytestconfig):
    """The directory of the project independent from where the tests are started"""
    return pathlib.Path(pytestconfig.rootdir)


@pytest.fixture
def configure_aiida_localhost(test_rootdir, aiida_localhost):
    prepend_text = f"""#!/bin/bash
export TEST_ROOTDIR={test_rootdir}
# Expand environment variables in symlink targets and update them to resolved paths

for link in *; do
    if [ -L "$link" ]; then
        target=$(readlink "$link")

        echo "Found symlink: $link -> $target"

        # Only process if target includes a variable
        if [[ "$target" =~ \\$[A-Za-z_][A-Za-z0-9_]* ]]; then
            # Use eval to expand environment variables
            eval "expanded=\\"$target\\""

            # Resolve to absolute path
            resolved=$(readlink -f "$expanded")

            if [ -e "$resolved" ]; then
                echo " -> Expanding to: $resolved"
                rm "$link"
                ln -s "$resolved" "$link"
            else
                echo " !! Expanded path does not exist: $resolved"
            fi
        else
            echo " -> No environment variable to expand."
        fi
    fi
done"""
    aiida_localhost.set_prepend_text(prepend_text)
