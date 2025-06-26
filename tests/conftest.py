import logging
import os
import pathlib
import shutil
import subprocess
import textwrap
import typing as t

import pytest
import requests
from aiida.common import NotExistent
from aiida.orm import Computer, load_computer

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
    parser.addoption(
        "--remote",
        action="store",
        default="localhost-ssh",
        help="Specify an aiida computer label for a remote machine that has been configured before tests and should be used.",
    )


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


# Copied over from aiida-core and made session-scoped, using `tmp_path_factory`
@pytest.fixture(scope="session")
def aiida_computer_session(tmp_path_factory) -> t.Callable[[], "Computer"]:
    """Return a factory to create a new or load an existing :class:`aiida.orm.computers.Computer` instance.

    The database is queried for an existing computer with the same ``label``, ``hostname``, ``scheduler_type`` and
    ``transport_type``. If it exists, it means it was probably created by this fixture in a previous call and it is
    simply returned. Otherwise a new instance is created. Note that the computer is not explicitly configured, unless
    ``configure_kwargs`` are specified. By default the ``localhost`` hostname is used with the ``core.direct`` and
    ``core.local`` scheduler and transport plugins.

    The factory has the following signature:

    :param label: The computer label. If not specified, a random UUID4 is used.
    :param hostname: The hostname of the computer. Defaults to ``localhost``.
    :param scheduler_type: The scheduler plugin to use. Defaults to ``core.direct``.
    :param transport_type: The transport plugin to use. Defaults to ``core.local``.
    :param minimum_job_poll_interval: The default minimum job poll interval to set. Defaults to 0.
    :param default_mpiprocs_per_machine: The default number of MPI procs to set. Defaults to 1.
    :param configuration_kwargs: Optional keyword arguments that, if defined, are used to configure the computer
        by calling :meth:`aiida.orm.computers.Computer.configure`.
    :return: A stored computer instance.
    """

    def factory(
        label: str | None = None,
        hostname="localhost",
        scheduler_type="core.direct",
        transport_type="core.local",
        minimum_job_poll_interval: int = 0,
        default_mpiprocs_per_machine: int = 1,
        configuration_kwargs: dict[t.Any, t.Any] | None = None,
    ) -> "Computer":
        import uuid

        from aiida.common.exceptions import NotExistent
        from aiida.orm import Computer

        label = label or f"test-computer-{uuid.uuid4().hex}"

        try:
            computer = Computer.collection.get(
                label=label, hostname=hostname, scheduler_type=scheduler_type, transport_type=transport_type
            )
        except NotExistent:
            # Create a temporary directory for this computer instance
            tmp_dir = tmp_path_factory.mktemp("aiida_computer")

            computer = Computer(
                label=label,
                hostname=hostname,
                workdir=str(tmp_dir),
                transport_type=transport_type,
                scheduler_type=scheduler_type,
            )
            computer.store()
            computer.set_minimum_job_poll_interval(minimum_job_poll_interval)
            computer.set_default_mpiprocs_per_machine(default_mpiprocs_per_machine)

        if configuration_kwargs:
            computer.configure(**configuration_kwargs)

        return computer

    return factory


@pytest.fixture(scope="session")
def aiida_remote_computer(request, aiida_computer_session, test_rootdir):
    comp_spec = request.config.getoption("remote")

    if comp_spec == "localhost-ssh":
        try:
            computer = load_computer("remote")
        except NotExistent:
            computer = aiida_computer_session(label="remote", hostname="localhost", transport_type="core.ssh")

            computer.configure(
                key_filename=f"{os.environ['HOME']}/.ssh/id_rsa",
                key_policy="AutoAddPolicy",
                safe_interval=0.1,
            )

    elif comp_spec == "test-integration":
        computer = aiida_computer_session(label="remote", hostname="localhost", transport_type="core.ssh")

        computer.configure(
            key_filename=f"{os.environ['HOME']}/.ssh/id_rsa",
            key_policy="AutoAddPolicy",
            safe_interval=0.1,
        )

        # required to load mpi and options for icon
        # FIXME: intermangles computer setup and CI specific needs, need to pass this information somehow from CI
        computer.set_prepend_text(f""". /home/runner/work/Sirocco/Sirocco/spack/share/spack/setup-env.sh || true
spack env activate {test_rootdir}""")

    elif comp_spec == "cscs-ci":
        msg = "Infrastructure for FirecREST net setup yet."
        raise NotImplementedError(msg)
    else:
        msg = f"Wrong `remote` marker specified: {comp_spec}. Choose either `localhost-ssh` or `cscs-ci`."
        raise ValueError(msg)

    return computer


@pytest.fixture(scope="session")
def test_rootdir(pytestconfig):
    """The directory of the project independent from where the tests are started"""
    return pathlib.Path(pytestconfig.rootdir)


@pytest.fixture
def minimal_config_path(tmp_path):
    minimal_config = textwrap.dedent(
        """
        name: minimal
        cycles:
          - minimal:
              tasks:
                - a:
        tasks:
          - a:
              plugin: shell
              computer: localhost
              command: some_command
        data:
          available:
            - c:
                computer: "localhost"
                src: "/c.txt"
          generated:
            - d:
                src: "d"
        """
    )
    minimal = tmp_path / "minimal.yml"
    minimal.write_text(minimal_config)
    return minimal
