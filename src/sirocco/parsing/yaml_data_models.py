from __future__ import annotations

import logging
import itertools
import re
import time
import typing
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    field_validator,
    model_validator,
)
from ruamel.yaml import YAML

from sirocco.parsing.cycling import Cycling, DateCycling, OneOff
from sirocco.parsing.target_cycle import DateList, LagList, NoTargetCycle, TargetCycle
from sirocco.parsing.when import AnyWhen, AtDate, BeforeAfterDate, When

ITEM_T = typing.TypeVar("ITEM_T")

LOGGER = logging.getLogger(__name__)

def list_not_empty(value: list[ITEM_T]) -> list[ITEM_T]:
    if len(value) < 1:
        msg = "At least one element is required."
        raise ValueError(msg)
    return value


@typing.overload
def is_absolute_path(value: None) -> None: ...


@typing.overload
def is_absolute_path(value: Path) -> Path: ...


def is_absolute_path(value: Path | None) -> Path | None:
    if value is not None and not value.is_absolute():
        msg = "The field must be an absolute path."
        raise ValueError(msg)
    return value


@typing.overload
def is_relative_path(value: None) -> None: ...


@typing.overload
def is_relative_path(value: Path) -> Path: ...


def is_relative_path(value: Path | None) -> Path | None:
    if value is not None and value.is_absolute():
        msg = "The field must be a relative path wrt. config directory."
        raise ValueError(msg)
    return value


def extract_merge_key_as_value(data: Any, new_key: str = "name") -> Any:
    if not isinstance(data, dict):
        return data
    if len(data) == 1:
        key, value = next(iter(data.items()))
        match key:
            case str():
                match value:
                    case str() if key == new_key:
                        pass
                    case dict() if new_key not in value:
                        data = value | {new_key: key}
                    case None:
                        data = {new_key: key}
                    case _:
                        msg = f"Expected a mapping, not a value (got {data})."
                        raise TypeError(msg)
            case _:
                msg = f"{new_key} must be a string (got {key})."
                raise TypeError(msg)
    return data


class _NamedBaseModel(BaseModel):
    """
    Base model for reading names from yaml keys *or* keyword args to the constructor.

    Reading from key-value pairs in yaml is also supported in order to enable
    the standard constructor usage from Python, as demonstrated in the below
    examples. On it's own it is not considered desirable.

    Examples:

        >>> _NamedBaseModel(name="foo")
        _NamedBaseModel(name='foo')

        >>> _NamedBaseModel(foo={})
        _NamedBaseModel(name='foo')

        >>> import textwrap
        >>> validate_yaml_content(
        ...     _NamedBaseModel,
        ...     textwrap.dedent('''
        ...     foo:
        ... '''),
        ... )
        _NamedBaseModel(name='foo')

        >>> validate_yaml_content(
        ...     _NamedBaseModel,
        ...     textwrap.dedent('''
        ...     name: foo
        ... '''),
        ... )
        _NamedBaseModel(name='foo')
    """

    model_config = ConfigDict(extra="forbid")
    name: str

    @model_validator(mode="before")
    @classmethod
    def reformat_named_object(cls, data: Any) -> Any:
        return extract_merge_key_as_value(data)


def select_when(spec: Any) -> When:
    match spec:
        case When():
            return spec
        case dict():
            if not all(k in ("at", "before", "after") for k in spec):
                msg = "when keys can only be 'at', 'before' or 'after'"
                raise KeyError(msg)
            if "at" in spec:
                if any(k in spec for k in ("before", "after")):
                    msg = "'at' key is incompatible with 'before' and after'"
                    raise KeyError(msg)
                return AtDate(**spec)
            return BeforeAfterDate(**spec)
        case _:
            raise TypeError


def select_target_cycle(spec: Any) -> TargetCycle:
    match spec:
        case TargetCycle():
            return spec
        case dict():
            if tuple(spec.keys()) not in (("date",), ("lag",)):
                msg = "target_cycle key can only be 'lag' or 'date' and not both"
                raise KeyError(msg)
            if "date" in spec:
                return DateList(dates=spec["date"])
            return LagList(lags=spec["lag"])
        case _:
            raise TypeError


def check_parameters_spec(params: Any) -> dict[str, Literal["all", "single"]]:
    if not isinstance(params, dict):
        raise TypeError
    for k, v in params.items():
        if v not in ("all", "single"):
            msg = f"parameter {k}: reference can only be 'single' or 'all', got {v}"
            raise ValueError(msg)
    return params


class TargetNodesBaseModel(_NamedBaseModel):
    """class for targeting other task or data nodes in the graph

    When specifying cycle tasks, this class gathers the required information for
    targeting other nodes, either input data or wait on tasks.

    """

    model_config = ConfigDict(**_NamedBaseModel.model_config | {"arbitrary_types_allowed": True})

    target_cycle: Annotated[TargetCycle, BeforeValidator(select_target_cycle)] = NoTargetCycle()
    when: Annotated[When, BeforeValidator(select_when)] = AnyWhen()
    parameters: Annotated[dict[str, Literal["all", "single"]], BeforeValidator(check_parameters_spec)] = {}


class ConfigCycleTaskInput(TargetNodesBaseModel):
    port: str


class ConfigCycleTaskWaitOn(TargetNodesBaseModel):
    pass


class ConfigCycleTaskOutput(_NamedBaseModel):
    """
    To create an instance of an output in a task in a cycle defined in a workflow file.
    """

    port: str | None = None


NAMED_BASE_T = typing.TypeVar("NAMED_BASE_T", bound=_NamedBaseModel)


def make_named_model_list_converter(
    cls: type[NAMED_BASE_T],
) -> typing.Callable[[list[NAMED_BASE_T | str | dict] | None], list[NAMED_BASE_T]]:
    def convert_named_model_list(values: list[NAMED_BASE_T | str | dict] | None) -> list[NAMED_BASE_T]:
        inputs: list[NAMED_BASE_T] = []
        if values is None:
            return inputs
        for value in values:
            match value:
                case str():
                    inputs.append(cls(name=value))
                case dict():
                    inputs.append(cls(**value))
                case _NamedBaseModel():
                    inputs.append(value)
                case _:
                    raise TypeError
        return inputs

    return convert_named_model_list


class ConfigCycleTask(_NamedBaseModel):
    """
    To create an instance of a task in a cycle defined in a workflow file.
    """

    inputs: Annotated[
        list[ConfigCycleTaskInput], BeforeValidator(make_named_model_list_converter(ConfigCycleTaskInput))
    ] = []
    outputs: Annotated[
        list[ConfigCycleTaskOutput], BeforeValidator(make_named_model_list_converter(ConfigCycleTaskOutput))
    ] = []
    wait_on: Annotated[
        list[ConfigCycleTaskWaitOn], BeforeValidator(make_named_model_list_converter(ConfigCycleTaskWaitOn))
    ] = []


def select_cycling(spec: Any) -> Cycling:
    match spec:
        case Cycling():
            return spec
        case dict():
            if spec.keys() != {"start_date", "stop_date", "period"}:
                msg = "cycling requires the 'start_date' 'stop_date' and 'period' keys and only these"
                raise KeyError(msg)
            return DateCycling(**spec)
        case _:
            raise TypeError


class ConfigCycle(_NamedBaseModel):
    """
    To create an instance of a cycle defined in a workflow file.
    """

    model_config = ConfigDict(**_NamedBaseModel.model_config | {"arbitrary_types_allowed": True})

    name: str
    tasks: list[ConfigCycleTask]
    cycling: Annotated[Cycling, BeforeValidator(select_cycling)] = OneOff()


@dataclass(kw_only=True)
class ConfigBaseTaskSpecs:
    """
    Common information for tasks.

    Any of these keys can be None, in which case they are inherited from the root task.
    """

    computer: str
    host: str | None = None
    account: str | None = None
    uenv: str | None = None
    view: str | None = None
    nodes: int | None = None  # SLURM option `--nodes`, AiiDA option `num_machines`
    walltime: str | None = None
    ntasks_per_node: int | None = None  # SLURM option `--ntasks-per-node`, AiiDA option `num_mpiprocs_per_machine`
    mem: int | None = None  # SLURM option `--mem` in MB, AiiDA option `max_memory_kb` in KB
    cpus_per_task: int | None = None  # SLURM option `--cpus_per_task`, AiiDA option `num_cores_per_mpiproc`
    mpi_cmd: str | None = None


class ConfigBaseTask(_NamedBaseModel, ConfigBaseTaskSpecs):
    """
    Config for generic task, no plugin specifics.
    """

    parameters: list[str] = Field(default_factory=list)

    @field_validator("walltime")
    @classmethod
    def validate_walltime_format(cls, value: str | None) -> str | None:
        """Validates that walltime string adheres to "%H:%M:%S" format"""
        if value is None:
            return None

        try:
            # This will raise ValueError if format is invalid
            time.strptime(value, "%H:%M:%S")
            # Return the original string, not the parsed time object
            return value  # noqa: TRY300
        except ValueError as e:
            msg = f"walltime must be in HH:MM:SS format, got '{value}'"
            raise ValueError(msg) from e

    @model_validator(mode="after")
    def validate_scheduler_parameters(self) -> ConfigBaseTask:
        # we pass these argument to aiida as resource, it performs a check to if it is able to compute the total
        # number of mpi procs. This triggers when passing any of these arguments to aiida
        if (self.nodes is not None or self.ntasks_per_node is not None or self.cpus_per_task is not None) and (
            self.nodes is None or self.ntasks_per_node is None or self.cpus_per_task is None
        ):
            msg = (
                "One of the fields 'nodes', 'ntasks_per_node' and 'cpus_per_task'"
                f" has been specified therefore all fields need to be specified for task {self}."
            )
            raise ValueError(msg)
        return self


class ConfigRootTask(ConfigBaseTask):
    plugin: ClassVar[Literal["_root"]] = "_root"


@dataclass(kw_only=True)
class ConfigSiroccoTaskSpecs:
    plugin: ClassVar[Literal["_sirocco"]] = "_sirocco"
    venv: Path | None = field(default=None, repr=False)


class ConfigSiroccoTask(ConfigBaseTask, ConfigSiroccoTaskSpecs): ...


@dataclass(kw_only=True)
class ConfigShellTaskSpecs:
    plugin: ClassVar[Literal["shell"]] = "shell"
    when_port_pattern: ClassVar[re.Pattern] = field(
        default=re.compile(r"\[(?P<opt>.*?){PORT(?P<sep_spec>\[sep=.+\])?::(?P<port>.+?)}\]"), repr=False
    )
    port_pattern: ClassVar[re.Pattern] = field(
        default=re.compile(r"{PORT(?P<sep_spec>\[sep=.+\])?::(?P<port>.+?)}"), repr=False
    )
    sep_pattern: ClassVar[re.Pattern] = field(default=re.compile(r"\[sep=(?P<sep>.+)\]"), repr=False)

    command: str
    # TODO: change "path" for "src"
    path: Path | None = field(
        default=None, repr=False, metadata={"description": ("Script file relative to the config directory.")}
    )

    # TODO: move str | None in the signature to str once ports are compulsery everywhere (also for output)
    def resolve_ports(self, input_labels: dict[str | None, list[str]]) -> str:
        """Replace port placeholders in command string with provided input labels.

        Returns a string corresponding to self.command with "{PORT::port_name}"
        placeholders replaced by the content provided in the input_labels dict.
        When multiple input nodes are linked to a single port (e.g. with
        parameterized data or if the `when` keyword specifies a list of lags or
        dates), the provided input labels are inserted with a separator
        defaulting to a " ". Specifying an alternative separator, e.g. a comma,
        is done via "{PORT[sep=,]::port_name}"

        Examples:

            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script {PORT::positionals} -l -c --verbose 2 --arg {PORT::my_arg}"
            ... )
            >>> task_specs.resolve_ports(
            ...     {"positionals": ["input_1", "input_2"], "my_arg": ["input_3"]}
            ... )
            './my_script input_1 input_2 -l -c --verbose 2 --arg input_3'

            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script {PORT::positionals} --multi_arg {PORT[sep=,]::multi_arg}"
            ... )
            >>> task_specs.resolve_ports(
            ...     {"positionals": ["input_1", "input_2"], "multi_arg": ["input_3", "input_4"]}
            ... )
            './my_script input_1 input_2 --multi_arg input_3,input_4'

            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script --input {PORT[sep= --input ]::repeat_input}"
            ... )
            >>> task_specs.resolve_ports({"repeat_input": ["input_1", "input_2", "input_3"]})
            './my_script --input input_1 --input input_2 --input input_3'

            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script [--when_opt {PORT::when_input}] --input {PORT[sep= --input ]::repeat_input}"
            ... )
            >>> task_specs.resolve_ports(
            ...     {
            ...         "when_input": ["some_when_input"],
            ...         "repeat_input": ["input_1", "input_2", "input_3"],
            ...     }
            ... )
            './my_script --when_opt some_when_input --input input_1 --input input_2 --input input_3'

            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script[ --when_opt {PORT::when_input}] --input {PORT[sep= --input ]::repeat_input}"
            ... )
            >>> task_specs.resolve_ports({"repeat_input": ["input_1", "input_2", "input_3"]})
            './my_script --input input_1 --input input_2 --input input_3'
            >>> task_specs.resolve_ports(
            ...     {"when_input": [], "repeat_input": ["input_1", "input_2", "input_3"]}
            ... )
            './my_script --input input_1 --input input_2 --input input_3'
        """

        def replace_port(cmd: str, match_obj: re.Match) -> str:
            full_match = match_obj.group(0)
            if (port := match_obj.group("port")) is None:
                msg = f"Wrong port specification: {full_match}"
                raise ValueError(msg)
            if (sep_spec := match_obj.group("sep_spec")) is None:
                sep = " "
            else:
                if (sep_match := self.sep_pattern.match(sep_spec)) is None:  # - ML - That can probably not happen
                    msg = f"Wrong separator specification: {sep_spec}"
                    raise ValueError(msg)
                if (sep := sep_match.group("sep")) is None:
                    msg = f"Wrong separator specification: {sep_spec}"
                    raise ValueError(msg)

            if "opt" in match_obj.groupdict():
                if (opt := match_obj.group("opt")) is None:
                    msg = f"Wrong port specification: {full_match}"
                    raise ValueError(msg)
                if not input_labels.get(port):
                    return cmd.replace(full_match, "")
                return cmd.replace(full_match, opt + sep.join(input_labels[port]))

            if port not in input_labels:
                msg = f"The input_labels dictionnary doesn't have the {port} key. If the port has a when condition, enclose the whole dedicated comand line part in square brackets e.g. command: ... [--arg={{PORT:my_port}}] ..."
                raise KeyError(msg)
            return cmd.replace(full_match, sep.join(input_labels[port]))

        cmd = self.command
        for when_port_match in self.when_port_pattern.finditer(cmd):
            cmd = replace_port(cmd, when_port_match)
        for port_match in self.port_pattern.finditer(cmd):
            cmd = replace_port(cmd, port_match)
        return cmd


class ConfigShellTask(ConfigBaseTask, ConfigShellTaskSpecs):
    """
    Represent a shell script to be run as part of the workflow.

    Examples:

        >>> import textwrap
        >>> my_task = validate_yaml_content(
        ...     ConfigShellTask,
        ...     textwrap.dedent(
        ...         '''
        ...     my_task:
        ...       plugin: shell
        ...       computer: localhost
        ...       command: "my_script.sh -n 1024 {PORT::current_sim_output}"
        ...       path: post_run_scripts/my_script.sh
        ...       walltime: 00:01:00
        ...     '''
        ...     ),
        ... )
        >>> my_task.walltime
        '00:01:00'
    """

    # We need to loosen up the extra='forbid' flag because of the plugin class var
    model_config = ConfigDict(**ConfigBaseTask.model_config | {"extra": "ignore"})

    @field_validator("path", mode="after")
    @classmethod
    def check_is_relative_path(cls, value: Path | None) -> Path | None:
        return is_relative_path(value)


@dataclass(kw_only=True)
class ConfigNamelistFileSpec: ...


class ConfigNamelistFile(BaseModel, ConfigNamelistFileSpec):
    """
    Validated namelist specifications.

    - path is the path to the namelist file considered as template
    - specs is a dictionnary containing the specifications of parameters
      to change in the original namelist file

    Example:

        >>> import textwrap
        >>> from_init = ConfigNamelistFile(
        ...     path="path/to/some.nml", specs={"block": {"key": "value"}}
        ... )
        >>> from_yml = validate_yaml_content(
        ...     ConfigNamelistFile,
        ...     textwrap.dedent(
        ...         '''
        ...         path/to/some.nml:
        ...           block:
        ...             key: value
        ...         '''
        ...     ),
        ... )
        >>> from_init == from_yml
        True
        >>> no_spec = ConfigNamelistFile(path="path/to/some.nml")
        >>> no_spec_yml = validate_yaml_content(ConfigNamelistFile, "path/to/some.nml")
    """

    specs: dict[str, Any] = field(default_factory=dict)
    path: Annotated[Path, AfterValidator(is_relative_path)] = field(repr=False)

    @model_validator(mode="before")
    @classmethod
    def merge_path_key(cls, data: Any) -> dict[str, Any]:
        if isinstance(data, str):
            return {"path": data}
        merged = extract_merge_key_as_value(data, new_key="path")
        if "specs" in merged:
            return merged
        path = merged.pop("path")
        return {"path": path, "specs": merged or {}}


@dataclass(kw_only=True)
class ConfigIconTaskSpecs:
    plugin: ClassVar[Literal["icon"]] = "icon"
    bin: Path | None = field(repr=True, default=None)
    bin_cpu: Path | None = field(repr=True, default=None)
    bin_gpu: Path | None = field(repr=True, default=None)
    wrapper_script: Path | None = field(
        default=None,
        repr=False,
        metadata={"description": "Path to wrapper script file relative to the config directory or absolute."},
    )
    target: Literal["santis_cpu", "santis_gpu"] | None = field(
        default=None,
        metadata={
            "description": "Use a predefined setup for the target machine. Ignore mpi_command, wrapper_script and env"
        },
    )
    assets: Path | None = field(
        default=None,
        repr=False,
        metadata={
            "description": "Path relative to config dir containing runtime files (environment setup, mpi command, etc...)"
        },
    )


class ConfigIconTask(ConfigBaseTask, ConfigIconTaskSpecs):
    """Class representing an ICON task configuration from a workflow file

    Examples:

    yaml snippet:

        >>> import textwrap
        >>> snippet = textwrap.dedent(
        ...     '''
        ...       ICON:
        ...         plugin: icon
        ...         computer: localhost
        ...         namelists:
        ...           - path/to/icon_master.namelist
        ...           - path/to/case_nml:
        ...               block_1:
        ...                 param_name: param_value
        ...         bin: /path/to/icon
        ...     '''
        ... )
        >>> icon_task_cfg = validate_yaml_content(ConfigIconTask, snippet)
    """

    # We need to loosen up the extra='forbid' flag because of the plugin class var
    model_config = ConfigDict(**ConfigBaseTask.model_config | {"extra": "ignore"})
    namelists: list[ConfigNamelistFile]

    @field_validator("namelists", mode="after")
    @classmethod
    def check_nmls(cls, nmls: list[ConfigNamelistFile]) -> list[ConfigNamelistFile]:
        # Make validator idempotent even if not used yet
        names = [nml.path.name for nml in nmls]
        if "icon_master.namelist" not in names:
            msg = "icon_master.namelist not found"
            raise ValueError(msg)
        return nmls

    @field_validator("bin", "bin_cpu", "bin_gpu", mode="after")
    @classmethod
    def check_is_absolute_path(cls, value: Path | None) -> Path | None:
        return is_absolute_path(value)

    @model_validator(mode="after")
    def check_bin_present(self) -> ConfigIconTask:
        match self.target:
            case None:
                if self.bin is None and self.bin_cpu is None and self.bin_gpu is None:
                    msg = "No ICON binary specified. If target is unset, specify at least one of 'bin', 'bin_cpu' or 'bin_gpu'"
                    raise ValueError(msg)
            case "santis_cpu":
                if self.bin is None and self.bin_cpu is None:
                    msg = f"{self.name}: target set to 'santis_cpu', specify one of 'bin' or 'bin_cpu'"
                    raise ValueError(msg)
                elif self.bin is not None and self.bin_cpu is not None:
                    msg = f"{self.name}: target set to 'santis_cpu', 'bin' and 'bin_cpu' specified, ignoring 'bin'"
                    LOGGER.warning(msg)
            case "santis_gpu":
                if self.bin is None and self.bin_gpu is None:
                    msg = f"{self.name}: target set to 'santis_gpu', specify one of 'bin' or 'bin_gpu'"
                    raise ValueError(msg)
                elif self.bin is not None and self.bin_gpu is not None:
                    msg = f"{self.name}: target set to 'santis_gpu', 'bin' and 'bin_gpu' specified, ignoring 'bin'"
                    LOGGER.warning(msg)
        return self


@dataclass(kw_only=True)
class ConfigBaseDataSpecs:
    format: str | None = None


class ConfigBaseData(_NamedBaseModel, ConfigBaseDataSpecs):
    """
    To create an instance of a data defined in a workflow file.

    Examples:

        yaml snippet:

            >>> import textwrap
            >>> snippet = textwrap.dedent(
            ...     '''
            ...       foo:
            ...     '''
            ... )
            >>> validate_yaml_content(ConfigBaseData, snippet)
            ConfigBaseData(format=None, name='foo', parameters=[])


        from python:

            >>> ConfigBaseData(name="foo")
            ConfigBaseData(format=None, name='foo', parameters=[])
    """

    parameters: list[str] = []


@dataclass(kw_only=True)
class ConfigAvailableDataSpecs:
    path: Annotated[Path, AfterValidator(is_absolute_path)]


class ConfigAvailableData(ConfigBaseData, ConfigAvailableDataSpecs):
    computer: str


@dataclass(kw_only=True)
class ConfigGeneratedDataSpecs:
    path: Path | None = None


class ConfigGeneratedData(ConfigBaseData, ConfigGeneratedDataSpecs): ...


class ConfigData(BaseModel):
    """
    To create the container of available and generated data

    Example:

        yaml snippet:

            >>> import textwrap
            >>> snippet = textwrap.dedent(
            ...     '''
            ...     available:
            ...       - foo:
            ...           computer: "localhost"
            ...           path: "/foo.txt"
            ...     generated:
            ...       - bar:
            ...           path: "bar.txt"
            ...     '''
            ... )
            >>> data = validate_yaml_content(ConfigData, snippet)
            >>> assert data.available[0].name == "foo"
            >>> assert data.generated[0].name == "bar"

        from python:

            >>> ConfigData()
            ConfigData(available=[], generated=[])
    """

    available: list[ConfigAvailableData] = []
    generated: list[ConfigGeneratedData] = []


def get_plugin_from_named_base_model(
    data: dict | ConfigRootTask | ConfigSiroccoTask | ConfigShellTask | ConfigIconTask,
) -> str:
    if isinstance(data, ConfigRootTask | ConfigSiroccoTask | ConfigShellTask | ConfigIconTask):
        return data.plugin
    name_and_specs = extract_merge_key_as_value(data)
    match name_and_specs.get("name", None):
        case "ROOT":
            return ConfigRootTask.plugin
        case "SIROCCO":
            return ConfigSiroccoTask.plugin
    plugin = name_and_specs.get("plugin", None)
    if plugin is None:
        msg = f"Could not find plugin name in {data}"
        raise ValueError(msg)
    return plugin


ConfigTask = Annotated[
    Annotated[ConfigRootTask, Tag(ConfigRootTask.plugin)]
    | Annotated[ConfigSiroccoTask, Tag(ConfigSiroccoTask.plugin)]
    | Annotated[ConfigIconTask, Tag(ConfigIconTask.plugin)]
    | Annotated[ConfigShellTask, Tag(ConfigShellTask.plugin)],
    Discriminator(get_plugin_from_named_base_model),
]


def check_parameters_lists(data: Any) -> dict[str, list]:
    if not isinstance(data, dict):
        raise TypeError
    for param_name, param_values in data.items():
        msg = f"""{param_name}: parameters must map a string to list of single values, got {param_values}"""
        if isinstance(param_values, list):
            for v in param_values:
                if isinstance(v, dict | list):
                    raise TypeError(msg)
        else:
            raise TypeError(msg)
    return data


class ConfigWorkflow(BaseModel):
    """
    The root of the configuration tree.

    Examples:

        minimal yaml to generate:

            >>> import textwrap
            >>> content = textwrap.dedent(
            ...     '''
            ...     name: minimal
            ...     scheduler: slurm
            ...     rootdir: /location/of/config
            ...     config_filename: config.yml
            ...     cycles:
            ...       - minimal_cycle:
            ...           tasks:
            ...             - task_a:
            ...     tasks:
            ...       - task_a:
            ...           plugin: shell
            ...           computer: localhost
            ...           command: "some_command"
            ...     data:
            ...       available:
            ...         - foo:
            ...             computer: localhost
            ...             path: /foo.txt
            ...       generated:
            ...         - bar:
            ...             path: bar
            ...     '''
            ... )
            >>> wf = validate_yaml_content(ConfigWorkflow, content)

        minimum programmatically created instance

            >>> wf = ConfigWorkflow(
            ...     name="minimal",
            ...     scheduler="slurm",
            ...     rootdir=Path("/location/of/config"),
            ...     config_filename="config.yml",
            ...     cycles=[ConfigCycle(minimal_cycle={"tasks": [ConfigCycleTask(task_a={})]})],
            ...     tasks=[
            ...         ConfigShellTask(
            ...             task_a={
            ...                 "plugin": "shell",
            ...                 "computer": "localhost",
            ...                 "command": "some_command",
            ...             }
            ...         )
            ...     ],
            ...     data=ConfigData(
            ...         available=[
            ...             ConfigAvailableData(
            ...                 name="foo",
            ...                 computer="localhost",
            ...                 path="/foo.txt",
            ...             )
            ...         ],
            ...         generated=[ConfigGeneratedData(name="bar", path="bar")],
            ...     ),
            ...     parameters={},
            ... )

    """

    rootdir: Path
    config_filename: str
    scheduler: Literal["slurm"]
    name: str
    front_depth: int = 2
    cycles: Annotated[list[ConfigCycle], BeforeValidator(list_not_empty)]
    tasks: Annotated[list[ConfigTask], BeforeValidator(list_not_empty)]
    data: ConfigData
    parameters: Annotated[dict[str, list], BeforeValidator(check_parameters_lists)] = {}

    @model_validator(mode="after")
    def check_parameters(self) -> ConfigWorkflow:
        task_data_list = itertools.chain(self.tasks, self.data.generated, self.data.available)
        for item in task_data_list:
            for param_name in item.parameters:
                if param_name not in self.parameters:
                    msg = f"parameter {param_name} in {item.name} specification not declared in parameters section"
                    raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def propagate_root_task(self) -> ConfigWorkflow:
        root_task: ConfigRootTask | None = None
        for task in self.tasks:
            if isinstance(task, ConfigRootTask):
                if root_task is not None:
                    msg = "Only one root task can be provided"
                    raise ValueError(msg)
                root_task = task
        if root_task is not None:
            for root_key, root_value in dict(root_task).items():
                for task in self.tasks:
                    if not isinstance(task, ConfigRootTask) and getattr(task, root_key, None) is None:
                        setattr(task, root_key, root_value)
        return self

    @classmethod
    def from_config_file(cls, config_path: str | Path) -> Self:
        """Creates a ConfigWorkflow instance from a config file, a yaml with the workflow definition.

        Args:
            config_path (str): The path of the config file to load from.

        Returns:
            OBJECT_T: An instance of the specified class type with data parsed and
            validated from the YAML content.
        """
        config_filename = Path(config_path).stem
        config_resolved_path = Path(config_path).resolve()
        if not config_resolved_path.exists():
            msg = f"Workflow config file in path {config_resolved_path} does not exists."
            raise FileNotFoundError(msg)
        if not config_resolved_path.is_file():
            msg = f"Workflow config file in path {config_resolved_path} is not a file."
            raise FileNotFoundError(msg)

        content = config_resolved_path.read_text()
        # An empty workflow is parsed to None object so we catch this here for a more understandable error
        if content == "":
            msg = f"Workflow config file in path {config_resolved_path} is empty."
            raise ValueError(msg)
        reader = YAML(typ="safe", pure=True)
        object_ = reader.load(StringIO(content))
        # If name was not specified, then we use filename without file extension
        if "name" not in object_:
            object_["name"] = config_filename
        object_["rootdir"] = config_resolved_path.parent
        object_["config_filename"] = Path(config_path).name
        adapter = TypeAdapter(cls)
        return adapter.validate_python(object_)


OBJECT_T = typing.TypeVar("OBJECT_T")


def validate_yaml_content(cls: type[OBJECT_T], content: str) -> OBJECT_T:
    """Parses the YAML content into a python object using generic types and subsequently validates it with pydantic.

    Args:
        cls (type[OBJECT_T]): The class type to which the parsed yaml content should
            be validated. It must be compatible with pydantic validation.
        content (str): The yaml content as a string.

    Returns:
        OBJECT_T: An instance of the specified class type with data parsed and
        validated from the YAML content.

    Raises:
        pydantic.ValidationError: If the YAML content cannot be validated
        against the specified class type.
        ruamel.yaml.YAMLError: If there is an error in parsing the YAML content.
    """
    return TypeAdapter(cls).validate_python(YAML(typ="safe", pure=True).load(StringIO(content)))
