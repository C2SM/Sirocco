from __future__ import annotations

import itertools
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, Self, TypeVar, overload

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    model_validator,
)
from ruamel.yaml import YAML

from sirocco.parsing._utils import validate_walltime_format
from sirocco.parsing.cycling import Cycling, DateCycling, OneOff
from sirocco.parsing.target_cycle import DateList, LagList, NoTargetCycle, TargetCycle
from sirocco.parsing.when import AnyWhen, AtDate, BeforeAfterDate, When

if TYPE_CHECKING:
    from collections.abc import Iterator


LOGGER = logging.getLogger(__name__)


class JinjaResolver:
    """Handles Jinja2 template rendering with variable resolution."""

    def __init__(self):
        """Initialize Jinja2 environment with strict settings."""
        from jinja2 import Environment, StrictUndefined

        self.env = Environment(
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            autoescape=True,
        )

    def load_variables_from_file(
        self,
        config_path: Path,
        vars_file_path: Path | None = None,
    ) -> dict[str, Any]:
        """Load variables from a YAML file (explicit or auto-detected).

        Args:
            config_path: Path to the config file (used for auto-detection)
            vars_file_path: Optional explicit path to variables file

        Returns:
            Dict of variables loaded from file

        Raises:
            FileNotFoundError: If vars_file_path is provided but doesn't exist
        """
        # Use explicitly provided vars file path, or auto-detect
        jinja_vars_file = None
        if vars_file_path:
            # Explicit path provided - use it directly
            jinja_vars_file = Path(vars_file_path).resolve()
            if not jinja_vars_file.exists():
                msg = f"Variables file not found: {jinja_vars_file}"
                raise FileNotFoundError(msg)
        else:
            # Auto-detect variables file in the config directory
            config_dir = config_path.parent
            for vars_name in ["vars.yml", "vars.yaml", "variables.yml", "variables.yaml"]:
                candidate = config_dir / vars_name
                if candidate.exists():
                    jinja_vars_file = candidate
                    break

        if jinja_vars_file:
            # Load variables from file
            vars_content = YAML(typ="safe", pure=True).load(jinja_vars_file.read_text())
            return vars_content or {}

        return {}

    def render(
        self,
        content: str,
        template_context: dict[str, Any] | None = None,
    ) -> str:
        """Render Jinja2 template with context variables.

        Args:
            content: Template content to render
            template_context: Dict of context variables to use in rendering

        Returns:
            Rendered template content

        Raises:
            ValueError: If template rendering fails or required variables are missing
        """
        context = template_context or {}

        try:
            template = self.env.from_string(content)
            return template.render(**context)
        except Exception as e:
            msg = f"Failed to render Jinja2 template: {e}"
            raise ValueError(msg) from e


def list_not_empty[ITEM_T](value: list[ITEM_T]) -> list[ITEM_T]:
    if len(value) < 1:
        msg = "At least one element is required."
        raise ValueError(msg)
    return value


@overload
def is_absolute_path(value: None) -> None: ...


@overload
def is_absolute_path(value: Path) -> Path: ...


def is_absolute_path(value: Path | None) -> Path | None:
    if value is not None and not value.is_absolute():
        msg = "The field must be an absolute path."
        raise ValueError(msg)
    return value


@overload
def is_relative_path(value: None) -> None: ...


@overload
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


class ConfigCycleTaskInput(TargetNodesBaseModel): ...


class ConfigCycleTaskWaitOn(TargetNodesBaseModel): ...


class ConfigCycleTaskOutput(_NamedBaseModel): ...


type NamedModelListConverter[T: _NamedBaseModel] = Callable[[list[T | str | dict] | None], list[T]]


def make_named_model_list_converter[NAMED_BASE_T: _NamedBaseModel](
    cls: type[NAMED_BASE_T],
) -> NamedModelListConverter[NAMED_BASE_T]:
    def convert_named_model_list(
        values: list[NAMED_BASE_T | str | dict] | None,
    ) -> list[NAMED_BASE_T]:
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


type NamedModelDictConverter[T: _NamedBaseModel] = Callable[
    [dict[str, list[T | str | dict]] | None], dict[str, list[T]]
]


def make_named_model_dict_converter[NAMED_BASE_T: _NamedBaseModel](
    cls: type[NAMED_BASE_T],
) -> NamedModelDictConverter[NAMED_BASE_T]:
    named_model_list_converter = make_named_model_list_converter(cls)

    def convert_named_model_dict(
        values: dict[str, list[NAMED_BASE_T | str | dict]] | None,
    ) -> dict[str, list[NAMED_BASE_T]]:
        if values is None:
            return {}
        return {port: named_model_list_converter(data_list) for port, data_list in values.items()}

    return convert_named_model_dict


class ConfigCycleTaskComponent(BaseModel):
    inputs: Annotated[
        dict[str, list[ConfigCycleTaskInput]],
        BeforeValidator(make_named_model_dict_converter(ConfigCycleTaskInput)),
    ] = {}
    outputs: Annotated[
        dict[str, list[ConfigCycleTaskOutput]],
        BeforeValidator(make_named_model_dict_converter(ConfigCycleTaskOutput)),
    ] = {}


class ConfigCycleTask(_NamedBaseModel):
    """
    To create an instance of a task in a cycle defined in a workflow file.
    """

    __SINGLE_COMPONENT_NAME__: ClassVar[Literal["__SINGLE_COMPONENT__"]] = "__SINGLE_COMPONENT__"
    components: dict[str, ConfigCycleTaskComponent] = {}
    wait_on: Annotated[
        list[ConfigCycleTaskWaitOn],
        BeforeValidator(make_named_model_list_converter(ConfigCycleTaskWaitOn)),
    ] = []

    @model_validator(mode="before")
    @classmethod
    def ensure_components(cls, data: dict[str, Any]) -> dict[str, Any]:
        """
        Ensure single component if no "components" key is provided
        """
        # NOTE: execute _NamedBaseModel before model validator first because of this ordering issue
        #       https://github.com/pydantic/pydantic/issues/10790
        data = extract_merge_key_as_value(data)
        if "components" not in data:
            single_comp: dict[str, Any] = {}
            for key in ("inputs", "outputs"):
                single_comp[key] = data.pop(key, {})
            data["components"] = {cls.__SINGLE_COMPONENT_NAME__: single_comp}
        return data


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
    partition: str | None = None
    uenv: str | None = None
    view: str | None = None
    nodes: int = 1  # SLURM option `--nodes`, AiiDA option `num_machines`
    walltime: Annotated[str | None, BeforeValidator(validate_walltime_format)] = None
    mem: int | None = None  # SLURM option `--mem` in MB, AiiDA option `max_memory_kb` in KB
    procs_per_node: int | None = None  # SLURM option `--ntasks-per-node`, AiiDA option `num_mpiprocs_per_machine`
    cores_per_proc: int | None = None  # SLURM option `--cpus-per-task`, AiiDA option `num_cores_per_mpiproc`
    mpi_cmd: str | None = None


class ConfigBaseTask(_NamedBaseModel, ConfigBaseTaskSpecs):
    """
    Config for generic task, no plugin specifics.
    """

    parameters: list[str] = Field(default_factory=list)


class ConfigRootTask(ConfigBaseTask):
    plugin: ClassVar[Literal["_root"]] = "_root"


@dataclass(kw_only=True)
class ConfigSiroccoTaskSpecs:
    plugin: ClassVar[Literal["_sirocco", "sirocco_continue"]] = "_sirocco"
    venv: Path | None = field(default=None, repr=False)


class ConfigSiroccoTask(ConfigBaseTask, ConfigSiroccoTaskSpecs): ...


@dataclass(kw_only=True)
class ConfigShellTaskSpecs:
    plugin: ClassVar[Literal["shell"]] = "shell"
    when_port_pattern: ClassVar[re.Pattern] = field(
        default=re.compile(r"\[(?P<opt>.*?){PORT(?P<sep_spec>\[sep=.+\])?::(?P<port>.+?)}\]"),
        repr=False,
    )
    port_pattern: ClassVar[re.Pattern] = field(
        default=re.compile(r"{PORT(?P<sep_spec>\[sep=.+\])?::(?P<port>.+?)}"),
        repr=False,
    )
    sep_pattern: ClassVar[re.Pattern] = field(default=re.compile(r"\[sep=(?P<sep>.+)\]"), repr=False)

    command: str
    # TODO: change "path" for "src"
    path: Annotated[Path | None, AfterValidator(is_relative_path)] = field(
        default=None,
        repr=False,
        metadata={"description": ("Script file relative to the config directory.")},
    )

    def resolve_ports(self, port_labels: dict[str, list[str]]) -> str:
        """Replace port placeholders in command string with provided input labels.

        Returns a string corresponding to self.command with "{PORT::port_name}"
        placeholders replaced by the content provided in the port_labels dict.
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
            >>> task_specs = ConfigShellTaskSpecs(
            ...     command="./my_script [--when_opt_1 {PORT::when_input_1}] [--when_opt_2 {PORT::when_input_2}]"
            ... )
            >>> task_specs.resolve_ports({})
            './my_script  '
            >>> task_specs.resolve_ports({"when_input_1": ["opt_1"]})
            './my_script --when_opt_1 opt_1 '
            >>> task_specs.resolve_ports({"when_input_2": ["opt_2"]})
            './my_script  --when_opt_2 opt_2'
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
                if not port_labels.get(port):
                    return cmd.replace(full_match, "")
                return cmd.replace(full_match, opt + sep.join(port_labels[port]))

            if port not in port_labels:
                msg = (
                    f"The port_labels dictionary doesn't have the {port} key. "
                    "If the port has a when condition, enclose the whole dedicated "
                    "command line part in square brackets "
                    "e.g. command: ... [--arg={{PORT::my_port}}] ..."
                )
                raise KeyError(msg)
            return cmd.replace(full_match, sep.join(port_labels[port]))

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
class ConfigIconExecutables:
    cpu: ConfigIconExe | None = None
    gpu: ConfigIconExe | None = None
    hiopy: ConfigHiopyExe | None = None
    procs_per_io_node: int = 0
    separate_io: bool = False

    def __post_init__(self) -> None:
        if self.cpu is None and self.gpu is None:
            msg = "at least one of 'cpu' or 'gpu' must be set"
            raise ValueError(msg)

    @property
    def tot_io_procs(self) -> int:
        return (
            self.gpu.tot_io_procs
            if self.gpu
            else 0 + self.cpu.tot_io_procs
            if self.cpu
            else 0 + self.hiopy.procs
            if self.hiopy
            else 0
        )


@dataclass(kw_only=True)
class ConfigIconExe:
    path: Path
    procs: dict[str, ConfigIconExeProc]
    icon4py_venv: Path | None = None
    gt4py_build_cache_dir: Path | None = None
    compute_procs_per_node: int | None = None
    sockets_per_node: int = 1

    @property
    def tot_io_procs(self) -> int:
        return sum(proc.tot_io_procs for proc in self.procs.values())

    def model_names(self) -> Iterator[str]:
        yield from self.procs


@dataclass(kw_only=True)
class ConfigIconExeProc:
    compute_weight: int = 1
    prefetch: int = 0
    restart: int = 0
    streams: int = 0

    @property
    def tot_io_procs(self) -> int:
        return self.prefetch + self.restart + self.streams


@dataclass(kw_only=True)
class ConfigHiopyExe:
    venv: Path
    procs: int = 0
    min_rank: int = field(init=False, repr=False)
    max_rank: int = field(init=False, repr=False)


def validate_executables(exes: ConfigIconExecutables) -> ConfigIconExecutables:
    if exes.cpu is None and exes.gpu is None:
        msg = "At least one of cpu or gpu executable must be specified"
        raise ValueError(msg)

    gpu_models = set(exes.gpu.model_names()) if exes.gpu else set()
    cpu_models = set(exes.cpu.model_names()) if exes.cpu else set()
    if common_models := gpu_models & cpu_models:
        msg = f"{common_models} sepcified for both cpu and gpu executables"
        raise ValueError(msg)

    if exes.separate_io and exes.procs_per_io_node == 0:
        msg = "procs_per_io_node must be > 0 when using separate_io"
        raise ValueError(msg)

    return exes


@dataclass(kw_only=True)
class ConfigIconTaskSpecs:
    plugin: ClassVar[Literal["icon"]] = "icon"
    exe: Annotated[ConfigIconExecutables, AfterValidator(validate_executables)]
    # TODO: remove bin, only kept for compatibility with AiiDA for now
    bin: Annotated[Path | None, AfterValidator(is_absolute_path)] = field(repr=True, default=None)
    # TODO: remove wrapper_script, only kept for compatibility with AiiDA for now
    wrapper_script: Path | None = field(
        default=None,
        repr=False,
        metadata={"description": "Path to wrapper script file relative to the config directory or absolute."},
    )
    runtime: Path | None = field(
        default=None,
        repr=False,
        metadata={
            "description": "Path relative to config dir containing runtime files (environment setup, mpi command, etc...)"
        },
    )
    # NOTE: Cannot use init=False as ConfigIconTask inherits from BaseModel which does not support it (yet?)
    #       Se possible workaround there: https://github.com/pydantic/pydantic/discussions/5929#discussioncomment-12936754
    target: Literal["cpu", "gpu", "hybrid", "__none__"] = field(repr=False, default="__none__")


def check_icon_mater_namelist(namelists: list[ConfigNamelistFile]) -> list[ConfigNamelistFile]:
    if "icon_master.namelist" not in (nml.path.name for nml in namelists):
        msg = "icon_master.namelist not found"
        raise ValueError(msg)
    return namelists


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
        ...         procs_per_node: 288
        ...         namelists:
        ...           - path/to/icon_master.namelist
        ...           - path/to/case_nml:
        ...               block_1:
        ...                 param_name: param_value
        ...         exe:
        ...           cpu:
        ...             path: /path/to/icon
        ...             procs:
        ...               atm: {}
        ...     '''
        ... )
        >>> icon_task_cfg = validate_yaml_content(ConfigIconTask, snippet)
    """

    # We need to loosen up the extra='forbid' flag because of the plugin class var
    model_config = ConfigDict(**ConfigBaseTask.model_config | {"extra": "ignore"})
    # Keep namelists here and not in ConfigIconTaskSpecs as the namelists attribute
    # gets also defined in core.IconTask which inherits from ConfigIconTaskSpecs
    namelists: Annotated[list[ConfigNamelistFile], AfterValidator(check_icon_mater_namelist)]

    @model_validator(mode="after")
    def set_target(self) -> ConfigIconTask:
        if self.exe.gpu and not self.exe.cpu and not self.exe.hiopy:
            self.target = "gpu"
        elif self.exe.cpu and not self.exe.gpu and not self.exe.hiopy:
            self.target = "cpu"
        else:
            self.target = "hybrid"
        return self

    # @model_validator(mode="after")
    # def set_defaults(self) -> ConfigIconTask:
    #     if self.target == "__none__":
    #         msg = f"error in validation order, target should be set, got {self.target}"
    #         raise ValueError(msg)
    #     match self.computer:
    #         case "santis":
    #             if self.target == "cpu" and not self.procs_per_node:
    #                 self.procs_per_node = 288
    #                 if self.cores_per_proc:
    #                     self.procs_per_node = self.procs_per_node // self.cores_per_proc
    #             if self.target == "gpu" and not self.procs_per_node:
    #                 self.procs_per_node = 4
    #                 if self.cores_per_proc:
    #                     self.procs_per_node = self.procs_per_node // self.cores_per_proc
    #     return self


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
    computer: str | None = None


@dataclass(kw_only=True)
class ConfigGeneratedDataSpecs:
    # Path is optional because certain task types (e.g., ICON tasks) compute
    # output paths programmatically at runtime based on port names
    # (e.g., 'finish_status' -> 'finish_xxx.status', 'latest_restart_file' -> computed from namelist).
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


def propagate_root_task_specs(config_task_list: list[ConfigTask | dict[str, dict]]) -> list[ConfigTask]:
    """propagate ROOT task specs and returns the list of updated ConfigTask"""

    if not config_task_list:
        msg = "At least one task is required."
        raise ValueError(msg)

    updated_config_task_list: list[ConfigTask] = []

    # Gat root specs if any
    root_specs: dict = {}
    for config_task in config_task_list:
        if root_specs:
            msg = "root specs already found, only one root task allowed"
            raise ValueError(msg)
        if isinstance(config_task, dict):
            name_and_specs: dict = extract_merge_key_as_value(config_task)
            if name_and_specs["name"] == "ROOT":
                root_specs = name_and_specs
                break
        if isinstance(config_task, ConfigRootTask):
            root_specs = dict(config_task)
            break

    # Propagate root specs
    root_specs.pop("name", None)
    root_specs.pop("parameters", None)
    for config_task in config_task_list:
        if isinstance(config_task, dict):
            name_and_specs = extract_merge_key_as_value(config_task)
            if name_and_specs["name"] != "ROOT":
                name_and_specs.update(root_specs)
                updated_config_task_list.append(TypeAdapter(ConfigTask).validate_python(name_and_specs))
        if isinstance(config_task, ConfigShellTask | ConfigIconTask):
            for root_key, root_value in root_specs.items():
                setattr(config_task, root_key, root_value)
            updated_config_task_list.append(config_task)

    return updated_config_task_list


class ConfigWorkflow(BaseModel):
    """
    The root of the configuration tree.

    Examples:

        minimal yaml to generate:

            >>> import textwrap
            >>> content = textwrap.dedent(
            ...     '''
            ...     name: minimal
            ...     engine: standalone
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
            ...     engine="standalone",
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
    resolved_config_path: str | None = Field(
        default=None,
        description="Path to the resolved config file (with Jinja2 variables replaced). Set automatically during loading.",
    )
    engine: Literal["standalone", "aiida"] = Field(
        default="standalone",
        description="Workflow execution engine to use.",
    )
    scheduler: Literal["slurm"] = "slurm"
    name: str
    cycles: Annotated[list[ConfigCycle], BeforeValidator(list_not_empty)]
    # TODO: Implement ROOT task specs propagation in this before validator to allow for compulsory specs only given through ROOT task
    # tasks: Annotated[list[ConfigTask], BeforeValidator(list_not_empty)]
    tasks: Annotated[list[ConfigTask], BeforeValidator(propagate_root_task_specs)]
    data: ConfigData
    parameters: Annotated[dict[str, list], BeforeValidator(check_parameters_lists)] = {}
    front_depth: int = Field(
        default=2,
        description="Number of active topological levels. n: n-1 levels of presubmitted tasks, i.e. 1: only ongoing tasks (no pre-submission), 2: one level of pre-submitted tasks (default), etc...",
        ge=1,
    )

    @model_validator(mode="after")
    def check_parameters(self) -> ConfigWorkflow:
        task_data_list = itertools.chain(self.tasks, self.data.generated, self.data.available)
        for item in task_data_list:
            for param_name in item.parameters:
                if param_name not in self.parameters:
                    msg = f"parameter {param_name} in {item.name} specification not declared in parameters section"
                    raise ValueError(msg)
        return self

    @classmethod
    def from_config_file(
        cls,
        config_path: str | Path,
        template_context: dict[str, Any] | None = None,
        jinja_vars_file_path: str | Path | None = None,
    ) -> Self:
        """Creates a ConfigWorkflow instance from a config file, a yaml with the workflow definition.

        All config files are processed as Jinja2 templates with the following features:
        - Use {{ VAR }} syntax for variables
        - Full Jinja2 features: conditionals, loops, filters, etc.
        - Missing variables raise clear errors (StrictUndefined)

        **Variable sources (in priority order):**
        1. Explicitly specified jinja_vars_file_path (if provided)
        2. Auto-detected vars.yml/vars.yaml/variables.yml/variables.yaml in config directory
        3. Explicitly provided template_context parameter (overrides all)

        Args:
            config_path (str): The path of the config file to load from.
            template_context (dict[str, Any] | None): Optional context variables to use in template rendering.
            jinja_vars_file_path (str | Path | None): Optional explicit path to variables file.

        Returns:
            ConfigWorkflow: An instance with data parsed and validated from the YAML content.

        Raises:
            FileNotFoundError: If config file doesn't exist or jinja_vars_file_path is provided but doesn't exist
            ValueError: If template rendering fails or file is empty
        """
        config_resolved_path = Path(config_path).resolve()
        if not config_resolved_path.exists():
            msg = f"Workflow config file in path {config_resolved_path} does not exists."
            raise FileNotFoundError(msg)
        if not config_resolved_path.is_file():
            msg = f"Workflow config file in path {config_resolved_path} is not a file."
            raise FileNotFoundError(msg)

        content = config_resolved_path.read_text()
        if content == "":
            msg = f"Workflow config file in path {config_resolved_path} is empty."
            raise ValueError(msg)

        # Determine config filename (without extension)
        config_filename = config_resolved_path.stem

        # Always render as Jinja2 template
        resolver = JinjaResolver()

        # Load variables from file (if any)
        context = resolver.load_variables_from_file(
            config_resolved_path, Path(jinja_vars_file_path) if jinja_vars_file_path else None
        )

        # Provided context overrides file-based ones
        if template_context:
            context.update(template_context)

        # Store original content to detect if templating actually changed anything
        original_content = content

        # Render the template
        try:
            content = resolver.render(content, context)
        except ValueError as e:
            # Re-raise with config path context
            msg = f"Failed to render Jinja2 template {config_resolved_path}: {e.args[0].split(': ', 1)[-1]}"
            raise ValueError(msg) from e.__cause__

        # Only write resolved config if templating actually changed the content
        resolved_config_path_str: str | None = None
        if content != original_content:
            resolved_config_path = config_resolved_path.parent / f"{config_resolved_path.stem}.resolved.yml"
            resolved_config_path.write_text(content)
            resolved_config_path_str = str(resolved_config_path)

        # Parse YAML and validate
        reader = YAML(typ="safe", pure=True)
        object_ = reader.load(StringIO(content))
        if "name" not in object_:
            object_["name"] = config_filename
        object_["rootdir"] = config_resolved_path.parent
        object_["config_filename"] = Path(config_path).name
        object_["resolved_config_path"] = resolved_config_path_str
        adapter = TypeAdapter(cls)
        return adapter.validate_python(object_)

    @classmethod
    def from_config_str(
        cls,
        content: str,
        template_context: dict[str, Any] | None = None,
        name: str | None = None,
        rootdir: Path | None = None,
    ) -> Self:
        """Creates a ConfigWorkflow instance from a YAML string.

        Useful for testing and programmatic workflow generation without file I/O.

        All content is processed as Jinja2 templates with the following features:
        - Use {{ VAR }} syntax for variables
        - Full Jinja2 features: conditionals, loops, filters, etc.
        - Missing variables raise clear errors (StrictUndefined)

        Args:
            content: YAML string containing the workflow definition
            template_context: Optional dict of context variables to use in template rendering
            name: Optional workflow name (defaults to "workflow")
            rootdir: Optional root directory for relative paths (defaults to cwd)

        Returns:
            ConfigWorkflow: An instance with data parsed and validated from the YAML content

        Raises:
            ValueError: If template rendering fails or content is empty
        """
        if content == "":
            msg = "Workflow config content is empty."
            raise ValueError(msg)

        # Render Jinja2 template with inline context only
        resolver = JinjaResolver()
        rendered_content = resolver.render(content, template_context)

        # Parse YAML and validate
        reader = YAML(typ="safe", pure=True)
        object_ = reader.load(StringIO(rendered_content))

        # Set defaults for metadata
        if "name" not in object_:
            object_["name"] = name or "workflow"
        object_["rootdir"] = rootdir or Path.cwd()
        object_["config_filename"] = "from_string"
        object_["resolved_config_path"] = None  # No file path for string-based configs

        adapter = TypeAdapter(cls)
        return adapter.validate_python(object_)


OBJECT_T = TypeVar("OBJECT_T")


def validate_yaml_content[OBJECT_T](cls: type[OBJECT_T], content: str) -> OBJECT_T:
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
