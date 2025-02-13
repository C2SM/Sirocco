from __future__ import annotations

import enum
import itertools
import time
import typing
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Self

from isoduration import parse_duration
from isoduration.types import Duration  # pydantic needs type # noqa: TCH002
from pydantic import (
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

from sirocco.parsing._utils import TimeUtils

ITEM_T = typing.TypeVar("ITEM_T")


def list_not_empty(value: list[ITEM_T]) -> list[ITEM_T]:
    if len(value) < 1:
        msg = "At least one element is required."
        raise ValueError(msg)
    return value


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

    name: str

    @model_validator(mode="before")
    @classmethod
    def reformat_named_object(cls, data: Any) -> Any:
        return cls.extract_merge_name(data)

    @classmethod
    def extract_merge_name(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if len(data) == 1:
            key, value = next(iter(data.items()))
            match key:
                case str():
                    match value:
                        case str() if key == "name":
                            pass
                        case dict() if "name" not in value:
                            data = value | {"name": key}
                        case None:
                            data = {"name": key}
                        case _:
                            msg = f"{cls.__name__} may only be used for named objects, not values (got {data})."
                            raise TypeError(msg)
                case _:
                    msg = f"{cls.__name__} requires name to be a str (got {key})."
                    raise TypeError(msg)
        return data


class _WhenBaseModel(BaseModel):
    """Base class for when specifications"""

    before: datetime | None = None
    after: datetime | None = None
    at: datetime | None = None

    @model_validator(mode="before")
    @classmethod
    def check_before_after_at_combination(cls, data: Any) -> Any:
        if "at" in data and any(k in data for k in ("before", "after")):
            msg = "'at' key is incompatible with 'before' and after'"
            raise ValueError(msg)
        if not any(k in data for k in ("at", "before", "after")):
            msg = "use at least one of 'at', 'before' or 'after' keys"
            raise ValueError(msg)
        return data

    @field_validator("before", "after", "at", mode="before")
    @classmethod
    def convert_datetime(cls, value: datetime | str | None) -> datetime | None:
        if value is None or isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value)


class TargetNodesBaseModel(_NamedBaseModel):
    """class for targeting other task or data nodes in the graph

    When specifying cycle tasks, this class gathers the required information for
    targeting other nodes, either input data or wait on tasks.

    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    date: list[datetime] = []  # this is safe in pydantic
    lag: list[Duration] = []  # this is safe in pydantic
    when: _WhenBaseModel | None = None
    parameters: dict = {}

    @model_validator(mode="before")
    @classmethod
    def check_lag_xor_date_is_set(cls, data: Any) -> Any:
        if "lag" in data and "date" in data:
            msg = "Only one key 'lag' or 'date' is allowed. Not both."
            raise ValueError(msg)
        return data

    @field_validator("lag", mode="before")
    @classmethod
    def convert_durations(cls, value) -> list[Duration]:
        if value is None:
            return []
        values = value if isinstance(value, list) else [value]
        return [parse_duration(value) for value in values]

    @field_validator("date", mode="before")
    @classmethod
    def convert_datetimes(cls, value) -> list[datetime]:
        if value is None:
            return []
        values = value if isinstance(value, list) else [value]
        return [datetime.fromisoformat(value) for value in values]

    @field_validator("parameters", mode="before")
    @classmethod
    def check_parameters_spec(cls, params: dict) -> dict:
        if not params:
            return {}
        for k, v in params.items():
            if v not in ("single", "all"):
                msg = f"parameter {k}: reference can only be 'single' or 'all', got {v}"
                raise ValueError(msg)
        return params


class ConfigCycleTaskInput(TargetNodesBaseModel):
    port: str | None = None


class ConfigCycleTaskWaitOn(TargetNodesBaseModel):
    pass


class ConfigCycleTaskOutput(_NamedBaseModel):
    """
    To create an instance of an output in a task in a cycle defined in a workflow file.
    """


NAMED_BASE_T = typing.TypeVar("NAMED_BASE_T", bound=_NamedBaseModel)


def make_named_model_list_converter(
    cls: type[NAMED_BASE_T],
) -> typing.Callable[[list[NAMED_BASE_T | str | dict] | None], list[NAMED_BASE_T]]:
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
                    msg = "Unsupported Type"
                    raise TypeError(msg)
        return inputs

    return convert_named_model_list


class ConfigCycleTask(_NamedBaseModel):
    """
    To create an instance of a task in a cycle defined in a workflow file.
    """

    inputs: Annotated[
        list[ConfigCycleTaskInput],
        BeforeValidator(make_named_model_list_converter(ConfigCycleTaskInput)),
    ] = []
    outputs: Annotated[
        list[ConfigCycleTaskOutput],
        BeforeValidator(make_named_model_list_converter(ConfigCycleTaskOutput)),
    ] = []
    wait_on: Annotated[
        list[ConfigCycleTaskWaitOn],
        BeforeValidator(make_named_model_list_converter(ConfigCycleTaskWaitOn)),
    ] = []


class ConfigCycle(_NamedBaseModel):
    """
    To create an instance of a cycle defined in a workflow file.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    tasks: list[ConfigCycleTask]
    start_date: datetime | None = None
    end_date: datetime | None = None
    period: Duration | None = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def convert_datetime(cls, value) -> None | datetime:
        return None if value is None else datetime.fromisoformat(value)

    @field_validator("period", mode="before")
    @classmethod
    def convert_duration(cls, value):
        return None if value is None else parse_duration(value)

    @model_validator(mode="before")
    @classmethod
    def check_start_date_end_date_period_combination(cls, data: Any) -> Any:
        if ("start_date" in data) ^ ("end_date" in data):
            msg = f"in cycle {data['name']}: both start_date and end_date must be provided or none of them."
            raise ValueError(msg)
        if "period" in data and "start_date" not in data:
            msg = f"in cycle {data['name']}: period provided without start and end dates."
        return data

    @model_validator(mode="after")
    def check_start_date_before_end_date(self) -> ConfigCycle:
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            msg = "For cycle {self._name!r} the start_date {start_date!r} lies after given end_date {end_date!r}."
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def check_period_is_not_negative_or_zero(self) -> ConfigCycle:
        if self.period is not None and TimeUtils.duration_is_less_equal_zero(self.period):
            msg = f"For cycle {self.name!r} the period {self.period!r} is negative or zero."
            raise ValueError(msg)
        return self


@dataclass(kw_only=True)
class ConfigBaseTaskSpecs:
    computer: str | None = None
    host: str | None = None
    account: str | None = None
    uenv: dict | None = None
    nodes: int | None = None
    walltime: str | None = None


class ConfigBaseTask(_NamedBaseModel, ConfigBaseTaskSpecs):
    """
    config for genric task, no plugin specifics
    """

    parameters: list[str] = Field(default_factory=list)

    @field_validator("walltime")
    @classmethod
    def convert_to_struct_time(cls, value: str | None) -> time.struct_time | None:
        """Converts a string of form "%H:%M:%S" to a time.time_struct"""
        return None if value is None else time.strptime(value, "%H:%M:%S")


class ConfigRootTask(ConfigBaseTask):
    plugin: ClassVar[Literal["_root"]] = "_root"


# By using a frozen class we only need to validate on initialization
@dataclass(frozen=True)
class ShellCliArgument:
    """A holder for a CLI argument to simplify access.

    Stores CLI arguments of the form "file", "--init", "{file}" or "{--init file}". These examples translate into
    ShellCliArguments ShellCliArgument(name="file", references_data_item=False, cli_option_of_data_item=None),
    ShellCliArgument(name="--init", references_data_item=False, cli_option_of_data_item=None),
    ShellCliArgument(name="file", references_data_item=True, cli_option_of_data_item=None),
    ShellCliArgument(name="file", references_data_item=True, cli_option_of_data_item="--init")

    Attributes:
        name: Name of the argument. For the examples it is "file", "--init", "file" and "file"
        references_data_item: Specifies if the argument references a data item signified by enclosing it by curly
            brackets.
        cli_option_of_data_item: The CLI option associated to the data item.
    """

    name: str
    references_data_item: bool
    cli_option_of_data_item: str | None = None

    def __post_init__(self):
        if self.cli_option_of_data_item is not None and not self.references_data_item:
            msg = "data_item_option cannot be not None if cli_option_of_data_item is False"
            raise ValueError(msg)

    @classmethod
    def from_cli_argument(cls, arg: str) -> ShellCliArgument:
        len_arg_with_option = 2
        len_arg_no_option = 1
        references_data_item = arg.startswith("{") and arg.endswith("}")
        # remove curly brackets "{--init file}" -> "--init file"
        arg_unwrapped = arg[1:-1] if arg.startswith("{") and arg.endswith("}") else arg

        # "--init file" -> ["--init", "file"]
        input_arg = arg_unwrapped.split()
        if len(input_arg) != len_arg_with_option and len(input_arg) != len_arg_no_option:
            msg = f"Expected argument of format {{data}} or {{option data}} but found {arg}"
            raise ValueError(msg)
        name = input_arg[0] if len(input_arg) == len_arg_no_option else input_arg[1]
        cli_option_of_data_item = input_arg[0] if len(input_arg) == len_arg_with_option else None
        return cls(name, references_data_item, cli_option_of_data_item)


@dataclass(kw_only=True)
class ConfigShellTaskSpecs:
    plugin: ClassVar[Literal["shell"]] = "shell"
    command: str = ""
    cli_arguments: list[ShellCliArgument] = field(default_factory=list)
    env_source_files: list[str] = field(default_factory=list)
    src: str | None = None


class ConfigShellTask(ConfigBaseTask, ConfigShellTaskSpecs):
    command: str = ""
    cli_arguments: list[ShellCliArgument] = Field(default_factory=list)
    env_source_files: list[str] = Field(default_factory=list)

    @field_validator("cli_arguments", mode="before")
    @classmethod
    def validate_cli_arguments(cls, value: str) -> list[ShellCliArgument]:
        return cls.parse_cli_arguments(value)

    @field_validator("env_source_files", mode="before")
    @classmethod
    def validate_env_source_files(cls, value: str | list[str]) -> list[str]:
        return [value] if isinstance(value, str) else value

    @staticmethod
    def split_cli_arguments(cli_arguments: str) -> list[str]:
        """Splits the CLI arguments into a list of separate entities.

        Splits the CLI arguments by whitespaces except if the whitespace is contained within curly brackets. For example
        the string
        "-D --CMAKE_CXX_COMPILER=${CXX_COMPILER} {--init file}"
        will be splitted into the list
        ["-D", "--CMAKE_CXX_COMPILER=${CXX_COMPILER}", "{--init file}"]
        """

        nb_open_curly_brackets = 0
        last_split_idx = 0
        splits = []
        for i, char in enumerate(cli_arguments):
            if char == " " and not nb_open_curly_brackets:
                # we ommit the space in the splitting therefore we only store up to i but move the last_split_idx to i+1
                splits.append(cli_arguments[last_split_idx:i])
                last_split_idx = i + 1
            elif char == "{":
                nb_open_curly_brackets += 1
            elif char == "}":
                if nb_open_curly_brackets == 0:
                    msg = "Invalid input for cli_arguments. Found a closing curly bracket before an opening in {cli_argumentss!r}"
                    raise ValueError(msg)
                nb_open_curly_brackets -= 1

        if last_split_idx != len(cli_arguments):
            splits.append(cli_arguments[last_split_idx : len(cli_arguments)])
        return splits

    @staticmethod
    def parse_cli_arguments(cli_arguments: str) -> list[ShellCliArgument]:
        return [ShellCliArgument.from_cli_argument(arg) for arg in ConfigShellTask.split_cli_arguments(cli_arguments)]


@dataclass(kw_only=True)
class ConfigNamelist:
    """Class for namelist specifications

    - path is the path to the namelist file considered as template
    - specs is a dictionnary containing the specifications of parameters
      to change in the original namelist file

    Example:

        >>> path = "/some/path/to/icon.nml"
        >>> specs = {
        ...     "first_nml_block": {"first_param": "a string value", "second_param": 0},
        ...     "second_nml_block": {"third_param": False},
        ... }
        >>> config_nml = ConfigNamelist(path=path, specs=specs)
    """

    path: Path
    specs: dict | None = None


@dataclass(kw_only=True)
class ConfigIconTaskSpecs:
    plugin: ClassVar[Literal["icon"]] = "icon"
    namelists: dict[str, ConfigNamelist]


class ConfigIconTask(ConfigBaseTask, ConfigIconTaskSpecs):
    """Class representing an ICON task configuration from a workflow file

    Examples:

    yaml snippet:

        >>> import textwrap
        >>> snippet = textwrap.dedent(
        ...     '''
        ...       ICON:
        ...         plugin: icon
        ...         namelists:
        ...           - path/to/icon_master.namelist
        ...           - path/to/case_nml:
        ...               block_1:
        ...                 param_name: param_value
        ...     '''
        ... )
        >>> icon_task_cfg = validate_yaml_content(ConfigIconTask, snippet)
    """

    @field_validator("namelists", mode="before")
    @classmethod
    def check_nmls(cls, nmls: dict[str, ConfigNamelist] | list[Any]) -> dict[str, ConfigNamelist]:
        # Make validator idempotent even if not used yet
        if isinstance(nmls, dict):
            return nmls
        if not isinstance(nmls, list):
            msg = f"expected a list got type {type(nmls).__name__}"
            raise TypeError(msg)
        namelists = {}
        master_found = False
        for nml in nmls:
            msg = f"was expecting a dict of length 1 or a string, got {nml}"
            if not isinstance(nml, str | dict):
                raise TypeError(msg)
            if isinstance(nml, dict) and len(nml) > 1:
                raise TypeError(msg)
            if isinstance(nml, str):
                path, specs = Path(nml), None
            else:
                path, specs = next(iter(nml.items()))
                path = Path(path)
            namelists[path.name] = ConfigNamelist(path=path, specs=specs)
            master_found = master_found or (path.name == "icon_master.namelist")
        if not master_found:
            msg = "icon_master.namelist not found"
            raise ValueError(msg)
        return namelists


class DataType(enum.StrEnum):
    FILE = enum.auto()
    DIR = enum.auto()


@dataclass(kw_only=True)
class ConfigBaseDataSpecs:
    type: DataType
    src: str
    format: str | None = None
    computer: str | None = None


class ConfigBaseData(_NamedBaseModel, ConfigBaseDataSpecs):
    """
    To create an instance of a data defined in a workflow file.

    Examples:

        yaml snippet:

            >>> import textwrap
            >>> snippet = textwrap.dedent(
            ...     '''
            ...       foo:
            ...         type: "file"
            ...         src: "foo.txt"
            ...     '''
            ... )
            >>> validate_yaml_content(ConfigBaseData, snippet)
            ConfigBaseData(type=<DataType.FILE: 'file'>, src='foo.txt', format=None, computer=None, name='foo', parameters=[])


        from python:

            >>> ConfigBaseData(name="foo", type=DataType.FILE, src="foo.txt")
            ConfigBaseData(type=<DataType.FILE: 'file'>, src='foo.txt', format=None, computer=None, name='foo', parameters=[])
    """

    parameters: list[str] = []


class ConfigAvailableData(ConfigBaseData):
    pass


class ConfigGeneratedData(ConfigBaseData):
    @field_validator("computer")
    @classmethod
    def invalid_field(cls, value: str | None) -> str | None:
        if value is not None:
            msg = "The field 'computer' can only be specified for available data."
            raise ValueError(msg)
        return value


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
            ...           type: "file"
            ...           src: "foo.txt"
            ...     generated:
            ...       - bar:
            ...           type: "file"
            ...           src: "bar.txt"
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
    data: dict | ConfigRootTask | ConfigShellTask | ConfigIconTask,
) -> str:
    if isinstance(data, ConfigRootTask | ConfigShellTask | ConfigIconTask):
        return data.plugin
    name_and_specs = ConfigBaseTask.extract_merge_name(data)
    if name_and_specs.get("name", None) == "ROOT":
        return ConfigRootTask.plugin
    plugin = name_and_specs.get("plugin", None)
    if plugin is None:
        msg = f"Could not find plugin name in {data}"
        raise ValueError(msg)
    return plugin


ConfigTask = Annotated[
    Annotated[ConfigRootTask, Tag(ConfigRootTask.plugin)]
    | Annotated[ConfigIconTask, Tag(ConfigIconTask.plugin)]
    | Annotated[ConfigShellTask, Tag(ConfigShellTask.plugin)],
    Discriminator(get_plugin_from_named_base_model),
]


class ConfigWorkflow(BaseModel):
    """
    The root of the configuration tree.

    Examples:

        minimal yaml to generate:

            >>> import textwrap
            >>> content = textwrap.dedent(
            ...     '''
            ...     name: minimal
            ...     rootdir: /location/of/config/file
            ...     cycles:
            ...       - minimal_cycle:
            ...           tasks:
            ...             - task_a:
            ...     tasks:
            ...       - task_a:
            ...           plugin: shell
            ...     data:
            ...       available:
            ...         - foo:
            ...             type: file
            ...             src: foo.txt
            ...       generated:
            ...         - bar:
            ...             type: dir
            ...             src: bar
            ...     '''
            ... )
            >>> wf = validate_yaml_content(ConfigWorkflow, content)

        minimum programmatically created instance

            >>> wf = ConfigWorkflow(
            ...     name="minimal",
            ...     rootdir=Path("/location/of/config/file"),
            ...     cycles=[ConfigCycle(minimal_cycle={"tasks": [ConfigCycleTask(task_a={})]})],
            ...     tasks=[ConfigShellTask(task_a={"plugin": "shell"})],
            ...     data=ConfigData(
            ...         available=[
            ...             ConfigAvailableData(name="foo", type=DataType.FILE, src="foo.txt")
            ...         ],
            ...         generated=[
            ...             ConfigGeneratedData(name="bar", type=DataType.DIR, src="bar")
            ...         ],
            ...     ),
            ...     parameters={},
            ... )

    """

    rootdir: Path
    name: str
    cycles: Annotated[list[ConfigCycle], BeforeValidator(list_not_empty)]
    tasks: Annotated[list[ConfigTask], BeforeValidator(list_not_empty)]
    data: ConfigData
    parameters: dict[str, list] = {}

    @field_validator("parameters", mode="before")
    @classmethod
    def check_parameters_lists(cls, data) -> dict[str, list]:
        for param_name, param_values in data.items():
            msg = f"""{param_name}: parameters must map a string to list of single values, got {param_values}"""
            if isinstance(param_values, list):
                for v in param_values:
                    if isinstance(v, dict | list):
                        raise TypeError(msg)
            else:
                raise TypeError(msg)
        return data

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
    def from_config_file(cls, config_path: str) -> Self:
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
