from __future__ import annotations

import itertools
import time
import typing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal

from isoduration import parse_duration
from isoduration.types import Duration  # pydantic needs type # noqa: TCH002
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)

from sirocco.parsing._utils import TimeUtils


class _NamedBaseModel(BaseModel):
    """Base class for all classes with a key that specifies their name.

    For example:

    .. yaml

        - property_name:
            property: true

    When parsing with this as parent class it is converted to
    `{"name": "propery_name", "property": True}`.
    """

    name: str

    def __init__(self, /, **data):
        super().__init__(**self.merge_name_and_specs(data))

    @staticmethod
    def merge_name_and_specs(data: dict) -> dict:
        """
        Converts dict of form

        `{my_name: {'spec_0': ..., ..., 'spec_n': ...}`

        to

        `{'name': my_name, 'spec_0': ..., ..., 'spec_n': ...}`

        by copy.
        """
        name_and_spec = {}
        if len(data) != 1:
            msg = f"Expected dict with one element of the form {{'name': specification}} but got {data}."
            raise ValueError(msg)
        name_and_spec["name"] = next(iter(data.keys()))
        # if no specification specified e.g. "- my_name:"
        if (spec := next(iter(data.values()))) is not None:
            name_and_spec.update(spec)
        return name_and_spec


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
    def convert_datetime(cls, value) -> datetime:
        if value is None:
            return None
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


class ConfigCycleTask(_NamedBaseModel):
    """
    To create an instance of a task in a cycle defined in a workflow file.
    """

    inputs: list[ConfigCycleTaskInput | str] | None = Field(default_factory=list)
    outputs: list[ConfigCycleTaskOutput | str] | None = Field(default_factory=list)
    wait_on: list[ConfigCycleTaskWaitOn | str] | None = Field(default_factory=list)

    @field_validator("inputs", mode="before")
    @classmethod
    def convert_cycle_task_inputs(cls, values) -> list[ConfigCycleTaskInput]:
        inputs = []
        if values is None:
            return inputs
        for value in values:
            if isinstance(value, str):
                inputs.append({value: None})
            elif isinstance(value, dict):
                inputs.append(value)
        return inputs

    @field_validator("outputs", mode="before")
    @classmethod
    def convert_cycle_task_outputs(cls, values) -> list[ConfigCycleTaskOutput]:
        outputs = []
        if values is None:
            return outputs
        for value in values:
            if isinstance(value, str):
                outputs.append({value: None})
            elif isinstance(value, dict):
                outputs.append(value)
        return outputs

    @field_validator("wait_on", mode="before")
    @classmethod
    def convert_cycle_task_wait_on(cls, values) -> list[ConfigCycleTaskWaitOn]:
        wait_on = []
        if values is None:
            return wait_on
        for value in values:
            if isinstance(value, str):
                wait_on.append({value: None})
            elif isinstance(value, dict):
                wait_on.append(value)
        return wait_on


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


@dataclass
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


@dataclass
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


@dataclass
class ConfigNamelist:
    """Class for namelist specifications"""

    path: Path
    specs: dict | None = None


@dataclass
class ConfigIconTaskSpecs:
    plugin: ClassVar[Literal["icon"]] = "icon"
    namelists: dict[str, ConfigNamelist] | None = None


class ConfigIconTask(ConfigBaseTask, ConfigIconTaskSpecs):
    # validation done here and not in ConfigNamelist so that we can still
    # import ConfigIconTaskSpecs in core._tasks.IconTask. Hence the iteration
    # over the namelists that could be avoided with a more raw pydantic design
    @field_validator("namelists", mode="before")
    @classmethod
    def check_nml(cls, nml_list: list[Any]) -> ConfigNamelist:
        if nml_list is None:
            msg = "ICON tasks need namelists, got none"
            raise ValueError(msg)
        if not isinstance(nml_list, list):
            msg = f"expected a list got type {type(nml_list).__name__}"
            raise TypeError(msg)
        namelists = {}
        master_found = False
        for nml in nml_list:
            msg = f"was expecting a dict of length 1 or a string, got {nml}"
            if not isinstance(nml, (str, dict)):
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


@dataclass
class ConfigBaseDataSpecs:
    type: str | None = None
    src: str | None = None
    format: str | None = None
    computer: str | None = None


class ConfigBaseData(_NamedBaseModel, ConfigBaseDataSpecs):
    """
    To create an instance of a data defined in a workflow file.
    """

    parameters: list[str] = []
    type: str | None = None
    src: str | None = None
    format: str | None = None

    @field_validator("type")
    @classmethod
    def is_file_or_dir(cls, value: str) -> str:
        """."""
        valid_types = ("file", "dir")
        if value not in valid_types:
            msg = f"Must be one of {valid_types}"
            raise ValueError(msg)
        return value


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
    """To create the container of available and generated data"""

    available: list[ConfigAvailableData] = []
    generated: list[ConfigGeneratedData] = []


def get_plugin_from_named_base_model(
    data: dict | ConfigRootTask | ConfigShellTask | ConfigIconTask,
) -> str:
    if isinstance(data, (ConfigRootTask, ConfigShellTask, ConfigIconTask)):
        return data.plugin
    name_and_specs = _NamedBaseModel.merge_name_and_specs(data)
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
            >>> import pydantic_yaml
            >>> config = textwrap.dedent(
            ...     '''
            ...     cycles:
            ...       - minimal_cycle:
            ...           tasks:
            ...             - task_a:
            ...     tasks:
            ...       - task_b:
            ...           plugin: shell
            ...     data:
            ...       available:
            ...         - foo:
            ...       generated:
            ...         - bar:
            ...     '''
            ... )
            >>> wf = pydantic_yaml.parse_yaml_raw_as(ConfigWorkflow, config)

        minimum programmatically created instance

            >>> empty_wf = ConfigWorkflow(cycles=[], tasks=[], data={})

    """

    name: str | None = None
    cycles: list[ConfigCycle]
    tasks: list[ConfigTask]
    data: ConfigData
    parameters: dict[str, list] = {}

    @field_validator("parameters", mode="before")
    @classmethod
    def check_parameters_lists(cls, data) -> dict[str, list]:
        for param_name, param_values in data.items():
            msg = f"""{param_name}: parameters must map a string to list of single values, got {param_values}"""
            if isinstance(param_values, list):
                for v in param_values:
                    if isinstance(v, (dict, list)):
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


ITEM_T = typing.TypeVar("ITEM_T")


def list_not_empty(value: list[ITEM_T]) -> list[ITEM_T]:
    if len(value) < 1:
        msg = "At least one element is required."
        raise ValueError(msg)
    return value


class CanonicalWorkflow(BaseModel):
    name: str
    rootdir: Path
    cycles: Annotated[list[ConfigCycle], AfterValidator(list_not_empty)]
    tasks: Annotated[list[ConfigTask], AfterValidator(list_not_empty)]
    data: ConfigData
    parameters: dict[str, list[Any]]

    @property
    def data_dict(self) -> dict[str, ConfigAvailableData | ConfigGeneratedData]:
        return {data.name: data for data in itertools.chain(self.data.available, self.data.generated)}

    @property
    def task_dict(self) -> dict[str, ConfigTask]:
        return {task.name: task for task in self.tasks}


def canonicalize_workflow(config_workflow: ConfigWorkflow, rootdir: Path) -> CanonicalWorkflow:
    if not config_workflow.name:
        msg = "Workflow name required for canonicalization."
        raise ValueError(msg)
    return CanonicalWorkflow(
        name=config_workflow.name,
        rootdir=rootdir,
        cycles=config_workflow.cycles,
        tasks=config_workflow.tasks,
        data=config_workflow.data,
        parameters=config_workflow.parameters,
    )


def load_workflow_config(workflow_config: str) -> CanonicalWorkflow:
    """
    Loads a python representation of a workflow config file.

    :param workflow_config: the string to the config yaml file containing the workflow definition
    """
    from pydantic_yaml import parse_yaml_raw_as

    config_path = Path(workflow_config)

    content = config_path.read_text()

    parsed_workflow = parse_yaml_raw_as(ConfigWorkflow, content)

    # If name was not specified, then we use filename without file extension
    if parsed_workflow.name is None:
        parsed_workflow.name = config_path.stem

    rootdir = config_path.resolve().parent

    return canonicalize_workflow(config_workflow=parsed_workflow, rootdir=rootdir)
    # return parsed_workflow
