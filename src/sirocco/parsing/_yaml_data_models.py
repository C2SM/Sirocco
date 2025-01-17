from __future__ import annotations

import enum
import functools
import itertools
import time
import typing
from dataclasses import dataclass
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


class _CliArgsBaseModel(BaseModel):
    """Base class for cli_arguments specifications"""

    # TODO: Even allow for `str`, or always require list?
    positional: str | list[str] | None = None
    # Field needed for child class doing pydantic parsing
    keyword: dict[str, str] | None = Field(default_factory=dict)
    flags: str | list[str] | None = None
    source_file: str | list[str] | None = None

    # TODO: Should we allow users to pass it without the hyphen(s), and prepend them automatically?
    # TODO: While convenient, it could be a bad idea, if users put in wrong things. Better to be explicit.
    @field_validator("keyword", mode="before")
    @classmethod
    def validate_keyword_args(cls, value):
        """Ensure keyword arguments start with '-' or '--'."""
        if value is not None:
            invalid_keys = [key for key in value if not key.startswith(("-", "--"))]
            if invalid_keys:
                invalid_kwarg_exc = f"Invalid keyword arguments: {', '.join(invalid_keys)}"
                raise ValueError(invalid_kwarg_exc)
        return value

    @field_validator("flags", mode="before")
    @classmethod
    def validate_flag_args(cls, value):
        """Ensure positional arguments start with '-' or '--'."""
        if value is not None:
            if isinstance(value, str):
                value = [value]
            invalid_flags = [arg for arg in value if not arg.startswith(("-", "--"))]
            if invalid_flags:
                invalid_flags_exc = f"Invalid positional arguments: {', '.join(invalid_flags)}"
                raise ValueError(invalid_flags_exc)
        return value


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
    def check_dict_single_item(cls, params: dict) -> dict:
        if not params:
            return {}
        for k, v in params.items():
            if v not in ("single", "all"):
                msg = f"parameter {k}: reference can only be 'single' or 'all', got {v}"
                raise ValueError(msg)
        return params


class ConfigCycleTaskInput(TargetNodesBaseModel):
    pass


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


@dataclass
class ConfigShellTaskSpecs:
    plugin: ClassVar[Literal["shell"]] = "shell"
    command: str = ""
    cli_arguments: _CliArgsBaseModel | None = None
    src: str | None = None


class ConfigShellTask(ConfigBaseTask, ConfigShellTaskSpecs):
    pass


@dataclass
class ConfigIconTaskSpecs:
    plugin: ClassVar[Literal["icon"]] = "icon"
    namelists: dict[str, str] | None = None


class ConfigIconTask(ConfigBaseTask, ConfigIconTaskSpecs):
    pass


class DataType(enum.StrEnum):
    FILE = enum.auto()
    DIR = enum.auto()


@dataclass
class ConfigBaseDataSpecs:
    type: DataType
    src: str
    format: str | None = None


class ConfigBaseData(_NamedBaseModel, ConfigBaseDataSpecs):
    """
    To create an instance of a data defined in a workflow file.

    Examples:

        yaml snippet:

            >>> import textwrap
            >>> import pydantic_yaml
            >>> snippet = textwrap.dedent(
            ...     '''
            ...       foo:
            ...         type: "file"
            ...         src: "foo.txt"
            ...     '''
            ... )
            >>> pydantic_yaml.parse_yaml_raw_as(ConfigBaseData, snippet)
            ConfigBaseData(type=<DataType.FILE: 'file'>, src='foo.txt', format=None, name='foo', parameters=[])


        from python:

            >>> ConfigBaseData(foo={"type": "file", "src": "foo.txt"})
            ConfigBaseData(type=<DataType.FILE: 'file'>, src='foo.txt', format=None, name='foo', parameters=[])
    """

    parameters: list[str] = []


class ConfigAvailableData(ConfigBaseData):
    pass


class ConfigGeneratedData(ConfigBaseData):
    pass


@functools.singledispatch
def canonicalize(value: Any) -> Any:  # noqa: ARG001 # value not accessed, as this is just a placeholder
    raise NotImplementedError


class CanonicalBaseData(BaseModel, ConfigBaseDataSpecs):
    """
    Canonical data model.

    Examples:

    >>> CanonicalBaseData(name="foo", type=DataType.FILE, src="foo.txt")
    CanonicalBaseData(type=<DataType.FILE: 'file'>, src='foo.txt', format=None, name='foo', parameters=[])
    """

    name: str
    parameters: list[str] = []


class CanonicalAvailableData(CanonicalBaseData):
    pass


class CanonicalGeneratedData(CanonicalBaseData):
    pass


@canonicalize.register
def canonicalize_available_data(value: ConfigAvailableData) -> CanonicalAvailableData:
    return CanonicalAvailableData(
        name=value.name,
        type=value.type,
        src=value.src,
        format=value.format,
        parameters=value.parameters,
    )


@canonicalize.register
def canonicalize_generated_data(value: ConfigGeneratedData) -> CanonicalGeneratedData:
    return CanonicalGeneratedData(
        name=value.name,
        type=value.type,
        src=value.src,
        format=value.format,
        parameters=value.parameters,
    )


class ConfigData(BaseModel):
    """
    To create the container of available and generated data

    Example:

        yaml snippet:

            >>> import textwrap
            >>> import pydantic_yaml
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
            >>> data = pydantic_yaml.parse_yaml_raw_as(ConfigData, snippet)
            >>> assert data.available[0].name == "foo"
            >>> assert data.generated[0].name == "bar"

        from python:

            >>> ConfigData()
            ConfigData(available=[], generated=[])
    """

    available: list[ConfigAvailableData] = []
    generated: list[ConfigGeneratedData] = []


class CanonicalData(BaseModel):
    available: list[CanonicalAvailableData] = []
    generated: list[CanonicalGeneratedData] = []


@canonicalize.register
def canonicalize_data(value: ConfigData) -> CanonicalData:
    return CanonicalData(
        available=[canonicalize(i) for i in value.available],
        generated=[canonicalize(i) for i in value.generated],
    )


def get_plugin_from_named_base_model(data: dict) -> str:
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
            ...             type: "file"
            ...             src: "foo.txt"
            ...       generated:
            ...         - bar:
            ...             type: "file"
            ...             src: some_task_output
            ...     '''
            ... )
            >>> wf = pydantic_yaml.parse_yaml_raw_as(ConfigWorkflow, config)

        minimum programmatically created instance

            >>> empty_wf = ConfigWorkflow(cycles=[], tasks=[], data={})

    """

    name: str | None = None
    rootdir: Path | None = None
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
    data: CanonicalData
    parameters: dict[str, list[Any]]
    data_dict: dict[str, CanonicalAvailableData | CanonicalGeneratedData]
    task_dict: dict[str, ConfigTask]


@canonicalize.register
def canonicalize_workflow(value: ConfigWorkflow) -> CanonicalWorkflow:
    if not value.name or not value.rootdir:
        msg = "Workflow name and root dir required for canonicalization."
        raise ValueError(msg)
    canon_data = canonicalize(value.data)
    return CanonicalWorkflow(
        name=value.name,
        rootdir=value.rootdir,
        cycles=value.cycles,
        tasks=value.tasks,
        data=canon_data,
        parameters=value.parameters,
        data_dict={data.name: data for data in canon_data.available + canon_data.generated},
        task_dict={task.name: task for task in value.tasks},
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

    parsed_workflow.rootdir = config_path.resolve().parent

    return canonicalize_workflow(parsed_workflow)
    # return parsed_workflow
