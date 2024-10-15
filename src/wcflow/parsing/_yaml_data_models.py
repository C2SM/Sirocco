from __future__ import annotations

import time
from datetime import datetime
from os.path import expandvars
from pathlib import Path
from typing import Any

from isoduration import parse_duration
from isoduration.types import Duration  # pydantic needs type # noqa: TCH002
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ._utils import TimeUtils


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
        name_and_spec = {}
        # - my_name:
        #     ...
        if len(data) != 1:
            msg = f"Expected dict with one element of the form {{'name': specification}} but got {data}."
            raise ValueError(msg)
        name_and_spec["name"] = next(iter(data.keys()))
        # if no specification specified e.g. "- my_name:"
        if (spec := next(iter(data.values()))) is not None:
            name_and_spec.update(spec)

        super().__init__(**name_and_spec)


class _LagDateBaseModel(BaseModel):
    """Base class for all classes containg a list of dates or time lags."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    date: list[datetime] = []  # this is safe in pydantic
    lag: list[Duration] = []  # this is safe in pydantic

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


class ConfigTask(_NamedBaseModel):
    """
    To create an instance of a task defined in a workflow file
    """

    command: str
    command_option: str | None = None
    host: str | None = None
    account: str | None = None
    plugin: str | None = None
    config: str | None = None
    uenv: dict | None = None
    nodes: int | None = None
    walltime: str | None = None
    src: str | None = None
    conda_env: str | None = None

    def __init__(self, /, **data):
        # We have to treat root special as it does not typically define a command
        if "ROOT" in data and "command" not in data["ROOT"]:
            data["ROOT"]["command"] = "ROOT_PLACEHOLDER"
        super().__init__(**data)

    @field_validator("command")
    @classmethod
    def expand_env_vars(cls, value: str) -> str:
        """Expands any environment variables in the value"""
        return expandvars(value)

    @field_validator("walltime")
    @classmethod
    def convert_to_struct_time(cls, value: str | None) -> time.struct_time | None:
        """Converts a string of form "%H:%M:%S" to a time.time_struct"""
        return None if value is None else time.strptime(value, "%H:%M:%S")


class ConfigData(_NamedBaseModel):
    """
    To create an instance of a data defined in a workflow file.
    """

    type: str
    src: str
    format: str | None = None

    @field_validator("type")
    @classmethod
    def is_file_or_dir(cls, value: str) -> str:
        """."""
        if value not in ["file", "dir"]:
            msg = "Must be one of 'file' or 'dir'."
            raise ValueError(msg)
        return value


class ConfigCycleTaskDepend(_NamedBaseModel, _LagDateBaseModel):
    """
    To create an instance of a input or output in a task in a cycle defined in a workflow file.
    """

    name: str  # name of the task it depends on
    cycle_name: str | None = None


class ConfigCycleTaskInput(_NamedBaseModel, _LagDateBaseModel):
    """
    To create an instance of an input in a task in a cycle defined in a workflow file.

    For example:

    .. yaml

        - my_input:
            date: ...
            lag: ...
    """

    arg_option: str | None = None


class ConfigCycleTaskOutput(_NamedBaseModel):
    """
    To create an instance of an output in a task in a cycle defined in a workflow file.
    """


class ConfigCycleTask(_NamedBaseModel):
    """
    To create an instance of a task in a cycle defined in a workflow file.
    """

    inputs: list[ConfigCycleTaskInput | str] | None = None
    outputs: list[ConfigCycleTaskOutput | str] | None = None
    depends: list[ConfigCycleTaskDepend | str] | None = None

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


class ConfigWorkflow(BaseModel):
    name: str | None = None
    start_date: datetime
    end_date: datetime
    cycles: list[ConfigCycle]
    tasks: list[ConfigTask]
    data: list[ConfigData]

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def convert_datetime(cls, value) -> None | datetime:
        return None if value is None else datetime.fromisoformat(value)

    @model_validator(mode="after")
    def check_start_date_before_end_date(self) -> ConfigWorkflow:
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            msg = "For workflow {self._name!r} the start_date {start_date!r} lies after given end_date {end_date!r}."
            raise ValueError(msg)
        return self


def load_workflow_config(workflow_config: str) -> ConfigWorkflow:
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

    return parsed_workflow
