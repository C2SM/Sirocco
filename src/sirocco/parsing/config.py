from __future__ import annotations

import dataclasses
import enum
import typing
from typing import Any

if typing.TYPE_CHECKING:
    import pathlib
    from datetime import datetime

    from isoduration import Duration


@dataclasses.dataclass
class AtSpec:
    at: datetime


@dataclasses.dataclass
class BeforeAfterSpec:
    before: datetime | None
    after: datetime | None


@dataclasses.dataclass
class Always: ...


WhenSpec: typing.TypeAlias = AtSpec | BeforeAfterSpec | Always


class ParamRef(enum.Enum):
    SINGLE = enum.auto()
    ALL = enum.auto()


@dataclasses.dataclass
class Input:
    name: str
    date: list[datetime]
    lag: list[Duration]
    when: WhenSpec
    parameters: dict[str, ParamRef]


@dataclasses.dataclass
class CycleTaskInput(Input): ...


@dataclasses.dataclass
class CycleTaskWaitOn(Input): ...


@dataclasses.dataclass
class CycleTaskOutput:
    name: str


@dataclasses.dataclass
class CycleTask:
    inputs: list[CycleTaskInput]
    outputs: list[CycleTaskOutput]
    wait_on: list[CycleTaskWaitOn]


@dataclasses.dataclass
class Cycle:
    name: str
    tasks: list[CycleTask]


@dataclasses.dataclass
class UndatedCycle(Cycle): ...


@dataclasses.dataclass
class DatedCycle(Cycle):
    start_date: datetime
    end_date: datetime
    period: Duration | None


@dataclasses.dataclass
class BaseWorkflowTask:
    name: str
    host: str
    uenv: dict[str, str]
    nodes: int
    walltime: str
    parameters: list[str]


@dataclasses.dataclass
class RootTask(BaseWorkflowTask): ...


@dataclasses.dataclass
class CliArg:
    value: str


@dataclasses.dataclass
class CliOption:
    name: str
    value: str


@dataclasses.dataclass
class CliFlag:
    name: str


@dataclasses.dataclass
class CliSourceFile:
    name: str


CliSignature: typing.TypeAlias = list[CliArg | CliOption | CliFlag | CliSourceFile]


@dataclasses.dataclass
class ShellTask(BaseWorkflowTask):
    command: str
    cli_arguments: CliSignature
    src: str


@dataclasses.dataclass
class IconTask(BaseWorkflowTask):
    namelists: dict[str, str]


WorkflowTask: typing.TypeAlias = RootTask | ShellTask | IconTask


class DataParam(enum.Enum):
    FILE = enum.auto()
    DIR = enum.auto()


@dataclasses.dataclass
class Data:
    name: str
    type: DataParam
    src: str
    format: str
    parameters: list[str]


class AvailableData(Data): ...


class GeneratedData(Data): ...


@dataclasses.dataclass
class Workflow:
    name: str
    rootdir: pathlib.Path
    cycles: list[Cycle]
    tasks: dict[str, WorkflowTask]
    data: dict[str, AvailableData | GeneratedData]
    parameters: dict[str, list[Any]]
