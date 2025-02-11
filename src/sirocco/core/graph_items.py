from __future__ import annotations

from dataclasses import dataclass, field
from itertools import chain, product
from typing import TYPE_CHECKING, Any, ClassVar, Self, TypeAlias, TypeVar, cast

from sirocco.parsing._yaml_data_models import (
    AnyWhen,
    AtDate,
    BeforeAfterDate,
    ConfigAvailableData,
    ConfigBaseDataSpecs,
    ConfigBaseTaskSpecs,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    from pathlib import Path

    from termcolor._types import Color

    from sirocco.parsing._yaml_data_models import (
        ConfigBaseData,
        ConfigCycleTask,
        ConfigCycleTaskWaitOn,
        ConfigTask,
        TargetNodesBaseModel,
    )


@dataclass(kw_only=True)
class GraphItem:
    """base class for Data Tasks and Cycles"""

    color: ClassVar[Color]

    name: str
    coordinates: dict


GRAPH_ITEM_T = TypeVar("GRAPH_ITEM_T", bound=GraphItem)


@dataclass(kw_only=True)
class Data(ConfigBaseDataSpecs, GraphItem):
    """Internal representation of a data node"""

    color: ClassVar[Color] = field(default="light_blue", repr=False)

    available: bool

    @classmethod
    def from_config(cls, config: ConfigBaseData, coordinates: dict) -> Self:
        return cls(
            name=config.name,
            type=config.type,
            src=config.src,
            available=isinstance(config, ConfigAvailableData),
            coordinates=coordinates,
        )


# contains the input data and its potential associated port
BoundData: TypeAlias = tuple[Data, str | None]


@dataclass(kw_only=True)
class Task(ConfigBaseTaskSpecs, GraphItem):
    """Internal representation of a task node"""

    plugin_classes: ClassVar[dict[str, type]] = field(default={}, repr=False)
    color: ClassVar[Color] = field(default="light_red", repr=False)

    inputs: list[BoundData] = field(default_factory=list)
    outputs: list[Data] = field(default_factory=list)
    wait_on: list[Task] = field(default_factory=list)
    config_rootdir: Path
    start_date: datetime | None = None
    end_date: datetime | None = None

    _wait_on_specs: list[ConfigCycleTaskWaitOn] = field(default_factory=list, repr=False)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.plugin in Task.plugin_classes:
            msg = f"Task for plugin {cls.plugin} already set"
            raise ValueError(msg)
        Task.plugin_classes[cls.plugin] = cls

    @classmethod
    def from_config(
        cls,
        config: ConfigTask,
        config_rootdir: Path,
        start_date: datetime | None,
        end_date: datetime | None,
        coordinates: dict[str, Any],
        datastore: Store,
        graph_spec: ConfigCycleTask,
    ) -> Task:
        inputs = [
            (data_node, input_spec.port)
            for input_spec in graph_spec.inputs
            for data_node in datastore.iter_from_cycle_spec(input_spec, coordinates)
        ]
        outputs = [datastore[output_spec.name, coordinates] for output_spec in graph_spec.outputs]
        # use the fact that pydantic models can be turned into dicts easily
        cls_config = dict(config)
        del cls_config["parameters"]
        if (plugin_cls := Task.plugin_classes.get(type(config).plugin, None)) is None:
            msg = f"Plugin {type(config).plugin!r} is not supported."
            raise ValueError(msg)

        new = plugin_cls(
            config_rootdir=config_rootdir,
            coordinates=coordinates,
            start_date=start_date,
            end_date=end_date,
            inputs=inputs,
            outputs=outputs,
            **cls_config,
        )  # this works because dataclass has generated this init for us

        # Store for actual linking in link_wait_on_tasks() once all tasks are created
        new._wait_on_specs = graph_spec.wait_on  # noqa: SLF001 we don't have access to self in a dataclass
        #                                                and setting an underscored attribute from
        #                                                the class itself raises SLF001

        return new

    def link_wait_on_tasks(self, taskstore: Store[Task]) -> None:
        self.wait_on = list(
            chain(
                *(
                    taskstore.iter_from_cycle_spec(wait_on_spec, self.coordinates)
                    for wait_on_spec in self._wait_on_specs
                )
            )
        )


@dataclass(kw_only=True)
class Cycle(GraphItem):
    """Internal reprenstation of a cycle"""

    color: ClassVar[Color] = field(default="light_green", repr=False)

    tasks: list[Task]


class Array[GRAPH_ITEM_T]:
    """Dictionnary of GRAPH_ITEM_T objects accessed by arbitrary dimensions"""

    def __init__(self, name: str) -> None:
        self._name = name
        self._dims: tuple[str, ...] = ()
        self._axes: dict[str, set] = {}
        self._dict: dict[tuple, GRAPH_ITEM_T] = {}

    def __setitem__(self, coordinates: dict, value: GRAPH_ITEM_T) -> None:
        # First access: set axes and initialize dictionnary
        input_dims = tuple(coordinates.keys())
        if self._dims == ():
            self._dims = input_dims
            self._axes = {k: set() for k in self._dims}
            self._dict = {}
        # check dimensions
        elif self._dims != input_dims:
            msg = f"Array {self._name}: coordinate names {input_dims} don't match Array dimensions {self._dims}"
            raise KeyError(msg)
        # Build internal key
        # use the order of self._dims instead of param_keys to ensure reproducibility
        key = tuple(coordinates[dim] for dim in self._dims)
        # Check if slot already taken
        if key in self._dict:
            msg = f"Array {self._name}: key {key} already used, cannot set item twice"
            raise KeyError(msg)
        # Store new axes values
        for dim in self._dims:
            self._axes[dim].add(coordinates[dim])
        # Set item
        self._dict[key] = value

    def __getitem__(self, coordinates: dict) -> GRAPH_ITEM_T:
        if self._dims != (input_dims := tuple(coordinates.keys())):
            msg = f"Array {self._name}: coordinate names {input_dims} don't match Array dimensions {self._dims}"
            raise KeyError(msg)
        # use the order of self._dims instead of param_keys to ensure reproducibility
        key = tuple(coordinates[dim] for dim in self._dims)
        return self._dict[key]

    def iter_from_cycle_spec(self, spec: TargetNodesBaseModel, ref_coordinates: dict) -> Iterator[GRAPH_ITEM_T]:
        # Check date references
        if "date" not in self._dims and (spec.lag or spec.date):
            msg = f"Array {self._name} has no date dimension, cannot be referenced by dates"
            raise ValueError(msg)
        if "date" in self._dims and ref_coordinates.get("date") is None and len(spec.date) == 0:
            msg = f"Array {self._name} has a date dimension, must be referenced by dates"
            raise ValueError(msg)

        for key in product(*(self._resolve_target_dim(spec, dim, ref_coordinates) for dim in self._dims)):
            yield self._dict[key]

    def _resolve_target_dim(self, spec: TargetNodesBaseModel, dim: str, ref_coordinates: Any) -> Iterator[Any]:
        if dim == "date":
            if not spec.lag and not spec.date:
                yield ref_coordinates["date"]
            if spec.lag:
                for lag in spec.lag:
                    yield ref_coordinates["date"] + lag
            if spec.date:
                yield from spec.date
        elif spec.parameters.get(dim) == "single":
            yield ref_coordinates[dim]
        else:
            yield from self._axes[dim]

    def __iter__(self) -> Iterator[GRAPH_ITEM_T]:
        yield from self._dict.values()


class Store[GRAPH_ITEM_T]:
    """Container for GRAPH_ITEM_T Arrays"""

    def __init__(self) -> None:
        self._dict: dict[str, Array[GRAPH_ITEM_T]] = {}

    def add(self, item: GRAPH_ITEM_T) -> None:
        graph_item = cast(GraphItem, item)  # mypy can somehow not deduce this
        name, coordinates = graph_item.name, graph_item.coordinates
        if name not in self._dict:
            self._dict[name] = Array[GRAPH_ITEM_T](name)
        self._dict[name][coordinates] = item

    def __getitem__(self, key: tuple[str, dict]) -> GRAPH_ITEM_T:
        name, coordinates = key
        if "date" in coordinates and coordinates["date"] is None:
            del coordinates["date"]
        if name not in self._dict:
            msg = f"entry {name} not found in Store"
            raise KeyError(msg)
        return self._dict[name][coordinates]

    def iter_from_cycle_spec(self, spec: TargetNodesBaseModel, ref_coordinates: dict) -> Iterator[GRAPH_ITEM_T]:
        ref_date: datetime | None = ref_coordinates.get("date")
        if ref_date is None:
            # Check if "at", "before" or "after" is used with a non-dated task
            if not isinstance(spec.when, AnyWhen):
                msg = "Cannot use a `when` specification in a one-off cycle"
                raise ValueError(msg)
        else:
            # Check if target items should be querried at all
            match spec.when:
                case AtDate():
                    if spec.when.at != ref_date:
                        return
                case BeforeAfterDate():
                    if (before := spec.when.before) is not None and before <= ref_date:
                        return
                    if (after := spec.when.after) is not None and after >= ref_date:
                        return
        # Yield items
        yield from self._dict[spec.name].iter_from_cycle_spec(spec, ref_coordinates)

    def __iter__(self) -> Iterator[GRAPH_ITEM_T]:
        yield from chain(*(self._dict.values()))
