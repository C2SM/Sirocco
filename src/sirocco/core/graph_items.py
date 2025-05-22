from __future__ import annotations

from dataclasses import dataclass, field
from itertools import chain, product
from typing import TYPE_CHECKING, Any, ClassVar, Self, TypeVar, cast

from sirocco.parsing.target_cycle import DateList, LagList, NoTargetCycle
from sirocco.parsing.yaml_data_models import (
    ConfigAvailableData,
    ConfigBaseDataSpecs,
    ConfigBaseTaskSpecs,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sirocco.parsing.cycling import CyclePoint
    from sirocco.parsing.yaml_data_models import (
        ConfigBaseData,
        ConfigCycleTask,
        ConfigCycleTaskWaitOn,
        ConfigTask,
        TargetNodesBaseModel,
    )


@dataclass(kw_only=True)
class GraphItem:
    """base class for Data Tasks and Cycles"""

    color: ClassVar[str]

    name: str
    coordinates: dict


GRAPH_ITEM_T = TypeVar("GRAPH_ITEM_T", bound=GraphItem)


@dataclass(kw_only=True)
class Data(ConfigBaseDataSpecs, GraphItem):
    """Internal representation of a data node"""

    color: ClassVar[str] = field(default="light_blue", repr=False)

    @classmethod
    def from_config(cls, config: ConfigBaseData, coordinates: dict) -> AvailableData | GeneratedData:
        data_class = AvailableData if isinstance(config, ConfigAvailableData) else GeneratedData
        return data_class(
            name=config.name,
            computer=config.computer,
            type=config.type,
            src=config.src,
            coordinates=coordinates,
        )


class AvailableData(Data):
    pass


class GeneratedData(Data):
    pass


@dataclass(kw_only=True)
class Task(ConfigBaseTaskSpecs, GraphItem):
    """Internal representation of a task node"""

    plugin_classes: ClassVar[dict[str, type[Self]]] = field(default={}, repr=False)
    color: ClassVar[str] = field(default="light_red", repr=False)

    inputs: dict[str, list[Data]] = field(default_factory=dict)
    outputs: dict[str | None, list[Data]] = field(default_factory=dict)
    wait_on: list[Task] = field(default_factory=list)
    config_rootdir: Path
    cycle_point: CyclePoint

    _wait_on_specs: list[ConfigCycleTaskWaitOn] = field(default_factory=list, repr=False)

    def __post_init__(self):
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.plugin in Task.plugin_classes:
            msg = f"Task for plugin {cls.plugin} already set"
            raise ValueError(msg)
        Task.plugin_classes[cls.plugin] = cls

    def input_data_nodes(self) -> Iterator[Data]:
        yield from chain(*self.inputs.values())

    def output_data_nodes(self) -> Iterator[Data]:
        yield from chain(*self.outputs.values())

    @classmethod
    def from_config(
        cls: type[Self],
        config: ConfigTask,
        config_rootdir: Path,
        cycle_point: CyclePoint,
        coordinates: dict[str, Any],
        datastore: Store,
        graph_spec: ConfigCycleTask,
    ) -> Task:
        inputs: dict[str, list[Data]] = {}
        for input_spec in graph_spec.inputs:
            if input_spec.port not in inputs:
                inputs[input_spec.port] = []
            inputs[input_spec.port].extend(datastore.iter_from_cycle_spec(input_spec, coordinates))

        outputs: dict[str | None, list[Data]] = {}
        for output_spec in graph_spec.outputs:
            if output_spec.port not in outputs:
                outputs[output_spec.port] = []
            outputs[output_spec.port].append(datastore[output_spec.name, coordinates])

        if (plugin_cls := Task.plugin_classes.get(type(config).plugin, None)) is None:
            msg = f"Plugin {type(config).plugin!r} is not supported."
            raise ValueError(msg)

        new = plugin_cls.build_from_config(
            config,
            config_rootdir=config_rootdir,
            coordinates=coordinates,
            cycle_point=cycle_point,
            inputs=inputs,
            outputs=outputs,
        )

        # Store for actual linking in link_wait_on_tasks() once all tasks are created
        new._wait_on_specs = graph_spec.wait_on  # noqa: SLF001 we don't have access to self in a dataclass
        #                                                and setting an underscored attribute from
        #                                                the class itself raises SLF001

        return new

    @classmethod
    def build_from_config(cls: type[Self], config: ConfigTask, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        return cls(**kwargs, **config_kwargs)

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

    color: ClassVar[str] = field(default="light_green", repr=False)

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
        if "date" not in self._dims and isinstance(spec.target_cycle, DateList | LagList):
            msg = f"Array {self._name} has no date dimension, cannot be referenced by dates"
            raise ValueError(msg)
        if "date" in self._dims and ref_coordinates.get("date") is None and not isinstance(spec.target_cycle, DateList):
            msg = f"Array {self._name} has a date dimension, must be referenced by dates"
            raise ValueError(msg)

        for key in product(*(self._resolve_target_dim(spec, dim, ref_coordinates) for dim in self._dims)):
            yield self._dict[key]

    def _resolve_target_dim(self, spec: TargetNodesBaseModel, dim: str, ref_coordinates: Any) -> Iterator[Any]:
        if dim == "date":
            match spec.target_cycle:
                case NoTargetCycle():
                    yield ref_coordinates["date"]
                case DateList():
                    yield from spec.target_cycle.dates
                case LagList():
                    for lag in spec.target_cycle.lags:
                        yield ref_coordinates["date"] + lag
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
        if name not in self._dict:
            msg = f"entry {name} not found in Store"
            raise KeyError(msg)
        return self._dict[name][coordinates]

    def iter_from_cycle_spec(self, spec: TargetNodesBaseModel, ref_coordinates: dict) -> Iterator[GRAPH_ITEM_T]:
        if spec.when.is_active(ref_coordinates.get("date")):
            yield from self._dict[spec.name].iter_from_cycle_spec(spec, ref_coordinates)

    def __iter__(self) -> Iterator[GRAPH_ITEM_T]:
        yield from chain(*(self._dict.values()))
