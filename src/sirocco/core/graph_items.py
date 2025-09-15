from __future__ import annotations

import enum
from dataclasses import dataclass, field
from itertools import chain, product
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, TypeVar, cast

from sirocco.parsing.target_cycle import DateList, LagList, NoTargetCycle
from sirocco.parsing.yaml_data_models import (
    ConfigAvailableData,
    ConfigAvailableDataSpecs,
    ConfigBaseDataSpecs,
    ConfigBaseTaskSpecs,
    ConfigGeneratedDataSpecs,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sirocco.parsing.cycling import CyclePoint
    from sirocco.parsing.yaml_data_models import (
        ConfigBaseData,
        ConfigCycleTask,
        ConfigCycleTaskWaitOn,
        ConfigTask,
        TargetNodesBaseModel,
    )


class MpiCmdPlaceholder(enum.Enum):
    """Placeholdes in the mpi_cmd"""

    # PRCOMMENT I just used the naming for this as used in Matthieu's sbatch scripts
    MPI_TOTAL_PROCS = "MPI_TOTAL_PROCS"


class TaskStatus(enum.Enum):
    ONGOING = 1
    COMPLETED = 2
    FAILED = 3


@dataclass(kw_only=True)
class GraphItem:
    """base class for Data Tasks and Cycles"""

    color: ClassVar[str]

    name: str
    coordinates: dict


GRAPH_ITEM_T = TypeVar("GRAPH_ITEM_T", bound=GraphItem)

_UNRESOLVED_PATH_ = Path("_UNRESOLVED_PATH_")


@dataclass(kw_only=True)
class Data(ConfigBaseDataSpecs, GraphItem):
    """Internal representation of a data node"""

    color: ClassVar[str] = field(default="light_blue", repr=False)
    downstream_tasks: list[Task] = field(default_factory=list, repr=False)
    _resolved_path: Path = _UNRESOLVED_PATH_

    @classmethod
    def from_config(cls, config: ConfigBaseData, coordinates: dict) -> AvailableData | GeneratedData:
        data_class = AvailableData if isinstance(config, ConfigAvailableData) else GeneratedData
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        return data_class(coordinates=coordinates, **config_kwargs)

    @property
    def resolved_path(self) -> Path:
        return self._resolved_path

    @resolved_path.setter
    def resolved_path(self, path: Path):
        if not path.is_absolute():
            msg = "resolved path must be absolute"
            raise ValueError(msg)
        self._resolved_path = path


@dataclass(kw_only=True)
class AvailableData(Data, ConfigAvailableDataSpecs):
    computer: str

    def __post_init__(self):
        self.resolved_path = self.path


@dataclass(kw_only=True)
class GeneratedData(Data, ConfigGeneratedDataSpecs):
    origin_task: Task = field(init=False, repr=False)

    @Data.resolved_path.getter  # type: ignore[attr-defined]
    def resolved_path(self) -> Path:
        """Make sure generated data path is resolved before returning it"""

        # NOTE: The call to resolve_output_data_paths() has been placed here
        #       in order to reduce algorithm complexity. There is no requirement
        #       for the parts using Data.resolved_path to ensure it has indeed
        #       been resolved.
        if self._resolved_path is _UNRESOLVED_PATH_:
            self.origin_task.resolve_output_data_paths()
        return self._resolved_path


@dataclass(kw_only=True)
class Task(ConfigBaseTaskSpecs, GraphItem):
    """Internal representation of a task node"""

    plugin_classes: ClassVar[dict[str, type[Self]]] = field(default={}, repr=False)
    color: ClassVar[str] = field(default="light_red", repr=False)
    SUBMIT_FILENAME: ClassVar[str] = field(default="run_script.sh", repr=False)
    STDOUTERR_FILENAME: ClassVar[str] = field(default="run_script.%j.o", repr=False)
    JOBID_FILENAME: ClassVar[str] = field(default=".jobid", repr=False)
    RANK_FILENAME: ClassVar[str] = field(default=".rank", repr=False)
    COOL_DOWN_FILENAME: ClassVar[str] = field(default=".cool-down", repr=False)
    CLEAN_UP_BEFORE_SUBMIT: ClassVar[bool] = field(default=True, repr=False)  # Clean up directory when submitting

    config_rootdir: Path
    run_dir: Path = field(init=False, repr=False)
    jobid_path: Path = field(init=False, repr=False)
    rank_path: Path = field(init=False, repr=False)
    cool_down_path: Path = field(init=False, repr=False)
    label: str = field(init=False, repr=False)
    jobid: str = field(default="_NO_ID_", repr=False)
    rank: int = field(init=False, repr=False)
    cycle_point: CyclePoint

    inputs: dict[str, list[Data]] = field(default_factory=dict)
    outputs: dict[str | None, list[GeneratedData]] = field(default_factory=dict)
    wait_on: list[Task] = field(default_factory=list)
    waiters: list[Task] = field(default_factory=list, repr=False)
    parents: list[Task] = field(default_factory=list, repr=False)
    children: list[Task] = field(default_factory=list, repr=False)

    _wait_on_specs: list[ConfigCycleTaskWaitOn] = field(default_factory=list, repr=False)

    def __post_init__(self):
        if set(self.inputs.keys()).intersection(self.outputs.keys()):
            msg = "port names must be unique, even between inputs and outputs"
            raise ValueError(msg)
        self.label = self.name
        if self.coordinates:
            self.label += "__" + "__".join(
                f"{key}_{value}".replace(" ", "_") for key, value in self.coordinates.items()
            )
        self.run_dir = (self.config_rootdir / "run" / self.label).resolve()
        self.jobid_path = self.run_dir / self.JOBID_FILENAME
        self.rank_path = self.run_dir / self.RANK_FILENAME
        self.cool_down_path = self.run_dir / self.COOL_DOWN_FILENAME

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.plugin in Task.plugin_classes:
            msg = f"Task for plugin {cls.plugin} already set"
            raise ValueError(msg)
        Task.plugin_classes[cls.plugin] = cls

    def input_data_nodes(self) -> Iterator[Data]:
        yield from chain(*self.inputs.values())

    def input_data_items(self) -> Iterator[tuple[str, Data]]:
        yield from ((key, value) for key, values in self.inputs.items() for value in values)

    def output_data_nodes(self) -> Iterator[GeneratedData]:
        yield from chain(*self.outputs.values())

    def output_data_items(self) -> Iterator[tuple[str | None, Data]]:
        yield from ((key, value) for key, values in self.outputs.items() for value in values)

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

        # Link new task to input and output data nodes
        for data in new.input_data_nodes():
            data.downstream_tasks.append(new)
        for data in new.output_data_nodes():
            data.origin_task = new

        return new

    @classmethod
    def build_from_config(cls: type[Self], config: ConfigTask, config_rootdir: Path, **kwargs: Any) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        return cls(config_rootdir=config_rootdir, **kwargs, **config_kwargs)

    def link_wait_on_tasks(self, taskstore: Store[Task]) -> None:
        self.wait_on = list(
            chain(
                *(
                    taskstore.iter_from_cycle_spec(wait_on_spec, self.coordinates)
                    for wait_on_spec in self._wait_on_specs
                )
            )
        )
        # Add self to waiters of upstream task
        for upstream_task in self.wait_on:
            upstream_task.waiters.append(self)

    def link_parents_children(self) -> None:
        # Parent tasks
        self.parents = self.unique_task_list(
            chain(
                self.wait_on,
                (data_in.origin_task for data_in in self.input_data_nodes() if isinstance(data_in, GeneratedData)),
            )
        )
        self.children = self.unique_task_list(
            chain(self.waiters, *chain(data_out.downstream_tasks for data_out in self.output_data_nodes()))
        )

    @staticmethod
    def unique_task_list(task_candidates: Iterator[Task]) -> list[Task]:
        unique_labels: list[str] = []
        unique_tasks: list[Task] = []
        for task in task_candidates:
            if task.label not in unique_labels:
                unique_labels.append(task.label)
                unique_tasks.append(task)
        return unique_tasks

    def load_jobid_and_rank(self) -> bool:
        if active := self.jobid_path.exists():
            self.jobid = self.jobid_path.read_text()
            self.rank = int(self.rank_path.read_text())
        return active

    def dump_jobid_and_rank(self):
        self.jobid_path.write_text(self.jobid)
        self.rank_path.write_text(str(self.rank))

    def runscript_lines(self) -> list[str]:
        raise NotImplementedError

    def resolve_output_data_paths(self) -> None:
        raise NotImplementedError

    def prepare_for_submission(self) -> None:
        raise NotImplementedError


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
