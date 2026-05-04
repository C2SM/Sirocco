import enum
from dataclasses import dataclass, field
from pathlib import Path

from sirocco.core.graph_items import Data, GeneratedData, TaskComponent
from sirocco.core.namelistfile import NamelistFile


class ModelType(enum.Enum):
    _value_: int
    ATMOSPHERE = 1
    OCEAN = 2
    LAND = 5


@dataclass(kw_only=True)
class IconModel:
    core_component: TaskComponent
    name: str
    is_master: bool
    task_run_dir: Path
    task_label: str
    namelist: NamelistFile
    model_type: ModelType
    min_rank: int = field(init=False)
    max_rank: int = field(init=False)

    @property
    def inputs(self) -> dict[str, list[Data]]:
        return self.core_component.inputs

    @property
    def outputs(self) -> dict[str, list[GeneratedData]]:
        return self.core_component.outputs

    @property
    def n_tasks(self) -> int:
        return self.max_rank - self.min_rank + 1

    @property
    def num_io_procs(self) -> int:
        return self.namelist["parallel_nml"]["num_io_procs"]

    @num_io_procs.setter
    def num_io_procs(self, value: int) -> None:
        self.namelist["parallel_nml"]["num_io_procs"] = value

    @property
    def num_prefetch_proc(self) -> int:
        return self.namelist["parallel_nml"]["num_prefetch_proc"]

    @num_prefetch_proc.setter
    def num_prefetch_proc(self, value: int) -> None:
        self.namelist["parallel_nml"]["num_prefetch_proc"] = value

    @property
    def num_restart_procs(self) -> int:
        return self.namelist["parallel_nml"]["num_restart_procs"]

    @num_restart_procs.setter
    def num_restart_procs(self, value: int) -> None:
        self.namelist["parallel_nml"]["num_restart_procs"] = value
