import enum
from dataclasses import dataclass, field
from pathlib import Path

import f90nml

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
    is_master_model: bool
    task_run_dir: Path
    task_label: str
    master_namelist_block: f90nml.Namelist
    namelist: NamelistFile
    model_type: ModelType
    _min_rank: int = field(init=False)
    _max_rank: int = field(init=False)

    @property
    def inputs(self) -> dict[str, list[Data]]:
        return self.core_component.inputs

    @property
    def outputs(self) -> dict[str, list[GeneratedData]]:
        return self.core_component.outputs

    # NOTE: min_rank and max_rank cannot be set with property setters
    #       because we don't have access to the master namelist from here.

    @property
    def min_rank(self) -> int:
        return self._min_rank

    @min_rank.setter
    def min_rank(self, value: int) -> None:
        self._min_rank = value
        self.master_namelist_block["model_min_rank"] = value

    @property
    def max_rank(self) -> int:
        return self._max_rank

    @max_rank.setter
    def max_rank(self, value: int) -> None:
        self._max_rank = value
        self.master_namelist_block["model_max_rank"] = value

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
