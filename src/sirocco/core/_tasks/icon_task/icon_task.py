from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from itertools import chain
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

from sirocco.core._tasks.icon_task.ports import IconModel, ModelType, PortHandler, restart_in_handler
from sirocco.core._tasks.icon_task.task_distribution import (
    allocate_tasks_from_weights,
    distribute_tasks_by_blocks,
    distribute_tasks_round_robin,
)
from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from collections.abc import Iterable

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class IconTask(yaml_data_models.ConfigIconTaskSpecs, Task):
    SUPPORTED_MACHINES: ClassVar[list[str]] = field(default=["santis"], repr=False)
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _MAIN: ClassVar[str] = field(default="main.sh", repr=False)
    namelists: list[NamelistFile]
    master_namelist: NamelistFile = field(init=False, repr=False)
    models: dict[str, IconModel] = field(default_factory=dict, repr=False)
    target: Literal["cpu", "gpu", "hybrid"] = field(init=False, repr=False)
    compute_nodes: int = field(init=False, repr=False)
    io_nodes: int = field(init=False, repr=False)
    n_procs: int = field(init=False, repr=False)
    io_rank_bounds: list[tuple[int, int]] = field(init=False, repr=False, default_factory=list)

    def __post_init__(self) -> None:
        super().__post_init__()

        # Set target
        self.set_target()

        # Set defaults
        self.set_defaults()

        # Set component models and namelists
        self.set_components_and_namelists()

        # Allocate ranks
        self.allocate_ranks()

        # Validate wrapper script
        # TODO: Remove this once aiida-icon is synced with standalone
        if self.wrapper_script is not None:
            self.wrapper_script = self.config_rootdir / self.wrapper_script
            if not self.wrapper_script.exists():
                msg = f"Wrapper script in path {self.wrapper_script} does not exist."
                raise FileNotFoundError(msg)
            if not self.wrapper_script.is_file():
                msg = f"Wrapper script in path {self.wrapper_script} is not a file."
                raise OSError(msg)

    def set_target(self) -> None:
        if self.exe.gpu and not self.exe.cpu:
            self.target = "gpu"
        elif self.exe.cpu and not self.exe.gpu:
            self.target = "cpu"
        else:
            self.target = "hybrid"

    def set_defaults(self) -> None:
        match self.computer:
            case "santis":
                if self.target == "cpu" and not self.ntasks_per_node:
                    self.ntasks_per_node = 288
                    if self.cpus_per_task:
                        self.ntasks_per_node = self.ntasks_per_node // self.cpus_per_task
                if self.target == "gpu" and not self.ntasks_per_node:
                    self.ntasks_per_node = 4
                    if self.cpus_per_task:
                        self.ntasks_per_node = self.ntasks_per_node // self.cpus_per_task

    def set_components_and_namelists(self) -> None:
        # Detect and set master namelist
        master_namelist: NamelistFile | None = None
        for namelist in self.namelists:
            if namelist.name == self._MASTER_NAMELIST_NAME:
                master_namelist = namelist
                break
        if master_namelist is None:
            msg = f"Failed to read master namelists. Could not find {self._MASTER_NAMELIST_NAME!r} in namelists {self.namelists}"
            raise ValueError(msg)
        self.master_namelist = master_namelist

        # Build dictionnary mapping component names to model namelists
        namelist_by_filename: dict[str, NamelistFile] = {nml.name: nml for nml in self.namelists}
        model_namelists: dict[str, NamelistFile] = {}
        model_types: dict[str, int] = {}
        for master_model_nml in self.master_namelist.iter_nml("master_model_nml"):
            if not isinstance((filename := master_model_nml.get("model_namelist_filename")), str):
                msg = f"{self.name}: master_model_nml does not contain a valid 'model_namelist_filename' parameter"
                raise KeyError(msg)
            if filename not in namelist_by_filename:
                msg = f"{self.name}: namelist {filename} required by {self._MASTER_NAMELIST_NAME!r} not found in provided namelists"
                raise KeyError(msg)
            if not isinstance(model_name := master_model_nml.get("model_name"), str):
                msg = f"{self.name}: master_model_nml associated to {filename} does not contain a valid 'model_name' parameter"
                raise KeyError(msg)
            model_namelists[model_name] = namelist_by_filename[filename]
            if not isinstance(model_type := master_model_nml.get("model_type"), int):
                msg = f"{self.name}: master_model_nml associated to {filename} does not contain a valid 'model_type' parameter"
                raise KeyError(msg)
            model_types[model_name] = model_type

        # Check if models and config component names match
        if (model_names := set(model_namelists.keys())) != (
            comp_names := {k for k in self.components if k != "master"}
        ):
            msg = f"{self.name}: models specified in {self._MASTER_NAMELIST_NAME} ({model_names}) don't match components from the config ({comp_names})"
            raise ValueError(msg)

        # Check if model names match between executables and task specs
        exe_model_names = (set(self.exe.gpu.model_names()) if self.exe.gpu else set()) | (
            set(self.exe.cpu.model_names()) if self.exe.cpu else set()
        )
        if model_names != exe_model_names:
            msg = f"{self.name}: model names specified for executables ({exe_model_names}) and task ({model_names}) don't match"
            raise ValueError(msg)

        # Build ICON models
        for comp_name, component in self.components.items():
            if comp_name != "master":
                self.models[comp_name] = IconModel(
                    name=comp_name,
                    core_component=component,
                    task_label=self.label,
                    task_run_dir=self.run_dir,
                    namelist=model_namelists[comp_name],
                    model_type=ModelType(model_types[comp_name]),
                )

    def allocate_ranks(self) -> None:
        """Allocate rank ranges to icon components"""

        match self.computer:
            case "santis" | "remote" | "localhost":  # "remote" and "localhost" for testing
                # NOTE: The following code only relies on the fact that cpu and gpu executables share the same nodes,
                #       so not it's not only santis specific.

                # number of IO nodes
                if self.target == "hybrid":
                    self.io_nodes = (
                        ceil(self.exe.tot_io_procs / self.exe.procs_per_io_node) if self.exe.tot_io_procs else 0
                    )
                    if self.io_nodes >= self.nodes:
                        msg = f"{self.name}: number of IO nodes (got {self.io_nodes}) must be < total number of nodes ({self.nodes})"
                        raise ValueError(msg)
                else:
                    self.io_nodes = 0

                # number of compute nodes
                self.compute_nodes = self.nodes - self.io_nodes

                # Allocate model ranks
                min_rank = 0
                for exe in (self.exe.gpu, self.exe.cpu):
                    if exe:
                        compute_weights: dict[str, int] = {
                            name: exe_proc.compute_weight for name, exe_proc in exe.procs.items()
                        }
                        match self.target:
                            case "cpu" | "gpu":
                                if self.ntasks_per_node is None:
                                    msg = "ntasks_per_node must be set for cpu or gpu targets"
                                    raise ValueError(msg)
                                compute_tasks = self.nodes * self.ntasks_per_node - exe.tot_io_procs
                            case "hybrid":
                                if exe.compute_tasks_per_node is None:
                                    msg = "compute_tasks_per_node must be set hybrid targets"
                                    raise ValueError(msg)
                                compute_tasks = self.compute_nodes * exe.compute_tasks_per_node
                        for model_name, n_tasks in allocate_tasks_from_weights(compute_tasks, compute_weights).items():
                            self.models[model_name].min_rank = min_rank
                            min_rank += n_tasks + exe.procs[model_name].tot_io_procs
                            self.models[model_name].max_rank = min_rank - 1
                            self.models[model_name].num_io_procs = exe.procs[model_name].streams
                            self.models[model_name].num_prefetch_proc = exe.procs[model_name].prefetch
                            self.models[model_name].num_restart_procs = exe.procs[model_name].restart

                # Allocate hiopy ranks
                if self.exe.hiopy:
                    self.exe.hiopy.min_rank = min_rank
                    min_rank += self.exe.hiopy.procs
                    self.exe.hiopy.max_rank = min_rank - 1

                # Total number of tasks
                self.n_procs = min_rank

            case _:
                msg = f"{self.name}: allocate_ranks not implemented for machine {self.computer}"
                raise NotImplementedError(msg)

        # Final check for rank allocation
        for model_name, model in self.models.items():
            if not hasattr(model, "min_rank") or not hasattr(model, "max_rank"):
                msg = f"{self.name}: model {model_name} missing min_rank and/or max_rank"
                raise RuntimeError(msg)

    def update_icon_namelists_from_workflow(self) -> None:
        if not isinstance(self.cycle_point, DateCyclePoint):
            msg = f"{self.name}: icon tasks must have a DateCyclePoint"
            raise TypeError(msg)
        self.master_namelist.update_from_specs(
            {
                "master_time_control_nml": {
                    # Use Z instead of +00:00 for UTC (ISO 8601 compact form)
                    "experimentStartDate": self.cycle_point.chunk_start_date.isoformat().replace("+00:00", "Z"),
                    "experimentStopDate": self.cycle_point.chunk_stop_date.isoformat().replace("+00:00", "Z"),
                    "restartTimeIntval": str(self.cycle_point.period),
                    "checkpointTimeIntval": str(self.cycle_point.period),
                },
                "master_nml": {
                    "lrestart": self.is_restart,
                    # TODO: check what this really means and if we need it set
                    #       up automatically from the workflow
                    "read_restart_namelists": self.is_restart,
                },
            }
        )
        for master_model_nml in self.master_namelist.iter_nml("master_model_nml"):
            model = self.models[master_model_nml["model_name"]]  # type: ignore
            master_model_nml["model_min_rank"] = model.min_rank
            master_model_nml["model_max_rank"] = model.max_rank

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from restart file(s)."""
        # Get restart status of first model
        restart_ref = bool(next(iter(self.models.values())).inputs.get(restart_in_handler.port_name, False))
        # Check if all models have the same restart status
        for model in self.models.values():
            if bool(model.inputs.get(restart_in_handler.port_name, False)) != restart_ref:
                msg = f"{self.name}: All CON models must have the same restart status"
                raise ValueError(msg)
        return restart_ref

    def get_exe_ranks_str(self, exe: yaml_data_models.ConfigIconExe) -> str:
        return ",".join(f"{self.models[name].min_rank}-{self.models[name].max_rank}" for name in exe.model_names())

    def generate_multi_prog_config(self) -> None:
        if self.target != "hybrid":
            msg = f"{self.name}: multi-prog config only for hybrid targets"
            raise ValueError(msg)
        match self.computer:
            case "santis":
                multi_prog_lines: list[str] = []
                if self.exe.gpu:
                    multi_prog_lines.append(
                        f"{self.get_exe_ranks_str(self.exe.gpu)} ./{self.computer}_gpu_wrapper.sh ./icon_gpu"
                    )
                if self.exe.cpu:
                    multi_prog_lines.append(
                        f"{self.get_exe_ranks_str(self.exe.cpu)} ./{self.computer}_cpu_wrapper.sh ./icon_cpu"
                    )
                if self.exe.hiopy:
                    multi_prog_lines.append(
                        f"{self.exe.hiopy.min_rank}-{self.exe.hiopy.max_rank} ./{self.computer}_hiopy_wrapper.sh"
                    )
                (self.run_dir / "multi-prog.conf").write_text("\n".join(multi_prog_lines))
            case _:
                msg = f"{self.name}: generate_multi_prog_config not implemented for machine {self.computer}"
                raise NotImplementedError(msg)

    def exe_compute_rank_bounds(self, exe: yaml_data_models.ConfigIconExe) -> Iterable[tuple[int, int]]:
        return (
            (self.models[name].min_rank, self.models[name].max_rank - exe.procs[name].tot_io_procs)
            for name in exe.model_names()
        )

    def exe_io_rank_bounds(self, exe: yaml_data_models.ConfigIconExe) -> Iterable[tuple[int, int]]:
        return (
            (self.models[name].max_rank - exe.procs[name].tot_io_procs + 1, self.models[name].max_rank)
            for name in exe.model_names()
        )

    def distribute_ranks(self) -> None:
        """Compute task distribution and dump to template file with placeholder node names"""

        if self.target != "hybrid":
            msg = f"{self.name}: arbitrary distribution only available for hybrid targets (for now)"
            raise ValueError(msg)
        match self.computer:
            case "santis":
                distributed_ranks: dict[int, int] = {}
                # Distribute compute ranks to compute nodes
                for exe in (self.exe.gpu, self.exe.cpu):
                    if exe:
                        distributed_ranks.update(
                            distribute_tasks_by_blocks(
                                self.exe_compute_rank_bounds(exe), tuple(range(self.compute_nodes))
                            )
                        )
                        self.io_rank_bounds.extend(self.exe_io_rank_bounds(exe))
                # Distribute io ranks to io nodes
                if self.exe.hiopy:
                    self.io_rank_bounds.append((self.exe.hiopy.min_rank, self.exe.hiopy.max_rank))
                if self.io_rank_bounds:
                    distributed_ranks.update(
                        distribute_tasks_round_robin(
                            self.io_rank_bounds,
                            tuple(range(self.compute_nodes + 1, self.compute_nodes + self.io_nodes + 1)),
                        )
                    )
                # Write node placeholders to temporary hostfile
                nid_length = len(str(self.nodes - 1))
                (self.run_dir / "hostfile-sirocco").write_text(
                    "\n".join(f"sirocco_nid{distributed_ranks[k]:0{nid_length}}" for k in range(self.n_procs))
                )

            case _:
                msg = f"{self.name}: distribute_ranks not implemented for machine {self.computer}"
                raise NotImplementedError(msg)

    def dump_namelists(
        self, directory: Path, filename_mode: Literal["append_coordinates", "raw"] = "append_coordinates"
    ) -> None:
        if not directory.exists():
            msg = f"Dumping path {directory} does not exist."
            raise OSError(msg)
        if not directory.is_dir():
            msg = f"Dumping path {directory} is not directory."
            raise OSError(msg)

        for namelist in self.namelists:
            filename = namelist.name
            if filename_mode == "append_coordinates":
                from datetime import datetime

                # Format coordinates - strip timezone from datetimes (all Sirocco datetimes are UTC)
                coord_strs: list[str] = []
                for p in self.coordinates.values():
                    if isinstance(p, datetime):
                        # Remove timezone for filename (keeps original format otherwise)
                        coord_strs.append(str(p.replace(tzinfo=None)))
                    else:
                        coord_strs.append(str(p))
                suffix = "_".join(coord_strs).replace(" ", "_")
                filename += "_" + suffix
            namelist.dump(directory / filename)

    def prepare_for_submission(self) -> None:
        """Generate or copy any file required at runtime to task run_dir"""

        # Check supported machine and set target
        if self.computer not in self.SUPPORTED_MACHINES:
            msg = (
                f"machine {self.computer} not suportted for icon task. Supported machines are {self.SUPPORTED_MACHINES}"
            )
            raise ValueError(msg)

        # Link ICON binaries
        if self.exe.cpu is not None:
            (self.run_dir / "icon_cpu").symlink_to(self.exe.cpu.path)
        if self.exe.gpu is not None:
            (self.run_dir / "icon_gpu").symlink_to(self.exe.gpu.path)

        # Take input/output ports specifications into account:
        # - adapt namelist parameters
        # - resolve data path
        # - link data
        for model in self.models.values():
            for port in chain(model.inputs, model.outputs):
                PortHandler.handle(port, model)

        # Dump namelists
        self.dump_namelists(directory=self.run_dir, filename_mode="raw")

        # Copy required runtime files
        if self.runtime is None:
            runtime_dir = Path(__file__).parent / self.computer / self.target
            if not runtime_dir.exists():
                msg = f"target {self.target} not defined for computer {self.computer}"
                raise ValueError(msg)
        else:
            runtime_dir = self.config_rootdir / self.runtime
            if not runtime_dir.is_dir():
                msg = f"{self.name}: 'runtime' directory not found at {runtime_dir}"
                raise ValueError(msg)
            if not (runtime_dir / self._MAIN).is_file():
                msg = f"{self._MAIN} not found in runtime at {runtime_dir}"
                raise ValueError(msg)
        shutil.copytree(runtime_dir, self.run_dir, dirs_exist_ok=True)

        # Hybrid target auxiliary files
        if self.target == "hybrid":
            self.generate_multi_prog_config()
            self.distribute_ranks()

    def runscript_lines(self) -> list[str]:
        lines: list[str] = []
        if self.target == "hybrid":
            # Total number of tasks
            lines.append(f"N_TASKS={self.n_procs}")
            # IO_RANKS env var (bash array of all io ranks)
            io_ranks: list[int] = list(
                chain(*(range(min_rank, max_rank + 1) for min_rank, max_rank in self.io_rank_bounds))
            )
            io_ranks_str = " ".join(f"'{r}'" for r in io_ranks)
            lines.append(f"IO_RANKS=({io_ranks_str})")
        lines.append(f"source ./{self._MAIN}")
        return lines

    def resolve_output_data_paths(self) -> None:
        for model in self.models.values():
            for port in model.outputs:
                PortHandler.handle(port, model)

    @classmethod
    def build_from_config(
        cls: type[Self], config: yaml_data_models.ConfigTask, config_rootdir: Path, **kwargs: Any
    ) -> Self:
        config_kwargs = dict(config)
        del config_kwargs["parameters"]
        # The following check is here for type checkers.
        # We don't want to narrow the type in the signature, as that would break liskov substitution.
        # We guarantee elsewhere this is called with the correct type at runtime
        if not isinstance(config, yaml_data_models.ConfigIconTask):
            raise TypeError

        config_kwargs["namelists"] = [
            NamelistFile.from_config(config=config_namelist, config_rootdir=config_rootdir)
            for config_namelist in config_kwargs["namelists"]
        ]

        self = cls(
            config_rootdir=config_rootdir,
            **kwargs,
            **config_kwargs,
        )
        self.update_icon_namelists_from_workflow()
        return self
