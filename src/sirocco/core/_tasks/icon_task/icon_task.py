from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from itertools import chain
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

from sirocco.core._tasks.icon_task.models import IconModel, ModelType
from sirocco.core._tasks.icon_task.ports import PortHandler, restart_in_handler
from sirocco.core._tasks.icon_task.task_distribution import (
    RankInfo,
    allocate_tasks_from_weights,
    distribute_procs_cyclic,
    distribute_procs_load_balanced,
)
from sirocco.core.graph_items import Task
from sirocco.core.namelistfile import NamelistFile
from sirocco.parsing import yaml_data_models
from sirocco.parsing.cycling import DateCyclePoint

if TYPE_CHECKING:
    from collections.abc import Iterable

    import f90nml

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class IconTask(yaml_data_models.ConfigIconTaskSpecs, Task):
    SUPPORTED_MACHINES: ClassVar[list[str]] = field(default=["santis"], repr=False)
    _MASTER_NAMELIST_NAME: ClassVar[str] = field(default="icon_master.namelist", repr=False)
    _MODEL_NML_KEYS: ClassVar[list[str]] = field(default=["master_model_nml", "jsb_model_nml"], repr=False)
    _MAIN: ClassVar[str] = field(default="main.sh", repr=False)
    namelists: list[NamelistFile]
    master_namelist: NamelistFile = field(init=False, repr=False)
    models: dict[str, IconModel] = field(default_factory=dict, repr=False)
    master_models: dict[str, IconModel] = field(default_factory=dict, repr=False)
    compute_nodes: int = field(init=False, repr=False)
    io_nodes: int = field(init=False, repr=False)
    n_procs: int = field(init=False, repr=False)
    ranks_info: dict[int, RankInfo] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()

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
        # Gather temporary information from namelists to build IconModel objects thereafter
        model_namelists: dict[str, NamelistFile] = {}
        master_namelist_model_nml_blocks: dict[str, f90nml.Namelist] = {}
        model_types: dict[str, int] = {}
        model_master: dict[str, bool] = {}
        for key in self._MODEL_NML_KEYS:
            for model_nml in self.master_namelist.iter_nml(key):
                if not isinstance((filename := model_nml.get("model_namelist_filename")), str):
                    msg = f"{self.name}: {key} does not contain a valid 'model_namelist_filename' parameter"
                    raise KeyError(msg)
                if filename not in namelist_by_filename:
                    msg = f"{self.name}: namelist {filename} required by {self._MASTER_NAMELIST_NAME!r} not found in provided namelists"
                    raise KeyError(msg)
                if not isinstance(model_name := model_nml.get("model_name"), str):
                    msg = f"{self.name}: {key} associated to {filename} does not contain a valid 'model_name' parameter"
                    raise KeyError(msg)
                model_namelists[model_name] = namelist_by_filename[filename]
                master_namelist_model_nml_blocks[model_name] = model_nml
                if key == "master_model_nml":
                    if not isinstance(model_type := model_nml.get("model_type"), int):
                        msg = f"{self.name}: master_model_nml associated to {filename} does not contain a valid 'model_type' parameter"
                        raise KeyError(msg)
                    model_types[model_name] = model_type
                    model_master[model_name] = True
                elif key == "jsb_model_nml":
                    model_types[model_name] = 5
                    model_master[model_name] = False

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

        # Build IconModel objects and fill in self.models
        for comp_name, component in self.components.items():
            if comp_name != "master":
                self.models[comp_name] = IconModel(
                    name=comp_name,
                    is_master_model=model_master[comp_name],
                    core_component=component,
                    task_label=self.label,
                    task_run_dir=self.run_dir,
                    master_namelist_block=master_namelist_model_nml_blocks[comp_name],
                    namelist=model_namelists[comp_name],
                    model_type=ModelType(model_types[comp_name]),
                )
        self.master_models = {model_name: model for model_name, model in self.models.items() if model.is_master_model}

    def allocate_ranks(self) -> None:
        """Allocate rank ranges to icon components"""

        match self.computer:
            case "santis" | "remote" | "localhost":  # "remote" and "localhost" for testing
                # NOTE: The following code only relies on the fact that cpu and gpu executables share the same nodes,
                #       so it's not only santis specific.

                # number of compute and IO nodes
                # ------------------------------
                self.io_nodes = ceil(self.exe.tot_io_procs / self.exe.procs_per_io_node) if self.exe.separate_io else 0
                if self.io_nodes >= self.nodes:
                    msg = f"{self.name}: number of IO nodes (got {self.io_nodes}) must be < total number of nodes ({self.nodes})"
                    raise ValueError(msg)
                self.compute_nodes = self.nodes - self.io_nodes

                # Allocate model ranks
                # --------------------
                min_rank = 0
                # gpu executable
                if self.exe.gpu:
                    if self.exe.gpu.compute_procs_per_node is None:
                        msg = "compute_procs_per_node must be set for a gpu executable"
                        raise ValueError(msg)
                    compute_procs = self.compute_nodes * self.exe.gpu.compute_procs_per_node
                    min_rank = self.allocate_exe_ranks(min_rank, compute_procs, self.exe.gpu, "gpu")
                # cpu executable
                if self.exe.cpu:
                    if self.exe.separate_io:
                        if self.exe.cpu.compute_procs_per_node is None:
                            msg = "compute_procs_per_node must be set when separate_io is True"
                            raise ValueError(msg)
                        compute_procs = self.compute_nodes * self.exe.cpu.compute_procs_per_node
                    else:
                        if self.procs_per_node is None:
                            msg = "procs_per_node must be set when separate_io is False"
                            raise ValueError(msg)
                        non_gpu_compute_procs_per_node = self.procs_per_node
                        if self.exe.gpu and self.exe.gpu.compute_procs_per_node:
                            non_gpu_compute_procs_per_node -= self.exe.gpu.compute_procs_per_node
                        compute_procs = self.nodes * non_gpu_compute_procs_per_node - self.exe.tot_io_procs
                    min_rank = self.allocate_exe_ranks(min_rank, compute_procs, self.exe.cpu, "cpu")

                # Allocate hiopy ranks
                # --------------------
                if self.exe.hiopy:
                    self.exe.hiopy.min_rank = min_rank
                    min_rank += self.exe.hiopy.procs
                    self.exe.hiopy.max_rank = min_rank - 1
                    for rank in range(self.exe.hiopy.min_rank, self.exe.hiopy.max_rank + 1):
                        self.ranks_info[rank] = RankInfo(
                            node_id=-1,
                            numa_node=-1,
                            model="hiopy",
                            target="hiopy",
                            pe_type="hiopy",
                        )

                # Total number of procs
                # ---------------------
                self.n_procs = min_rank

            case _:
                msg = f"{self.name}: allocate_ranks not implemented for machine {self.computer}"
                raise NotImplementedError(msg)

        # Final check for rank allocation
        # -------------------------------
        for model_name, model in self.master_models.items():
            if model.min_rank < 0 or model.max_rank < 0:
                msg = f"{self.name}: model {model_name}  min_rank and/or max_rank not set"
                raise RuntimeError(msg)

    def allocate_exe_ranks(
        self, min_rank: int, compute_procs: int, exe: yaml_data_models.ConfigIconExe, target: Literal["cpu", "gpu"]
    ) -> int:
        compute_weights: dict[str, int] = {name: exe_proc.compute_weight for name, exe_proc in exe.procs.items()}

        for model_name, n_compute_procs in allocate_tasks_from_weights(compute_procs, compute_weights).items():
            max_rank = min_rank + n_compute_procs + exe.procs[model_name].tot_io_procs - 1
            for rank in range(min_rank, max_rank + 1):
                self.ranks_info[rank] = RankInfo(
                    node_id=-1,
                    numa_node=-1,
                    model=model_name,
                    target=target,
                    pe_type="compute" if rank < min_rank + n_compute_procs else "io",
                )
            self.master_models[model_name].min_rank = min_rank
            self.master_models[model_name].max_rank = max_rank
            self.master_models[model_name].num_io_procs = exe.procs[model_name].streams
            self.master_models[model_name].num_prefetch_proc = exe.procs[model_name].prefetch
            self.master_models[model_name].num_restart_procs = exe.procs[model_name].restart

            min_rank = max_rank + 1

        return min_rank

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

        for jsb_control_nml in self.master_namelist.iter_nml("jsb_control_nml"):
            jsb_control_nml["restart_jsbach"] = self.is_restart

    @property
    def is_restart(self) -> bool:
        """Check if the icon task starts from restart file(s)."""
        # Get restart status of first master model
        restart_ref = bool(next(iter(self.master_models.values())).inputs.get(restart_in_handler.port_name, False))
        # Check if all master models have the same restart status
        for model in self.master_models.values():
            if bool(model.inputs.get(restart_in_handler.port_name, False)) != restart_ref:
                msg = f"{self.name}: All CON models must have the same restart status"
                raise ValueError(msg)
        return restart_ref

    def get_exe_ranks_str(self, exe: yaml_data_models.ConfigIconExe) -> str:
        return ",".join(
            f"{self.master_models[name].min_rank}-{self.master_models[name].max_rank}" for name in exe.model_names()
        )

    def generate_multi_prog_config(self) -> None:
        match self.computer:
            case "santis":
                multi_prog_lines: list[str] = []
                if self.exe.gpu:
                    multi_prog_lines.append(
                        f"{self.get_exe_ranks_str(self.exe.gpu)} ./santis_icon_wrapper.sh ./icon_gpu"
                    )
                if self.exe.cpu:
                    multi_prog_lines.append(
                        f"{self.get_exe_ranks_str(self.exe.cpu)} ./santis_icon_wrapper.sh ./icon_cpu"
                    )
                if self.exe.hiopy:
                    multi_prog_lines.append(
                        f"{self.exe.hiopy.min_rank}-{self.exe.hiopy.max_rank} ./santis_hiopy_wrapper.sh"
                    )
                (self.run_dir / "multi-prog.conf").write_text("\n".join(multi_prog_lines))
            case _:
                msg = f"{self.name}: generate_multi_prog_config not implemented for machine {self.computer}"
                raise NotImplementedError(msg)

    def get_exe_compute_rank_bounds(self, exe: yaml_data_models.ConfigIconExe) -> Iterable[tuple[int, int]]:
        return (
            (self.master_models[name].min_rank, self.master_models[name].max_rank - exe.procs[name].tot_io_procs)
            for name in exe.model_names()
        )

    def get_exe_io_rank_bounds(self, exe: yaml_data_models.ConfigIconExe) -> Iterable[tuple[int, int]]:
        return (
            (self.master_models[name].max_rank - exe.procs[name].tot_io_procs + 1, self.master_models[name].max_rank)
            for name in exe.model_names()
        )

    def distribute_ranks(self) -> None:
        """Compute task distribution and dump to template file with placeholder node names"""

        match self.computer:
            case "santis":
                io_rank_bounds: Iterable[tuple[int, int]] = chain(
                    self.get_exe_io_rank_bounds(self.exe.gpu) if self.exe.gpu else (),
                    self.get_exe_io_rank_bounds(self.exe.cpu) if self.exe.cpu else (),
                )

                # Distribute gpu compute ranks to compute nodes (can be all nodes)
                if self.exe.gpu:
                    distribute_procs_load_balanced(
                        ranks_info=self.ranks_info,
                        rank_bounds=self.get_exe_compute_rank_bounds(self.exe.gpu),
                        nodes=tuple(range(self.compute_nodes)),
                        n_sockets=self.exe.gpu.sockets_per_node,
                    )
                if self.exe.separate_io:
                    # Distribute cpu compute ranks to compute nodes
                    if self.exe.cpu:
                        distribute_procs_load_balanced(
                            self.ranks_info,
                            self.get_exe_compute_rank_bounds(self.exe.cpu),
                            tuple(range(self.compute_nodes)),
                            n_sockets=self.exe.cpu.sockets_per_node,
                        )
                    # Distribute io ranks to io nodes
                    if self.exe.cpu:
                        n_sockets = self.exe.cpu.sockets_per_node
                    elif self.exe.gpu:
                        n_sockets = self.exe.gpu.sockets_per_node
                    else:
                        n_sockets = 1
                    distribute_procs_cyclic(
                        ranks_info=self.ranks_info,
                        rank_bounds=io_rank_bounds,
                        nodes=tuple(range(self.compute_nodes + 1, self.compute_nodes + self.io_nodes + 1)),
                        n_sockets=n_sockets,
                    )
                else:
                    # Distribute cpu compute ranks + io ranks to all nodes
                    if self.exe.cpu:
                        n_sockets = self.exe.cpu.sockets_per_node
                    elif self.exe.gpu:
                        n_sockets = self.exe.gpu.sockets_per_node
                    else:
                        n_sockets = 1
                    rank_bounds = chain(
                        self.get_exe_io_rank_bounds(self.exe.cpu) if self.exe.cpu else (), io_rank_bounds
                    )
                    distribute_procs_load_balanced(
                        ranks_info=self.ranks_info,
                        rank_bounds=rank_bounds,
                        nodes=tuple(range(self.nodes)),
                        n_sockets=n_sockets,
                    )

                # Write annotated hostfile
                nid_length = len(str(self.nodes - 1))
                (self.run_dir / "hostfile-sirocco").write_text(
                    "\n".join(
                        "sirocco_nid{node_id:0{nid_length}} {numa_node} {pe_type} {target} {model}".format(
                            **self.ranks_info[k], nid_length=nid_length
                        )
                        for k in range(self.n_procs)
                    )
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
            runtime_dir = Path(__file__).parent / "runtime" / self.computer
            if not runtime_dir.exists():
                msg = f"runtime not defined for computer {self.computer}"
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

        # Hostfile and multi-prog config file
        self.distribute_ranks()
        if self.target == "hybrid":
            self.generate_multi_prog_config()

    def runscript_lines(self) -> list[str]:
        lines: list[str] = []
        # Total number of tasks
        lines.append(f"export N_PROCS={self.n_procs}")
        lines.append(f"export SIROCCO_TARGET={self.target}")
        if self.exe.gpu:
            if self.exe.gpu.icon4py_venv:
                lines.append(f"export ICON4PY_VENV={self.exe.gpu.icon4py_venv}")
            if self.exe.gpu.gt4py_build_cache_dir:
                lines.append(f"export GT4PY_BUILD_CACHE_DIR={self.exe.gpu.gt4py_build_cache_dir}")
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
