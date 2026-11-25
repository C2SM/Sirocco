"""ICON restart modes and the stable links used for checkpoints."""

from typing import Literal, cast

import f90nml

RESTART_MODE_T = Literal["sync", "async", "joint procs multifile", "dedicated procs multifile"]
RESTART_MODES = ("sync", "async", "joint procs multifile", "dedicated procs multifile")


def restart_mode(namelist: f90nml.Namelist, writers: int) -> RESTART_MODE_T:
    """Resolve ICON's blank mode using the allocated writer count."""
    mode = namelist.get("io_nml", {}).get("restart_write_mode", "")
    if not isinstance(mode, str):
        msg = f"restart_write_mode must be a string, got {mode!r}"
        raise TypeError(msg)
    mode = mode.strip()
    if not mode:
        return "async" if writers > 0 else "sync"
    if mode not in RESTART_MODES:
        msg = f"Unsupported restart_write_mode {mode!r}, expected one of {RESTART_MODES} or a blank string"
        raise ValueError(msg)
    return cast("RESTART_MODE_T", mode)


def restart_domain_ids(namelist: f90nml.Namelist) -> list[int]:
    """Identify simulated domains from the dynamics-grid array (excluding domain 0)."""
    grid = namelist.get("grid_nml", {})
    filenames = grid.get("dynamics_grid_filename")
    if filenames is None:
        return [1]
    if not isinstance(filenames, list):
        return [1]
    lower_bounds = grid.start_index.get("dynamics_grid_filename", [1])
    first = lower_bounds[0]
    if first is None:
        first = 1
    domains = [first + i for i, name in enumerate(filenames) if first + i > 0 and name]
    if not domains:
        msg = "No simulated domain found in grid_nml.dynamics_grid_filename"
        raise ValueError(msg)
    return domains


def restart_link_names(model_name: str, namelist: f90nml.Namelist, writers: int) -> list[str]:
    """Return restart links in ascending domain order, or one multifile link."""
    if "multifile" in restart_mode(namelist, writers):
        return [f"multifile_restart_{model_name}.mfr"]
    return [f"restart_{model_name}_DOM{domain:02d}.nc" for domain in restart_domain_ids(namelist)]


def validate_restart(namelist: f90nml.Namelist, writers: int) -> None:
    """Validate restart parameters and writer resources."""
    if isinstance(writers, bool) or not isinstance(writers, int) or writers < 0:
        msg = "num_restart_procs must be a non-negative integer"
        raise ValueError(msg)
    mode = restart_mode(namelist, writers)
    if mode in ("async", "dedicated procs multifile") and writers == 0:
        msg = f"restart_write_mode={mode!r} requires exe cpu/gpu procs.<model>.restart > 0"
        raise ValueError(msg)
    if mode in ("sync", "joint procs multifile") and writers:
        msg = f"restart_write_mode={mode!r} requires exe cpu/gpu procs.<model>.restart = 0"
        raise ValueError(msg)
    for section, key in (("io_nml", "nrestart_streams"), ("parallel_nml", "restart_chunk_size")):
        value = namelist.get(section, {}).get(key, 1)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            msg = f"{section}.{key} must be a positive integer"
            raise ValueError(msg)
