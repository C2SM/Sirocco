---
icon: custom_icons/icosahedron
title: ICON task
---

<!-- Cap headersize to 0.8rem for this page. Otherwise specifications like `target_cycle` don't read well -->
<style>
h4, h5, h6 {
  font-size: 0.8rem !important;
  text-transform: none !important;
  font-weight: bold;
}
</style>

# ICON task

The ICON plugin is designed as an interface between the Sirocco workflow and the [ICON model :lucide-external-link:](https://www.icon-model.org/). It will generate the final run scripts executed on the system based on specifications like namelists, MPI ranks distribution, hardware properties, etc ...

## Supported machines

ICON tasks can currently only run on the **Santis** machine at CSCS, _i.e._ with [`computer: santis`](../configuration/tasks.md#computer), as they rely on machine specific [`runtime`](#runtime) scripts and MPI ranks mapping. On this machine:

- the [`uenv`](../configuration/tasks.md#uenv) task specification is **required**.
- contrary to other tasks, `uenv` and [`view`](../configuration/tasks.md#view) are not set in the job header. They are exported as `ICON_UENV` and `ICON_VIEW` in the submitted run script, together with potential [squashfs images](#squash), for further use by the [`runtime`](#runtime) scripts.

!!! example "Other machines"

    We plan to soon support other specific machines out of the box, as well as arbitrary ones, provided users bring the required specifications, like the `runtime` scripts and the node topology (see [issue #304 :lucide-external-link:](https://github.com/C2SM/Sirocco/issues/304)).

## Components
As mentioned in the [cycles](../configuration/cycles.md#tasks) section, we need to introduce the `components` level between tasks and their inputs/outputs in the `cycles` root section where the workflow graph topology is defined. In order to avoid conflicts between identical ports of models listed in `icon_master.namelist` (and bring clarity), each of them is mapped to a component with its own inputs and outputs. Restart state or output streams would be typical conflicting inputs without the component level. The `cycles` section hence reads like the following

```sirocco-yaml
cycles:
  - cycle_name:
      tasks:
        - icon_task:
            components:
              master:
                inputs: [...]
              atm:
                inputs: [...]
                outputs: [...]
              ocean:
                inputs: [...]
                outputs: [...]
              land:
                inputs: [...]
```
Each of the models listed in `icon_master.namelist` must have a corresponding component specified. So `atm`, `ocean` and `land` in this example. On top of these, a special and optional `master` component is available to host ports that are not specifically linked to a component, like [basic input linking](../configuration/cycles.md#special-input-linking-ports). Note that the `link` and `link_content` ports as well as [`squash`](#squash) are only available in the `master` component.

## Ports

Contrary to Shell tasks, ICON task ports are pre-defined. Listed below are the currently supported ones. The typical associated action is adapting namelists to reflect an input data path or linking input data in the task run directory.

This list regularly gets extended as use cases start to cover more ICON features. Should a port be missing, the [basic input linking](../configuration/cycles.md#special-input-linking-ports) might first help circumventing this lack. Then please open a pull request on the [Sirocco repository :lucide-external-link:](https://github.com/C2SM/Sirocco/pulls).

### Input ports

#### `squash`

**component types**: master
<br>
**data instances**: any
<br>
**actions**:

- All data instances of that port are [squashfs :lucide-external-link:](https://en.wikipedia.org/wiki/SquashFS) images that will be mounted at runtime at the mount points given in the [`squash_mount`](#squash_mount) task specification. So far, this feature is only supported on  machines using the [uenv technology :lucide-external-link:](https://docs.cscs.ch/software/uenv/).

#### `restart_in`

**component types**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- Validate `io_nml:restart_write_mode`: `"sync"`, `"async"`, `"joint procs multifile"`, or `"dedicated procs multifile"`. A blank mode selects `"sync"` with zero restart writers and `"async"` otherwise.
- Link the associated data as `multifile_restart_{model.name}.mfr` for multifile modes, or `restart_{model.name}_DOM01.nc`, etc. for single-file modes.

#### `dynamics_grid_file`

**component types**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `grid_nml > dynamics_grid_filename`.

#### `ifs2icon`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `initicon_nml > ifs2icon_filename`.

#### `latbc`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it as `latbc` in the task run directory.
- Reflect data path in `limarea_nml > latbc_path`.

#### `extpar_file`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `extpar_nml > extpar_filename`.

#### `ecrad_data`

**component types**: atmosphere
<br>
**data instances**: 1
<br>

**actions**:

- If the associated data is generated, link it as `ecrad_data` in the task run directory.
- Reflect data path in `radiation_nml > ecrad_data_path`.

#### `ghg_file`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `radiation_nml > ghg_filename` namelist parameter.

#### `rrtmg_lw`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it as `rrtmg_lw.nc` in the task run directory.
- Reflect the data path in `nwp_phy_nml > lrtm_filename` namelist parameter.

#### `rrtmg_sw`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `rrtmg_sw.nc`.

#### `bc_solar_sw`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `bc_solar_irradiance_sw_b14.nc`.

#### `atm_plumes`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `MACv2.0-SP_v1.nc`.

#### `cloud_opt_props`

**component types**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it as `CldOptProps.nc` in the task run directory.
-  Reflect data path in `nwp_phy_nml > cldopt_filename`.

#### `jsb_ifs`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

 - If the associated data is generated, link it in the task run directory with its original name.
 - Reflect data path in the `jsb_model_nml > ifs_filename`.

#### `jsb_fract`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_model_nml > fract_filename`.

#### `jsb_seb_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > bc_filename`.

#### `jsb_seb_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > ic_filename`.

#### `jsb_rad_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > bc_filename`.

#### `jsb_rad_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > ic_filename`.

#### `jsb_turb_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > bc_filename`.

#### `jsb_turb_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > ic_filename`.

#### `jsb_sse_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > bc_filename`.

#### `jsb_sse_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > ic_filename`.

#### `jsb_hydro_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > bc_filename`.

#### `jsb_hydro_bc_sso`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > bc_sso_filename`.

#### `jsb_hydro_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > ic_filename`.

#### `jsb_pheno_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > bc_filename`.

#### `jsb_pheno_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > ic_filename`.

#### `jsb_disturb_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > bc_filename`.

#### `jsb_disturb_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > ic_filename`.

#### `jsb_hd_bc`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > bc_filename`.

#### `jsb_hd_ic`

**component types**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > ic_filename`.

#### `ocean_inistate`

**component types**: ocean
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `ocean_initialConditions_nml > InitialState_InputFileName` namelist parameter.

### Output ports

#### `restart_out`

**component types**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- Validate the same restart modes and data counts as [`restart_in`](#restart_in).
- Resolve the generated data paths to `multifile_restart_{model.name}.mfr` for multifile modes, or `restart_{model.name}_DOM01.nc`, etc. for single-file modes.

#### `output_streams`

**component types**: atmosphere, ocean
<br>
**data instances**: any
<br>
**actions**:

- Check that the number of output streams declared as `output_nml` sections matches the number of associated data pieces.
- Ensure that all `output_nml` streams specify an output sub-directory using the combination of `filename_format` and `output_filename` parameters (see in [ICON namelist overview :lucide-external-link:](https://docs.icon-model.org/2026.04/documentation/buildrun/buildrun_namelist_overview.html#output-nml) ). For instance `filename_format = 'atm_mon/atm_mon_<datetime2>'` is a valid entry while `filename_format = 'atm_mon_<datetime2>'` isn't.
- Create these sub-directories in the task run directory.

#### `finish_status`

**component types**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**: None

## Task specifications

The following ICON task specifications come on top of [the ones used for any task](../configuration/tasks.md#all-tasks).

### `namelists`

**type**: sequence
<br>
**required**
<br>
**description**: Sequence of namelist specifications. This list must contain the master `icon_master.namelist` and each of the component namelists found in it. In a simple case, a namelist specification can simply be the path to the namelist file relative to the configuration directory like this
```sirocco-yaml
tasks:
  - icon_task:
      [...]
      namelists:
        - "path/to/icon_master.namelist"
        - "path/to/atmo_namelist.nml"
        - "path/to/ocean_namelist.nml"
```
Namelist parameters can also be overwritten or created by turning the simple namelist path above into a mapping of that path to the modified parameters like in the following
```sirocco-yaml
tasks:
  - icon_task:
      [...]
      namelists:
        - "path/to/icon_master.namelist"
        - "path/to/atmo_namelist.nml":
            parallel_nml:
              nblocks_e: 1
              nproma_sub: 10469
              io_proc_chunk_size: 12
            run_nml:
              modelTimeStep: "PT20S"
              num_lev: 120
              msg_level: 10
        - "path/to/ocean_namelist.nml":
            parallel_nml:
              nproma: 16
            run_nml:
              modelTimeStep: "PT2M"
```

#### Repeated namelist sections

Some sections appear several times in a namelist file, typically `output_nml` with one occurrence per output stream. A specific occurrence is targeted by appending its index between square brackets to the section name, **starting at 1** and following the order of the namelist file. Without index, the overwrite applies to **all** occurrences of the section.
```sirocco-yaml
tasks:
  - icon_task:
      [...]
      namelists:
        - "path/to/icon_master.namelist"
        - "path/to/atmo_namelist.nml":
            output_nml:  # (1)!
              filetype: 5
            output_nml[2]:  # (2)!
              output_interval: "PT6H"
```

1.  Applies to all `output_nml` sections.
2.  Only applies to the second `output_nml` section of the file.

A new occurrence of the section is appended by using the index right after the last existing one, _e.g._ `output_nml[3]` for a namelist file containing 2 `output_nml` sections, or `output_nml[1]` if it does not contain any. Several occurrences can be appended at once, in any order, as long as no index is skipped. Overwrites given without index also apply to the appended occurrences, while the ones given with an index always take precedence.

#### Monthly values for `allow_overcast`

The `nwp_tuning_nml > allow_overcast` overwrite accepts, on top of a single value, a sequence of 12 monthly values (January to December). Each task instance then gets the value corresponding to the month of its cycle occurrence.
```sirocco-yaml
tasks:
  - icon_task:
      [...]
      namelists:
        - "path/to/icon_master.namelist"
        - "path/to/atmo_namelist.nml":
            nwp_tuning_nml:
              allow_overcast: [0.9, 0.9, 0.85, 0.8, 0.75, 0.7, 0.7, 0.75, 0.8, 0.85, 0.9, 0.9]
```
This requires each cycle occurrence to lie within a single calendar month, _e.g._ with a monthly cycle starting on the first day of a month.

#### Namelist entries managed by Sirocco

The namelists written to the task run directory are not plain copies of the ones provided in the configuration directory. Some entries directly follow from the workflow description, so Sirocco sets them itself. They are listed below and **take precedence** over both the original namelist files and the overwrites specified in `namelists`: there is no need to set them and any value given for them is ignored.

| Namelist file | Entry | Value set by Sirocco |
|---|---|---|
| master namelist | `master_time_control_nml > experimentStopDate` | stop date of the current [cycle occurrence](../glossary.md#cycle-occurrence) |
| master namelist | `master_time_control_nml > restartTimeIntval` | [cycle period](../configuration/cycles.md#cycling) |
| master namelist | `master_time_control_nml > checkpointTimeIntval` | [cycle period](../configuration/cycles.md#cycling) |
| master namelist | `master_nml > lrestart` | `.true.` if the task restarts (see below), `.false.` otherwise |
| master namelist | `master_nml > read_restart_namelists` | same as `lrestart` |
| master namelist | `jsb_control_nml > restart_jsbach` | same as `lrestart` |
| master namelist | `master_model_nml > model_min_rank` and `model_max_rank` | MPI rank range allocated to the model from the [`exe`](#exe) specifications |
| model namelists | `parallel_nml > num_io_procs` | [`streams`](#procs) of the model |
| model namelists | `parallel_nml > num_prefetch_proc` | [`prefetch`](#procs) of the model |
| model namelists | `parallel_nml > num_restart_procs` | [`restart`](#procs) of the model |
| model namelists | entries associated to [input ports](#input-ports) | path of the data linked to the port |

The dates and intervals imply that an ICON task always simulates exactly one cycle occurrence, writing its restart state at the end of it. ICON tasks therefore have to belong to a recurring [cycle](../configuration/cycles.md#cycling).

A task is **restarting** when its [`restart_in`](#restart_in) port links some data, which typically depends on a [`when`](../configuration/cycles.md#when) condition, and is otherwise considered a **cold start**. All the master models of a task must have the same restart status.

!!! warning "`experimentStartDate` is left to the user"

    `master_time_control_nml > experimentStartDate` is not set by Sirocco since it might precede the cycle start date, _e.g._ when continuing a previous simulation. For a cold start however, it must be specified and match the cycle `start_date`, otherwise the workflow is rejected.

Conversely, Sirocco relies on the following entries, which thus have to be correctly set by the user:

- `master_model_nml` (and `jsb_model_nml`) in `icon_master.namelist`: `model_name`, which must match the [component](#components) names, `model_namelist_filename`, which must be the file name of one of the provided `namelists`, and `model_type` for `master_model_nml`.
- `io_nml > restart_write_mode` must be `"joint procs multifile"` for models using the [`restart_in`](#restart_in) or [`restart_out`](#restart_out) ports.
- `output_nml` sections, see the [`output_streams`](#output_streams) port.


### `yac_coupling`

**type**: string
<br>
**required** for coupled runs, _i.e._ with several master models, ignored otherwise
<br>
**description**: Path, relative to the configuration directory, to the YAC coupling file `coupling.yaml`. It is copied to the task run directory.

### `exe`

**type**: mapping
<br>
**required**
<br>
**description**: This section groups executables specification and MPI ranks allocation and mapping. The example below shows all the sub-specifications which are detailed thereafter.

``` sirocco-yaml
tasks:
  - icon_task:
      exe:
        separate_io: true
        procs_per_io_node: 4
        cpu:
          path: "path/to/icon_cpu"
          procs:
            ocean:
              compute_weight: 3
              streams: 3
              prefetch: 1
              restart: 0
            land:
              compute_weight: 1
              streams: 2
        gpu:
          path: "path/to/icon_gpu"
          compute_procs_per_node: 4
          icon4py_venv: "path/to/icon4py/venv"
          gt4py_build_cache_dir: "../.."
          procs:
            atm:
              compute_weight: 1
              streams: 2
```

??? example "Hiopy"

    A `hiopy` section at the same level as `cpu` and `gpu` exists as a placeholder for now and will be fully implemented in `v0.2.0`.

#### Resources allocation and mapping

Based on the `exe` specifications, Sirocco allocates MPI processes to the models, including potential processes dedicated to I/O. Ranks are mapped by blocks over the requested nodes and then by sub-blocks over NUMA nodes. This design choice keeps close subdomains together as much as possible while also load balancing the memory requirement over the nodes.

The resulting mapping is written to the `hostfile-sirocco` file in the task run directory, one line per MPI rank giving its node, NUMA node, process type, target and model. This file is scheduler independent: applying it is the job of the [`runtime`](#runtime) scripts. The default runtime for Santis, for instance, does so with SLURM's arbitrary distribution (`srun --distribution=arbitrary`).

##### `separate_io`

**type**: boolean
<br>
**default**: `False`
<br>
**description**: Map I/O ranks to dedicated nodes, separating them from compute ranks. This can be useful if memory becomes a limiting factor.

##### `procs_per_io_node`

**type**: integer
<br>
**default**: 0
<br>
**description**: The number of MPI processes per I/O dedicated node when `separate_io` is activated. Must be $> 0$ in that case.

#### `cpu` and `gpu`

**type**: mapping
<br>
**required**: at least one of them
<br>
**description**: Description of the ICON executable and associated MPI processes. At least one of the `cpu` or `gpu` executable must be set but hybrid runs with both are also supported. A corresponding `SIROCCO_TARGET` environment variable is exported for further use in [`runtime`](#runtime), possible values being `"cpu"`, `"gpu"` or `"hybrid"`.

For each of `cpu` and `gpu`, the following can or must be set.

##### `path`

**type**: string
<br>
**required**
<br>
**description**: Path to the ICON executable. It can be a relative path only in the case where the icon executable is located in a [squashfs image](#squash) and the corresponding [mount point](#squash_mount) is relative. Other relative paths will not be resolved.

##### `procs`

**type**: mapping
<br>
**required**
<br>
**description**: mapping of the master model (component) names associated to this executable to the different types of MPI processes. For single executable cases, all master models must be included while they must be split across `cpu` and `gpu` for hybrid cases. The 4 types of MPI processes are the following with their default values:
```sirocco-yaml
tasks:
  - icon_task:
      exe:
        cpu:
          procs:
            atm:
              compute_weight: 1
              streams: 0
              prefetch: 0
              restart: 0
```
All of them are integers. `streams`, `prefetch` and `restart` are absolute numbers of MPI processes while `compute_weight`, as the name indicates, is a relative number. Components will get a number of compute processes by dispatching the compute resources, so excluding I/O processes, according to the specified weights. For instance, dedicating $50/100$ of the compute resources to the `atm` component, $35/100$ to `ocean` and $15/100$ to `land` would be done with
```sirocco-yaml
tasks:
  - icon_task:
      exe:
        cpu:
          procs:
            atm:
              compute_weight: 50
            ocean:
              compute_weight: 35
            land:
              compute_weight: 15
```
Of course the final proportions might not exactly match the specified weights as the numbers of allocated compute processes are rounded to the closest possible integer. This design enables scalable configurations where requesting more (less) nodes overall simply implies increasing (decreasing) [`nodes`](../configuration/tasks.md#nodes) and no other calculation to make the new configuration fit on the requested resources.


##### `compute_procs_per_node`

**type**: integer
<br>
**required** (mostly)
<br>
**description**: Number of MPI compute processes per node, so excluding I/O processes. It is required for `gpu` executables. For `cpu`, it has to be provided when [`separate_io`](#separate_io) is activated, otherwise it is ignored and [`procs_per_node`](../configuration/tasks.md#procs_per_node) gets required. Make sure, in that case, that `procs_per_node` includes all MPI processes: `gpu` and `cpu`, compute and I/O.

##### `icon4py_venv`
**type**: string
<br>
**optional**
<br>
**description**: Path to the [icon4py :lucide-external-link:](https://github.com/C2SM/icon4py) virtual environment. If provided to the `gpu` executable (`cpu` is ignored for now), it will be exported as `ICON4PY_VENV` in the submitted runscript so that subsequent [`runtime`](#runtime) can make use of it.

##### `gt4py_build_cache_dir`

**type**: string
<br>
**optional**
<br>
**description**: Path to the [gt4py :lucide-external-link:](https://github.com/GridTools/gt4py) build cache dir. It is used at runtime by the submitted ICON task so that relative paths are resolved from the task run directory. When using the standalone engine, given how Sirocco organizes [task run directories](../orchestration.md#workflow-folder-hierarchy), specifying `..` can be useful as all icon tasks would point to the same path, avoiding cache filling by each of them. If provided to the `gpu` executable (`cpu` is ignored for now), it will be exported as `GT4PY_BUILD_CACHE_DIR` in the submitted runscript so that subsequent [`runtime`](#runtime) can make use of it.

### `runtime`

**type**: string
<br>
**optional**
<br>
**description**: Path to a directory relative to the configuration directory containing the set of scripts used at runtime to replace the default ones. The only requirement is that this directory contains a `main.sh` that will be sourced by the submitted job script. For supported machines, the default `runtime` directories are hosted in the Sirocco repository under [`src/sirocco/core/tasks/icon_task/runtime` :lucide-external-link:](https://github.com/C2SM/Sirocco/tree/main/src/sirocco/core/tasks/icon_task/runtime).

Each machine having its own characteristics (chips, network, MPI library, scheduler, compilers, ...), it would be impossible to maintain a composable runtime environment where each piece can be customized independently. The adopted strategy is thus to bundle
all these characteristics in the `runtime` directory and customizing it is done by replacing the whole directory. The most expected usage is to copy the default machine specific `runtime` and modify where required.

### `squash_mount`

**type**: sequence of strings
<br>
**optional**
<br>
**description**: runtime mount points of the squashfs images specified in the [squash port](#squash) given in the same order. Relative paths are interpreted as relative to the task run directory and corresponding directories will be created. So far, this feature is only supported on  machines using the [uenv technology :lucide-external-link:](https://docs.cscs.ch/software/uenv/).
