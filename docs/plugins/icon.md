---
icon: custom_icons/icosahedron
title: ICON task
---

<!-- Cap headersize to 0.8rem for this page. Otherwise specifications like `target_cycle` don't read well -->
<style>
h4, h5, h6 {
  font-size: 0.8rem !important;
  text-transform: none !important;
  text-width: bold;
}
</style>

# ICON task

The ICON plugin is designed as an interface between the Sirocco workflow and the [ICON model :lucide-external-link:](icon-model.org). It will generate the final run scripts executed on the system based on specifications like namelists, MPI ranks distribution, hardware properties, etc ...

## Components
As mentioned in the [cycles](../../configuration/cycles#tasks) section, we need to introduce the `components` level between tasks and there inputs/outputs in the `cycles` root section where the workflow graph topology is defined. In order to avoid conflicts between identical ports of models listed in `icon_master.namelist` (and bring clarity), each of them is mapped to a component with its own inputs and outputs. Restart state or output streams would be typical conflicting inputs without the component level. The `cycles` section hence reads like the following

```sirocco-yaml
cycles:
  - cycle_name:
      tasks:
        - icon_task:
            components:
              master:
                inputs: [...]
              atmo:
                inputs: [...]
                outputs: [...]
              ocean:
                inputs: [...]
                outputs: [...]
              land:
                inputs: [...]
```
Each of the models listed in `icon_master.namelist` must have a corresponding component specified. So `atmo`, `ocean` and `land` in this example. On top of these, a special and optional `master` component is available to host ports that are not specifically linked to a component, like [basic input linking](../../configuration/cycles/#special-input-linking-ports).

## Ports

Contrary to Shell tasks, ICON task ports are pre-defined. Listed below are the currently supported ones. The typical associated action is adapting namelists to reflect an input data path or linking input data in the task run directory.

This list regularly gets extended as use cases start to cover more ICON features. Should a port be missing, the [basic input linking](../../configuration/cycles/#special-input-linking-ports) might first help circumventing this lack. Then please open a pull request on the [Sirocco repository :lucide-external-link:](https://github.com/C2SM/Sirocco/pulls).

### Input ports

#### `squash`

**components**: master
<br>
**data instances**: any
<br>
**actions**:

- Each data instance of that port are [squashfs :lucide-external-link:](https://en.wikipedia.org/wiki/SquashFS) images that will be mounted at runtime at the mount points given in the [`squash_mount`](#squash_mount) task specification. So far, this feature is only supported on  machines using the [uenv technology :lucide-external-link:](link to CSCS docs).

#### `finish_status`

**components**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**: None

#### `restart_in`

**components**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.
- Link the associated data as `multifile_restart_{model.name}.mfr` in the task run directory.

#### `dynamics_grid_file`

**components**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `grid_nml > dynamics_grid_filename`.

#### `ifs2icon`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `initicon_nml > ifs2icon_filename`.

#### `extpar_file`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `extpar_nml > extpar_filename`.

#### `ecrad_data`

**components**: atmosphere
<br>
**data instances**: 1
<br>

**actions**:

- If the associated data is generated, link it as `ecrad_data` in the task run directory.
- Reflect data path in `radiation_nml > ecrad_data_path`.

#### `ghg_file`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `radiation_nml > ghg_filename` namelist parameter.

#### `rrtmg_lw`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect the data path in `nwp_phy_nml > lrtm_filename` namelist parameter.

#### `rrtmg_sw`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `rrtmg_sw.nc`.

#### `bc_solar_sw`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `bc_solar_irradiance_sw_b14.nc`.

#### `atm_plumes`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- Link associated data in the task run directory as `MACv2.0-SP_v1.nc`.

#### `cloud_opt_props`

**components**: atmosphere
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it as `CldOptProps.nc` in the task run directory.
-  Reflect data path in `nwp_phy_nml > cldopt_filename`.

#### `jsb_ifs`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

 - If the associated data is generated, link it in the task run directory with its original name.
 - Reflect data path in the `jsb_model_nml > ifs_filename`.

#### `jsb_fract`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_model_nml > fract_filename`.

#### `jsb_seb_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > bc_filename`.

#### `jsb_seb_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > ic_filename`.

#### `jsb_rad_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > bc_filename`.

#### `jsb_rad_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > ic_filename`.

#### `jsb_turb_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > bc_filename`.

#### `jsb_turb_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > ic_filename`.

#### `jsb_sse_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > bc_filename`.

#### `jsb_sse_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > ic_filename`.

#### `jsb_hydro_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > bc_filename`.

#### `jsb_hydro_bc_sso`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml:bc > sso_filename`.

#### `jsb_hydro_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > ic_filename`.

#### `jsb_pheno_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > bc_filename`.

#### `jsb_pheno_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > ic_filename`.

#### `jsb_disturb_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > bc_filename`.

#### `jsb_disturb_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > ic_filename`.

#### `jsb_hd_bc`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > bc_filename`.

#### `jsb_hd_ic`

**components**: land
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > ic_filename`.

#### `ocean_inistate`

**components**: ocean
<br>
**data instances**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `ocean_initialConditions_nml > InitialState_InputFileName` namelist parameter.

### Output ports

#### `restart_out`

**components**: atmosphere, ocean
<br>
**data instances**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.

#### `output_streams`

**components**: atmosphere, ocean
<br>
**data instances**: any
<br>
**actions**:

- Check that the number of output streams declared as `output_nml` sections matches the number of associated data pieces.
- Ensure that all `output_nml` streams specify an output sub-directory using the combination of `filename_format` and `output_filename` parameters (see in [ICON namelist overiew :lucide-external-link:](https://docs.icon-model.org/2026.04/documentation/buildrun/buildrun_namelist_overview.html#output-nml) ). For instance `filename_format = 'atm_mon/atm_mon_<datetime2>'` is a valid entry while `filename_format = 'atm_mon_<datetime2>'` isn't.
- Create these sub-directories in the task run directory.

## Task specifications

The following ICON task specifications come on top of [the ones used for any task](../../configuration/tasks#all-tasks).

### `namelists`

**type**: sequence
<br>
**required**
<br>
**description**: Sequence of namelist specifications. This list must contain the master `icon_master.namelist` and each of the component namelists found in it. In a simple case, a namelist specification can simply be the path to the namelist file relative to the configuration root directory like this
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
        - "path/to/ocean_namelist.nml"
            parallel_nml:
              nproma: 16
            run_nml:
              modelTimeStep: "PT2M"
```


### `yac_copupling`

**type**: str
<br>
**optional**
<br>
**description**: Relative path to the configuration root directory pointing to the YAC coupling file `coupling.yaml`.

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
            atmo:
              compute_weight: 1
              streams: 2
```

??? example "Hiopy"

    A `hiopy` section at the same level as `cpu` and `gpu` exists as a placeholder for now and will be fully implemented in `v0.2.0`.

#### Resources allocation and mapping

Based on the `exe` specifications, Sirocco will allocate MPI processes to master models, including potential processes dedicated to I/O. With the SLURM scheduler, MPI ranks are then mapped by blocks over the requested nodes and finally by sub-blocks over NUMA nodes. This design choice keeps close subdomains together as much as possible while also load balancing the memory requirement over the nodes.

##### `separate_io`

**type**: logical
<br>
**default**: `False`
<br>
**description**: Map I/O ranks to dedicated nodes, separating them from compute ranks. This can be useful if memory becomes a limiting factor.

##### `procs_per_io_node`

**type**: integer
<br>
**default**: 0
<br>
**description**: The number of MPI processes per I/O dedicated node when `sparate_io` is activated. Must be $> 0$ in that case.

#### `cpu` and `gpu`

**type**: mapping
<br>
**required**: at least one of them
<br>
**description**: Description of the ICON executable and associated MPI processes. At least one of the `cpu` or `gpu` executable must be set but hybrid runs with both are also supported. For each of them, the following can or must be set.

##### `path`

**type**: str
<br>
**required**
<br>
**description**: Path to the ICON executable. It can be a relative path only in the case where the icon executable is located in a [squashfs image](#squash) and the corresponding [mount point](#squash_mount) is relative. Other relative paths will not be resolved.

##### `procs`

**type**: mapping
<br>
**required**
<br>
**description**: mapping of the master model (component) names associated to this executable to the different types of MPI processes. For single executable cases, all master models must be included while they must be split accross `cpu` and `gpu` for hybrid cases. The 4 type of MPI processes are the following with their default values:
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
All of them are integers. `streams`, `prefetch` and `restart` are absolute nunmbers of MPI processes while `compute_weight`, as the name indicates, is a relative number. Components will get a number of compute processes by dispatching the compute resources, so excluding I/O processes, according to the specified weights. For instance, dedicating $50/100$ of the compute resources to the `atm` component, $35/100$ to `ocean` and $15/100$ to `land` would be done with
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
Of course the final proportions might not exactly match the specified weights as the numbers of allocated compute processes are rounded to the closest possible integer. This design enables scalable configurations where requesting more (less) nodes overall simply implies increasing (decreasing) [`nodes`](../../configuration/tasks/#nodes) and no other calculation to make the new configuration fit on the requested resources.


##### `compute_procs_per_node`

**type**: integer
<br>
**required** (mostly)
<br>
**description**: Number of MPI compute processes per node, so excluding I/O processes. It is required for `gpu` executables. For `cpu`, it has to be provided when [`separate_io`](#separate_io) is activated, otherwise it is ignored and [`procs_per_node`](../../configuration/tasks/#procs_per_node) gets required. Make sure, in that case, that `procs_per_node` includes all MPI processes: `gpu` and `cpu`, compute and I/O.

##### `icon4py_venv`
**type**: str
<br>
**optional**
<br>
**description**: Path to the [icon4py :lucide-external-link:](https://github.com/C2SM/icon4py) virtual environment. If provided, it will be exported as `ICON4PY_VENV` in the submitted runscript so that subsequent [`runtime`](#runtime) can make use of it.

##### `gt4py_build_cache_dir`

**type**: str
<br>
**optional**
<br>
**description**: Path to the [gt4py :lucide-external-link:](https://github.com/GridTools/gt4py) build cache dir. It is used at runtime by the submitted ICON task so that relative paths are resolved from the task run directory. Given how Sirocco organizes task run directories (TODO: missing link), specifying `..` can be useful as all icon tasks would point to the same path, avoiding cache filling by each of them.

### `runtime`

**type**: str
<br>
**optional**
<br>
**description**: Path to a directory relative to the configuration root directory containing the set of scripts used at runtime to replace the default ones. The only requirement is that this directory contains a `main.sh` that will be sourced by the submitted job script. For supported machines, the default `runtime` directories are hosted in the Sirocco repository under [`src/sirocco/core/tasks/icon_task/runtime` :lucide-external-link:](https://github.com/C2SM/Sirocco/tree/main/src/sirocco/core/tasks/icon_task/runtime).

Each machine having its own characteristics (chips, network, MPI library, scheduler, compilers, ...), it would be impossible to maintain a composable runtime environment where each piece can be customized independently. The adopted strategy is thus to bundle
all these characteristics in the `runtime` directory and customizing it is done by replacing the whole directory. The most expected usage is to copy the default machine specific `runtime` and modify where required.

### `squash_mount`

**type**: sequence of str
<br>
**optional**
<br>
**description**: runtime mount points of the squashfs images specified in the [squash port](#squash) given in the same order. Relative paths are interpreted as relative to the task run directory and corresponding directories will be created. So far, this feature is only supported on  machines using the [uenv technology :lucide-external-link:](link to CSCS docs).
