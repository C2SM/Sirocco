---
icon: custom_icons/icosahedron
title: ICON task
---

# ICON task

The ICON plugin is designed as an interface between the Sirocco workflow and the [ICON model :lucide-external-link:](icon-model.org). It will generate the final run scripts executed on the system based on specifications like namelists, MPI ranks distribution, hardware properties, etc ...

## Components
As mentioned in the [cycles](../../configuration/cycles#tasks) section, we need to introduce the `components` level between tasks and there inputs/outputs in the `cycles` root section where the workflow graph topology is defined. In order to avoid conflicts between identical ports of models listed in `icon_master.namelist` (and bring clarity), each of them is mapped to a component with its own inouts and outouts. Restart state or output streams would be typical conflicting inputs without the component level. The `cycles` section hence reads like the following

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
Each of the models listed in `icon_master.namelist` must have a correponding component specified. So `atmo`, `ocean` and `land` in this example. On top of these, a special and optional `master` component is avaialble to host ports that are not specifically linked to a compomnent, like [basic input linking](../../configuration/cycles/#special-input-linking-ports).

## Ports

Listed below are the currently supported ports for icon tasks. What they typically do is adapting namelists to refelct an input data path or link input data in the task run directory.

This list regularly gets extended as use cases start to cover more ICON features. Should a port be missing, the [basic input linking](../../configuration/cycles/#special-input-linking-ports) might first help circumventing this lack. Then please open a pull request on the [Sirocco repository :lucide-external-link:](https://github.com/C2SM/Sirocco/pulls).

### `squash`

**components**: master
<br>
**number of data pieces**: any
<br>
**actions**:

- Each data instance of that port are [squashfs :lucide-external-link:](https://en.wikipedia.org/wiki/SquashFS) images that will be mounted at runtime at the mount points given in the [`squash_mount`](#squash_mount) task specification.

### `finish_status`

**components**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**: None

### `restart_in`

**components**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.
- Link the associated data as `multifile_restart_{model.name}.mfr` in the task run directory.

### `restart_out`

**components**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.

### `output_streams`

**components**: atmosphere, ocean
<br>
**number of data pieces**: any
<br>
**actions**:

- Check that the number of output streams declared as `output_nml` sections matches the number of associated data pieces.
- Ensure that all `output_nml` streams specify an output sub-directory using the combinaiton of `filename_format` and `output_filename` parameters (see in [ICON namelist overiew :lucide-external-link:](https://docs.icon-model.org/2026.04/documentation/buildrun/buildrun_namelist_overview.html#output-nml) ). For instance `filename_format = 'atm_mon/atm_mon_<datetime2>'` is a valid entry while `filename_format = 'atm_mon_<datetime2>'` isn't.
- Create these sub-directories in the task run directory.

### `dynamics_grid_file`

**components**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `grid_nml > dynamics_grid_filename`.

### `ifs2icon`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `initicon_nml > ifs2icon_filename`.

### `extpar_file`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `extpar_nml > extpar_filename`.

### `ecrad_data`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>

**actions**:

- If the associated data is generated, link it as `ecrad_data` in the task run directory.
- Reflect data path in `radiation_nml > ecrad_data_path`.

### `ghg_file`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `radiation_nml > ghg_filename` namelist parameter.

### `rrtmg_lw`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect the data path in `nwp_phy_nml > lrtm_filename` namelist parameter.

### `rrtmg_sw`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `rrtmg_sw.nc`.

### `bc_solar_sw`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `bc_solar_irradiance_sw_b14.nc`.

### `atm_plumes`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `MACv2.0-SP_v1.nc`.

### `cloud_opt_props`

**components**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it as `CldOptProps.nc` in the task run directory.
-  Reflect data path in `nwp_phy_nml > cldopt_filename`.

### `jsb_ifs`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

 - If the associated data is generated, link it in the task run directory with its original name.
 - Reflect data path in the `jsb_model_nml > ifs_filename`.

### `jsb_fract`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_model_nml > fract_filename`.

### `jsb_seb_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > bc_filename`.

### `jsb_seb_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > ic_filename`.

### `jsb_rad_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > bc_filename`.

### `jsb_rad_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > ic_filename`.

### `jsb_turb_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > bc_filename`.

### `jsb_turb_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > ic_filename`.

### `jsb_sse_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > bc_filename`.

### `jsb_sse_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > ic_filename`.

### `jsb_hydro_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > bc_filename`.

### `jsb_hydro_bc_sso`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml:bc > sso_filename`.

### `jsb_hydro_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > ic_filename`.

### `jsb_pheno_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > bc_filename`.

### `jsb_pheno_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > ic_filename`.

### `jsb_disturb_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > bc_filename`.

### `jsb_disturb_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > ic_filename`.

### `jsb_hd_bc`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > bc_filename`.

### `jsb_hd_ic`

**components**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > ic_filename`.

### `ocean_inistate`

**components**: ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `ocean_initialConditions_nml > InitialState_InputFileName` namelist parameter.

## ICON task specifications

The following ICON task specifications come on top of [the ones used for any task](../../configuration/tasks#all-tasks).

### `plugin`

**type**: string
<br>
**choices**: "icon"
<br>
**description**: plugin name

### `namelists`

**type**: sequence
<br>
**required**
<br>
**description**: Sequence of namelist specifications. This list must contain the master `icon_master.namelist` and each of the component namelists found in it. In a simple case, a namelist specification can simply be the path to the namelist file relative to the configuration root directory like this
```sirocco-yaml
tasks:
  - icon_task:
      plugin: icon
      namelists:
        - "path/to/icon_master.namelist"
        - "path/to/atmo_namelist.nml"
        - "path/to/ocean_namelist.nml"
```
Namelist parameters can also be overwritten or created by turning the simple namelist path above into a mapping of that path to the modified parameters like in the following
```sirocco-yaml
tasks:
  - icon_task:
      plugin: icon
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

### `runtime`

### `squash_mount`

**type**: sequence of str
<br>
**optional**
<br>
**description**: run time mount points of the data instances specified in the [squash port](#squash) in that same order. Relative paths are interpreted as relative to the task run directory and corresponding directories will be created.
