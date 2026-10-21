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

### `restart_in`

**valid models**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.
- Link the associated data as `multifile_restart_{model.name}.mfr` in the task run directory.

### `restart_out`

**valid models**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- Check `io_nml:restart_write_mode` for supported options, so far only `"joint procs multifile"`.

### `output_streams`

**valid models**: atmosphere, ocean
<br>
**number of data pieces**: any
<br>
**actions**:

- Check that the number of output streams declared as `output_nml` sections matches the number of associated data pieces.
- Check that all output streams specify an output sub-directory using the combinaiton of `filename_format` and `output_filename` parameters (see in [ICON namelist overiew :lucide-external-link:](https://docs.icon-model.org/2026.04/documentation/buildrun/buildrun_namelist_overview.html#output-nml) ). For instance `filename_format = 'atm_mon/atm_mon_<datetime2>'` is a valid entry while `filename_format = 'atm_mon_<datetime2>'` isn't.
- Create these sub-directories in the task run directory.

### `finish_status`

**valid models**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**: None


### `dynamics_grid_file`

**valid models**: atmosphere, ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `grid_nml > dynamics_grid_filename`.

### `ifs2icon`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `initicon_nml > ifs2icon_filename`.

### `extpar_file`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `extpar_nml > extpar_filename`.

### `ecrad_data`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>

**actions**:

- If the associated data is generated, link it as `ecrad_data` in the task run directory.
- Reflect data path in `radiation_nml > ecrad_data_path`.

### `ghg_file`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `radiation_nml > ghg_filename` namelist parameter.

### `rrtmg_lw`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect the data path in `nwp_phy_nml > lrtm_filename` namelist parameter.

### `rrtmg_sw`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `rrtmg_sw.nc`.

### `bc_solar_sw`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `bc_solar_irradiance_sw_b14.nc`.

### `atm_plumes`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- Link associated data in the task run directory as `MACv2.0-SP_v1.nc`.

### `cloud_opt_props`

**valid models**: atmosphere
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it as `CldOptProps.nc` in the task run directory.
-  Reflect data path in `nwp_phy_nml > cldopt_filename`.

### `jsb_ifs`

**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

 - If the associated data is generated, link it in the task run directory with its original name.
 - Reflect data path in the `jsb_model_nml > ifs_filename`.

### `jsb_fract`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_model_nml > fract_filename`.

### `jsb_seb_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > bc_filename`.

### `jsb_seb_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_seb_nml > ic_filename`.

### `jsb_rad_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > bc_filename`.

### `jsb_rad_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_rad_nml > ic_filename`.

### `jsb_turb_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > bc_filename`.

### `jsb_turb_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_turb_nml > ic_filename`.

### `jsb_sse_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > bc_filename`.

### `jsb_sse_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_sse_nml > ic_filename`.

### `jsb_hydro_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > bc_filename`.

### `jsb_hydro_bc_sso`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml:bc > sso_filename`.

### `jsb_hydro_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hydro_nml > ic_filename`.

### `jsb_pheno_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > bc_filename`.

### `jsb_pheno_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_pheno_nml > ic_filename`.

### `jsb_disturb_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > bc_filename`.

### `jsb_disturb_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_disturb_nml > ic_filename`.

### `jsb_hd_bc`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > bc_filename`.

### `jsb_hd_ic`
**valid models**: land
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `jsb_hd_nml > ic_filename`.

### `ocean_inistate`
**valid models**: ocean
<br>
**number of data pieces**: 1
<br>
**actions**:

- If the associated data is generated, link it in the task run directory with its original name.
- Reflect data path in `ocean_initialConditions_nml > InitialState_InputFileName` namelist parameter.

## ICON task specifications

### `plugin`

**type**: string
<br>
**choices**: "icon"
<br>
**description**: plugin name
