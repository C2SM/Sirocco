---
icon: custom_icons/icosahedron
title: ICON task
---

# ICON task

The ICON plugin is designed as an interface between the Sirocco workflow and the [ICON model :lucide-external-link:](icon-model.org). It will generate the final run scripts executed on the system based on specifications like namelists, MPI ranks distribution, hardware properties, etc ...

## Example
```sirocco-yaml
cycles:
  - dayly:
      [...]
      tasks:
        - icon:
            components:
              master:
                inputs:
                  squash: [icon_squash]
                  link_content: [icon_link_input]
                  link: [lctlib_nlct21]
              atmo:
                inputs:
                  # TODO: ecrad or rrtmg ? => Probably remove rrtmg
                  ecrad_data: [ecrad_data]
                  cloud_opt_props: [ECHAM6_CldOptProps]
                  rrtmg_lw: [rrtmg_lw]
                  rrtmg_sw: [rrtmg_sw]
                  dynamics_grid_file: [atmo_grid]
                  extpar_file: [extpar_file]
                  ifs2icon: [analysis_file]
                  bc_solar_sw: [solar_irradiance]
                  restart_file:
                    - restart_atm:
                        when:
                          after: *root_start_date
                        target_cycle:
                          lag: -P1D
                outputs:
                  latest_restart_file: [restart_atm]
                  output_streams: [atm_mon, atm_mon2d]
              ocean:
                inputs:
                  dynamics_grid_file: [ocean_grid]
                  ocean_inistate: [ocean_init_state]
                  restart_file:
                    - restart_ocean:
                        when:
                          after: *root_start_date
                        target_cycle:
                          lag: -P1D
                outputs:
                  latest_restart_file: [restart_ocean]
                  output_streams: [oce_3h, oce_day]
              land:
                inputs:
                  jsb_ifs: [analysis_file]
                  jsb_fract: [land_frac]
                  jsb_hd_bc: [bc_land_hd]
                  jsb_hd_ic: [ic_land_hd]
                  jsb_seb_bc: [bc_land_phys]
                  jsb_seb_ic: [ic_land_soil]
                  jsb_rad_bc: [bc_land_phys]
                  jsb_rad_ic: [ic_land_soil]
                  jsb_turb_bc: [bc_land_phys]
                  jsb_turb_ic: [ic_land_soil]
                  jsb_sse_bc: [bc_land_soil]
                  jsb_sse_ic: [ic_land_soil]
                  jsb_hydro_bc: [bc_land_soil]
                  jsb_hydro_ic: [ic_land_soil]
                  jsb_hydro_bc_sso: [bc_land_sso]
                  jsb_pheno_bc: [bc_land_phys]
                  jsb_pheno_ic: [ic_land_soil]
                  jsb_disturb_bc: [bc_land_phys]
                  jsb_disturb_ic: [ic_land_soil]
tasks:
  - icon:
      plugin: icon
      uenv: "icon-dsl/25.12:2454892898"
      view: "default"
      squash_mount: [ICON_MOUNT]
      partition: tier0
      walltime: 01:00:00
      nodes: 4
      sockets_per_node: 4
      procs_per_node: 24
      cores_per_proc: 12
      exe:
        gpu:
          compute_procs_per_node: 4
          path: "/path/to/icon_gpu"
          icon4py_venv: "/path/to/icon4py/venv"
          gt4py_build_cache_dir: "../.."
          procs:
            atmo:
              compute_weight: 1
              streams: 2
        cpu:
          path: "/path/to/icon_cpu"
          procs:
            ocean:
              compute_weight: 1
              streams: 2
      namelists:
        - ./ICON/icon_master.namelist
        - ./ICON/NAMELIST_R02B07-R02B07_control_1979_atmo:
            parallel_nml:
              nblocks_e: 1
              nproma_sub: 10469
              io_proc_chunk_size: 12
            run_nml:
              modelTimeStep: "PT20S"
              num_lev: 120
              msg_level: 10
            io_nml:
              restart_write_mode: "joint procs multifile"
            nwp_phy_nml:
              dt_rad: 600
              dt_conv: 20
              dt_sso: 20
              dt_gwd: 20
            radiation_nml:
              ecrad_isolver: 2
        - ./ICON/NAMELIST_R02B07-R02B07_control_1979_ocean:
            parallel_nml:
              nproma: 16
            run_nml:
              modelTimeStep: "PT2M"
        - ./ICON/NAMELIST_R02B07-R02B07_control_1979_land
      yac_coupling: "ICON/coupling.yaml"
```

## Components
As mentioned in the [cycles](../../configuration/cycles#tasks) section,

## Ports

## ICON task specifications

### `plugin`

**type**: string
<br>
**choices**: "icon"
<br>
**description**: plugin name
