# ICON-ESM R02B08 with carbon cycle

Coupled ICON-ESM experiment at R02B08 on CSCS Alps (santis), translated from
`jcp/run_scripts/santis/jcp_r2b8_icon4py.run`:

| Component | Target | Ranks |
|-----------|--------|-------|
| atmosphere (AES) and land (JSBACH) | GPU, dynamical core through ICON4Py (`icon4py_mode` substitute) | 4 per node |
| ocean and biogeochemistry (HAMOCC) | CPU | 100 per node |

Atmosphere and ocean exchange fields through YAC every 12 minutes (`ICON/coupling.yaml`).
The workflow runs on 16 nodes with monthly cycles from 2020-01-01 to 2020-06-01; each cycle
restarts from the atmosphere and ocean restarts of the previous one.

## Executables

Both executables are built from the `jcp_on_santis_icon4py_spack` branch of icon-mpim with
`jcp/build_scripts/build.sh` (Spack, uenv `icon/26.7:v1`):

| Path in `sirocco_vars.yaml` | Variant | Spack environment |
|-----------------------------|---------|-------------------|
| `ICON_GPU_BUILD` | `icon4py-gpu` | `santis_icon4py_double` |
| `ICON_CPU_BUILD` | `icon4py-cpu` | `santis_cpu_double_py_substitute` |

Both environments must contain `+rte-rrtmgp` (configure option `--enable-rte-rrtmgp`).
Without it, the AES radiative transfer is compiled out without any warning or error and the
model runs without radiation. To check an existing build:

```
grep -o -- '--[a-z]*able-rte-rrtmgp' <build>/config.log | sort -u
```

## Running

1. Adapt `sirocco_vars.yaml` (account, Python environment, ICON builds, input and cache directories).
2. `sirocco verify tests/cases/esm_carbon/config`
3. `sirocco start tests/cases/esm_carbon/config`

Notes:

- `sbatch`, and therefore `sirocco start`, fails if a uenv is loaded in the submitting shell.
  Submit from a shell without a uenv.
- The ICON task declares ports for all files that AES reads under fixed names
  (`bc_land_*.nc`, `bc_sst.nc`, `bc_sic.nc`), including those read only at a cold start.
- The `SIROCCO` task is required so that the continuation jobs receive the Slurm account.
- The GT4Py, CuPy and CUDA JIT caches are placed under `CACHE_DIR` on iopsstor. They consist of
  many small files (inode quota on capstor), and concurrent writes of all GPU ranks to the
  default CUDA cache in the home directory stall the model.
- The first run with an empty GT4Py cache compiles the ICON4Py stencils, which can take more
  than 30 minutes before the first time step.
- `runtime/main.sh` adds a hang watchdog: if the job log does not grow for `WATCHDOG_TIMEOUT`
  seconds (`WATCHDOG_INIT_TIMEOUT` before the first time step), it collects node, GPU, network
  and gdb diagnostics in `diag.<jobid>/` and aborts the model step.
- The `cxil_map: write error` messages of some ocean ranks during initialization are harmless.

## Verified setups

Tested on 2026-10-19 with shortened cycles (`stop_date` 2020-01-03, 16 nodes, filled GT4Py cache):

| Setup | Cycles | Runtime per cycle | Result |
|-------|--------|-------------------|--------|
| smoke test (`period: P1D`) | cold start 2020-01-01, restart 2020-01-02 | 24 min | completed |
| continuous (`period: P2D`) | cold start 2020-01-01 | 39 min | completed |

- Radiation is active: the global mean outgoing longwave radiation on day 2 is 247 W/m².
- Runs are bit-reproducible: the first cycle of the smoke test and the continuous run are
  identical over the first day.
- Restarts are not bit-identical: the restarted cycle departs from the continuous run in the
  last digits a few steps after the restart, and the difference then grows over time.
