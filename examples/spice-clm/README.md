# SPICE sp001 (gcm2icon) with Sirocco

ICON-CLM limited-area simulation over Germany (0.11 deg) driven by ERA5, reproducing the
SPICE experiment `sp001` on CSCS Alps (santis). ICON runs through the Sirocco ICON plugin;
all other steps are shell tasks.

## Workflow

| Task | Cycle | SPICE equivalent | Purpose |
|------|-------|------------------|---------|
| `install` | once | `config.sh` (build part) | clone and build [clm-tools](https://github.com/C2SM/clm-tools) |
| `prep` | monthly | `prep.job.sh` | untar ERA5 (3-hourly), unzip, `ccaf2icaf` |
| `init` | once | `conv2icon.job.sh` part I, links of `icon.job.sh` | initial state, remapping helpers, aerosol/ozone links |
| `conv2icon` | monthly | `conv2icon.job.sh` parts II, III | `LOWBC.nc` and lateral boundary files in `latbc/`; deletes the `prep` output |
| `icon` | monthly | `icon.job.sh` | ICON on 4 GPU nodes, restart from previous month |
| `arch` | monthly | `arch.job.sh` | completeness and SAMOVAR checks, CF correction, tar files |
| `post` | monthly | `post.job.sh` | monthly time series on the rotated lat/lon grid |
| `clean` | monthly | clean-up in `post.job.sh` | deletes boundary data, raw ICON output and corrected streams after a successful `post` |
| `post_yearly` | yearly | `post_yearly_cmor.sh` | yearly time series from the monthly ones (only with `ITYPE_TS` 2 or 3) |

The shell scripts use [clm-tools](https://github.com/C2SM/clm-tools) (`cfu`, `ccaf2icaf`,
Python venv, SAMOVAR and `functions.sh`), which are taken from SPICE and licensed under
GPL-3.0-or-later. The `install` task clones the version given by `TOOLS_REPO` and
`TOOLS_VERSION` in `sirocco_vars.yaml` and builds it, so no SPICE installation is needed.
The compute nodes need outbound internet access for `git clone` and `pip install`.

## Running

The monthly values of `allow_overcast` (`by_month`) require Sirocco with
[#269](https://github.com/C2SM/Sirocco/pull/269).

1. Set `SIROCCO_VENV` (and, if needed, `ACCOUNT` and dates) in `sirocco_vars.yaml`.
2. `sirocco verify examples/spice-clm`
3. `sirocco start examples/spice-clm` (standalone engine; status with `sirocco stviz`)

Task run directories are created in `examples/spice-clm/run/`.

## One-year case

`examples/spice-clm-1year` runs the same workflow from 2002-01-01 to 2003-01-01 and adds the
yearly time series. It contains only its own `sirocco_vars.yaml`; `sirocco.yaml`, `scripts/` and
`ICON/` are links to `spice-clm`. Run it with `sirocco start examples/spice-clm-1year`.

`ITYPE_TS` in `sirocco_vars.yaml` selects the time series as in SPICE:

| `ITYPE_TS` | Time series |
|------------|-------------|
| 1 | monthly (`spice-clm`) |
| 2 | yearly; `post_yearly` deletes the monthly ones it has combined |
| 3 | monthly and yearly (`spice-clm-1year`) |

With 2 or 3, the workflow gets a yearly cycle with one `post_yearly` task per year, which
starts when `post` has finished for all twelve months (and for December of the previous year).
`START_DATE` and `STOP_DATE` must therefore be on 1 January. The yearly time series are written
to `yearly/<variable>/<variable>_<first>-<last>.nc` in the run directory of `post_yearly`,
together with `HSURF` and `FR_LAND`. For instantaneous variables, the last time step of
December (1 January, 00 UTC) is moved to the next year; after the last year it is stored as
`<variable>_<YYYYMMDDHH>.nc`. With `ITYPE_TS: 2`, the time series of December are kept until
`post_yearly` of the following year has used them.

## Differences from SPICE

- Restarts use `joint procs multifile` (required by Sirocco) instead of a dedicated restart process.
- Output windows use `output_bounds` instead of absolute `output_start`/`output_end`,
  because Sirocco only updates the experiment dates per chunk. For the same reason, the time axis
  of the ICON output and of the time series refers to the start of the month
  (e.g. `minutes since 2002-2-1`), while SPICE refers to the start of the experiment
  (`minutes since 2002-1-1`). The dates are the same.
- Archives, time series, restart files and logs stay in the task run directories; SPICE moves
  them to separate directories and deletes the whole scratch directory at the end of the chain.
- The topography reference `z_mc-topography_c.nc` is an output of `arch` (`const/`); SPICE
  stores it with the time series in `post/`.
- The global attribute `icon-clm_version` is `ICON-2025.10`; SPICE rc-2.4.1 writes `ICON-2024.07`.
- The yearly time series are built by a separate `post_yearly` task, while SPICE builds them in the
  `post` job of the following January (or of the last month). Partial years are not supported.
  With `ITYPE_TS: 2`, SPICE keeps the time series of all Januaries and Decembers until the end of
  the run.

## Comparison with SPICE

The reference is [SPICE](https://clm-community.gitlab-pages.dkrz.de/public/spice/) rc-2.4.1
(commit `383eb47`) with `src/patches/icon-2025.10.patch` applied, configured with
`config.sh -s cscs-alps` and run for 2002-01 and 2002-02 with ICON 2025.10. To obtain
identical results, the generated SPICE experiment was adapted as follows:

- `job_settings`: `ICONDIR` points to the ICON 2025.10 build and `NUM_RESTART_PROCS=0`.
- `scripts/icon.job.sh`: ICON is launched with the run scripts of the Sirocco ICON plugin
  (16 compute ranks, 1 output and 1 prefetch rank).
- `subchain`: the ICON job requests `--gpus-per-node=4`.

All data variables of the archived ICON output (3550 files), the monthly time series
(296 files) and the constant fields (5 files) are bit-identical.

`scale_cdnc_mode` must be `0` for historical runs before 2015, as set by SPICE rc-2.4.1.
Earlier versions of the SPICE ICON 2025.10 patch set it to `1`. Like any other change of the
model configuration, this alters the results at almost every grid point within the first month
(e.g. 2 m temperature by up to 7 K), because small perturbations grow in the chaotic flow. The
monthly mean over the domain changes only little (2 m temperature by 0.003 K in January 2002).

To repeat the comparison, run (with a Python that provides `netCDF4` and `cftime`, e.g. the
venv of clm-tools):

```bash
PYTHON=/path/to/clm-tools/venv/bin/python examples/spice-clm/compare/compare_runs.sh \
  /path/to/spice/chain examples/spice-clm/run /path/to/scratch/dir 2002-01 2002-02
```

`compare/compare_nc.py` compares two directory trees of netCDF files variable by variable. It
compares time axes as dates and ignores global attributes that record the host or software
history and the ICON version label.

### Known issues in SPICE rc-2.4.1

- The shell variable `init_mode` is not set in `icon.job.sh`, so `init_latbc_from_fg` is taken
  from the last entry of `init_latbc_from_fg_options` and becomes `.true.` for every `init_mode`,
  contrary to the comment in the script. `.true.` is the intended setting: at a cold start, ICON
  then takes the lateral boundary data at the start date from the initial state instead of a
  separately remapped boundary file, so both are consistent.
- `GA_ICON_CLM_VERSION` in `job_settings` is `ICON-2024.07`, also when ICON 2025.10 is used.
- The last `post` job of a chain exits with status 1, although all steps succeed, because it
  tries to remove the file `finish_message`, which only exists if `NOTIFICATION_ADDRESS` is set.
- `subchain start` fails when called inside a uenv session, since `sbatch` refuses to run there.
