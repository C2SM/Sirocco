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
[#269](https://github.com/C2SM/Sirocco/pull/269). Results identical to SPICE beyond the first
two months also require [#273](https://github.com/C2SM/Sirocco/pull/273), which keeps
`experimentStartDate` at the start of the experiment (see
[Experiment start date](#experiment-start-date)).

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

### Results of the one-year run

The one-year case was run twice on santis for 2002 with `ITYPE_TS: 3`: first with Sirocco
without [#273](https://github.com/C2SM/Sirocco/pull/273), then with #273. The first run
completed in about 6 hours 50 minutes, including queue waiting time. The second run took about
5 hours 10 minutes, not counting an interruption of about 4 hours, during which Slurm cancelled
all new jobs because of an exceeded file quota on scratch. The tasks of different months run in
parallel as far as their dependencies allow; the longest task runtimes were:

| Task | First run | Second run | Walltime |
|------|-----------|------------|----------|
| `prep` | 13:25 | 17:17 | 30:00 |
| `conv2icon` | 1:37 | 1:23 | 30:00 |
| `icon` | 19:39 | 18:47 | 30:00 |
| `arch` | 7:46 | 6:05 | 30:00 |
| `post` | 2:57 | 4:22 | 1:00:00 |
| `clean` | 0:13 | 0:09 | 15:00 |
| `post_yearly` | 0:46 | 0:19 | 2:00:00 |

All `prep` jobs start at once and several of them share a node, which slows them down; with a
walltime of 15 minutes, `prep` of July 2002 timed out in an earlier attempt.

`post_yearly` produced 148 yearly time series (17 GB), the same number as the monthly time
series, plus `HSURF` and `FR_LAND` and 115 files with the time step 2003-01-01 00 UTC of the
instantaneous variables. For all 148 variables, the time steps and values of the yearly time
series are identical to those of the twelve monthly ones (e.g. 2920 three-hourly steps of
`T_2M`).

The second run is identical to SPICE for the whole year (see
[Comparison with SPICE](#comparison-with-spice)): all archived ICON output files (21910), monthly
time series (1776), constant fields (5) and yearly output files (265). The results of the first
run differ from SPICE from March on (see [Experiment start date](#experiment-start-date)). The
combination with December of the previous year (runs of more than one year) and the deletion with
`ITYPE_TS: 2` have not yet been tested with a full run.

## Differences from SPICE

- Restarts use `joint procs multifile` (required by Sirocco) instead of a dedicated restart process.
- Output windows use `output_bounds` instead of absolute `output_start`/`output_end`,
  because Sirocco only updates the experiment dates per chunk. With
  [#273](https://github.com/C2SM/Sirocco/pull/273), the time axis of the ICON output and of the
  time series refers to the start of the experiment (e.g. `seconds since 2002-1-1`), as in
  SPICE. Without it, the time axis refers to the start of the month
  (e.g. `seconds since 2002-3-1`); the dates are the same.
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
`config.sh -s cscs-alps` and run with ICON 2025.10 for 2002-01 and 2002-02 and, with
`ITYPE_TS=3` (monthly and yearly time series), for the whole year 2002. To obtain identical
results, the generated SPICE experiment was adapted as follows:

- `job_settings`: `ICONDIR` points to the ICON 2025.10 build and `NUM_RESTART_PROCS=0`.
- `scripts/icon.job.sh`: ICON is launched with the run scripts of the Sirocco ICON plugin
  (16 compute ranks, 1 output and 1 prefetch rank).
- `subchain`: the ICON job requests `--gpus-per-node=4`.

For 2002-01 and 2002-02, all archived ICON output files (3550), monthly time series (296) and
constant fields (5) are identical: all values, data types, dimensions, attributes and the
storage layout agree. For the whole year 2002 with
[#273](https://github.com/C2SM/Sirocco/pull/273), all archived ICON output files (21910),
monthly time series (1776), constant fields (5) and yearly output files of `post_yearly` (265)
are identical.

`scale_cdnc_mode` must be `0` for historical runs before 2015, as set by SPICE rc-2.4.1.
Earlier versions of the SPICE ICON 2025.10 patch set it to `1`. Like any other change of the
model configuration, this alters the results at almost every grid point within the first month
(e.g. 2 m temperature by up to 7 K), because small perturbations grow in the chaotic flow. The
monthly mean over the domain changes only little (2 m temperature by 0.003 K in January 2002).

### Experiment start date

SPICE keeps `experimentStartDate` at the start of the experiment (2002-01-01) in every month.
Sirocco without [#273](https://github.com/C2SM/Sirocco/pull/273) sets it to the start of each
month. In the one-year comparison, this was the only reason for differing results:

- January and February 2002 are identical in both cases.
- Without #273, the results differ from 2002-03-02 06 UTC on. The differences first appear
  in the outermost rows of the lateral boundary zone and then spread over the whole domain.
- The lateral and lower boundary data of March, produced again with the scripts of SPICE and
  of this example, are identical (250 files). Repeated ICON runs reproduce the results of both
  SPICE and Sirocco exactly, so the difference is systematic.
- With `experimentStartDate` set to 2002-01-01 and otherwise unchanged input, the Sirocco run
  of March is identical to SPICE at all 744 hourly output times.

ICON thus depends on the time elapsed since `experimentStartDate` in a way that only shows up
at some boundary update times; the exact mechanism in ICON was not investigated. With #273,
the whole year 2002 is identical to SPICE.

To repeat the comparison, run (with a Python that provides `netCDF4` and `cftime`, e.g. the
venv of clm-tools):

```bash
PYTHON=/path/to/clm-tools/venv/bin/python examples/spice-clm/compare/compare_runs.sh \
  /path/to/spice/chain examples/spice-clm/run /path/to/scratch/dir 2002-01 2002-02
```

For the one-year case, list all months and compare the yearly output separately:

```bash
/path/to/clm-tools/venv/bin/python examples/spice-clm/compare/compare_nc.py \
  /path/to/spice/chain/work/sp001/post/yearly \
  examples/spice-clm-1year/run/post_yearly__date_2002-01-01T00-00-00/yearly
```

`compare/compare_nc.py` compares two directory trees of netCDF files variable by variable: the
file format, the global attributes and, for each variable, the data type, dimensions,
attributes, compression, chunking and all values, which must be exactly equal. It compares time
axes as dates and ignores global attributes that record the host or software history and the
ICON version label. File sizes are not compared, since these attributes differ in length.

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
