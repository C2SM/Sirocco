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

The shell scripts use [clm-tools](https://github.com/C2SM/clm-tools) (`cfu`, `ccaf2icaf`,
Python venv, SAMOVAR and `functions.sh`), which are taken from SPICE and licensed under
GPL-3.0-or-later. The `install` task clones the version given by `TOOLS_REPO` and
`TOOLS_VERSION` in `sirocco_vars.yaml` and builds it, so no SPICE installation is needed.
The compute nodes need outbound internet access for `git clone` and `pip install`.

## Running

1. Set `SIROCCO_VENV` (and, if needed, `ACCOUNT` and dates) in `sirocco_vars.yaml`.
2. `sirocco verify examples/spice-clm`
3. `sirocco start examples/spice-clm` (standalone engine; status with `sirocco stviz`)

Task run directories are created in `examples/spice-clm/run/`.

## Differences from SPICE

- Restarts use `joint procs multifile` (required by Sirocco) instead of a dedicated restart process.
- Output windows use `output_bounds` instead of absolute `output_start`/`output_end`,
  because Sirocco only updates the experiment dates per chunk.
- `init_latbc_from_fg = .false.` is set explicitly. The SPICE
  scripts never set the shell variable `init_mode`, which turns `init_latbc_from_fg` into `.true.` by mistake.
- Archives, time series, restart files and logs stay in the task run directories; SPICE moves
  them to separate directories and deletes the whole scratch directory at the end of the chain.
