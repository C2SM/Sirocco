# Changes for 90-second ICON Test Simulation

Adaptations made to run a short 90-second test instead of the original 18-hour simulation.

## config.yaml

- **Simulation period**: Changed from 18h to 90 seconds (`stop_date: '2020-01-20T00:01:30'`)
- **Cycle period**: Changed from `PT6H` to `PT30S`
- **SLURM resources**:
  - `ntasks_per_node: 3` (2 compute + 1 I/O)
  - `mem: 240000` MB (needed for R02B06 grid)
- **num_io_procs**: Set to 1 (required - synchronous I/O has a known bug in ICON)

## icon_master.namelist

- `checkpointTimeIntval`: Changed to `"PT30S"`
- `restartTimeIntval`: Changed to `"PT30S"`
- `experimentStopDate`: Changed to `"2020-01-20T00:01:30"`

## NAMELIST_DYAMOND_R02B06L120

- `num_io_procs = 1` (must be >= 1)
- `divdamp_order = 4` (reduced from 24; required when `dt_checkpoint < 2.5h`)
- Physics call frequencies adapted:
  - `dt_conv = 30`, `dt_ccov = 30`
  - `dt_sso = 60`, `dt_gwd = 60`, `dt_rad = 60`
- Accumulation intervals: `precip_interval`, `runoff_interval`, `maxt_interval`, `melt_interval` set to `"PT30S"`
- Output settings: `output_interval`, `file_interval` set to `"PT30S"`
