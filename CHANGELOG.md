# Changelog

## v0.1.0 (YYYY-MM-DD)

First release of Sirocco.

### Workflow description

- YAML configuration (`sirocco.yaml`) with optional Jinja templating through `sirocco_vars.yaml`.
- Workflow graph made of task and data nodes, linked through named input and output ports.
- Recurring cycles, with `target_cycle` (relative lags, absolute and partially fixed dates) and `when` to target task and data instances.
- Explicit task dependencies with `wait_on`.
- Parameterized tasks and data with `single` and `all` targeting modes.

### Plugins

- `shell` plugin: user-provided files and command with port placeholders, custom separators and vanishing placeholders.
- `icon` plugin: interface to the ICON model with pre-defined ports, namelist handling, YAC coupling, CPU, GPU and hybrid executables, and MPI ranks allocation and mapping.

### Orchestration

- Deep propagating task front for ahead-of-time task submission.
- Standalone engine running autonomously on the HPC system, with the SLURM scheduler.
- AiiDA engine (under development).

### Command line interface

- Checking commands: `verify`, `represent`, `visualize`, `stviz`.
- Standalone controlling commands: `start`, `stop` (with cool-down mode), `restart`.
- AiiDA commands: `run`, `submit`.

### Requirements

- Python `>=3.12`.
