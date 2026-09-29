---
title: Tasks
icon: lucide/list-todo
---

# Tasks

The root level `tasks` section is where tasks are declared and their concrete content specified. Even though most of it depends on the task plugin and is described in the [corresponding section](../../plugins), some specifications are common to all plugins and described here. This section is a list mapping task names to their content specification and typically looks like this:
```sirocco-yaml
tasks:
  - ROOT:
      account: "my_cluster_account"
  - SIROCCO:
      walltime: 00:05:00
      uenv: "sirocco/v0.1.0"
  - task name 1:
      plugin: "shell"
      [...]
  - task name 2:
      plugin: "icon"
      [...]
```

## The special `ROOT` task

If a task named `ROOT` is declared, it's attributes are propagated as default for all the other tasks. This is typically useful to set scheduling task specifications like accounts, partitions, etc...

## The special `SIROCCO` task

When using the standalone orchestrator, the task named `SIROCCO` hosts specifications for the special Sirocco task who's role is to check the current state of the workflow and submit new task jobs as needed.

### Specifications

#### `venv`

**type**: string
<br>
**optional**
<br>
**description**: absolute path to the virtual environment to activate in order to have access to the `sirocco` CLI. For instance, if specifying `venv: /pat/to/.venv`, `source /pat/to/.venv/bin/activate` will be added to the special Sirocco task run script.

## All tasks

### Specifications

#### `plugin`

**type**: string
<br>
**required** except for `ROOT` and `SIROCCO` tasks which are identified by their special names.
<br>
**values** `"shell"` or `"icon"`
<br>
**description**: designates the type of task. The 2 supported [plugins](../../plugins) for now are [shell](../../plugins/shell) and [icon](../../plugins/icon)


#### `computer`

**type**: string
<br>
**required**
<br>
**description**: used to identify the HPC system and potentially set default values.

!!! warning "soon changing"

    `computer` should be specified at the root workflow level and not per task as all tasks are expected to run on the same HPC system.

#### `account`

**type**: string
<br>
**optional**
<br>
**description**: account used on the HPC system. Equivalent of SLURM `--account`. Reflected in the run script header.

#### `partition`

**type**: string
<br>
**optional**
<br>
**description**: partition used on the HPC system. Equivalent of SLURM `--partition`. Reflected in the run script header.

#### `walltime`

**type**: string
<br>
**format** "HH:MM:SS"
<br>
**optional**
<br>
**description**: requested time for the task job. Equivalent of SLURM `--time`. Reflected in the run script header.

#### `nodes`

**type**: integer
<br>
**default**: 1
<br>
**description**: requested number of nodes on the HPC. Equivalent of SLURM `--nodes`. Reflected in the run script header. When set, the corresponding environment variable `N_NODES` is exported in the submitted run script.

#### `sockets_per_nodes`

**type**: integer
<br>
**default**: 1
<br>
**description**: Number of physical coherent sub-units per node. On the Säntis machine, it maps to `--gpus-per-node`. The ICON plugin uses it to dispatch MPI processes on different [NUMA :lucide-external-link:](https://en.wikipedia.org/wiki/Non-uniform_memory_access) nodes.

#### `gpus-per-node`

**type**: integer
<br>
**optional**
<br>
**description**: Number of GPU devices per node. Equivalent of SLURM `--gpus-per-node`. Reflected in the run script header.

#### `gres`

**type**: string
<br>
**optional**
<br>
**description**: SLURM `--gres` option. Reflected in the run script header.

#### `gres_flags`

**type**: string
<br>
**optional**
<br>
**description**: SLURM `--gres-flags` option. Reflected in the run script header.

#### `procs_per_node`

**type**: integer
<br>
**optional**
<br>
**description**: number of MPI processes per node. Equivalent of SLURM `--ntasks-per-node`. When set, the corresponding environment variable `PROCS_PER_NODE` is exported in the submitted run script so that users can specify it as the `srun --ntasks-per-node` option.

#### `cores_per_proc`

**type**: integer
<br>
**optional**
<br>
**description**: number of cores per MPI process. When set, the corresponding environment variable `CORES_PER_PROC` is exported in the submitted run script so that users can specify it as the `srun --cpus-per-task` option.

#### `uenv`

**type**: string
<br>
**format**: Follow CSCS docs for [SLURM integration :lucide-external-link:](https://docs.cscs.ch/software/uenv/using/#slurm-integration)
<br>
**optional**
<br>
**description**: CSCS [user environment :lucide-external-link:](https://docs.cscs.ch/software/uenv).

#### `view`

**type**: string
<br>
**optional**
<br>
**description**: CSCS user environment [view :lucide-external-link:](https://docs.cscs.ch/software/uenv/using/#views)
