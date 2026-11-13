---
title: Tasks
icon: lucide/list-todo
---

# Tasks

The root level `tasks` section is where tasks are declared and their concrete content specified but no graph related information like input/output relationships which is the topic of the [`cycles`](../../configuration/cycles) section. Any task referred to in `cycles` must be declared here in `tasks`.

Even though most of the task content depends on the plugin and is described in the [corresponding section](../../plugins), some specifications are common to all plugins and described here. This section is a sequence of mappings from task names to their content and typically looks like this:
```sirocco-yaml
tasks:
  - ROOT:
      account: "my_cluster_account"
  - SIROCCO:
      walltime: "00:05:00"
      uenv: "sirocco/v0.1.0"
  - task name 1:
      plugin: "shell"
      [...]
  - task name 2:
      plugin: "icon"
      [...]
```

## The special `ROOT` task

If a task named `ROOT` is declared, its attributes are propagated as default for all the other tasks. This is typically useful to set scheduling task specifications like accounts, partitions, etc...

## The special `SIROCCO` task

When using the standalone orchestrator, the task named `SIROCCO` hosts specifications for the special Sirocco task whose role is to check the current state of the workflow, submit new task jobs as needed and dump the new state to file. This task needs to find the `sirocco` executable in `PATH`, which can be achieved in different ways:

- through the virtual environment where it is installed via the [`venv`](#venv) specification (see below)
- provided by a [`uenv`](#uenv) in a certain [`view`](#view)
- readily available from a [fresh `bash` login](../../orchestration/#bash-shebang-and-base-environment), for instance installed system-wide or loaded from `.bashrc`

#### `venv`

**type**: string
<br>
**optional**
<br>
**description**: absolute path to the virtual environment to activate in order to have access to the `sirocco` CLI. For instance, if specifying `venv: /path/to/.venv`, `source /path/to/.venv/bin/activate` will be added to the special Sirocco task run script so that it takes precedence over other methods for providing the `sirocco` executable.

## All tasks

### Specifications

#### `plugin`

**type**: string
<br>
**required** except for `ROOT` and `SIROCCO` tasks which are identified by their special names.
<br>
**choices** : `"shell"` or `"icon"`
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

#### `parameters`

**type**: sequence of strings
<br>
**optional**
<br>
**description**: Sequence of parameter names which must be declared in [the corresponding root level section](parameters.md#specifications). One instance is created for each combination of their values. See [Parameters](parameters.md) for how parameters are declared, attached and targeted across sections.

#### `account`

**type**: string
<br>
**optional**
<br>
**description**: account used on the HPC system. Equivalent to SLURM `--account`. Reflected in the run script header.

#### `partition`

**type**: string
<br>
**optional**
<br>
**description**: partition used on the HPC system. Equivalent to SLURM `--partition`. Reflected in the run script header.

#### `walltime`

**type**: string
<br>
**format** "HH:MM:SS"
<br>
**optional**
<br>
**description**: requested time for the task job. Equivalent to SLURM `--time`. Reflected in the run script header.

#### `nodes`

**type**: integer
<br>
**default**: 1
<br>
**description**: requested number of nodes on the HPC. Equivalent to SLURM `--nodes`. Reflected in the run script header. When set, the corresponding environment variable `N_NODES` is exported in the submitted run script.

#### `sockets_per_node`

**type**: integer
<br>
**default**: 1
<br>
**description**: Number of physical coherent sub-units per node. On the Säntis machine, it maps to `--gpus-per-node`. The [ICON plugin](../../plugins/icon) uses it to dispatch MPI processes on different [NUMA :lucide-external-link:](https://en.wikipedia.org/wiki/Non-uniform_memory_access) nodes.

#### `gpus_per_node`

**type**: integer
<br>
**optional**
<br>
**description**: Number of GPU devices per node. Equivalent to SLURM `--gpus-per-node`. Reflected in the run script header.

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
**description**: number of MPI processes per node. Equivalent to SLURM `--ntasks-per-node`. When set, the corresponding environment variable `PROCS_PER_NODE` is exported in the submitted run script so that users can specify it as the `srun --ntasks-per-node` option.

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
