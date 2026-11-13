---
title: Tasks
icon: lucide/list-todo
---

# Tasks

The root level `tasks` section is where tasks are declared and their concrete content specified but no graph related information like input/output relationships which is the topic of the [`cycles`](cycles.md) section. Any task referred to in `cycles` must be declared here in `tasks`.

Even though most of the task content depends on the plugin and is described in the [corresponding section](../plugins/plugins.md), some specifications are common to all plugins and described here. This section is a sequence of mappings from task names to their content and typically looks like this:
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

When using the standalone engine, the task named `SIROCCO` hosts the specifications of the [Sirocco continuation task](../orchestration.md#standalone-engine), the job propagating the task front: it checks the current state of the workflow, submits new tasks as needed and dumps the new state to file. This task needs to find the `sirocco` executable in `PATH`, which can be achieved in different ways:

- through the virtual environment where it is installed via the [`venv`](#venv) specification (see below)
- provided by a [`uenv`](#uenv) in a certain [`view`](#view)
- readily available from a [fresh `bash` login](../orchestration.md#bash-shebang-and-base-environment), for instance installed system-wide or loaded from `.bashrc`

#### `venv`

**type**: string
<br>
**optional**
<br>
**description**: absolute path to the virtual environment to activate in order to have access to the `sirocco` CLI. For instance, if specifying `venv: /path/to/.venv`, `source /path/to/.venv/bin/activate` will be added to the continuation task run script so that it takes precedence over other methods for providing the `sirocco` executable.

## All tasks

### Specifications

Specifications describing requested resources are translated into options of the scheduler [selected for the workflow](configuration.md#scheduler). The correspondence is listed in [scheduler options](configuration.md#scheduler-options).

#### `plugin`

**type**: string
<br>
**required** except for `ROOT` and `SIROCCO` tasks which are identified by their special names.
<br>
**choices** : `"shell"` or `"icon"`
<br>
**description**: designates the type of task. The 2 supported [plugins](../plugins/plugins.md) for now are [shell](../plugins/shell.md) and [icon](../plugins/icon.md)

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
**description**: account charged for the resources used by the task. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `partition`

**type**: string
<br>
**optional**
<br>
**description**: partition (or queue) of the HPC system the task is submitted to. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `walltime`

**type**: string
<br>
**format** "HH:MM:SS"
<br>
**optional**
<br>
**description**: maximum run time requested for the task. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `nodes`

**type**: integer
<br>
**default**: 1
<br>
**description**: number of requested nodes. Set in the job header (see [scheduler options](configuration.md#scheduler-options)). When set, the corresponding environment variable `N_NODES` is exported in the submitted run script.

#### `sockets_per_node`

**type**: integer
<br>
**default**: 1
<br>
**description**: Number of physical coherent sub-units per node. On the Säntis machine, it maps to `--gpus-per-node`. The [ICON plugin](../plugins/icon.md) uses it to dispatch MPI processes on different [NUMA :lucide-external-link:](https://en.wikipedia.org/wiki/Non-uniform_memory_access) nodes.

#### `gpus_per_node`

**type**: integer
<br>
**optional**
<br>
**description**: number of requested GPU devices per node. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `gres`

**type**: string
<br>
**optional**
<br>
**schedulers**: SLURM only
<br>
**description**: generic resources requested for the task. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `gres_flags`

**type**: string
<br>
**optional**
<br>
**schedulers**: SLURM only
<br>
**description**: flags controlling how generic resources are allocated. Set in the job header (see [scheduler options](configuration.md#scheduler-options)).

#### `procs_per_node`

**type**: integer
<br>
**optional**
<br>
**description**: number of MPI processes per node. It is not set in the job header: when set, the corresponding environment variable `PROCS_PER_NODE` is exported in the submitted run script so that users can pass it to their MPI launcher (see [scheduler options](configuration.md#scheduler-options)).

#### `cores_per_proc`

**type**: integer
<br>
**optional**
<br>
**description**: number of cores per MPI process. It is not set in the job header: when set, the corresponding environment variable `CORES_PER_PROC` is exported in the submitted run script so that users can pass it to their MPI launcher (see [scheduler options](configuration.md#scheduler-options)).

#### `uenv`

**type**: string
<br>
**format**: Follow CSCS docs for [SLURM integration :lucide-external-link:](https://docs.cscs.ch/software/uenv/using/#slurm-integration)
<br>
**optional**
<br>
**description**: CSCS [user environment :lucide-external-link:](https://docs.cscs.ch/software/uenv). Set in the job header on machines supporting uenvs (see [scheduler options](configuration.md#scheduler-options)).

#### `view`

**type**: string
<br>
**optional**
<br>
**description**: CSCS user environment [view :lucide-external-link:](https://docs.cscs.ch/software/uenv/using/#views). Set in the job header on machines supporting uenvs (see [scheduler options](configuration.md#scheduler-options)).
