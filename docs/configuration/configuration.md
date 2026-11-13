---
title: Configuration
icon: lucide/folder-cog
---

## Definition

A Sirocco configuration is a directory containing the main `sirocco.yaml` file plus all the human readable files necessary to fully configure the workflow tasks, organized in arbitrary subdirectory structures. They can be software configuration files, source code, scripts, etc ...

The current chapter describes the YAML dialect in which this configuration is expressed. The terminology used will thus follow the YAML one when needed, typically for _sequences_ or _mappings_. YAML dialects can sometimes be difficult to read and/or learn because the dialect keywords don't stand out when looking at the file. That's why Sirocco keywords will be highlighted with a <span style="color: var(--md-code-hl-function-color);">specific color</span> throughout the documentation.

## Templating

Sirocco also supports templating of the `sirocco.yaml` file via `jinja` placeholders, e.g. `{{ MY_DATA_PATH }}`. They must be defined in the following way in a file named `sirocco_vars.yaml` placed at the root of the configuration directory (_i.e._ alongside `sirocco.yaml`):

```yaml title="sirocco_vars.yaml"
MY_DATA_PATH: "/path/to/somewhere"
```
A resulting `sirocco.resolved.yaml` will be dumped, reflecting the actual configuration used.

## Root level entries

The root level of `sirocco.yaml` reads like this:

```sirocco-yaml title="sirocco.yaml"
scheduler: slurm
front_depth: 2
cycles:
  [...]
tasks:
  [...]
data:
  [...]
parameters:
  [...]
```

### `scheduler`

**type**: string
<br>
**choices**: `"slurm"`
<br>
**required** for the standalone engine
<br>
**description**: scheduler running on the HPC system. So far, [SLURM :lucide-external-link:](https://slurm.schedmd.com/) is the only supported one.

#### Scheduler options

Task specifications describing requested resources are translated into options of the selected scheduler as follows.

| Task specification | SLURM |
|---|---|
| [`account`](tasks.md#account) | `#SBATCH --account` |
| [`partition`](tasks.md#partition) | `#SBATCH --partition` |
| [`walltime`](tasks.md#walltime) | `#SBATCH --time` |
| [`nodes`](tasks.md#nodes) | `#SBATCH --nodes` |
| [`gpus_per_node`](tasks.md#gpus_per_node) | `#SBATCH --gpus-per-node` |
| [`gres`](tasks.md#gres) | `#SBATCH --gres` |
| [`gres_flags`](tasks.md#gres_flags) | `#SBATCH --gres-flags` |
| [`uenv`](tasks.md#uenv) | `#SBATCH --uenv` (uenv machines only) |
| [`view`](tasks.md#view) | `#SBATCH --view` (uenv machines only) |
| [`procs_per_node`](tasks.md#procs_per_node) | not in the header, exported as `PROCS_PER_NODE`, typically for `srun --ntasks-per-node` |
| [`cores_per_proc`](tasks.md#cores_per_proc) | not in the header, exported as `CORES_PER_PROC`, typically for `srun --cpus-per-task` |

### `front_depth`

**type**: integer
<br>
**choices**: $d \geq 1$
<br>
**default**: `2`
<br>
**description**: depth of the submitted [task front](../orchestration.md#the-deep-propagating-task-front). Tasks with rank $r < d$ are submitted. A depth of `1` thus corresponds to only submitting tasks whose dependencies are satisfied, a depth of `2` corresponds to one generation of tasks submitted ahead-of-time, etc ...

### `cycles`

[Section](cycles.md) describing the **workflow graph**: the groups of potentially recurring tasks, their [inputs](../glossary.md#inputs) and [outputs](../glossary.md#outputs) and potential _wait on_ dependencies between tasks.

### `tasks`

[Section](tasks.md) specifying the tasks settings (scheduling info, scripts, config files, etc ...) but no graph related information which is the topic of `cycles` above.

### `data`

[Section](data.md) describing data settings (essentially paths). Same as the tasks section, it doesn't provide graph related information.

### `parameters`

Optional [section](parameters.md) describing parameters used to parameterize tasks and data. Typically for ensemble or sensitivity experiments.
