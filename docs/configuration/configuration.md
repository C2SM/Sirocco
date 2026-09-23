---
title: Configuration
icon: lucide/folder-cog
---

## Definition

A Sirocco configuration is a directory containing the main `sirocco.yaml` file plus all the human readable files necessary to fully configure the workflow tasks, organized in arbitrary subdirectory structures. They can be software configration files, source code, scripts, etc ...

The current chapter describes the YAML dialect in which this configuration is expressed. The terminology used will thus follow the YAML one when needed, typically for _sequences_ or _mappings_. YAML dialects can sometimes be difficult to read and/or learn because the dialect keywords don't stand out when looking at a the file. That's why Sirocco keywords will be highlighted with a <span style="color: var(--sirocco-kw-color);">specific color</span> throughout the documentation.

## Templating

Sirocco also supports templating of the `sirocco.yaml` file via `jinja` placeholders, e.g. `{{ MY_DATA_PATH }}`, They must be defined in the following way in a file named `vars.yaml` placed at the root of the configuration directory (_i.e._ alongside `sirocco.yaml`):

```yaml title="vars.yaml"
MY_DATA_PATH: "/path/to/somewhere"
```
A resulting `sirocco_resolved.yaml` will be dumped, reflecting the actual configuration used.

## Root level entries

The root level of `sirocco.yaml` reads like this:

```yaml
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
**possible values**: `"slurm"`
<br>
**required** for the standalone engine
<br>
**description**: scheduler running on the HPC system. So far, [SLURM :lucide-external-link:](https://slurm.schedmd.com/) is the only supported one.

### `front_depth`

**type**: integer
<br>
**possible values**: $\geq 1$
<br>
**default**: `2`
<br>
**description**: depth of the submitted [task front](../glossary#task-front). Tasks with rank $r < n$ are submitted. A depth of `1` thus corresponds to only the currently active tasks, a depth of `2` corresponds to one generation of tasks submitted ahead of time, etc ...

### `cycles: [...]`

[Section](../configuration/cycles) describing the **workflow graph**: the groups of potentially recurring tasks, their [inputs](../glossary#inputs) and [outputs]((../glossary#outputs)) and potential _wait on_ depdencies between tasks.

### `tasks: [...]`

[Section](../configuration/tasks) specifying the tasks setings (scheduling info, scripts, config files, etc ...) but no graph related information which is the topic of `cycles` above.

### `data: [...]`

[Section](../configuration/data) describing data settings (essentially paths). Same as the tasks section, it doesn't provide grah related information.

### `parameters: [...]`

Optional [section](../configuration/data) descibing parameters used to parametrize tasks and data. Typically for ensemble or sensitivity experiments.
