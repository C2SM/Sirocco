---
icon: simple/shell
title: Shell task
---

# Shell task

The `shell` plugin allows users to bring their own set of files to specify the task content. These files can be of any nature: shell scripts, python code, a source tree to compile, configuration files, etc ... The only requirement is the ability to interface with the Sirocco workflow, _i.e._ use inputs and outputs as defined in the [`cycles` section](../configuration/cycles.md). This is done through the `command` specification (see below).

A shell task description would typically look like this
```sirocco-yaml
tasks:
  - task name 1:
      plugin: shell
      [...]
      src: relative/path/to/task_1_src
      command: ./task_1_src/myscript.sh {PORT::port_name_1} --kw_arg1={PORT::port_name_2}
```

## Ports

For _shell_ tasks, the choice of port names is entirely free. The concrete link when building the actual task happens through the [port resolution](#port-resolution) of the task `command`.

## Task specifications

The following shell task specifications come on top of [the ones used for any task](../configuration/tasks.md#all-tasks).

### `src`

**type**: string
<br>
**optional**
<br>
**description**: Path relative to the configuration directory where files required for the task are stored. Be it a file or a directory, it will be copied with the same name in the task run directory.


### `command`

**type**: string
<br>
**required**
<br>
**description**: Main entry point of the task. It will typically execute one of the files provided in the `src` path but can also point to executables installed on the system. `command` is also the place where a _shell_ task integrates with the rest of the workflow through port placeholders: the resolved command (see below) will be included in the submitted run script.

#### Port resolution

##### Placeholders

In the `cycles` root section of the configuration file, ports have been defined for each task that formally map inputs and outputs to a particular role for that task execution. Still, this piece of information is not yet explicit for the task itself and this is where port _placeholders_ come into play. As shown in the example above, `command` strings can contain placeholders formatted as `{PORT::port_name}`. They will be resolved by substituting the path of the data instance(s) corresponding to `port_name` in the workflow graph.

##### Separators

If the port links several data instances, their path will by default be separated with a space. This is intended for the most common case which is positional arguments like in the following example.

```sirocco-yaml
tasks:
  - task name 1:
      [...]
      command: my_command {PORT::port_name}
```

will resolve the command as

``` shell
my_command data_path_1 data_path_2 ...
```


Still, the separator can be specified in the placeholder with the syntax `{PORT[sep=...]::port_name}`. The following 2 examples are common use cases.

```sirocco-yaml
tasks:
  - task name 1:
      [...]
      command: my_command --opt-arg={PORT[sep=,]::port_name}
```

will resolve the command as

``` shell
my_command --opt-arg=data_path_1,data_path_2,...
```

```sirocco-yaml
tasks:
  - task name 1:
      [...]
      command: my_command --repeat-arg={PORT[sep= --repeat-arg=]::port_name}  # (1)!
```

1. Note the space at the beginning of the separator. Otherwise arguments will not be separated.

will resolve the command as

``` shell
my_command --repeat-arg=data_path_1 --repeat-arg=data_path_2 ...
```


##### Vanishing placeholders

For some instances of a task, typically the first or last one of a cycle, it can also be necessary that port placeholders vanish. In a Sirocco workflow, this happens when using the [`when` keyword](../configuration/cycles.md#when). In such a case the port placeholder can be made vanishing by using extra brackets surrounding the entire part of the command that needs to be ignored if the port does not link any data. For instance
```sirocco-yaml
tasks:
  - task name 1:
      [...]
      command: my_command {PORT::port_name_1} [--opt-arg {PORT::port_name_2}]
```

might either be resolved as

```my_command data_path_1 --opt-arg data_path_2```

or

```my_command data_path_1```

depending on whether `port_name_2` links data or not.

## Exported variables

Before invoking the task `command` in the submitted run script, some environment variables are exported to provide context information or propagate information provided in the configuration file.

- `SIROCCO_START_DATE` and `SIROCCO_STOP_DATE`: start and stop date of the [current cycle occurrence](../configuration/cycles.md#cycling)
- `N_NODES`: if provided, [number of requested nodes](../configuration/tasks.md#nodes)
- `PROCS_PER_NODE`: if provided, [number of requested MPI processes per node](../configuration/tasks.md#procs_per_node)
- `CORES_PER_PROC`: if provided, [number of requested cores per MPI process](../configuration/tasks.md#cores_per_proc)

!!! failure "Parameter values not exported yet"

    The values of the [parameters](../configuration/parameters.md) of a task instance are not exported yet, so that tasks cannot adapt their behavior to them for now.
