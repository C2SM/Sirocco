---
icon: simple/shell
title: Shell task
---

# Shell task plugin

The `shell` plugin allows users to bring their own set of files to specify the task content. These files can be of any nature: shell scripts, python code, a source tree to compile, configuration files, etc ... The only requirement is the ability to interface with the Sirocco workflow, _i.e_. use inputs and outputs as defined in the [`cycles` section](../../configuration/cycles).This is done through the `command` specification (see below).

A shell task description would typically look like this
```sirocco-yaml
tasks:
  - task name 1:
      plugin: shell
      [...]
      src: relative/path/to/task_1_src
      command: ./task_1_src/myscript.sh {PORT::port_name_1} --kw_arg1={PORT::port_name_2}
```

## Specifications

### `plugin`

**type**: string
<br>
**choices**: "shell"
<br>
**description**: plugin name

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
**description**: Main entry point of the task. It will typically execute one of the files provided in the `src` path but can also point to executables installed on the system. `command` is also the place where a _shell_ task integrates with the rest of the workflow through port placeholders. See below.

#### Port resolution

##### Placeholders

In the `cycles` root section of the configuration file, ports have been defined for each task that formally map inputs and outputs to a particular role for that task execution. Still, this piece of information is not yet explicit for the task itself and this is where port _placeholders_ come into play. As shown in the example above, `command` strings can contain placeholders like formatted as `{PORT::port_name}`. They will be resolved by substituting the path of the data instance(s) corresponding to `port_name` in the workflow graph.

##### Separators

If the port links several data instances, their path will by default be separated with a space. This is intended for the most common case which is positional arguments like in the following example.

```sirocco-yaml
tasks:
  - task name 1:
      plugin: shell
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
      plugin: shell
      [...]
      command: my_command --opt-arg={PORT[sep=,]::port_name}
```

will resolve the command as

``` shell
my_command --opt_arg=data_path_1,data_path_2,...
```

```sirocco-yaml
tasks:
  - task name 1:
      plugin: shell
      [...]
      command: my_coammnd --repeat-arg={PORT[sep= --repeat-arg=]::port_name}
```

will resolve the command as

``` shell
my_coammnd --repeat-arg=data_path_1 --repeat-arg=data_path_2 ...
```


##### Vanishing placeholders

For some instances of a task, typically the first or last one, it can also necessary that port placeholders vanish. In a Sirocco workflow, this happens when using the [`when` keyword](../../configuration/cycles#when). In such a case the port placeholder can be made vanishing by using extra brackets surrounding the entire part of the command that needs to be ignored if the port does not link any data. For isntance
```
my_command {PORT::port_name_1} [--opt-arg {PORT::port_name_2}]
```
might either be resolved as `my_command data_path_1 --opt-arg data_path_2` or `my_command data_path_1` depending if `port_name_2` links data or not.
