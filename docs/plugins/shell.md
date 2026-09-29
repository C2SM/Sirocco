---
icon: simple/shell
title: Shell task
---

# Shell task plugin

The `shell` plugin allows users to bring their own set of files to specify the task content. These files can be of any nature: shell scripts, python code, a source tree to compile, configuration files, etc ... The only requirement is the ability to interface with the Sirocco workflow, i.e. use inputs and outputs as defined in the [`cycles` section](../../configuration/cycles).This is done through the `command` specification (see below)

A shell task description would typically look like this
```sirocco-yaml
tasks:
  - task name 1:
      plugin: shell
      nodes: 1
      walltime: 01:00:00
      src: relative/path/to/task_1_src
      command: ./task_1_src/myscript.sh {PORT::port_name_1} --kw_arg1={PORT::port_name_2}
```

## Specifications
