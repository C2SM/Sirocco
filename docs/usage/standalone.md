---
title: Standalone
icon: lucide/server
---

## :lucide-layers: Requirements

To install and run Sirocco in standalone mode, you will need an HPC system

- using the [SLURM :lucide-external-link:](https://slurm.schedmd.com/) scheduler, for now the only supported one
- with Python `>=3.10` installed

## :lucide-arrow-down-to-line: Install

Sirocco is a pure Python package available on PyPI. Directly install it on the HPC system (no root access required)
```shell
[uv] pip install Sirocco
```

It's recommended to install it in a virtual environment that will later be specified in the workflow configuration.

Sirocco is ran through a command line interface. Check your installation with
```shell
sirocco --help
```

## :lucide-folder-cog: Configure

Create the [configuration](../configuration) on the HPC system. It will be the root directory of the workflow. A `run` directory will be created inside it and each task will run in a dedicated subdirectory there. It will be named following the pattern `<task_name>__<coord_name_x>_<coord_value_x>__<coord_name_y>_<coord_value_y>__[...]`, where `<coord_...>` refer to the [coordinates](../glossary/#coordinate) of the task.

## :lucide-check: Check

Sirocco provides a few commands to help you check the workflow configuration.

### `sirocco resolve`
Apply the `jinja` templating and write the resulting config file to `sirocco_resolved.yaml`

## :lucide-terminal: Control

Controlling the workflow happens through a command line interface.
