---
title: Standalone
icon: lucide/server
---

## :lucide-layers: Requirements

To install and run Sirocco in standalone mode, you will need an HPC system

- using the [SLURM :lucide-external-link:](https://slurm.schedmd.com/) scheduler, for now the only supported one
- with Python `>=3.10` installed

## :lucide-arrow-down-to-line: Install

Sirocco is a pure Python package available on PyPI. Directly install it on the HPC system (no root access required) from [pypi.org :lucide-external-link:](https://pypi.org/) or from a fresh clone for development. Using [`uv` :lucide-external-link:](https://docs.astral.sh/uv/) is highly recommended.
```shell
uv pip install Sirocco
```
or
```shell
git clone ...
cd Sirocco
uv sync
```

The virtual environment in which sirocco is installed can later be specified [in the workflow configuration](../configuration/tasks/#venv).

Sirocco is ran through a command line interface. Check your installation with
```shell
sirocco --help
```

## :lucide-terminal: CLI

Once the workflow [configuration](../configuration) is created on the HPC, it can be handled by the `sirocco` CLI. `sirocco` sub-commands can be classified in 2 groups: checking and controlling. All of them take the workflow root directory as the only positional argument defaulting to the current directory and have a `--help` option.

## :lucide-check: Checking

Sirocco provides a few commands to help users check the workflow configuration.

### `verify`
Try to load the configuration. This is helpful to catch validation issues.

### `represent`
Unroll the workflow graph and print it as a string formatted ala `yaml`. It is mainly used as a serialization method for testing but can also help debugging.

### `stviz`
bla

## :material-play-pause: Controlling

### `start`
bla

### `stop`
bla

### `restart`
bla
