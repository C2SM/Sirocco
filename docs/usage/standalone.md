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
git clone url/of/remote/sirocco/repo
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

### :lucide-check: Checking

Sirocco provides a few commands to help users check the workflow configuration.

#### `verify`

Try to load the configuration. This is helpful to catch validation issues.

#### `represent`

Unroll the workflow graph and print it as a string formatted ala `yaml`. It is mainly used as a serialization method for testing but can also help debugging.

#### `visualize`

Generate a visual representation of the workflow graph as an `svg` file which name defaults to `WORFLOW_DIRNAME.svg`. An example is displayed [in the cycles section](../../configuration/cycles/#example). It is pretty useful to confirm that the workflow graph corresponds to the intended nodes relationship.

!!! warning "long graph representation"

    The graph representation is produced via the [graphviz](https://graphviz.org/) library with the [dot layout engine](https://graphviz.org/docs/layouts/dot/). Trying to display the entire graph for long production workflows can lead to very long processing or even non converging algorithm for nodes and edges placement. It is advised to reduce the workflow duration for visualization.

#### `stviz`

Generate a graphical representation similar to `visualize` focusing on the [task front](../../orchestration#the-deep-propagating-submitted-task-front) and dump it to `status.svg`. Only cycles containing tasks from the front are represented. Ongoing tasks (rank $0$) and involved input /output data are displayed in bold. Other front tasks (here only rank $1$ is left) and involved input /output data are unchanged compared to `visualize`. Other tasks and data nodes are grayed out. Task ranks are also added to the tooltip (hover on the task nodes to see).

<!-- Use object to keep the embedded javascript runable -->
<object type="image/svg+xml" data="../../assets/example_config/status.svg" width="100%">
  Your browser does not support interactive SVGs.
</object>
<figcaption markdown="span"  style="text-align: left;  font-style: normal; font-size: smaller;">
*Example status graph corresponding to the execution of [the configuration example](../../configuration/cycles/#example) with a front depth of 2*.
</figcaption>

### :material-play-pause: Controlling

Sirocco has the 3 following self-explanatory commands to control the workflow. Each of them, as well as workflow continuation tasks, are logging information to `sirocco.log` located in the workflow root directory. By default Sirocco uses [ANSI escape codes](https://en.wikipedia.org/wiki/ANSI_escape_code) for coloring, which can be disable with `--no-ansi`.

#### `start`

Start the workflow. Use `--cleanup` to force a fresh start from an existing workflow and wipe the `run` directory.

#### `stop`

Stop the workflow, which means cancel all front tasks. Use `--cool-down` to let rank $0$ tasks gracefully terminate and only cancel ahead-of-time submitted ones.

!!! warning "Manual task cancellation"

    Do not cancel tasks manually, this would break the workflow state.

#### `restart`

Restart a stopped workflow, even if it was stopped in cool-down mode. Rank $0$ tasks will be picked up if not already completed.

#### Troubleshooting

In the case of a failing job, the controlling Sirocco task will cancel all other tasks and, if needed, the workflow can be later restarted via `sìrocco restart`.

Sirocco is also designed so that any workflow task can be re-submitted interactively, so not as part of the workflow, for testing or troubleshooting purposes.

!!! example "Recovery"

    A workflow recovery mechanism will be implemented in a later version

### :material-remote: Remote handling

!!! example "Remote checking and controlling"

    Remote checking and controlling from users machines through either [FirecREST :lucide-external-link:](https://eth-cscs.github.io/firecrest-v2/) or `ssh` will be implemented in a later version.
