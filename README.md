<p align="center">
  <img alt="Sirocco logo" src="./docs/assets/Sirocco_Logo2026_BrunRVB.svg#gh-light-mode-only" width="500" />
  <img alt="Sirocco logo" src="./docs/assets/Sirocco_Logo2026_BeigeRVB.svg#gh-dark-mode-only" width="500" />
</p>

<p align="center">
  <a href="https://sirocco.ethz.ch"><b>Documentation</b></a> ·
  <a href="https://sirocco.ethz.ch/usage/standalone/">Getting started</a> ·
  <a href="./CHANGELOG.md">Changelog</a>
</p>

**Sirocco** is a Python workflow tool for climate and weather applications running on HPC systems. Workflows are described in a YAML dialect inspired by [cylc](https://cylc.github.io/) and [AiiDA](https://www.aiida.net/), and unrolled into a graph of tasks and data.

- **Data as graph nodes.** Tasks explicitly declare the data they consume and produce, through named ports. No task needs to make assumptions on where others read or write, so workflows are truly composable.
- **Cycles.** Recurring sets of tasks with their own periodicity, so that describing climate and weather workflows feels natural.
- **Parameters.** Ensemble and sensitivity experiments through parameterized tasks and data.
- **Deep propagating task front.** Tasks are submitted ahead of time, chained by scheduler dependencies, in a window that moves through the graph as tasks complete. Tasks no longer wait for their parents to finish before entering the queue.
- **Plugins.** A general purpose `shell` plugin and a dedicated plugin for the [ICON](https://www.icon-model.org) model, covering complex setups such as coupled runs on hybrid CPU/GPU architectures.
- **Two engines.** A standalone engine running autonomously on the HPC system, with the scheduler as the sole daemon, and an [AiiDA](https://www.aiida.net/) based engine (under development) running on a dedicated server.

## Example

A model running month by month, restarting from its previous state, with each monthly output post-processed:

```yaml
scheduler: slurm
cycles:
  - monthly:
      cycling:
        start_date: "2026-01-01T00:00"
        stop_date: "2026-07-01T00:00"
        period: "P1M"
      tasks:
        - model:
            inputs:
              init:
                - initial state:
                    when:
                      at: "2026-01-01T00:00"
              restart_in:
                - restart:
                    when:
                      after: "2026-01-01T00:00"
                    target_cycle:
                      lag: "-P1M"
            outputs:
              restart_out: [restart]
              output: [model output]
        - plot:
            inputs:
              data: [model output]
            outputs:
              figures: [figures]
tasks:
  - ROOT:
      computer: santis
      account: my_account
      walltime: "01:00:00"
  - model:
      plugin: shell
      src: scripts/run_model.sh
      command: ./run_model.sh [--init {PORT::init}] [--restart {PORT::restart_in}]
      nodes: 4
  - plot:
      plugin: shell
      src: scripts/plot.py
      command: python plot.py {PORT::data}
data:
  available:
    - initial state:
        path: /path/to/initial_state.nc
  generated:
    - restart:
        path: restart.nc
    - model output:
        path: output
    - figures:
        path: figures
```

See the [configuration documentation](https://sirocco.ethz.ch/configuration/configuration/) for the complete syntax.

## Getting started

Sirocco requires Python `>=3.12` and, for the standalone engine, an HPC system using the [SLURM](https://slurm.schedmd.com/) scheduler (currently the only supported one). Install it on the HPC system, preferably with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install sirocco
```

Then, from the configuration directory (containing `sirocco.yaml`):

```bash
sirocco verify      # validate the configuration
sirocco visualize   # render the unrolled workflow graph as an svg
sirocco start       # start the workflow
sirocco stviz       # render the current state of the task front
```

See [Usage](https://sirocco.ethz.ch/usage/standalone/) for all commands, including `stop` and `restart`.

## Development

Sirocco uses [hatch](https://hatch.pypa.io) to manage environments:

```bash
git clone https://github.com/C2SM/Sirocco.git
cd Sirocco
hatch test                # run the tests
hatch test --all          # run the tests on all supported Python versions
hatch check fmt           # check formatting
hatch check code          # lint
hatch run types:check     # type check
hatch run docs:serve      # live preview of the documentation
```

## License

Sirocco is released under the [MIT license](./LICENSE).

## Acknowledgments

We would like to acknowledge the [EXCLAIM](https://exclaim.ethz.ch) and [Swiss Twins](https://www.cscs.ch/about/collaborations/swisstwins) projects for the initial funding of Sirocco.
