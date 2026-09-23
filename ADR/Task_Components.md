# Tasks Components level

## Summary

In order to properly implement multi-component coupled simulations, we need to introduce a level between the task and inputs/outputs, namely `components`. The components of a coupled climate model, and in particular ICON, indeed have there own inputs and outputs, potentially using the same ports between different components. Typically in ICON, the so-called _master_ models, i.e., the one coupled through `yac`, all have their own restart and output streams.

This feature is introduced in [PR #245](https://github.com/C2SM/Sirocco/pull/245).

## Architecture

The `inputs` and `outputs` attributes of `graph_items.Task` are replaced by `components: dict[str, Taskcomponent]` mapping the component names to the task component simply defined by
```python
@dataclass(kw_only=True)
class TaskComponent:
    """Internal representation of a task component"""

    inputs: dict[str, list[Data]]
    outputs: dict[str, list[GeneratedData]]
```

Not all task require a components level. Actually, only the ICON plugin does in the current state of Sirocco. Other tasks (shell and sirocco) thus have a fake single component named `__SINGLE_COMPONENT__` and `inputs` and `outputs` become properties returning the `inputs` and `outputs` of that single component, so that the interface to the rest of the code remains the same for them.

## ICON plugin

### Components

The components being a part of the graph definition, they need to be specified in the `cycles` section of the configuration for ICON like this:
```yaml
cycles:
  - first icon:
      [...]
      tasks:
        - icon:
            components:
              master:
                inputs: [...]
              atmo:
                inputs: [...]
                outputs: [...]
              ocean:
                inpouts: [...]
                outputs: [...]
```
where each key under `components` must correspond to a master model name found in the master namelist. `master` is an exception that hosts some special purpose ports like `squash`

### `models.py` and `ports.py`

The icon task module was becoming too large so that `ports.py` and `models.py` have been introduced alongside `icon_task.py`.

`models.py` holds the `IconModel` class which is the ICON declination of the higher level Sirocco component concept. Models have different attributes associated to them like a namelist or MPI ranks.

`ports.py` hosts the main `PortHandler` class which role is to reflect in the ICON setup whatever the port implies, e.g. modifying a namelist parameter, checking the compatibility of a namelist parameter, set output data paths, etc ... ports are not unique across models so that a `PortHandler` instance can act on different models. Typically restart and output stream ports can be used with any master model (atmosphere, ocean, ...)


### hardware architecture and MPI ranks distribution

Since we introduce multiple components for ICON, we also need to introduce the way to distribute ranks between them. That's why the `ConfigIconTaskSpec` class now contains an `exe` attribute hosting a `cpu` and a `gpu` attribute of type `ConfigIconExe`. Among other specifications, the later hosts the mapping between each master model and the corresponding MPI processses. A key element of this design is scalability with the total requested number of nodes, i.e., users should not have to adapt the configuration when asking for different numbers of nodes, it should just scale. That's why users specify compute processes with weights and not absolute numbers. IO procs are given in absolute numbers.

This is sufficient to map master models to the number of MPI processes. To go further and map MPI processes to MPI ranks and even NUMA nodes, the icon task generates an annotated hostfile that will be used at runtime. It follows the SLURM hostfile logic and adds further information on each line like the NUMA node, the type of process (compute, io), the target architecture (cpu or gpu), etc ...
