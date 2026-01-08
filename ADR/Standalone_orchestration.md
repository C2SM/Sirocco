# Standalone Orchestration Design

## Summary

The [standalone orchestration PR](https://github.com/C2SM/Sirocco/pull/224) introduces a native orchstration, i.e. fully implemented in the sirocco package as ooposed to using AiiDA packages. The idea is to rely on the machine job scheduler as the only deamon involved. In order to orchestrate the workflow, a special Sirocco task is introduced that runs on the target machine alongside the other workflow tasks, submits them when needed and resubmits iself.

In order to allow for tasks to be submitted ahead of time (requirement for the proper usage of the target cluster, think large priority jobs), the PR also introduces another new concept, namely a propagating front of submitted tasks.

## First Implementation

### Propagating front of submitted tasks

Amongst other additional attributes and methods (see below), the `core.Workflow` class now hosts the `front` and `front_depth` attributes
```python
class Workflow:
    ...
    def __init__(
        self,
        ...
        front_depth: int,
        ...
    ) -> None:
        ...
        self.front_depth = front_depth
        self.front: list[list[Task]] = [[] for _ in range(self.front_depth)]
        ...
```
In very few words, the front can be understood as the first `front_depth` propagating (hence dynamical) topological generations of an extracted graph that only contains task nodes.

#### Extracted task graph

Defining the front first requires the definition of this _extracted graph that only contains task nodes_. We introduce it in the form of new explicit links between task nodes stored in `parents` and `children` attributes in `core.Task`.
We also introduce `waiters` as attributes to `core.Task`, `downstream_tasks` to `core.Data` and `origin_task` to `core.GeneratedData` to facilitate the resolution of task children and parents.
```python
class Task(ConfigBaseTaskSpecs, GraphItem):
    ...
    wait_on: list[Task] = field(default_factory=list)
    waiters: list[Task] = field(default_factory=list, repr=False)
    parents: list[Task] = field(default_factory=list, repr=False)
    children: list[Task] = field(default_factory=list, repr=False)
    ...
    
class Data(ConfigBaseDataSpecs, GraphItem):
    ...
    downstream_tasks: list[Task] = field(default_factory=list, repr=False)
    ...
    
class GeneratedData(Data, ConfigGeneratedDataSpecs):
    origin_task: Task = field(init=False, repr=False)
    ...
```

#### Front initiaition and task ranks 

At workflow start, the front coincides with the frist `front_depth` [topological generations](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html). Each task in the workflow is also getting a `rank`. It is set to the index of the front generation it belongs to (`0 <= rank <= front_depth-1`) for tasks in the front and `front_depth` for the others.

#### Propagation

The front is then propagating through the task DAG in the following way:
- By definition, ongoing tasks (not necessarily running, this depends on the available resources) are necessarily in the first front generation (rank `0`). When such a task completes, its rank is set to `-1` and it is removed from the front.
- Then tasks are recursively promoted from generation `k` to `k-1` if the max rank of parent tasks is `k-2` (task rank is updated accordingly)
- Finally new tasks are recruted amongst the children of the before last generation (`front_depth-2`) and added to the last generation (`front_depth-1`) if the max rank of a parent task is `front_depth-2` (task rank is updated accordingly).

### The Sirocco task

This is the special orchestrating task that does not appear in the IR graph. It is running on the target machine and submitted to the scheduler as any other task with a dependency on any of the rank `0` tasks terminating. In other words, if any of the currently running tasks finishes, regardless of its exit status, the sirocco orchastrating task is allowed by the scheduler to start. Its main role is to:
1. get the state of the workflow which is stored locally on disk
2. Get the new status of rank `0` tasks by interrogating the scheduler and subsequently propagate the front, which includes the submission of new last generation tasks.
3. update the workflow status and dump it to disk
4. Resubmit itself

### Scheduling capabilities for tasks

As tasks are directly submitted by Sirocco and not AiiDA, 2 important elements are also added to core classes:
- `core.Data` has a `resolved_path` attribute that is used by tasks to know the concrete location of data nodes.
- All task (so `core.IconTask` and `core.ShellTask` and `core.SiroccoContinueTask`) implement 3 methods that replicate part of what AiiDA plugins (`aiida-shell` and `aiida-icon`) do:
  - `runscript_lines` returns the core lines specific to the task to be added to the submittedd job script.
  - `resolve_output_data_paths` sets the `recolved_path` attribute of output data
  - `prepare_for_submission` adds any required file to the task run directory (e.g. icon namelsists, users scripts)

### Interaction with the workflow

TODO
Added methods to `core.Workflow`: starting, stopping, restarting and, visualization and status visualizuation.

## Future refactor

TODO
