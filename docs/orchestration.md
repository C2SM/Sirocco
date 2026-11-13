# Orchestration :lucide-wand-sparkles:

Sirocco workflows can be orchestrated by two engines. A **standalone** one running autonomously on the HPC cluster and an **[AiiDA :lucide-external-link:](https://aiida.net/)** based one, still under development, which runs on a dedicated server.

## The deep propagating submitted task front

In order to optimize the resources usage of the HPC cluster, both engines maximize the amount of information given to the scheduler by implementing the new concept of deep propagating task front. Taking advantage of the deterministic nature of climate and weather workflows, it enables ahead-of-time task submission. Using rather grandiose language, one could qualify this front as a **deep**, **propagating** set of **dynamical topological generations**. To explain the concept, we will consider the [directed acyclic graph (DAG) :lucide-external-link:](https://en.wikipedia.org/wiki/Directed_acyclic_graph) constituted by task nodes only, removing data nodes from the [full workflow graph](configuration/cycles/#cycles). Edges between nodes thus become task dependencies, either derived from input/output relationships or direct [_wait on_](configuration/cycles/#wait_on) dependencies.

- [**topological generations** :lucide-external-link:](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html) partition a DAG into sequential levels, the so-called "generations", based on their dependency depth. In this structure, nodes in generation 0 have no parents. Parents of nodes in generation 1 are only from generation 0, and more generally, parents of a node in generation $R$ are only from generations $<R$. We call $R$ the _global rank_.
- **deep front**. Topological generations partition the _whole_ graph into generations. Our front has a _finite depth_, $d$, corresponding to the number of generations of submitted tasks taken into account. A depth of 1 would correspond to only ongoing tasks without ahead-of-time submission. A depth of 2 implies 1 generation submitted ahead-of-time, etc... In order to keep track of the propagating front, each task is attributed a _local rank_ $r$ indicating the front generation it belongs to or if it is out of the front:
    - $r=-1$ for completed tasks
    - $0 \leq r \leq d-1$ for tasks within the front ($0$ for ongoing tasks, $d-1$ for last ahead-of-time submitted generation)
    - $r=d$ for not yet submitted tasks

- **propagating dynamical generations**. At the beginning of the workflow execution, the front will coincide with the first $d$ global topological generations, i.e. all tasks in a front generation share the same global rank $R$. As the workflow progresses, some tasks are terminating, exiting the front and getting a local rank $r=-1$, tasks within the front are potentially moved to older generations (getting a lower $r$) and new ones are getting submitted to the last generation with $r=d-1$. Generations are updated dynamically, so that they do not necessarily coincide anymore with global topological generations. Think about a workflow where a branch of the DAG advances faster than others. The front will be distorted and dynamical generations will contain tasks with different global ranks $R$: higher for tasks of the fast branch, lower for others.

## Standalone engine

The standalone orchestrating engine enables Sirocco workflows to run autonomously on the HPC cluster, relying on the scheduler as the sole running daemon.

### Front propagation

At the start of a workflow, the Sirocco process

1. submits required jobs, i.e. the $d$ front generations, to the scheduler with adequate dependencies,
2. submits a workflow continuation job depending on the termination of any rank 0 task, regardless of the exit status
3. and finally dumps the workflow state to file.

When the Sirocco continuation task kicks in, it

1. loads the state in memory,
2. updates the front by interrogating the scheduler regarding the status of rank 0 tasks and submitting new ones,
3. re-submits a continuation task
4. and finally dumps the workflow state to file.

### Workflow folder hierarchy

The standalone engine executes workflows _inside their [configuration directory](configuration/configuration)_ so that "workflow root directory" is also used to designate the root of a running workflow which technically also remains the "configuration root directory". Starting the execution, Sirocco creates a `run` directory at the workflow root directory and each task will run inside its own run directory at `run/task_rundir_name`, where `task_rundir_name` is a unique name looking like this:
```shell
<task_name>__<coordinate1_name>_<coordinate1_value>__<coordinate2_name>_<coordinate2_value>__[...]
```
[Coordinates](glossary/#coordinates) of a task are either its date, in which case `<coordinatex_name>` is `"date"` and `<coordinatex_value>` is the date formatted as `"YYYY-MM-DDThh-mm-ss"` or a [parameter](configuration/parameters), in which case `"<coordinatex_name>"` and `<coordinatex_value>` are the parameters name and value.

### `bash` shebang and base environment

Sirocco tasks are all submitted as _bash tasks_ with the `#!/bin/bash -l` shebang. Their base environment, i.e. before any modification by the task itself, is exactly the same as from a fresh `bash` login (for instance through `ssh`). This means that environment variables set after login to the interactive session will be ignored. Any environment variable relevant for workflow tasks must be explicitly specified at adequate places in the configuration.
