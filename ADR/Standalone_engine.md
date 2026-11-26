# Standalone Engine

## Summary

The [standalone orchestration PR](https://github.com/C2SM/Sirocco/pull/224) implemented the native orchestration directly in the core classes (`core.Workflow`, `core.Task`, `core.Data`), and its [ADR](Standalone_orchestration.md) already listed moving it out of `core` as future work. The [standalone engine PR](https://github.com/C2SM/Sirocco/pull/303) does that refactor: the orchestration moves to `sirocco.engines.standalone`, next to `sirocco.engines.aiida`, and `core` only holds the workflow graph that all engines share.

The behaviour of the standalone orchestration is unchanged (front propagation, Sirocco task). This document records how the engine-specific state and behaviour are split from `core`, and why.

## Context

Before this refactor, core graph items carried standalone run-time state and behaviour:

- `core.Workflow`: the front, the completed tasks, the scheduler, the state file, `start`/`continue`/`restart`/`stop`,
- `core.Task`: `jobid`, `rank`, `run_dir`, `base_env`, `runscript_lines()`, `prepare_for_submission()`, `resolve_output_data_paths()`,
- `core.Data`: `resolved_path`, with lazy resolution through the origin task,
- `core.IconTask` and its `PortHandler`s: namelist updates, data links and output paths written in the run directory,
- `core.GraphItem`: `viz_status`, set by `sirocco stviz`.

`core` also hosted standalone-only code outside the graph items: the scheduler (`core/scheduler.py`), the `SIROCCO` task (`core/tasks/sirocco_task.py`) and `TaskStatus`.

This made `core` hard to reuse for the AiiDA engine, which needs the graph but none of that state, and made it unclear which parts of the ICON job preparation are engine-specific and which are knowledge about ICON that every engine needs.

The main difficulty is that the engine needs run-time state for a few tasks only (the ones in the front), while the graph structure (parents, children, inputs, outputs) stays on the core objects.

## Decisions

### Composition: the engine wraps `core.Workflow`

`StandaloneEngine` (`engine.py`) holds a `core.Workflow`, built once per sirocco command from the config, and adds the engine state next to it:

| Attribute | Type | Role |
|---|---|---|
| `jobids` | `JobIdTable` | job id of each submitted task |
| `front` | `Front` | front generations and completed tasks, and the rank of each task |
| `cool_down_tasks` | `list[core.Task]` | rank 0 tasks left running by `stop` in cool-down mode |
| `task_handlers` | `TaskHandlerTable` | engine behaviour per task (see below), created on first access |
| `data_paths` | `DataPathTable` | resolved path of each data node, computed on demand, never stored |
| `scheduler` | `Scheduler` | submits task handlers, holds no task state |
| `sirocco_continue_task` | `SiroccoContinueTask` | the job running `sirocco continue` |

All tables are keyed by label (name + coordinates), which is unique per graph item, and only hold entries for the items the engine has touched.

Alternatives considered:

- **Inheritance** (`StandaloneWorkflow(core.Workflow)`, `StandaloneTask(core.Task)`, ...): the graph is built by `core` from the config, so engine subclasses of the graph items would have to be injected into the graph construction, or the graph rebuilt with them. It also keeps state and graph in the same objects, which is what we wanted to get rid of.
- **Engine-side copy of the graph**, i.e. engine `Task`/`Data` classes wrapping or inheriting from `core.GraphItem`, held in their own `core.Store`s: the edges stay on core objects, so a core → engine lookup is needed anyway, unless the whole graph is duplicated. `Store` is eager and built for cycle-spec resolution, while the engine only needs state for the front. And the front has to own the ranks anyway (see below).
- **Job id on the `PluginHandler`**: rejected to keep handlers stateless with respect to the orchestration. They only know how to run a task, the engine knows where it stands.

### The front owns the ranks

`Front` (`state.py`) holds the generations and the completed tasks, as references to the core `Task` objects. The rank of a task is the generation it is in: `k` in generation `k`, `COMPLETED` once done, `depth` if not submitted yet. Previously, `core.Workflow` set `Task.rank` by hand wherever it changed the front lists (initialization, state loading, propagation), and nothing guaranteed that rank and lists matched. Now only `Front` sets the rank, and only when it moves a task between its lists. Tasks enter the front through `Front.place`, at submission or when loading the state file, and then only move through `Front.promote`, one generation at a time. A rank can therefore never disagree with the front lists and can only decrease. The propagation rule (a task belongs one generation after its least advanced parent) is written once, in `Front.generation_after_parents`.

### Plugin handlers instead of task methods

What the engine does with a task of a given plugin is implemented by a `PluginHandler` subclass (`plugin_handlers/`):

- run directory and run script path,
- output path resolution (`resolve_output_paths`),
- run directory preparation (`prepare_rundir_content`),
- run script lines and submission settings (`submit_filename`, `clean_up_rundir`, `scheduler_uenv`, ...).

A handler names the task class it handles as a type parameter, e.g. `IconHandler(PluginHandler[IconTask])`. The same parameter types its `task` attribute and registers the class for that task's plugin in `PluginHandler.handler_classes` through `__init_subclass__`. One handler instance is created per task, on first access to `engine.task_handlers[task]`.

Alternatives considered:

- **`functools.singledispatch` on the task type** (first prototype): it works for single functions, but spreads a plugin's behaviour over several dispatch tables, gives no place for per-plugin settings and doesn't force a plugin to implement everything. An ABC groups it all and fails early on a missing method.
- **Narrowing `task: IconTask` by annotation in the subclass**: rejected by type checkers as an unsafe override of a mutable attribute. The type parameter avoids it and doubles as the registration key.
- **Converging with the AiiDA engine's structure** (`match task: case ShellTask/IconTask` and parallel Shell/Icon class pairs split by concern): not done here, to keep the standalone side readable. Convergence, if any, should go towards registries on the AiiDA side, together with its own refactor ([#247](https://github.com/C2SM/Sirocco/issues/247)) and the pluginification of Sirocco (as of now, planed for later).

All registries (core tasks, task handlers, ICON special port handlers, schedulers) now work the same way: a plain dict class variable filled when subclassing, with a `None` default for subclasses that should not register. Built-in handlers are registered by importing `sirocco.engines.standalone`, which imports `plugin_handlers/`. A third-party plugin would subclass `PluginHandler[ItsTask]` the same way from its own package. Discovering such packages (e.g. through entry points) is left to the pluginification work.

### Paths belong to the engine

Where data lives and where tasks run depends on the engine (AiiDA has its own remote folders), so `Data.resolved_path`, `Task.run_dir`, `Task.RUN_ROOT` and the path resolution methods are removed from `core`. `DataPathTable` resolves paths on demand: asking for the path of a generated data node that is not known yet asks the handler of its origin task to resolve all its outputs, so the order in which tasks are processed doesn't matter. Resolving a path again to a different value is an error, instead of a silent override.

`TaskHandlerTable` and `DataPathTable` depend on each other (handlers read and write paths, resolving a generated path needs a handler). The engine creates both and wires them together. Handlers only import `DataPathTable` for type checking, so there is no import cycle.

### ICON: knowledge in core, materialization in the engine

Most of the ICON job preparation is not specific to the standalone engine: the AiiDA engine also has to run ICON on the HPC. The split follows the question "is this knowledge about ICON, or a way to put files on disk and submit a job?".

- **Ports.** Core keeps a declarative, frozen `IconPort` (formerly `PortHandler`): valid model types, namelist section and parameter, link name. The engine's `IconPortHandler` applies regular ports from that declaration. Ports that cannot be described declaratively are flagged as special in core (`IconPort.is_special`), and each engine has to implement them: in the standalone engine, a subclass of `IconPortHandler` per special port. Importing the engine fails if a special port declared in core is not implemented.
- **Runtime script interface.** `core/tasks/icon_task/launch.py` is the engine-neutral contract with the ICON runtime scripts: runtime folder and entry point, supported machines and uenv requirement, executable link names, hostfile and multi-prog content, exported environment. It doesn't touch the file system. Engines only materialize it. The AiiDA engine is expected to use the same runtime scripts once it catches up.
- **Preparation doesn't alter the task.** It used to reset the task `uenv`/`view`, so a second preparation failed. The run script now exports them for the runtime scripts and the handler setting `scheduler_uenv` keeps them out of the scheduler header.

Port behaviour itself is kept 1:1 with the previous implementation. Its refactoring (port direction, de-duplicated applications, validation at graph construction) is left to a follow-up PR.

### Settings: only `front_depth` stays in core

Engine-specific settings move to the engine: `scheduler` (now required, no `slurm` default), the `SIROCCO` task (now required instead of silently using defaults), `base_env` (computed on first submission only, so that read-only commands don't spawn a login shell), `TaskStatus`, `WorkflowStatus` and the run script settings. The unused `engine` config field is removed.

`front_depth` stays in `core.Workflow`, as submitting tasks ahead of time is shared by all engines: the AiiDA engine reads it for its own rolling window.

`SiroccoContinueTask` moves to the engine. It still subclasses `core.Task` so that the scheduler can submit it like any other task, although it is not part of the graph.

### State visualization out of the graph

`sirocco stviz` used to write display statuses onto core graph items. It is now built in `engines/standalone/state_viz.py` from the engine state, and `vizgraph.py` stays a generic renderer.

## Consequences

- `core` is the graph only and can be used by any engine without carrying standalone state.
- Supporting a new task plugin in the standalone engine means adding a `PluginHandler` subclass, without touching `core` or the engine.
- Engine state is looked up through tables (`engine.jobids[task]`, `engine.front.rank(task)`, `engine.data_paths[data]`, `engine.task_handlers[task]`) instead of task attributes. This is a bit more verbose but makes the owner of each piece of state explicit.
- Some engine-specific config still sits in the shared config models (`scheduler`, `ConfigSiroccoTask`): there is a single workflow config for all engines, from which `StandaloneEngine.from_config_workflow` picks its own settings. Separating them would need a place for engine-specific settings in the config, which is not designed yet.
- The remote control work ([#283](https://github.com/C2SM/Sirocco/pull/283), [#288](https://github.com/C2SM/Sirocco/pull/288)) is rebased on this refactor rather than the other way round.

## Open points

- Machine checks in `launch.py` only hold for the standalone engine, since `computer` means a machine for the standalone engine and an AiiDA computer label for the AiiDA engine, see [#304](https://github.com/C2SM/Sirocco/issues/304).
- `SiroccoContinueTask` is a `core.Task` only so the scheduler can submit it, and registers its plugin among the core task classes. [#288](https://github.com/C2SM/Sirocco/pull/288) adds a `SiroccoStartTask` with the same need, so this is better addressed there or after it.
- The AiiDA engine doesn't use the shared ICON pieces (`IconPort`, `launch.py`) yet, see [#247](https://github.com/C2SM/Sirocco/issues/247).
