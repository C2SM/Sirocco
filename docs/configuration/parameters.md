---
title: Parameters
icon: lucide/variable
---

# Parameters

Parameterized workflows are supported by Sirocco, typically for ensemble or sensitivity experiments. Since Sirocco includes data nodes in the workflow graph, parameters apply to both task and data instances. As each configuration section is documented in its own place, the parameter related specifications are spread across several of them. This page gathers them and shows how they are assembled in a [complete example](#example-of-a-parameterized-workflow).

!!! failure "Parameters not yet fully functional"

    Even though workflows with parameterized tasks and pieces of data can be generated, the parameter values are not yet exposed to the tasks themselves.

## Overview

Using parameters is a three step process:

| Step | Where | What |
|---|---|---|
| **Declare** | root level [`parameters`](#specifications) section | parameter names and their possible values |
| **Attach** | [`tasks`](tasks.md#parameters) and [`data`](data.md) entries, under `parameters` | which tasks and pieces of data are instantiated for each combination of parameter values |
| **Target** | [`cycles`](cycles.md#parameters) section, under `parameters` of an input or a `wait_on` entry | which instances of a parameterized node are targeted, using one of the two [targeting modes](#targeting-modes) `single` or `all` (default) |

!!! warning "full parameter space"

    Note that a task or data instance will be generated for each possible combination of the parameters attached to it, _i.e._ the full matrix will be used. It is not possible to select only a subset of these possible combinations.

## Specifications

This root level section is short and simple. It is just a mapping from parameter names to the sequence of their possible values and reads like this:
```sirocco-yaml
parameters:
  foo: [0, 1, 2]
  bar: [1.5, 2.9]
```

## Rules

### Targeting modes

When a task targets a parameterized node, either as an input or in `wait_on`, the targeted instances are selected parameter by parameter, using one of the two following modes:

- `single`: only the instance with the same value as the current task is targeted. This requires the current task to be parameterized by the same parameter.
- `all`: all instances, _i.e._ all values of that parameter, are targeted.

!!! warning "`all` is the default"

    Any parameter of the targeted node that is not explicitly mentioned is resolved with the `all` mode. For instance, if a task parameterized by `foo` takes as input a piece of data also parameterized by `foo` without specifying `foo: single`, each task instance will receive the data instances for _all_ values of `foo`, not only its own.

Modes given for parameters the targeted node does not carry are ignored.

### Outputs

A generated piece of data must carry exactly the same parameters as the task producing it. Parameters can therefore neither be added nor dropped through outputs. Reducing a parameter dimension, _e.g._ computing statistics over ensemble members, is done with a task that is not parameterized by that parameter and takes the data instances as input in the `all` mode. This is what the `statistics foo` and `statistics bar` tasks do in the [example below](#example-of-a-parameterized-workflow).

### Available data

Available data can be parameterized as well, for instance to provide different initial conditions for each ensemble member. Since available data does not belong to any cycle, its instances only have parameter [coordinates](../glossary.md#coordinates) and no date.

## Example of a parameterized workflow

In order to better understand where and how parameters are declared, attached and targeted, the graph representation of a parameterized workflow is displayed below and the corresponding `sirocco.yaml` configuration file, with all mentions of parameters highlighted, comes right after. Click on the :material-plus-circle: markers for explanations.

<figure class="svg-viewer" data-zoom="2" style="--svg-viewer-height: 40vh;" markdown="block">
  <div class="svg-viewer__toolbar">
    <button type="button" data-svg-zoom="in" title="Zoom in">+</button>
    <button type="button" data-svg-zoom="out" title="Zoom out">−</button>
    <button type="button" data-svg-zoom="reset" title="Reset zoom">Reset</button>
  </div>
  <div class="svg-viewer__viewport">
    <!-- object keeps the embedded javascript runnable -->
    <object type="image/svg+xml" data="../../assets/example_parameter_config.svg">
      Your browser does not support interactive SVGs.
    </object>
  </div>
  <figcaption markdown="span" style="text-align: left; font-style: normal; font-size: smaller;">
  _Graph representation of a parameterized workflow._ Note the parameter values associated with the nodes when hovering over them.
  </figcaption>
</figure>

```sirocco-yaml hl_lines="35-37 45-47 54-56 63-64 73 76 79 89 92 95 98 101-103" title="sirocco.yaml"
start_date: &root_start_date '2026-01-01T00:00'
stop_date: &root_stop_date '2026-05-01T00:00'
scheduler: slurm
cycles:
  - bi-monthly:
      cycling:
        start_date: *root_start_date
        stop_date: *root_stop_date
        period: "P2M"
      tasks:
        - pre proc:
            inputs:
              raw_input: [raw input data]
            outputs:
              processed: [boundary conditions]
            wait_on:
              - model: # (1)!
                  when:
                    after: "2026-03-01T00:00"
                  target_cycle:
                    lag: "-P4M"
        - model:
            inputs:
              init:
                - initial conditions:
                    when:
                      at: *root_start_date
              bc: [boundary conditions]
              restart_in:
                - restart_file:
                    when:
                      after: *root_start_date
                    target_cycle:
                      lag: "-P2M"
                    parameters: # (2)!
                      foo: single
                      bar: single
            outputs:
              output_streams: [stream 1]
              restart_out: [restart_file]
        - post proc:
            inputs:
              in:
                - stream 1:
                    parameters: # (3)!
                      foo: single
                      bar: single
            outputs:
              out: [processed]
        - statistics foo:
            inputs:
              in:
                - processed:
                    parameters: # (4)!
                      foo: all
                      bar: single
            outputs:
              out: [stat foo]
        - statistics bar:
            inputs:
              in:
                - stat foo:
                    parameters: # (5)!
                      bar: all
            outputs:
              out: [stat bar]

tasks:
  - ROOT: [...]
  - pre proc: [...]
  - model:
      [...]
      parameters: [foo, bar] # (6)!
  - post proc:
      [...]
      parameters: [foo, bar]
  - statistics foo:
      [...]
      parameters: [bar] # (7)!
  - statistics bar: [...]
data:
  available:
     - raw input data: [...]
     - initial conditions: [...]
  generated:
     - boundary conditions: [...]
     - stream 1:
         [...]
         parameters: [foo, bar] # (8)!
     - restart_file:
         [...]
         parameters: [foo, bar]
     - processed:
         [...]
         parameters: [foo, bar]
     - stat foo:
         [...]
         parameters: [bar]
     - stat bar: [...]

parameters: # (9)!
  foo: [0, 1, 2]
  bar: [1.5, 2.9]
```

1.  No `parameters` are given here although `model` is parameterized by `foo` and `bar`. The default `all` mode applies, so each `pre proc` instance waits on all 6 `model` instances of the targeted cycle occurrence. Note that with only 2 cycle occurrences, the `when` condition is never met, so this dependency doesn't appear in the graph.
2.  Each `model` instance continues from its own restart file. Without `single`, the default `all` mode would feed all 6 restart files to each `model` instance.
3.  Each `post proc` instance processes the output of the `model` instance with the same `foo` and `bar` values.
4.  Reduction over `foo`: each `statistics foo` instance gathers the 3 `processed` instances (one per `foo` value) sharing its own `bar` value.
5.  Reduction over `bar`: the single `statistics bar` instance gathers both `stat foo` instances.
6.  Attaching `foo` and `bar` to `model` creates $3 \times 2 = 6$ instances per cycle occurrence.
7.  Only parameterized by `bar`, so 2 instances per cycle occurrence. Since `foo` is dropped, it has to be reduced over in the inputs with the `all` mode (see 4).
8.  Generated data must carry exactly the same parameters as the task producing it: `stream 1` is an output of `model`, so it is parameterized by `foo` and `bar`. The same holds for `restart_file` (`model`), `processed` (`post proc`) and `stat foo` (`statistics foo`).
9.  Declaration of the parameters and their possible values.

### Reading the graph

The following table summarizes, for each cycle occurrence, the parameterized tasks of the example and how many instances are created.

| Task | Parameters | Instances per cycle occurrence | Gathers |
|---|---|---|---|
| `model`, `post proc` | `foo`, `bar` | $3 \times 2 = 6$ | — |
| `statistics foo` | `bar` | 2 | 3 `processed` instances (all `foo` values) |
| `statistics bar` | — | 1 | 2 `stat foo` instances (all `bar` values) |
