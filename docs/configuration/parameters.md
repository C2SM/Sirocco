---
title: Parameters
icon: lucide/variable
---

# Parameters

Parameterized workflows are supported by Sirocco. Since Sirocco includes data nodes in the workflow graph, parameters can apply to both task and data instances. The overall design is to declare parameters and their possible values in this [root level `parameters` section](#specifications), apply them to task and data by referencing the parameter names in the different task and data descriptions (root level [`tasks`](../../configuration/tasks/#parameters) and [`data`](../../configuration/data/#parameters) sections) and finally specify in the `cycles` section [which mode](../../configuration/cycles/#parameters) should be used when targetting parameterized nodes (target all or a single one with the same value).

!!! warning "full parameter space"

    Note that a task or data instance will be generated for each possible combination of the parameters attached to it, _i.e._ the full matrix will be used. It is not possible to select only a subset of these possible combinations.

## Specifications

 This root level section is short and simple. It is just a sequence of mappings from parameter names to the sequence of possible values and reads like this:
 ```sirocco-yaml
 parameters:
   foo: [0, 1, 2]
   bar: [1.5, 2.9]
 ```

## Example of a parameterized workflow

In order to better understand where and how parameters are defined, referenced and used, the graph representation of a parameterized workflow is displayed below and the corresponding `sirocco.yaml` configuration file with all the relevant mentions of parameters comes right after.

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
stop_date: &root_stop_date '2026-08-01T00:00'
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
              - model:
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
                    parameters:
                      foo: single
                      bar: single
            outputs:
              output_streams: [stream 1]
              restart_out: [restart_file]
        - post proc:
            inputs:
              in:
                - stream 1:
                    parameters:
                      foo: single
                      bar: single
            outputs:
              out: [processed]
        - statistics foo:
            inputs:
              in:
                - processed:
                    parameters:
                      foo: all
                      bar: single
            outputs:
              out: [stat foo]
        - statistics bar:
            inputs:
              in:
                - stat foo:
                    parameters:
                      bar: all
            outputs:
              out: [stat bar]

tasks:
  - ROOT: [...]
  - pre proc: [...]
  - model:
      [...]
      parameters: [foo, bar]
  - post proc:
      [...]
      parameters: [foo, bar]
  - statistics foo:
      [...]
      parameters: [bar]
  - statistics bar: [...]
data:
  available:
     - raw input data: [...]
     - initial conditions: [...]
  generated:
     - boundary conditions: [...]
     - stream 1:
         [...]
         parameters: [foo, bar]
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

parameters:
  foo: [0, 1, 2]
  bar: [1.5, 2.9]
```
