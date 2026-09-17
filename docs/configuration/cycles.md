---
title: Cycles
icon: lucide/refresh-cw
---

# Cycles

Sirocco represents workflows as [directed acyclic graphs :lucide-external-link:](https://en.wikipedia.org/wiki/Directed_acyclic_graph), like the one displayed below, where nodes are instances of a task or a piece of data. Their **content**, _e.g._ task scripts or data path, is declared in the dedicated [`tasks`](tasks) and [`data`](data) sections while the current `cylces` section is only concerned with the **graph topology**, _i.e._ the relationships between the nodes. Yet, each mention of them, as well as potential [parameters](parameters), must refer to a declared counterpart.

In order to describe the graph topology, the `cycles` section describes:

- the **cycles** themselves, *i.e.* sets of recurring tasks sharing a common periodicity.
- tasks **inputs** and **outputs**
- potential added *manual* dependencies between tasks, called **wait_on**, on top of the ones derived from inputs/outputs relationships.

## Example

The example below shows a short but comprehensive graph covering most of the dedicated Sirocco syntax, at the exception of parameters.

!!! tip "hover and click"

    In case the following image is to small, you can hover on the nodes and cycles to display information with a larger font. Also clicking nodes will highlight direct parents and children relationships.

<!-- Use object to keep the embedded javascript runable -->
<object type="image/svg+xml" data="../../assets/example_config/example.svg" width="100%">
  Your browser does not support interactive SVGs.
</object>
<figcaption markdown="span"  style="text-align: left;  font-style: normal; font-size: smaller;">
*Example workflow graph*. Nodes vertical position indicate their rank, starting with 0 (no parents) at the top. Red rectangles represent **tasks**, green and blue ellipses represent **available data** and  **generated data** and light rectangular areas indicate **cycle** occurrences. Solid lines represent **input/output** relationships while dashed lines indicate a **`wait_on`** dependency.
</figcaption>

<!-- large image in a fix sized window with sliders: Might be useful for a parameterized example-->
<!-- <div style="width: 100%; height: 400px; overflow: auto"> -->
<!-- <div style="transform: scale(4); transform-origin: top left; width: 600px; height: 400px;"> -->
<!-- <object type="image/svg+xml" data="/assets/large.svg" width="100%"> -->
<!--   Your browser does not support interactive SVGs. -->
<!-- </object> -->
<!-- </div> -->
<!-- </div> -->

The corresponding `cycles` section reads like the following.

```yaml title="sirocco.yaml"
var_start_date: &root_start_date "2026-01-01T00:00"  # (1)!
var_stop_date: &root_stop_date "2026-08-01T00:00"
[...]
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
                - restart:
                    when:
                      after: *root_start_date
                    target_cycle:
                      lag: "-P2M"
            outputs:
              output_streams: [stream 1, stream 2]
              restart_out: [restart]
  - 4 monthly:
      cycling:
        start_date: *root_start_date
        stop_date: *root_stop_date
        period: "P4M"
      tasks:
        - post proc:
            inputs:
              in:
                - stream 1:
                    target_cycle:
                      lag: ["P0M", "P2M"]
                - stream 2:
                    target_cycle:
                      lag: ["P0M", "P2M"]
            outputs:
              out: [processed]
        - clean up:
            inputs:
              tbc:
                - boundary conditions:
                    target_cycle:
                      lag: ["P0M", "P2M"]
                - stream 1:
                    target_cycle:
                      lag: ["P0M", "P2M"]
                - stream 2:
                    target_cycle:
                      lag: ["P0M", "P2M"]
            wait_on: [post proc]
tasks:
  [...]
data:
  [...]
```

1.  These are not Sirocco specific entries but `yaml` [anchors and aliases :lucide-external-link:](https://yaml.cc/tutorial/advanced-features.html)

## `cycles` specifications

The first level of the `cycles` section is a sequence of mappings of cycle names to their description.

```yaml
cycles:
  - cycle name 1:
      [...]
  - cycle name 2:
      [...]
```

### `cycling`

```yaml
cycles:
  - cycle name 1:
      cycling:
        start_date: "START_DATE"
        stop_date: "STOP_DATE"
        period: "PERIOD"
```

**type**: mapping
<br>
**optional**: if omitted, the cycle will be considered a *one-off* cycle, *i.e.* happening only once.
<br>
**description**: recurrence of the cycle. `"START_DATE"`, `"STOP_DATE"` and `"PERIOD"` must all be specified in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601). Each occurrence of the cycle gets itself a start and stop date later named "occurrence start date" and "occurrence stop date" to avoid confusion. These are naturally computed by iterating every `"PERIOD"`, starting from `"START_DATE"`, the last occurrence stop date being exactly `"STOP DATE"` even if the duration of that last cycle occurrence is shorter than `"PERIOD"`. Finally, cycle occurrences, tasks ha-penning within it and associated generated pieces of data receive a `date` attribute which, by convention, is the cycle occurrence start date. It will be used later to target different task or data instances in ambiguous cases.

### `tasks`

```yaml
cycles:
  - cycle name 1:
      [...]
      tasks:
        - task name 1:
            inputs: [...]
            outputs: [...]
            wait_on: [...]
        - task name 2: [...]

```

**type**: sequence of mapping
<br>
**required**
<br>
**description**: The `tasks` subsection is a sequence of mappings of task names to graph related specifications, namely `inputs`, `outputs` and `wait_on`, each of them being optional.

!!! warning "Tasks declaration and references"

    These task names must be references to tasks declared in the [root level `tasks`](tasks) section, which can be confusing at first.

??? info "Tasks components"

    In reality a `components` level is introduced between the task name and `inputs` and `outputs` (`wait_on` stays right bellow the task name). This is hidden to the user for basic ["shell tasks"](/plugins/shell), while it needs to be explicitly provided for ["icon tasks"](/plugins/icon). It is omitted here for simplicity.

#### Ports

In all generality, from a task perspective, linking a piece of data as an input or an output is not sufficient to be able to use it unambiguously. In some cases, the data path might contain enough information to do so but it is not guaranteed. The missing piece of information is the role that this piece (potentially these pieces) of data plays for that specific task, which is what the **port** concept introduces. Ports are indeed specified as mappings between a role, the port name,  and a sequence of associated pieces of data. For instance, in [the example](#example), the `inputs` section for the task `model` is composed of 3 ports: `init`, `bc` and `restart_in`. Together with the inclusion of data as proper graph nodes, ports thus make Sirocco workflows truly composable, in that no task needs to make assumptions on the behavior of others, essentially which piece of data is written where.

#### `inputs`

**type**: mapping
<br>
**optional**
<br>
**description**: The `inputs` section is a mapping between port names and a sequence of associated data instances. Elements of that sequence might be a simple data name or, in ambiguous cases, a mapping of that data name to some more information.

!!! warning "Data declaration and references"

    Just like tasks, these data names must be references to data declared in the [root level `data`](data) section, which can be confusing at first.

There are two cases where the targeted data instance is unambiguous: if the data instance doesn't have any date or parameters (_e.g._ available data) or when the data was generated in the same cycle. This is for instance the case for the `bc` port of the `model` task. Even though the `boundary condition` data is generated at each occurrence of the `bi-monthly` cycle by the `pre proc` task, the default instance taken into account is the one generated in the same cycle as the current one. The configuration then looks like
```yaml
cycle name:
  [...]
  tasks:
    - task name:
        inputs:
          port name: ["data name"]
```

In other cases, it is necessary to provide more information about which data instance is targeted, which is enabled by the `target_cycle` and `parameters` specifications. On top of that, `when` can specify when the input data should be considered at all.
```yaml
cycle name:
  [...]
  tasks:
    - task name:
        inputs:
          port name:
            - data name:
              target_cycle: [...]
              parameters: [...]
              when: [...]
```

##### `target_cycle`

**type**: mapping
<br>
**optional**
<br>
**description**: As the name indicates, this setting gives us means to target a specific cycle occurrence. A relative `lag` can be given with
```yaml
target_cycle:
  lag: relative_lag
```
where `lag` is a duration specified in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or a sequence of them (_e.g._ the `stream` port in [the example](#example)) when targeting several occurrences, e.g. for the `post proc` task of the `4 monthly` cycle. The targeted data instance is the one whose `date` attribute is obtained by adding `relative_lag`, which can be negative, to the current cycle occurrence `date` (the one to which the task belongs).

Instead of specifying a relative lag, an absolute date (or, again, a sequence of them) can be given with
```yaml
target_cycle:
  date: absolute_date
```

##### `when`

**type**: mapping
<br>
**optional**
<br>
**description**: The `when` keyword can used to specify wether or not a data instance needs to be taken into account. This is typically used when the behavior of the task differs between the first (last) occurrence and the other ones. In the example above, the `model` task requires `initial conditions` as input at the first occurrence and later expects `restart`. The specification is the following
```yaml
when:
  at: at_date
  before: before_date
  after: after_date
```
`before` and `after` correspond to $\leq$ and $\geq$ in the date and time space. They can be simultaneously specified, while `at` is exclusive.

##### `parameters`

**type**: mapping
<br>
**optional**
<br>
**description**: Tasks and pieces of data can be parameterized. Specifying which instances are targeted, is done via
```yaml
parameters:
  parameter name: "mode_name"
```

!!! warning "Parameters declaration and references"

    Just like tasks and data, these parameter names must be references to parameters declared in the [root level `parameters`](parameters) section, which can be confusing at first.

For now, 2 options are supported, namely `"all"` and `"single"`.

 - `"all"` means that, regarding that parameter, all instances of the parameterized piece of data are taken into account.
 - `"single"` means that, regarding that parameter, only the data instance with the same parameter value as the task is taken into account. This involves that the task itself is parameterized by the same parameter.

!!! failure "Parameters not yet fully functional"

    Even though workflows with parameterized tasks and pieces of data can be generated, the parameter values are not yet exposed to the tasks themselves.



#### `outputs`

**type**: mapping
<br>
**optional**
<br>
**description**: In the same way as `inputs`, the `outputs` section maps port names to lists of data instances. Since there cannot be any ambiguity on the date and parameters of the later, there are no `target_cycle` and `parameters` specifications. `when` is also not available for now, even though one could imagine cases where a task would or not output some data depending on the cycle occurrence.

#### `wait_on`

**type**: mapping
<br>
**optional**
<br>
**description**:  On top of dependencies between tasks derived from the `inputs`/`outputs` relationships, `wait_on` enables the addition of explicit dependencies between tasks. Forcing a task to explicitly wait for the completion of some others can be very useful to prevent too early execution. In our case, such a dependency is introduced for the `pre proc` task on the occurrence of `model` 2 cycles before. This prevents the execution of all `pre proc` instances at the beginning of the workflow which could fill up the HPC with unnecessary data and jobs. Also the
`clean up` task has an explicit dependency on `post proc` to avoid deleting data the later still needs.

Concretely `wait_on` is a list of task instances. Exactly as input data instances, they can be further specified using the [`target_cycle`](#target_cycle), [`parameters`](#parameters) and [`when`](#when) keywords.

<!-- Local Variables: -->
<!-- jinx-local-words: "composable" -->
<!-- End: -->
