---
title: Cycles
icon: lucide/refresh-cw
---

<!-- Cap headersize to 0.8rem for this page. Otherwise specifications like `target_cycle` don't read well -->
<style>
h4, h5, h6 {
  font-size: 0.8rem !important;
  text-transform: none !important;
  font-weight: bold;
}
</style>

# Cycles

Sirocco represents workflows as [directed acyclic graphs :lucide-external-link:](https://en.wikipedia.org/wiki/Directed_acyclic_graph), like the one displayed below, where nodes are instances of a task or a piece of data. Their **content**, _e.g._ task scripts or data path, is declared in the dedicated [`tasks`](tasks.md) and [`data`](data.md) sections while the current `cycles` section is only concerned with the **graph topology**, _i.e._ the relationships between the nodes. Yet, each mention of them, as well as potential [parameters](parameters.md), must refer to a declared counterpart.

In order to describe the graph topology, the `cycles` section describes:

- the **cycles** themselves, *i.e.* sets of recurring tasks sharing a common periodicity.
- tasks **inputs** and **outputs**
- potential added *manual* dependencies between tasks, called **wait_on**, on top of the ones derived from inputs/outputs relationships.

## Example

The example below shows a short but comprehensive graph covering most of the dedicated Sirocco syntax.

!!! tip "hover and click"

    In case the following image is too small, you can hover on the nodes and cycles to display information with a larger font. Also clicking nodes will highlight direct parents and children relationships.

<!-- Use object to keep the embedded javascript runnable -->
<object type="image/svg+xml" data="../../assets/example_config/example.svg" width="100%">
  Your browser does not support interactive SVGs.
</object>
<figcaption markdown="span"  style="text-align: left;  font-style: normal; font-size: smaller;">
*Example workflow graph*. The vertical position of a node indicates its rank, starting with 0 (no parents) at the top. Red rectangles represent **tasks**, green and blue ellipses represent **available data** and  **generated data** and light rectangular areas indicate **cycle** occurrences. Solid lines represent **input/output** relationships while dashed lines indicate a **`wait_on`** dependency.
</figcaption>

The corresponding `cycles` section reads like the following.

```sirocco-yaml title="sirocco.yaml"
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
                - restart_file:
                    when:
                      after: *root_start_date
                    target_cycle:
                      lag: "-P2M"
            outputs:
              output_streams: [stream 1, stream 2]
              restart_out: [restart_file]
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

1.  These are not Sirocco specific entries but YAML [anchors and aliases :lucide-external-link:](https://yaml.cc/tutorial/advanced-features.html)

## `cycles` specifications

The first level of the `cycles` section is a sequence of mappings of cycle names to their description.

```sirocco-yaml
cycles:
  - cycle name 1:
      [...]
  - cycle name 2:
      [...]
```

### `cycling`

```sirocco-yaml
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
**description**: recurrence of the cycle. `"START_DATE"`, `"STOP_DATE"` and `"PERIOD"` must all be specified in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601). Each occurrence of the cycle gets itself a start and stop date later named "occurrence start date" and "occurrence stop date" to avoid confusion. These are naturally computed by iterating every `"PERIOD"`, starting from `"START_DATE"`, the last occurrence stop date being exactly `"STOP_DATE"` even if the duration of that last cycle occurrence is shorter than `"PERIOD"`. Finally, cycle occurrences, tasks happenning within it and associated generated pieces of data receive a `date` attribute which, by convention, is the cycle occurrence start date. It will be used later to target different task or data instances in ambiguous cases.

### `tasks`

```sirocco-yaml
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

**type**: sequence of mappings
<br>
**required**
<br>
**description**: The `tasks` subsection is a sequence of mappings of task names to graph related specifications, namely `inputs`, `outputs` and `wait_on`, each of them being optional.

!!! warning "Tasks declaration and references"

    These task names must be references to tasks declared in the [root level `tasks`](tasks.md) section, which can be confusing at first.

!!! info "Tasks components"

    In reality a `components` level is introduced between the task name and `inputs` and `outputs` (`wait_on` stays right below the task name). This is hidden to the user for basic ["shell tasks"](../plugins/shell.md), while it needs to be explicitly provided for ["icon tasks"](../plugins/icon.md#components). It is omitted here for simplicity.

#### Ports

In all generality, from a task perspective, linking a piece of data as an input or an output is not sufficient to be able to use it unambiguously. In some cases, the data path might contain enough information to do so but it is not guaranteed. The missing piece of information is the role that this piece (potentially these pieces) of data plays for that specific task, which is what the **port** concept introduces. Ports are indeed specified as mappings between a role, the port name,  and a sequence of associated pieces of data. For instance, in [the example](#example), the `inputs` section for the task `model` is composed of 3 ports: `init`, `bc` and `restart_in`. They can also be understood as a formal equivalent of command line arguments.  Together with the inclusion of data as proper graph nodes, ports thus make Sirocco workflows truly composable, in that no task needs to make assumptions on the behavior of others, essentially which piece of data is written where.

#### `inputs`

**type**: mapping
<br>
**optional**
<br>
**description**: The `inputs` section is a mapping between port names and a sequence of associated data instances. Elements of that sequence might be a simple data name or, in ambiguous cases, a mapping of that data name to [targeting specifications](#targeting-nodes). Note that some [special port names](#special-input-linking-ports) are reserved for a specific behavior.

!!! warning "Data declaration and references"

    Just like tasks, these data names must be references to data declared in the [root level `data`](data.md) section, which can be confusing at first.

There are two cases where the targeted data instance is unambiguous: if the data instance doesn't have any date or parameters (_e.g._ available data) or when the data was generated in the same cycle. This is for instance the case for the `bc` port of the `model` task. Even though the `boundary conditions` data is generated at each occurrence of the `bi-monthly` cycle by the `pre proc` task, the default instance taken into account is the one generated in the same cycle as the current one. The configuration then looks like
```sirocco-yaml
cycle name:
  [...]
  tasks:
    - task name:
        inputs:
          port name: ["data name"]
```

In other cases, it is necessary to provide more information about which data instance is targeted, using the [`target_cycle`](#target_cycle), [`when`](#when) and [`parameters`](#parameters) specifications described in [Targeting nodes](#targeting-nodes).
```sirocco-yaml
cycle name:
  [...]
  tasks:
    - task name:
        inputs:
          port name:
            - data name:
                target_cycle: [...]
                when: [...]
                parameters: [...]
```

##### Special input linking ports

As explained in the [definition above](#ports), ports are required to define inputs and outputs in all generality but can sometimes be superfluous. In the case where a task expects inputs with a predefined path or name (or has defaults for it), Sirocco provides the two following reserved port names that will simply link the targeted piece(s) of data in the task run directory.

###### `link`

**type**: sequence
<br>
**optional**
<br>
**description**: link any referenced data instance in the task run directory. Since data might not exist yet at submission time, linking happens at run time through injected commands in the submitted task run script.

###### `link_content`

**type**: sequence
<br>
**optional**
<br>
**description**: assume each referenced data instance is a directory and link all elements under it in the task run directory. Since data might not exist yet at submission time, linking happens at run time through injected commands in the submitted task run script.

#### `outputs`

**type**: mapping
<br>
**optional**
<br>
**description**: In the same way as `inputs`, the `outputs` section maps port names to lists of data instances. Since there cannot be any ambiguity on the date and parameters of the latter, the [targeting specifications](#targeting-nodes) are not available. In particular, `when` is not available for now, even though one could imagine cases where a task would or would not output some data depending on the cycle occurrence.

#### `wait_on`

**type**: sequence
<br>
**optional**
<br>
**description**:  On top of dependencies between tasks derived from the `inputs`/`outputs` relationships, `wait_on` enables the addition of explicit dependencies between tasks. Forcing a task to explicitly wait for the completion of some others can be very useful to prevent too early execution. In our case, such a dependency is introduced for the `pre proc` task on the occurrence of `model` 2 cycles before. This prevents the execution of all `pre proc` occurrences at the beginning of the workflow which could fill up the HPC with unnecessary data and jobs. Also the `clean up` task has an explicit dependency on `post proc` to avoid deleting data the latter still needs.

Concretely `wait_on` is a sequence of task instances. Elements of that sequence might be a simple task name or a mapping of that task name to [targeting specifications](#targeting-nodes), exactly as for `inputs`:
```sirocco-yaml
cycle name:
  [...]
  tasks:
    - task name:
        wait_on:
          - other task name:
              target_cycle: [...]
              when: [...]
              parameters: [...]
```

#### Targeting nodes

When a task refers to another node of the graph, either a data instance in [`inputs`](#inputs) or a task instance in [`wait_on`](#wait_on), the following specifications select which instances of that **targeted node** are meant. They are all optional and expressed relative to the **current task**, _i.e._ the task instance being described, with its own `date` and parameter values. Without any of them, the targeted instance is the one belonging to the same cycle occurrence as the current task (and, for parameters, see the default [below](#parameters)).

##### `target_cycle`

**type**: mapping
<br>
**optional**
<br>
**description**: As the name indicates, this setting gives us means to target a specific cycle occurrence. A relative `lag` can be given with
```sirocco-yaml
target_cycle:
  lag: relative_lag
```
where `lag` is a duration specified in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or a sequence of them when targeting several occurrences, _e.g._ for the `post proc` task of the `4 monthly` cycle. The targeted instance is the one whose `date` attribute is obtained by adding `relative_lag`, which can be negative, to the `date` of the current task (the start date of the cycle occurrence it belongs to).

Instead of specifying a relative lag, an absolute date (or, again, a sequence of them) can be given with
```sirocco-yaml
target_cycle:
  date: absolute_date
```

###### Partial dates

Dates given with `date` can also be _partially fixed_: some of their fields are replaced by placeholders which are filled in with the corresponding fields of the current task `date`. The supported placeholders are `%Y` (year), `%m` (month), `%d` (day), `%H` (hour), `%M` (minute) and `%S` (second). Apart from these placeholders, the date must follow the ISO 8601 format `YYYY-MM-DDThh:mm[:ss]`. For instance, `'%Y-01-01T00:00'` targets the instance dated January 1st of the current task year.

This is typically useful when cycles of different periods interact and the targeted occurrence cannot be expressed as a constant lag. In the example below, the half-yearly `biannual_postprocessing` task processes the output of the yearly `icon` task of the same year: both of its occurrences in a given year target the `icon` occurrence starting on January 1st of that year, which would require a lag of `P0M` for the first one and `-P6M` for the second one.
```sirocco-yaml
cycles:
  - yearly:
      cycling:
        start_date: "2026-01-01T00:00"
        stop_date: "2028-01-01T00:00"
        period: "P1Y"
      tasks:
        - icon:
            outputs:
              output: [annual_icon]
  - half_yearly:
      cycling:
        start_date: "2026-01-01T00:00"
        stop_date: "2028-01-01T00:00"
        period: "P6M"
      tasks:
        - biannual_postprocessing:
            inputs:
              input:
                - annual_icon:
                    target_cycle:
                      date: "%Y-01-01T00:00"
```

!!! warning "Partial dates restrictions"

    - Partial dates require the current task to have a `date`, _i.e._ to belong to a recurring cycle.
    - Once resolved, the date must correspond to an existing instance of the targeted node, exactly as for absolute dates.
    - Partial dates are only supported by `target_cycle`, not by [`when`](#when).

##### `when`

**type**: mapping
<br>
**optional**
<br>
**description**: The `when` keyword can be used to specify whether or not the targeted node needs to be taken into account at all, depending on the `date` of the current task. This is typically used when the behavior of the task differs between the first (last) occurrence and the other ones. In the example above, the `model` task requires `initial conditions` as input at the first occurrence and later expects `restart_file`. Similarly, `pre proc` only waits on `model` after `2026-03-01T00:00`. The specification is the following
```sirocco-yaml
when:
  at: at_date
  before: before_date
  after: after_date
```
`before` and `after` correspond to $<$ and $>$, so _strictly_ lower and greater, for dates. They can be simultaneously specified, while `at` is exclusive.

##### `parameters`

**type**: mapping
<br>
**optional**
<br>
**description**: Tasks and pieces of data can be parameterized. When the targeted node is parameterized, specifying which of its instances are targeted is done, parameter by parameter, via
```sirocco-yaml
parameters:
  parameter name: "targeting_mode"
```

!!! warning "Parameters declaration and references"

    Just like tasks and data, these parameter names must be references to parameters declared in the [root level `parameters`](parameters.md) section, which can be confusing at first.

2 targeting modes are supported: `"all"` and `"single"`.

 - `"all"` means that, regarding that parameter, all instances of the targeted node are taken into account. **This is the default** for any parameter of the targeted node which is not mentioned.
 - `"single"` means that, regarding that parameter, only the instance of the targeted node with the same parameter value as the current task is taken into account. This requires that the current task itself is parameterized by the same parameter.

See [Parameters](parameters.md) for the complete set of rules and an [annotated example](parameters.md#example-of-a-parameterized-workflow).

!!! failure "Parameters not yet fully functional"

    Even though workflows with parameterized tasks and pieces of data can be generated, the parameter values are not yet exposed to the tasks themselves.

<!-- Local Variables: -->
<!-- jinx-local-words: "composable" -->
<!-- End: -->
