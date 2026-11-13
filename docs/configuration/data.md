---
title: Data
icon: lucide/database
---

# Data

In the Sirocco context, what is considered as a piece of data, or data node in the graph perspective, is a piece of information stored on a file system at a certain location and produced by a piece of software. This definition distinguishes it, especially in the input case, from human produced files dedicated to task configuration. A FORTRAN namelist, for instance is not an input data node to a task but part of its configuration. As such, it belongs to the [workflow configuration](../../configuration/configuration).

In the same way as [`tasks`](../../configuration/tasks), the root level `data` section hosts data properties but no graph related information like input/output relationships which is the topic of the [`cycles`](../../configuration/cycles) section. Any data referred to in `cycles` must be declared here in `data`.

Sirocco has 2 types of data: already available or generated as output by a task. The root level `data` section of the [example graph shown in Cycles](../../configuration/cycles/#example) looks like the following:
```sirocco-yaml
data:
  available:
     - raw input data:
         path: "/path/to/raw_input_data"
     - initial conditions:
         path: "/path/to/initial_conditions"
  generated:
     - boundary conditions:
         path: processed_data
     - stream 1: {}
     - stream 2: {}
     - restart_file: {}
     - processed:
         path: processed_output
```

### `available`

**type**: sequence
<br>
**optional**
<br>
**description**: Sequence of mappings from data names to their properties.

#### `path`

**type**: string
<br>
**required**
<br>
**description**: Absolute path to the piece of data.

#### `parameters`

**type**: sequence of strings
<br>
**optional**
<br>
**description**: Sequence of parameter names which must be declared in [the corresponding root level section](parameters.md#specifications). One instance is created for each combination of their values. Available data instances have no date. See [Parameters](parameters.md) for how parameters are declared, attached and targeted across sections.

### `generated`

**type**: sequence
<br>
**optional**
<br>
**description**: Sequence of mappings from data names to their properties.

#### `path`

**type**: string
<br>
**optional**
<br>
**description**: Path to the piece of data. Relative path will be resolved with respect to the run directory of the task of which it is an output. In some cases, like specific plugin output ports, `path` can be ignored and the data path is inferred from the task behavior.

#### `parameters`

**type**: sequence of strings
<br>
**optional**
<br>
**description**: Sequence of parameter names which must be declared in [the corresponding root level section](parameters.md#specifications). One instance is created for each combination of their values. Generated data must carry exactly the same parameters as the task producing it. See [Parameters](parameters.md) for how parameters are declared, attached and targeted across sections.
