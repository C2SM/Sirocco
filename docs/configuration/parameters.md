---
title: Parameters
icon: lucide/variable
---

# Parameters

Parameterized workflows are supported by Sirocco. In order to make tasks and data instances parameterized, parameters declared in this root level section should be referenced in the task ([here](../../configuration/tasks/#parameters)) and data ([here](../../configuration/data/#parameters) or [here](../../configuration/data/#parameters_1)) descriptions. In order to build the Workflow graph, it should be specified in the `cycles` section [which kind of relationship to use](../../configuration/cycles/#parameters).

!!! warning "full parameter space"

    Note that a task or data instance will be generated for each possible combination of the parameters attached to it, _i.e._ the full matrix will be used. It is not possible to select only a subset of these possible combinations.
