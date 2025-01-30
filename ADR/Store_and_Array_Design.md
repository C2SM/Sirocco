# `Sirocco.core.graph_items.Store` Design

# Initial version [2024-11-11] 

## Understanding the intended usage

In the current yaml format we specify all data nodes in the same way, whether they are

- determined before we start (avalilable)
- generated once (generated)
- generated periodically in a cycle (generated)

Although available and generated data nodes are in separate sections


```yaml
data:
  available:
    - grid_file:
        type: file
        src: $PWD/examples/files/data/grid
    - obs_data:
        type: file
        src: $PWD/examples/files/data/obs_data
    ...
  generated:
    - extpar_file:
        type: file
        src: output
    - icon_input:
        type: file
        src: output
    - icon_restart:
        type: file
        format: ncdf
        src: restart
    ...
```


And they are eventually turned into the same data structure (`core.Data`) with an optional `.date` attribute, which is none except for the cyclically generated case.

When building the "unrolled" dependency graph, we also don't know whether we are building a recurring or "one-off" task node, except from their context:

```yaml
cycles:
  - bimonthly_tasks:
      start_date: *root_start_date
      end_date: *root_end_date
      period: P2M
      tasks:
        - icon:  # recurring task is in a cycle with start & end dates
            inputs:
              - icon_restart:
                  lag: -P2M
            outputs: [icon_output, icon_restart]
  - lastly:
      tasks:
        - cleanup:  # one-off task looks the same but is in a cycle without start & end dates
            depends:
              - icon:
                  date: 2026-05-01T00:00

```

There is again only one data structure for tasks, whether they are recurring or not, and data and tasks are stored side-by side, as nodes in the same graph.

A further constraint for the design at the moment is that we want to distinguish three different cases when accessing a data point / task:

- one-off (access with `None` as date): return if stored, KeyError if not
- recurring (access with a valid date):
    - return node if stored and there is a node for the date
    - return `None` if the date is too earlier / later than the earliest / latest stored node
    - ValueError if the date is in the right range but there is no node stored for it

**[SEE UPDATE]** To this end the `TimeSeries` data structure was introduced, which takes care of storing all the data points by date for recurring nodes.

```python
icon_output = TimeSeries()
icon_output[datetime.fromisoformat("2024-01-01")] = Data.from_config(...)
icon_output[datetime.fromisoformat("2025-01-01")] = Data.from_config(...)

icon_output.start_date  # is now 2024-01-01
icon_output.end_date  # is now 2025-01-01
icon_output[datetime.fromisoformat("2024-01-01")]  # will return the first entry
icon_output[datetime.fromisoformat("2026-01-01")]  # will return None and log a warning
icon_output[datetime.fromisoformat("2024-06-01")]  # will raise an Error
```

This means the checking logic to decide whether we are storing a one-off data point / task or a recurring one (in this case we initialize a `TimeSeries` for it) has to go somewhere. The choices are:

- At creation of the "unrolled" nodes (this is currently done in nested for loops and branches would increase the complexity of that code even more)
    - pro: no custom container needed
    - con: either very complex or requires twice as many containers to keep recurring and one-offs apart

```python
data: dict[str, node | TimeSeries]
for ...:
    for ...:
        for ...:
            ...
            if date_or_none_from_context:
                data[name][date_from_config_or_none] = Data.from_config(...)
            else:
                data[name] = Data.from_config(...) # this might be a different container to simplify access logic
...
# repeat the same thing later for tasks
# and on access later
for name, item in data.items()
    if isinstance(item, TimeSeries):
        if not access_date:
            raise ... # we must access with a date
        else:
            data_point = item[access_date]
    else:
        data_point = item
    ...
...
        
# under the assumption that we are looping over unrolled nodes and do again not know whether they are recurring. If they are stored separately, this would be simpler but twice as many loops.

for name, data_point in one_off_data.items():
    ...
    
for name, data_series in recurring_data.items():
    ...
```

- At insertion into the container we use (the current choice with `Store`)
    - pro:
        - reduces the amount of containers
        - reduces the complexity of code interfacing with `core.WorkFlow`
    - con: additional container class to maintain (however, it does *not* need to conform to standard container interfaces)

```python

data = Store()
for ...:
    for ...:
        for ...:
            data[name, date_or_none_from_context] = Data.from_config(...)
            
...
# on access later
for name in data:
    data_point = data[name, access_date_or_none]
    ...
    
# or simply
for data_point in data.values():
    name = data.name
    date_or_none = data.date
    ...
```

If we were not using `TimeSeries`, this would open up the following additional option:

- Store in a flat mapping with (name, date) as the key instead of T
    - pro: can use a dict
    - con:
        - (unless above constraint is dropped): the logic in `TimeSeries` would have to be implemented external to the mapping and would be more complex.
            - either the mapping would be custom and do the same job as `TimeSeries`, except for multiple recurring data nodes
            - or the functionality would have to be implemented external to a standard mapping and would have to do even more checking
        - If not hosted in the `Workflow` class directly, a cumbersome logic will have to be reproduced each time we need to access the nodes, like generating the `WorkGraph` or the visualization graph. If hosted in `WorkFlow`, this is not less maintenance than the `Store` and `TimeSeries` classes but less clean.

## Temporary Conclusion

All-in-all we (Rico, Matthieu) think `Store` is a good enough design for now, as the maintenance burden is low, given that `Sirocco` is more of an app and less of a library. Therefore `Store` should not be confronted with expectations to support any `Mapping` functionality beyond what we need inside `Sirocco` itself.

## Further developments potentially affecting this design

We will at some point introduce parameterized tasks (and thus data as well). This will add other dimensions to the `Store`. 

## Reasons to change

- If we find ourselves implementing more and more standard container functionality on `Store` it is time to reconsider.
- If the code for turning the IR into a WorkGraph suffers from additional complexity due to the design of `Store`, then it needs to be changed
- Potentially, changes to the config schema might necessitate or unlock a different design here


# UPDATE [2025-01-29]

## The `Array` class

Since the introduction of parameterized tasks, the `Store` and `Timeseries` design evolved.

The `Timeseries` class became the more generic `Array` class. This makes the date part of the `Array` dimensions, along with potential parameters. Objects stored in an `Array` are accessed with their `coordinates` which is a `dict` mapping the dimension name to its value. `Store` is now a container for `Array` objects.

The following 2 changes also simplified the code and made it cleaner:
- Accessing nodes without any date or parameter is simplified as `Array` allows for empty coordinates (`{}`) so that these cases are not captured and treated in a special way in `Store` anymore.
- The need for special handling of nodes with dates out of range (just ignoring them, which is rather dirty) also disappeared with the introduction of the `when` keyword in the config format.


