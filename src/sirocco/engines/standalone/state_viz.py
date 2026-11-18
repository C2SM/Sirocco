"""State visualization of a running standalone workflow (`sirocco stviz`).

Builds a generic `VizGraph` restricted to the cycles around the current front,
with display statuses and front ranks taken from the engine:

- tasks of the first front generation and their data are "active",
- tasks of the next generations are "waiting", and so is their data unless already active,
- everything else in these cycles is "inactive".
"""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

from sirocco.vizgraph import VizGraph

if TYPE_CHECKING:
    from sirocco import core
    from sirocco.engines.standalone.engine import StandaloneEngine
    from sirocco.vizgraph import VIZ_STATUS_T


def state_vizgraph(engine: StandaloneEngine) -> VizGraph:
    # Cycles of the front tasks, with all their tasks, the tasks these wait on and their data.
    # Dicts keyed by label keep each item once, in order of first appearance.
    cycles: dict[str, core.Cycle] = {task.cycle.label: task.cycle for task in chain(*engine.front)}
    tasks: dict[str, core.Task] = {}
    data: dict[str, core.Data] = {}
    for task in chain(*(cycle.tasks for cycle in cycles.values())):
        tasks.setdefault(task.label, task)
        for data_node in chain(task.input_data_nodes(), task.output_data_nodes()):
            data.setdefault(data_node.label, data_node)
        for wait_task in task.wait_on:
            tasks.setdefault(wait_task.label, wait_task)

    # Everything is inactive, except front tasks and their data
    statuses: dict[str, VIZ_STATUS_T] = dict.fromkeys(chain(tasks, data), "inactive")
    for task in engine.front[0]:
        statuses[task.label] = "active"
        for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
            statuses[data_node.label] = "active"
    for task in chain(*engine.front[1:]):
        statuses[task.label] = "waiting"
        for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
            if statuses.get(data_node.label) == "inactive":
                statuses[data_node.label] = "waiting"

    return VizGraph(
        name=engine.workflow.name,
        tasks=tasks.values(),
        data=data.values(),
        cycles=cycles.values(),
        statuses=statuses,
        ranks={label: engine.rank(task) for label, task in tasks.items()},
    )
