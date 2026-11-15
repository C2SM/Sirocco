"""Status visualization of a running standalone workflow (`sirocco stviz`).

Builds a generic `VizGraph` restricted to the cycles around the current front,
with display statuses and front ranks taken from the engine:

- tasks of the first front generation and their data are "active",
- tasks of the next generations are "waiting", and so is their data unless already active,
- everything else in these cycles is "inactive".
"""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

from sirocco import core
from sirocco.vizgraph import VizGraph

if TYPE_CHECKING:
    from sirocco.core.graph_items import GRAPH_ITEM_T
    from sirocco.engines.standalone.engine import StandaloneEngine
    from sirocco.vizgraph import VIZ_STATUS_T


def _add_to_unique_list(
    item: GRAPH_ITEM_T,
    item_list: list[GRAPH_ITEM_T],
    labels: list[str],
    statuses: dict[str, VIZ_STATUS_T],
) -> None:
    if item.label not in labels:
        labels.append(item.label)
        item_list.append(item)
        # default to inactive status
        if isinstance(item, core.Data | core.Task):
            statuses[item.label] = "inactive"


def status_vizgraph(engine: StandaloneEngine) -> VizGraph:
    cycles: list[core.Cycle] = []
    tasks: list[core.Task] = []
    data: list[core.Data] = []
    cycle_labels: list[str] = []
    task_labels: list[str] = []
    data_labels: list[str] = []
    statuses: dict[str, VIZ_STATUS_T] = {}

    # Identify cycles
    for task in chain(*(engine.front[:])):
        _add_to_unique_list(task.cycle, cycles, cycle_labels, statuses)

    # Identify tasks and data nodes
    for task in chain(*(cycle.tasks for cycle in cycles)):
        _add_to_unique_list(task, tasks, task_labels, statuses)
        for data_node in chain(task.input_data_nodes(), task.output_data_nodes()):
            _add_to_unique_list(data_node, data, data_labels, statuses)
        for wait_task in task.wait_on:
            _add_to_unique_list(wait_task, tasks, task_labels, statuses)

    # Set status for front tasks and data
    for task in engine.front[0]:
        statuses[task.label] = "active"
        for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
            statuses[data_node.label] = "active"
    for task in chain(*(engine.front[1:])):
        statuses[task.label] = "waiting"
        for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
            if statuses.get(data_node.label) == "inactive":
                statuses[data_node.label] = "waiting"

    ranks = {task.label: engine.rank(task) for task in tasks}
    return VizGraph(
        name=engine.workflow.name,
        tasks=tasks,
        data=data,
        cycles=cycles,
        statuses=statuses,
        ranks=ranks,
    )
