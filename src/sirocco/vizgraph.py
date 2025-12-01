from __future__ import annotations

import enum
from colorsys import hsv_to_rgb
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from lxml import etree
from pygraphviz import AGraph

from sirocco import core

if TYPE_CHECKING:
    from collections.abc import Iterable
    from core.graph_items import viz_status_t, GRAPH_ITEM_T


def hsv_to_hex(h: float, s: float, v: float) -> str:
    r, g, b = hsv_to_rgb(h, s, v)
    return "#{:02x}{:02x}{:02x}".format(*map(round, (255 * r, 255 * g, 255 * b)))


def node_colors(
    h: float, *, status: viz_status_t = "undefined"
) -> dict[str, str]:
    match status:
        case "undefined" | "waiting":
            fill = hsv_to_hex(h / 365, 0.15, 1)
            border = hsv_to_hex(h / 365, 1, 0.20)
            font = hsv_to_hex(h / 365, 1, 0.15)
        case "active":
            fill = hsv_to_hex(h / 365, 0.25, 1)
            border = hsv_to_hex(h / 365, 1, 0.3)
            font = hsv_to_hex(h / 365, 1, 0.2)
        case "inactive":
            fill = hsv_to_hex(h / 365, 0.03, 0.9)
            border = hsv_to_hex(h / 365, 0.03, 0.6)
            font = hsv_to_hex(h / 365, 0.03, 0.5)
    return {"fillcolor": fill, "color": border, "fontcolor": font}


def edge_color(
    h: float, *, status: viz_status_t = "undefined"
) -> dict[str, str]:
    match status:
        case "undefined" | "waiting":
            color = hsv_to_hex(h / 365, 0.05, 0.5)
        case "active":
            color = hsv_to_hex(h / 365, 0.05, 0.25)
        case "inactive":
            color = hsv_to_hex(h / 365, 0.05, 0.85)
    return {"color": color}


class Hue(enum.Enum):
    _value_: int
    DATA_AV = 116
    DATA_GEN = 214
    TASK = 354
    EDGE = 252


class EdgeFace(enum.Enum):
    _value_: dict[str, Any]
    __EDGE = {"penwidth": 1.5}  # noqa: RUF012
    __ACTIVE = {"penwidth": 2}  # noqa: RUF012
    __WAIT = {"style": "dashed"}  # noqa: RUF012

    EDGE = __EDGE | edge_color(Hue.EDGE.value) 
    EDGE_ACTIVE = __EDGE | __ACTIVE | edge_color(Hue.EDGE.value, status="active")
    EDGE_INACTIVE =  __EDGE | edge_color(Hue.EDGE.value, status="inactive")
    WAIT_ON_EDGE = EDGE | __WAIT
    WAIT_ON_EDGE_ACTIVE = EDGE_ACTIVE | __WAIT
    WAIT_ON_EDGE_INACTIVE = EDGE_INACTIVE | __WAIT

    @classmethod
    def from_status(
            cls, task_status: viz_status_t, *, wait: bool = False
    ) -> EdgeFace:
        if task_status == "active":
            return cls.WAIT_ON_EDGE_ACTIVE if wait else cls.EDGE_ACTIVE
        if task_status == "waiting":
            return cls.WAIT_ON_EDGE if wait else cls.EDGE
        if task_status == "inactive":
            return cls.WAIT_ON_EDGE_INACTIVE if wait else cls.EDGE_INACTIVE
        return cls.WAIT_ON_EDGE if wait else cls.EDGE


class NodeFace(enum.Enum):
    _value_: dict[str, Any]
    __NODE = {"style": "filled", "fontname": "Adwaita Sans", "fontsize": 14, "penwidth": 2}  # noqa: RUF012
    __DATA = __NODE | {"shape": "ellipse"}
    __TASK = __NODE | {"shape": "box"}
    __ACTIVE = {"penwidth": 3.5, "fontsize": 20}

    CLUSTER = {"bgcolor": "#F6F5F4", "color": None, "fontsize": 16}  # noqa: RUF012
    DATA_AV = __DATA | node_colors(Hue.DATA_AV.value)
    DATA_AV_ACTIVE = __DATA | __ACTIVE | node_colors(Hue.DATA_AV.value, status="active")
    DATA_AV_INACTIVE = __DATA | node_colors(Hue.DATA_AV.value, status="inactive")
    DATA_GEN = __DATA | node_colors(Hue.DATA_GEN.value)
    DATA_GEN_ACTIVE = __DATA | __ACTIVE | node_colors(Hue.DATA_GEN.value, status="active")
    DATA_GEN_INACTIVE = __DATA | node_colors(Hue.DATA_GEN.value, status="inactive")
    TASK = __TASK | node_colors(Hue.TASK.value)
    TASK_ACTIVE = __TASK | __ACTIVE | node_colors(Hue.TASK.value, status="active")
    TASK_INACTIVE = __TASK | node_colors(Hue.TASK.value, status="inactive")

    @classmethod
    def from_node(cls, node: core.GraphItem) -> NodeFace:
        match node:
            case core.Task():
                if node.viz_status == "active":
                    return cls.TASK_ACTIVE
                if node.viz_status == "waiting":
                    return cls.TASK
                if node.viz_status == "inactive":
                    return cls.TASK_INACTIVE
                return cls.TASK
            case core.AvailableData():
                if node.viz_status == "active":
                    return cls.DATA_AV_ACTIVE
                if node.viz_status == "waiting":
                    return cls.DATA_AV
                if node.viz_status == "inactive":
                    return cls.DATA_AV_INACTIVE
                return cls.DATA_AV
            case core.GeneratedData():
                if node.viz_status == "active":
                    return cls.DATA_GEN_ACTIVE
                if node.viz_status == "waiting":
                    return cls.DATA_GEN
                if node.viz_status == "inactive":
                    return cls.DATA_GEN_INACTIVE
                return cls.DATA_GEN
            case core.Cycle():
                return cls.CLUSTER
            case _:
                msg = "unrecognized node type"
                raise ValueError(msg)


class VizGraph:
    """Class for visualizing a Sirocco workflow"""

    def __init__(
        self,
        name: str,
        tasks: Iterable[core.Task],
        data: Iterable[core.Data],
        cycles: Iterable[core.Cycle],
    ) -> None:
        self.name = name
        # self.agraph = AGraph(name=name, fontname="Fira Sans", newrank=True)
        self.agraph = AGraph(name=name, fontname="Adwaita Sans", newrank=True)
        for data_node in data:
            self.agraph.add_node(
                data_node,
                tooltip=self.tooltip(data_node),
                label=data_node.name,
                **NodeFace.from_node(data_node).value,
            )

        for task in tasks:
            self.agraph.add_node(
                task,
                tooltip=self.tooltip(task),
                label=task.name,
                **NodeFace.from_node(task).value,
            )

        # NOTE: For some reason, clusters need to have a unique name that starts with 'cluster'
        #       otherwise they are not taken into account. Hence the k index.
        k = 1
        for cycle in cycles:
            cluster_nodes: list[core.Data | core.Task] = []
            for task in cycle.tasks:
                cluster_nodes.append(task)
                for data_node in task.input_data_nodes():
                    self.agraph.add_edge(
                        data_node,
                        task,
                        **EdgeFace.from_status(task.viz_status).value,
                    )
                for data_node in task.output_data_nodes():
                    if isinstance(data_node, core.GeneratedData):
                        cluster_nodes.append(data_node)
                    self.agraph.add_edge(
                        task,
                        data_node,
                        **EdgeFace.from_status(task.viz_status).value,
                    )
                for wait_task in task.wait_on:
                    self.agraph.add_edge(
                        wait_task,
                        task,
                        **EdgeFace.from_status(task.viz_status, wait=True).value,
                    )
            self.agraph.add_subgraph(
                cluster_nodes,
                name=f"cluster_{cycle.name}_{k}",
                clusterrank="global",
                label=self.tooltip(cycle),
                tooltip=self.tooltip(cycle),
                **NodeFace.from_node(cycle).value,
            )
            k += 1

    @staticmethod
    def tooltip(node: core.GraphItem) -> str:
        tt: list[str] = [node.name]
        tt.extend(f"  {k}: {v}" for k, v in node.coordinates.items())
        if isinstance(node, core.Task) and node.viz_status != "undefined":
            tt.append(f"  rank: {node.rank}")
        return "\n".join(tt)

    def draw(self, file_path: Path | None = None, **kwargs):
        # draw graphviz dot graph to svg file
        self.agraph.layout(prog="dot")
        if file_path is None:
            file_path = Path(f"./{self.name}.svg")

        self.agraph.draw(path=file_path, format="svg", **kwargs)

        # Add interactive capabilities to the svg graph thanks to
        # https://github.com/BartBrood/dynamic-SVG-from-Graphviz

        # Parse svg
        svg = etree.parse(file_path)  # noqa: S320 this svg is safe as generated internaly
        svg_root = svg.getroot()
        # Add 'onload' tag
        svg_root.set("onload", "addInteractivity(evt)")
        # Add css style for interactivity
        this_dir = Path(__file__).parent
        style_file_path = this_dir / "svg-interactive-style.css"
        node = etree.Element("style")
        node.text = style_file_path.read_text()
        svg_root.append(node)
        # Add scripts
        js_file_path = this_dir / "svg-interactive-script.js"
        node = etree.Element("script")
        node.text = etree.CDATA(js_file_path.read_text())
        svg_root.append(node)

        # write svg again
        svg.write(file_path)

    @classmethod
    def from_core_workflow(cls, workflow: core.Workflow) -> Self:
        return cls(name=workflow.name, tasks=workflow.tasks, data=workflow.data, cycles=workflow.cycles)

    @classmethod
    def from_config_file(cls, config_path: str) -> Self:
        return cls.from_core_workflow(core.Workflow.from_config_file(config_path))

    @staticmethod
    def add_to_unique_list(item: GRAPH_ITEM_T, item_list: list[GRAPH_ITEM_T], labels: list[str]) -> None:
        if item.label not in labels:
            labels.append(item.label)
            item_list.append(item)
            # default to inactive status
            if isinstance(item, core.Data | core.Task):
                item.viz_status = "inactive"
    
    @classmethod
    def from_status_workflow(cls, workflow: core.Workflow) -> Self:
        cycles: list[core.Cycle] = []
        tasks: list[core.Task] = []
        data: list[core.Data] = []
        cycle_labels: list[str] = []
        task_labels: list[str] = []
        data_labels: list[str] = []

        # Identify cycles
        for task in chain(*(workflow.front[:])):
            cls.add_to_unique_list(task.cycle, cycles, cycle_labels)

        # Identify tasks and data nodes
        for task in chain(*(cycle.tasks for cycle in cycles)):
            cls.add_to_unique_list(task, tasks, task_labels)
            for data_node in chain(task.input_data_nodes(), task.output_data_nodes()):
                cls.add_to_unique_list(data_node, data, data_labels)
            for wait_task in task.wait_on:
                cls.add_to_unique_list(wait_task, tasks, task_labels)

        # Set status for front tasks and data
        for task in workflow.front[0]:
            task.viz_status = "active"
            for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
                data_node.viz_status = "active"
        for task in chain(*(workflow.front[1:])):
            task.viz_status = "waiting"
            for data_node in chain(task.output_data_nodes(), task.input_data_nodes()):
                if data_node.viz_status == "inactive":
                    data_node.viz_status = "waiting"
                
        return cls(name=workflow.name, tasks=tasks, data=data, cycles=cycles)
