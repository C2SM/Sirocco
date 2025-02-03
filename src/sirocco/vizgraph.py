from __future__ import annotations

from colorsys import hsv_to_rgb
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from lxml import etree
from pygraphviz import AGraph

if TYPE_CHECKING:
    from sirocco.core.graph_items import Store
from sirocco.core import Workflow


def hsv_to_hex(h: float, s: float, v: float) -> str:
    r, g, b = hsv_to_rgb(h, s, v)
    return "#{:02x}{:02x}{:02x}".format(*map(round, (255 * r, 255 * g, 255 * b)))


def node_colors(h: float) -> dict[str, str]:
    fill = hsv_to_hex(h / 365, 0.15, 1)
    border = hsv_to_hex(h / 365, 1, 0.20)
    font = hsv_to_hex(h / 365, 1, 0.15)
    return {"fillcolor": fill, "color": border, "fontcolor": font}


class VizGraph:
    """Class for visualizing a Sirocco workflow"""

    node_base_kw: ClassVar[dict[str, Any]] = {"style": "filled", "fontname": "Fira Sans", "fontsize": 14, "penwidth": 2}
    edge_base_kw: ClassVar[dict[str, Any]] = {"color": "#77767B", "penwidth": 1.5}
    data_node_base_kw: ClassVar[dict[str, Any]] = node_base_kw | {"shape": "ellipse"}

    data_av_node_kw: ClassVar[dict[str, Any]] = data_node_base_kw | node_colors(116)
    data_gen_node_kw: ClassVar[dict[str, Any]] = data_node_base_kw | node_colors(214)
    task_node_kw: ClassVar[dict[str, Any]] = node_base_kw | {"shape": "box"} | node_colors(354)
    io_edge_kw: ClassVar[dict[str, Any]] = edge_base_kw
    wait_on_edge_kw: ClassVar[dict[str, Any]] = edge_base_kw | {"style": "dashed"}
    cluster_kw: ClassVar[dict[str, Any]] = {"bgcolor": "#F6F5F4", "color": None, "fontsize": 16}

    def __init__(self, name: str, cycles: Store, data: Store) -> None:
        self.name = name
        self.agraph = AGraph(name=name, fontname="Fira Sans", newrank=True)
        for data_node in data:
            gv_kw = self.data_av_node_kw if data_node.available else self.data_gen_node_kw
            self.agraph.add_node(data_node, tooltip=self.tooltip(data_node), label=data_node.name, **gv_kw)

        k = 1
        for cycle in cycles:
            # NOTE: For some reason, clusters need to have a unique name that starts with 'cluster'
            #       otherwise they are not taken into account. Hence the k index.
            cluster_nodes = []
            for task_node in cycle.tasks:
                cluster_nodes.append(task_node)
                self.agraph.add_node(
                    task_node, label=task_node.name, tooltip=self.tooltip(task_node), **self.task_node_kw
                )
                # NOTE: _ stands for the unsued port name
                for data_node, _ in task_node.inputs:
                    self.agraph.add_edge(data_node, task_node, **self.io_edge_kw)
                for data_node in task_node.outputs:
                    self.agraph.add_edge(task_node, data_node, **self.io_edge_kw)
                    cluster_nodes.append(data_node)
                for wait_task_node in task_node.wait_on:
                    self.agraph.add_edge(wait_task_node, task_node, **self.wait_on_edge_kw)
            self.agraph.add_subgraph(
                cluster_nodes,
                name=f"cluster_{cycle.name}_{k}",
                clusterrank="global",
                label=self.tooltip(cycle),
                tooltip=self.tooltip(cycle),
                **self.cluster_kw,
            )
            k += 1

    @staticmethod
    def tooltip(node) -> str:
        return "\n".join(chain([node.name], (f"  {k}: {v}" for k, v in node.coordinates.items())))

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
    def from_core_workflow(cls, workflow: Workflow):
        return cls(workflow.name, workflow.cycles, workflow.data)

    @classmethod
    def from_config_file(cls, config_path: str):
        return cls.from_core_workflow(Workflow.from_config_file(config_path))
