import dataclasses
import functools
import textwrap
from typing import Any

from termcolor import colored

from sirocco import core


@dataclasses.dataclass(kw_only=True)
class PrettyPrinter:
    """
    Pretty print unrolled workflow graph elements in a reproducible and human readable format.

    This can be used to compare workflow graphs by their string representation.
    Colored output can be enabled by setting ".colors" to True, this will use terminal control characters,
    which makes it less suited for uses other than human viewing.
    """

    indentation: int = 2  # how many spaces to indent block content by
    colors: bool = False  # True for color output (term control chars)

    def indent(self, string: str) -> str:
        """Indent by the amount set on the instance"""
        return textwrap.indent(string, prefix=" " * self.indentation)

    def as_block(self, header: str, body: str) -> str:
        """
        Format as a block with a header line and indented lines of block body text.

        Example:

        >>> print(PrettyPrinter().as_block("header", "foo\\nbar"))
        header:
          foo
          bar
        """
        return f"{header}:\n{self.indent(body)}"

    def as_item(self, content: str) -> str:
        """
        Format as an item in an unordered list.

        Works for single lines as well as multi line (block) content.

        Example:

        >>> print(PrettyPrinter().as_item("foo"))
        - foo

        >>> pp = PrettyPrinter()
        >>> print(pp.as_item(pp.as_block("header", "multiple\\nlines\\nof text")))
        - header:
            multiple
            lines
            of text
        """
        if not content:
            return "- "
        lines = content.splitlines()
        if len(lines) == 1:
            return f"- {content}"
        header = lines[0]
        body = textwrap.indent("\n".join(lines[1:]), prefix="  ")
        return f"- {header}\n{body}"

    @functools.singledispatchmethod
    def format(self, obj: Any) -> str:
        """
        Dispatch formatting based on node type.

        Default implementation simply calls str()
        """
        # this prevents empty strings being not visibliy displayed
        return repr(obj) if isinstance(obj, str) else str(obj)

    @format.register
    def format_basic(self, obj: core.GraphItem) -> str:
        """
        Default formatting for GraphItem.

        Can also be used explicitly to get a single line representation of any node.

        Example:

        >>> from datetime import datetime
        >>> import pathlib
        >>> from sirocco.parsing.cycling import DateCyclePoint
        >>> print(
        ...     PrettyPrinter().format_basic(
        ...         core.Task(
        ...             name="foo",
        ...             config_rootdir=pathlib.Path("."),
        ...             cycle_point=DateCyclePoint(
        ...                 start_date=datetime(1000, 1, 1),
        ...                 stop_date=datetime(1000, 1, 2),
        ...                 chunk_start_date=datetime(1000, 1, 1),
        ...                 chunk_stop_date=datetime(1000, 1, 2),
        ...             ),
        ...             coordinates={"date": datetime(1000, 1, 1).date()},
        ...         )
        ...     )
        ... )
        foo [date: 1000-01-01]
        """
        name = obj.name
        if obj.coordinates:
            coords = ", ".join([f"{name}: {value}" for name, value in obj.coordinates.items()])
            coords = f"[{coords}]"
        else:
            coords = None
        if self.colors:
            name = colored(name, obj.color, attrs=["bold"])
            coords = colored(coords, obj.color) if coords else None
        return f"{name} {coords}" if coords else name

    @format.register
    def format_workflow(self, obj: core.Workflow) -> str:
        cycles = "\n".join(self.format(cycle) for cycle in obj.cycles)
        return self.as_block("cycles", cycles)

    @format.register
    def format_cycle(self, obj: core.Cycle) -> str:
        tasks = self.as_block("tasks", "\n".join(self.format(task) for task in obj.tasks))
        return self.as_item(self.as_block(self.format_basic(obj), tasks))

    @format.register
    def format_task(self, obj: core.Task) -> str:
        sections = []
        if obj.inputs:
            sections.append(
                self.as_block(
                    "input",
                    "\n".join(self.as_item(self.format_basic(inp)) for inp in obj.input_data_nodes()),
                )
            )
        if obj.outputs:
            sections.append(
                self.as_block(
                    "output",
                    "\n".join(self.as_item(self.format_basic(output)) for output in obj.output_data_nodes()),
                )
            )
        if obj.wait_on:
            sections.append(
                self.as_block(
                    "wait on",
                    "\n".join(self.as_item(self.format_basic(waiton)) for waiton in obj.wait_on),
                )
            )

        # Handling remaining member variables
        repr_attrs = [field_name for field_name, field in obj.__dataclass_fields__.items() if field.repr]
        # removing attributs that are specifically handled above
        repr_attrs.remove("inputs")
        repr_attrs.remove("outputs")
        repr_attrs.remove("wait_on")
        repr_attrs.remove("config_rootdir")

        for attr_name in repr_attrs:
            attr = getattr(obj, attr_name)
            if attr is None:
                continue
            formatted_attr = self.format(attr)
            formatted_attr_name = attr_name.replace("_", " ")
            sections.append(f"{formatted_attr_name}: {formatted_attr}")

        return self.as_item(
            self.as_block(
                self.format_basic(obj),
                "\n".join(sections),
            )
        )
