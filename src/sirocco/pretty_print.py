import dataclasses
import functools
import textwrap
from dataclasses import is_dataclass
from typing import Any, Literal

from termcolor import colored

from sirocco import core
from sirocco.parsing.cycling import CyclePoint


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

    def indent(self, string: str, offset: int = 0) -> str:
        """Indent by the amount set on the instance"""
        return textwrap.indent(string, prefix=" " * (self.indentation + offset))

    def dataclass_to_dict(self, obj: Any, exclude: list[str] | None = None) -> dict[str, Any]:
        """Convert dataclass to dict before formatting"""
        if exclude is None:
            exclude = []
        return {
            field_name: getattr(obj, field_name)
            for field_name, field in obj.__dataclass_fields__.items()
            if field.repr and field_name not in exclude
        }

    def graphitem_label(self, obj: core.GraphItem) -> str:
        """
        Default formatting for GraphItem.

        Can also be used explicitly to get a single line representation of any node.

        Example:

        >>> from datetime import datetime
        >>> import pathlib
        >>> from sirocco.parsing.cycling import DateCyclePoint
        >>> print(
        ...     PrettyPrinter().graphitem_label(
        ...         core.Task(
        ...             name="foo",
        ...             computer="localhost",
        ...             config_rootdir=pathlib.Path("."),
        ...             cycle_point=DateCyclePoint(
        ...                 start_date=datetime(1000, 1, 1),
        ...                 stop_date=datetime(1000, 1, 2),
        ...                 chunk_start_date=datetime(1000, 1, 1),
        ...                 chunk_stop_date=datetime(1000, 1, 2),
        ...                 period="P1D",
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

    @functools.singledispatchmethod
    def format(
        self,
        obj: Any,
        parent: Literal["none", "list", "dict", "list_first_dict_item"],
        exclude: list[str] | None = None,
    ) -> str:
        """
        Dispatch formatting based on node type.

        Default implementation:
        - turn dataclasses into dicts
        - otherwise simply call str()
        """
        if is_dataclass(obj):
            return self.format_dict(self.dataclass_to_dict(obj, exclude=exclude), parent=parent)
        if isinstance(obj, str) and "\x1b[" not in obj:
            return " " + repr(obj)
        else:
            return " " + str(obj)

    @format.register
    def format_dict(
        self,
        obj: dict,
        parent: Literal["none", "list", "dict", "list_first_dict_item"],
        exclude: list[str] | None = None,
    ) -> str:
        """
        Format dictionnaries
        """

        if exclude is None:
            exclude = []
        dict_to_format = {k: v for k, v in obj.items() if k not in exclude and v is not None}
        if len(dict_to_format) == 0:
            return " {}"
        match parent:
            case "none" | "dict" | "list_first_dict_item":
                sections = (f"{key}:{self.format(value, parent='dict')}" for key, value in dict_to_format.items())
                match parent:
                    case "none":
                        return "\n".join(sections)
                    case "dict":
                        return "\n" + self.indent("\n".join(sections))
                    case "list_first_dict_item":
                        return "\n" + self.indent("\n".join(sections), offset=2)
            case "list":
                iter_items = iter(obj.items())
                key, value = next(iter_items)
                first_section = f" {key}:{self.format(value, parent='list_first_dict_item')}"
                if len(dict_to_format) == 1:
                    return first_section
                next_sections = (f"{key}:{self.format(value, parent='dict')}" for key, value in iter_items)
                return "\n".join((first_section, self.indent("\n".join(next_sections))))

    @format.register
    def format_list(self, obj: list, parent: Literal["none", "list", "dict", "list_first_dict_item"]) -> str:
        """
        Format lists
        """

        if len(obj) == 0:
            return " []"
        sections = (f"-{self.format(item, parent='list')}" for item in obj)
        match parent:
            case "none":
                return "\n".join(sections)
            case "list" | "dict":
                return "\n" + self.indent("\n".join(sections))
            case "list_first_dict_item":
                return "\n" + self.indent("\n".join(sections), offset=2)

    @format.register
    def format_workflow(self, obj: core.Workflow) -> str:
        return self.format({"cycles": list(obj.cycles)}, parent="none")

    @format.register
    def format_cycle(self, obj: core.Cycle, parent: Literal["none", "list", "dict", "list_first_dict_item"]) -> str:
        return self.format(
            {self.graphitem_label(obj): self.dataclass_to_dict(obj, exclude=["name", "coordinates"])}, parent=parent
        )

    @format.register
    def format_task(self, obj: core.Task, parent: Literal["none", "list", "dict", "list_first_dict_item"]) -> str:
        obj_dict = self.dataclass_to_dict(obj, exclude=["name", "coordinates", "config_rootdir", "RUN_ROOT"])
        if obj.wait_on:
            obj_dict["wait_on"] = [self.graphitem_label(task) for task in obj.wait_on]
        else:
            del(obj_dict["wait_on"])
        return self.format({self.graphitem_label(obj): obj_dict}, parent=parent)

    @format.register
    def format_data(self, obj: core.Data, parent: Literal["none", "list", "dict", "list_first_dict_item"]) -> str:  # noqa: ARG002
        return " " + self.graphitem_label(obj)

    @format.register
    def format_namelist(
        self,
        obj: core.NamelistFile,
        parent: Literal["none", "list", "dict", "list_first_dict_item"],  # noqa: ARG002
    ) -> str:
        return " " + obj.name

    @format.register
    def format_cycle_point(
        self, obj: CyclePoint, parent: Literal["none", "list", "dict", "list_first_dict_item"]
    ) -> str:  # noqa: ARG002
        return " " + str(obj)
