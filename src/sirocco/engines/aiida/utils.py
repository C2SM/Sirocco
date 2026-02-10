"""Utility functions for AiiDA engine module.

Helper functions for AiiDA-specific operations.
AiiDA-specific domain transformations are in adapter.py.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

__all__ = [
    "PortLabelMapper",
    "serialize_coordinates",
    "split_cmd_arg",
]


def split_cmd_arg(command_line: str, script_name: str | None = None) -> tuple[str, str]:
    """Split command line into command and arguments.

    If script_name is provided, finds the script in the command line and
    returns everything after it as arguments. This handles various patterns:
    - "script.sh arg1 arg2" → args: "arg1 arg2"
    - "bash script.sh arg1 arg2" → args: "arg1 arg2"
    - "uenv run /path/to/env -- script.sh arg1" → args: "arg1"

    Args:
        command_line: Full command line string
        script_name: Script name to find and split on

    Returns:
        Tuple of (command_prefix, arguments)
    """
    if script_name:
        # Get just the basename for matching
        script_basename = Path(script_name).name

        parts = command_line.split()
        for i, part in enumerate(parts):
            # Check if this part ends with or equals the script basename
            part_basename = Path(part).name
            if part_basename == script_basename:
                # Everything after the script name is arguments
                args = " ".join(parts[i + 1 :])
                cmd = " ".join(parts[: i + 1])
                return cmd, args

    # Fallback: simple split on first space
    split = command_line.split(sep=" ", maxsplit=1)
    if len(split) == 1:
        return command_line, ""
    return split[0], split[1]


def serialize_coordinates(coordinates: dict) -> dict:
    """Convert coordinates dict to JSON-serializable format.

    Converts datetime objects to ISO format strings.
    """
    from datetime import datetime

    serialized = {}
    for key, value in coordinates.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


class PortLabelMapper:
    """Bidirectional mapper between port names and data labels.

    Manages the relationship between AiiDA ports and data labels, supporting
    both forward (label → port) and reverse (port → labels) lookups. A single
    port can have multiple labels associated with it.

    Example::

        mapper = PortLabelMapper()
        mapper.add("output_file", "task_a_result")
        mapper.add("output_file", "task_b_result")

        # Forward lookup: label → port
        port = mapper.get_port_for_label("task_a_result")  # "output_file"

        # Reverse lookup: port → labels
        labels = mapper.get_labels_for_port(
            "output_file"
        )  # ["task_a_result", "task_b_result"]

        # Export as dict (label → port)
        mapping = mapper.to_dict()  # {"task_a_result": "output_file", ...}
    """

    def __init__(self) -> None:
        """Initialize empty bidirectional mappings."""
        self._label_to_port: dict[str, str] = {}
        self._port_to_labels: dict[str, list[str]] = defaultdict(list)

    def add(self, port: str, label: str) -> None:
        """Add a mapping from label to port.

        Args:
            port: Port name
            label: Data label
        """
        self._label_to_port[label] = port
        if label not in self._port_to_labels[port]:
            self._port_to_labels[port].append(label)

    def add_many(self, port: str, labels: list[str]) -> None:
        """Add multiple labels for a single port.

        Args:
            port: Port name
            labels: List of data labels
        """
        for label in labels:
            self.add(port, label)

    def from_dict(self, mapping: dict[str, str]) -> PortLabelMapper:
        """Populate mapper from a dict of {label: port}.

        Args:
            mapping: Dict mapping labels to ports

        Returns:
            Self for chaining
        """
        for label, port in mapping.items():
            self.add(port, label)
        return self

    def get_port_for_label(self, label: str) -> str | None:
        """Get the port name for a given label.

        Args:
            label: Data label to look up

        Returns:
            Port name, or None if label not found
        """
        return self._label_to_port.get(label)

    def get_labels_for_port(self, port: str) -> list[str]:
        """Get all labels associated with a port.

        Args:
            port: Port name to look up

        Returns:
            List of labels (empty if port not found)
        """
        return list(self._port_to_labels.get(port, []))

    def has_label(self, label: str) -> bool:
        """Check if a label exists in the mapping.

        Args:
            label: Label to check

        Returns:
            True if label is mapped to a port
        """
        return label in self._label_to_port

    def has_port(self, port: str) -> bool:
        """Check if a port exists in the mapping.

        Args:
            port: Port to check

        Returns:
            True if port has at least one label
        """
        return port in self._port_to_labels

    def to_dict(self) -> dict[str, str]:
        """Export as a dictionary of {label: port}.

        Returns:
            Dict mapping all labels to their ports
        """
        return dict(self._label_to_port)

    def get_all_ports(self) -> list[str]:
        """Get all unique port names.

        Returns:
            List of port names
        """
        return list(self._port_to_labels.keys())

    def get_all_labels(self) -> list[str]:
        """Get all unique labels.

        Returns:
            List of labels
        """
        return list(self._label_to_port.keys())

    def __len__(self) -> int:
        """Return the number of label mappings."""
        return len(self._label_to_port)

    def __repr__(self) -> str:
        """Return string representation."""
        return f"PortLabelMapper({len(self)} labels across {len(self._port_to_labels)} ports)"
