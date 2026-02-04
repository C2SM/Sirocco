"""Utility functions for workgraph module.

General utilities that are not AiiDA-specific.
AiiDA-specific transformations are in adapter.py.
"""

from __future__ import annotations

from pathlib import Path


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
