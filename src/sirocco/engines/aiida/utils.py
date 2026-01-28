"""Utility functions for workgraph module."""

from __future__ import annotations

from pathlib import Path

from sirocco import core


def replace_invalid_chars_in_label(label: str) -> str:
    """Replaces chars in the label that are invalid for AiiDA.

    The invalid chars ["-", " ", ":", "."] are replaced with underscores.
    """
    invalid_chars = ["-", " ", ":", "."]
    for invalid_char in invalid_chars:
        label = label.replace(invalid_char, "_")
    return label


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


def label_placeholder(data: core.Data) -> str:
    """Create a placeholder string for data."""
    from sirocco.engines.aiida.adapter import AiiDAAdapter

    return f"{{{AiiDAAdapter.get_label(data)}}}"


def parse_mpi_cmd_to_aiida(mpi_cmd: str) -> str:
    """Parse MPI command and translate placeholders to AiiDA format."""
    from sirocco.engines.aiida.adapter import AiiDAAdapter

    for placeholder in core.MpiCmdPlaceholder:
        mpi_cmd = mpi_cmd.replace(
            f"{{{placeholder.value}}}",
            f"{{{AiiDAAdapter.translate_mpi_placeholder_static(placeholder)}}}",
        )
    return mpi_cmd


def process_shell_argument_placeholders(arguments_template: str | None, placeholder_to_node_key: dict) -> list[str]:
    """Process argument template and replace placeholders with actual node keys.

    Handles both standalone placeholders like {data_label} and embedded placeholders
    like --pool=data_label where data_label should be replaced with a node key.

    Args:
        arguments_template: Template string with {placeholder} or bare placeholder syntax
        placeholder_to_node_key: Dict mapping placeholder names to node keys

    Returns:
        List of processed arguments with placeholders replaced
    """
    if not arguments_template:
        return []

    arguments_list = arguments_template.split()
    processed_arguments = []

    for arg in arguments_list:
        # Check if this argument is a standalone placeholder like {data_label}
        if arg.startswith("{") and arg.endswith("}"):
            placeholder_name = arg[1:-1]  # Remove the braces
            # Map to the actual node key if we have a mapping
            if placeholder_name in placeholder_to_node_key:
                actual_node_key = placeholder_to_node_key[placeholder_name]
                processed_arguments.append(f"{{{actual_node_key}}}")
            else:
                # Keep original if no mapping found
                processed_arguments.append(arg)
        else:
            # Check if any data label from placeholder_to_node_key appears in this arg
            # This handles cases like --pool=tmp_data_pool where tmp_data_pool needs replacement
            processed_arg = arg
            for data_label, node_key in placeholder_to_node_key.items():
                if data_label in arg:
                    processed_arg = arg.replace(data_label, f"{{{node_key}}}")
                    break
            processed_arguments.append(processed_arg)

    return processed_arguments


def get_default_wrapper_script():
    """Get default wrapper script based on task type"""
    # Import the script directory from aiida-icon
    import aiida.orm
    from aiida_icon.site_support.cscs.alps import SCRIPT_DIR

    # TODO: There's also `santis_cpu.sh`. Also gpu available.
    # This should be configurable by the users
    default_script_path = SCRIPT_DIR / "todi_cpu.sh"
    return aiida.orm.SinglefileData(file=default_script_path)


def get_wrapper_script_aiida_data(task):
    """Get AiiDA SinglefileData for wrapper script if configured"""
    import aiida.orm

    if task.wrapper_script is not None:
        return aiida.orm.SinglefileData(str(task.wrapper_script))
    return get_default_wrapper_script()


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
