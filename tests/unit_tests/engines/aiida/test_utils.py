"""Unit tests for sirocco.engines.aiida.utils module."""

from datetime import UTC, datetime

import pytest
from rich.pretty import pprint

from sirocco.engines.aiida.utils import serialize_coordinates, split_cmd_arg


@pytest.mark.parametrize(
    ("command_string", "script_name", "expected_cmd", "expected_args"),
    [
        # Simple script name
        ("script.sh arg1 arg2", "script.sh", "script.sh", "arg1 arg2"),
        # Bash prefix
        ("bash script.sh arg1 arg2", "script.sh", "bash script.sh", "arg1 arg2"),
        # Script with path
        ("python /path/to/script.py --flag value", "script.py", "python /path/to/script.py", "--flag value"),
        # Uenv wrapper
        ("uenv run /path/to/env -- script.sh arg1", "script.sh", "uenv run /path/to/env -- script.sh", "arg1"),
        # No script name
        ("command arg1 arg2", None, "command", "arg1 arg2"),
        # No arguments
        ("command", None, "command", ""),
    ],
)
def test_split_cmd_arg(command_string, script_name, expected_cmd, expected_args):
    """Test command/argument splitting for various scenarios."""
    cmd, args = split_cmd_arg(command_string, script_name)

    print(f"\n=== Command split: '{command_string}' ===")
    pprint({"command": cmd, "arguments": args})

    assert cmd == expected_cmd
    assert args == expected_args


@pytest.mark.parametrize(
    ("coords", "expected_checks"),
    [
        # Datetime only
        (
            {"date": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)},
            lambda s: isinstance(s["date"], str) and "2026-01-01" in s["date"],
        ),
        # Mixed types
        (
            {
                "date": datetime(2026, 1, 1, 12, 30, 45, tzinfo=UTC),
                "member": 0,
                "name": "test",
                "value": 123.456,
            },
            lambda s: (
                isinstance(s["date"], str)
                and "2026-01-01" in s["date"]
                and s["member"] == 0
                and s["name"] == "test"
                and s["value"] == 123.456
            ),
        ),
        # Empty coordinates
        ({}, lambda s: s == {}),
        # No datetime
        ({"member": 0, "name": "test"}, lambda s: s == {"member": 0, "name": "test"}),
    ],
)
def test_serialize_coordinates(coords, expected_checks):
    """Test coordinate serialization for various input types."""
    serialized = serialize_coordinates(coords)

    print("\n=== Coordinate serialization ===")
    pprint({"input": coords, "output": serialized})

    assert expected_checks(serialized)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
