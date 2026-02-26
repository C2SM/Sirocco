"""Unit tests for sirocco.engines.aiida.utils module."""

from datetime import UTC, datetime

import pytest
from rich.pretty import pprint

from sirocco.engines.aiida.utils import PortLabelMapper, serialize_coordinates, split_cmd_arg


@pytest.mark.parametrize(
    ("command_string", "script_name", "expected_cmd", "expected_args"),
    [
        # Simple script name
        pytest.param("script.sh arg1 arg2", "script.sh", "script.sh", "arg1 arg2", id="simple_script"),
        # Bash prefix
        pytest.param("bash script.sh arg1 arg2", "script.sh", "bash script.sh", "arg1 arg2", id="bash_prefix"),
        # Script with path
        pytest.param(
            "python /path/to/script.py --flag value",
            "script.py",
            "python /path/to/script.py",
            "--flag value",
            id="script_with_path",
        ),
        # Uenv wrapper
        pytest.param(
            "uenv run /path/to/env -- script.sh arg1",
            "script.sh",
            "uenv run /path/to/env -- script.sh",
            "arg1",
            id="uenv_wrapper",
        ),
        # No script name
        pytest.param("command arg1 arg2", None, "command", "arg1 arg2", id="no_script_name"),
        # No arguments
        pytest.param("command", None, "command", "", id="no_arguments"),
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
        pytest.param(
            {"date": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)},
            lambda s: isinstance(s["date"], str) and "2026-01-01" in s["date"],
            id="datetime_only",
        ),
        # Mixed types
        pytest.param(
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
            id="mixed_types",
        ),
        # Empty coordinates
        pytest.param({}, lambda s: s == {}, id="empty_coordinates"),
        # No datetime
        pytest.param({"member": 0, "name": "test"}, lambda s: s == {"member": 0, "name": "test"}, id="no_datetime"),
    ],
)
def test_serialize_coordinates(coords, expected_checks):
    """Test coordinate serialization for various input types."""
    serialized = serialize_coordinates(coords)

    print("\n=== Coordinate serialization ===")
    pprint({"input": coords, "output": serialized})

    assert expected_checks(serialized)


class TestPortLabelMapper:
    """Test PortLabelMapper bidirectional mapping."""

    def test_empty_mapper(self):
        """Test empty mapper state."""
        mapper = PortLabelMapper()

        print("\n=== Empty mapper ===")
        print(f"Length: {len(mapper)}")
        print(f"Repr: {mapper!r}")

        assert len(mapper) == 0
        assert mapper.get_all_ports() == []
        assert mapper.get_all_labels() == []
        assert mapper.to_label_port_dict() == {}

    def test_add_single_mapping(self):
        """Test adding a single label-port mapping."""
        mapper = PortLabelMapper()
        mapper.add("output_file", "task_a_result")

        print("\n=== Single mapping ===")
        print(f"Port for 'task_a_result': {mapper.get_port_for_label('task_a_result')}")
        print(f"Labels for 'output_file': {mapper.get_labels_for_port('output_file')}")

        assert mapper.get_port_for_label("task_a_result") == "output_file"
        assert mapper.get_labels_for_port("output_file") == ["task_a_result"]
        assert len(mapper) == 1

    def test_add_multiple_labels_to_same_port(self):
        """Test adding multiple labels to the same port."""
        mapper = PortLabelMapper()
        mapper.add("output_file", "task_a_result")
        mapper.add("output_file", "task_b_result")

        print("\n=== Multiple labels same port ===")
        print(f"Labels for 'output_file': {mapper.get_labels_for_port('output_file')}")

        labels = mapper.get_labels_for_port("output_file")
        assert len(labels) == 2
        assert "task_a_result" in labels
        assert "task_b_result" in labels
        assert len(mapper) == 2

    def test_add_many(self):
        """Test add_many method."""
        mapper = PortLabelMapper()
        mapper.add_many("input_files", ["file1", "file2", "file3"])

        print("\n=== Add many ===")
        print(f"Labels for 'input_files': {mapper.get_labels_for_port('input_files')}")

        labels = mapper.get_labels_for_port("input_files")
        assert len(labels) == 3
        assert labels == ["file1", "file2", "file3"]
        assert len(mapper) == 3

    def test_from_label_port_dict(self):
        """Test populating from a label→port dictionary."""
        mapping = {
            "task_a_output": "output_file",
            "task_b_output": "output_file",
            "task_c_result": "results",
        }
        mapper = PortLabelMapper().from_label_port_dict(mapping)

        print("\n=== From label→port dict ===")
        pprint(mapper.to_label_port_dict())

        assert mapper.get_port_for_label("task_a_output") == "output_file"
        assert mapper.get_port_for_label("task_c_result") == "results"
        assert len(mapper.get_labels_for_port("output_file")) == 2
        assert len(mapper) == 3

    def test_from_port_labels_dict(self):
        """Test populating from a port→labels dictionary."""
        mapping = {
            "output_file": ["task_a_output", "task_b_output"],
            "results": ["task_c_result"],
        }
        mapper = PortLabelMapper().from_port_labels_dict(mapping)

        print("\n=== From port→labels dict ===")
        pprint(mapper.to_port_labels_dict())

        assert mapper.get_port_for_label("task_a_output") == "output_file"
        assert mapper.get_port_for_label("task_c_result") == "results"
        assert len(mapper.get_labels_for_port("output_file")) == 2
        assert len(mapper) == 3

        # Verify it's the same as from_label_port_dict (just different input format)
        mapper2 = PortLabelMapper().from_label_port_dict(
            {
                "task_a_output": "output_file",
                "task_b_output": "output_file",
                "task_c_result": "results",
            }
        )
        assert mapper.to_label_port_dict() == mapper2.to_label_port_dict()
        assert mapper.to_port_labels_dict() == mapper2.to_port_labels_dict()

    def test_get_port_for_nonexistent_label(self):
        """Test getting port for a label that doesn't exist."""
        mapper = PortLabelMapper()
        mapper.add("output", "result")

        print("\n=== Nonexistent label ===")
        print(f"Port for 'nonexistent': {mapper.get_port_for_label('nonexistent')}")

        assert mapper.get_port_for_label("nonexistent") is None

    def test_get_labels_for_nonexistent_port(self):
        """Test getting labels for a port that doesn't exist."""
        mapper = PortLabelMapper()
        mapper.add("output", "result")

        print("\n=== Nonexistent port ===")
        print(f"Labels for 'nonexistent': {mapper.get_labels_for_port('nonexistent')}")

        assert mapper.get_labels_for_port("nonexistent") == []

    def test_has_label(self):
        """Test checking if a label exists."""
        mapper = PortLabelMapper()
        mapper.add("output", "result")

        print("\n=== Has label ===")
        print(f"Has 'result': {mapper.has_label('result')}")
        print(f"Has 'nonexistent': {mapper.has_label('nonexistent')}")

        assert mapper.has_label("result") is True
        assert mapper.has_label("nonexistent") is False

    def test_has_port(self):
        """Test checking if a port exists."""
        mapper = PortLabelMapper()
        mapper.add("output", "result")

        print("\n=== Has port ===")
        print(f"Has 'output': {mapper.has_port('output')}")
        print(f"Has 'nonexistent': {mapper.has_port('nonexistent')}")

        assert mapper.has_port("output") is True
        assert mapper.has_port("nonexistent") is False

    def test_get_all_ports(self):
        """Test getting all unique ports."""
        mapper = PortLabelMapper()
        mapper.add("output_file", "result1")
        mapper.add("output_file", "result2")
        mapper.add("data", "result3")

        print("\n=== All ports ===")
        print(f"All ports: {mapper.get_all_ports()}")

        ports = mapper.get_all_ports()
        assert len(ports) == 2
        assert "output_file" in ports
        assert "data" in ports

    def test_get_all_labels(self):
        """Test getting all unique labels."""
        mapper = PortLabelMapper()
        mapper.add("output_file", "result1")
        mapper.add("output_file", "result2")
        mapper.add("data", "result3")

        print("\n=== All labels ===")
        print(f"All labels: {mapper.get_all_labels()}")

        labels = mapper.get_all_labels()
        assert len(labels) == 3
        assert "result1" in labels
        assert "result2" in labels
        assert "result3" in labels

    def test_to_label_port_dict(self):
        """Test exporting to label→port dictionary."""
        mapper = PortLabelMapper()
        mapper.add("output", "result1")
        mapper.add("data", "result2")

        print("\n=== To label→port dict ===")
        pprint(mapper.to_label_port_dict())

        result = mapper.to_label_port_dict()
        assert result == {"result1": "output", "result2": "data"}

    def test_to_port_labels_dict(self):
        """Test exporting to port→labels dictionary."""
        mapper = PortLabelMapper()
        mapper.add("output_file", "result1")
        mapper.add("output_file", "result2")
        mapper.add("data", "result3")

        print("\n=== To port→labels dict ===")
        pprint(mapper.to_port_labels_dict())

        result = mapper.to_port_labels_dict()
        assert result == {"output_file": ["result1", "result2"], "data": ["result3"]}

        # Verify it returns a copy (not the internal list)
        result["output_file"].append("new_label")
        assert mapper.get_labels_for_port("output_file") == ["result1", "result2"]

    def test_duplicate_label_overwrites(self):
        """Test that adding same label with different port overwrites."""
        mapper = PortLabelMapper()
        mapper.add("port1", "label")
        mapper.add("port2", "label")

        print("\n=== Duplicate label ===")
        print(f"Port for 'label': {mapper.get_port_for_label('label')}")
        print(f"Labels for 'port1': {mapper.get_labels_for_port('port1')}")
        print(f"Labels for 'port2': {mapper.get_labels_for_port('port2')}")

        # Latest mapping wins
        assert mapper.get_port_for_label("label") == "port2"
        assert "label" in mapper.get_labels_for_port("port2")
        # Old port still has the label in its list (expected behavior)
        assert "label" in mapper.get_labels_for_port("port1")

    def test_repr(self):
        """Test string representation."""
        mapper = PortLabelMapper()
        mapper.add("output", "result1")
        mapper.add("output", "result2")
        mapper.add("data", "result3")

        print("\n=== Repr ===")
        print(repr(mapper))

        repr_str = repr(mapper)
        assert "3 labels" in repr_str
        assert "2 ports" in repr_str
