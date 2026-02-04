"""Unit tests for sirocco.engines.aiida.dependencies module."""

from unittest.mock import Mock, patch

import pytest
from rich.pretty import pprint

from sirocco import core
from sirocco.engines.aiida.dependencies import (
    build_dependency_mapping,
    build_slurm_dependency_directive,
    collect_available_data_inputs,
)


class TestBuildSlurmDependencyDirective:
    """Test SLURM dependency directive construction."""

    def test_single_job_id(self):
        """Test directive with single job ID."""
        job_ids = {"task1": Mock(value=12345)}

        directive = build_slurm_dependency_directive(job_ids)

        print("\n=== SLURM dependency directive (single job) ===")
        print(directive)

        assert directive == "#SBATCH --dependency=afterok:12345 --kill-on-invalid-dep=yes"

    def test_multiple_job_ids(self):
        """Test directive with multiple job IDs."""
        job_ids = {
            "task1": Mock(value=12345),
            "task2": Mock(value=67890),
            "task3": Mock(value=11111),
        }

        directive = build_slurm_dependency_directive(job_ids)

        print("\n=== SLURM dependency directive (multiple jobs) ===")
        print(directive)

        assert directive.startswith("#SBATCH --dependency=afterok:")
        assert "12345" in directive
        assert "67890" in directive
        assert "11111" in directive
        assert "--kill-on-invalid-dep=yes" in directive


class TestCollectAvailableDataInputs:
    """Test collecting AvailableData inputs for a task."""

    def test_collect_single_input(self):
        """Test collecting a single AvailableData input."""
        # Create mock task with one AvailableData input
        task = Mock(spec=core.Task)
        input_data = Mock(spec=core.AvailableData)
        input_data.name = "input1"
        input_data.coordinates = {}
        task.input_data_items.return_value = [("port1", input_data)]

        # Create mock data nodes mapping
        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="input1"):
            aiida_nodes = {"input1": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

        print("\n=== Available data inputs ===")
        pprint(result)

        assert "port1" in result
        assert result["port1"] == aiida_nodes["input1"]

    def test_collect_multiple_inputs(self):
        """Test collecting multiple AvailableData inputs."""
        task = Mock(spec=core.Task)

        input1 = Mock(spec=core.AvailableData)
        input1.name = "input1"
        input1.coordinates = {}

        input2 = Mock(spec=core.AvailableData)
        input2.name = "input2"
        input2.coordinates = {}

        task.input_data_items.return_value = [
            ("port1", input1),
            ("port2", input2),
        ]

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["input1", "input2"]):
            aiida_nodes = {"input1": Mock(), "input2": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

        assert len(result) == 2
        assert "port1" in result
        assert "port2" in result

    def test_collect_no_inputs(self):
        """Test collecting when task has no AvailableData inputs."""
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = []

        result = collect_available_data_inputs(task, {})

        assert result == {}

    def test_collect_skip_generated_data(self):
        """Test that GeneratedData is skipped."""
        task = Mock(spec=core.Task)

        available = Mock(spec=core.AvailableData)
        available.name = "available"
        available.coordinates = {}

        generated = Mock(spec=core.GeneratedData)
        generated.name = "generated"
        generated.coordinates = {}

        task.input_data_items.return_value = [
            ("port1", available),
            ("port2", generated),
        ]

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["available", "generated"]):
            aiida_nodes = {"available": Mock(), "generated": Mock()}

            result = collect_available_data_inputs(task, aiida_nodes)

        # Should only include AvailableData
        assert len(result) == 1
        assert "port1" in result
        assert "port2" not in result


class TestBuildDependencyMapping:
    """Test building dependency mappings for tasks."""

    def test_build_mapping_no_dependencies(self):
        """Test building mapping for task with no dependencies."""
        task = Mock(spec=core.Task)
        task.input_data_items.return_value = []

        workflow = Mock(spec=core.Workflow)
        workflow.tasks = []

        mapping = build_dependency_mapping(task, workflow, {})

        assert mapping.port_mapping == {}
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}

    def test_build_mapping_available_data_only(self):
        """Test building mapping for task with only AvailableData inputs."""
        task = Mock(spec=core.Task)

        available = Mock(spec=core.AvailableData)
        available.name = "input"
        available.coordinates = {}

        task.input_data_items.return_value = [("port1", available)]

        workflow = Mock(spec=core.Workflow)
        workflow.tasks = []

        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", return_value="input"):
            mapping = build_dependency_mapping(task, workflow, {})

        # AvailableData should not create dependencies
        assert mapping.port_mapping == {}
        assert mapping.parent_folders == {}
        assert mapping.job_ids == {}

    def test_build_mapping_with_generated_data(self):
        """Test building mapping for task with GeneratedData inputs."""
        # Create producer task
        producer = Mock(spec=core.Task)
        producer.name = "producer"
        producer.coordinates = {}

        output_data = Mock(spec=core.GeneratedData)
        output_data.name = "output"
        output_data.coordinates = {}
        output_data.path = None

        producer.output_data_items.return_value = [("output_port", output_data)]

        # Create consumer task
        consumer = Mock(spec=core.Task)
        consumer.input_data_items.return_value = [("input_port", output_data)]

        # Create workflow
        workflow = Mock(spec=core.Workflow)
        workflow.tasks = [producer]

        # Mock task outputs (producer has completed)
        from sirocco.engines.aiida.adapter import AiidaAdapter

        with patch.object(AiidaAdapter, "build_graph_item_label", side_effect=["producer", "output", "output"]):
            task_outputs = {
                "producer": Mock(
                    remote_folder=Mock(value=123),
                    job_id=Mock(value=456),
                )
            }

            mapping = build_dependency_mapping(consumer, workflow, task_outputs)

        print("\n=== Dependency mapping ===")
        pprint(
            {"port_mapping": mapping.port_mapping, "parent_folders": mapping.parent_folders, "job_ids": mapping.job_ids}
        )

        # Should create dependency
        assert "input_port" in mapping.port_mapping
        assert len(mapping.port_mapping["input_port"]) == 1
        assert mapping.port_mapping["input_port"][0].dep_label == "producer"

        assert "producer" in mapping.parent_folders
        assert mapping.parent_folders["producer"].value == 123

        assert "producer" in mapping.job_ids
        assert mapping.job_ids["producer"].value == 456


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
