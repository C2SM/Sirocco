"""Tests for the sirocco CLI interface.

These tests focus on the CLI layer, testing command parsing and basic integration
rather than the underlying functionality which should be tested elsewhere.
"""

import subprocess
import textwrap
from unittest.mock import Mock, patch
from aiida import orm, load_profile

import pytest
import typer.testing

from sirocco.cli import app


@pytest.fixture
def runner():
    """Create a typer test runner."""
    return typer.testing.CliRunner()


@pytest.fixture
def sample_workflow_file(tmp_path):
    """Create a minimal valid workflow file for testing."""
    workflow_content = textwrap.dedent(
        """
        name: test_workflow
        cycles:
          - test_cycle:
              tasks:
                - test_task:
        tasks:
          - test_task:
              plugin: shell
              command: "echo hello"
        data:
          available:
            - input_data:
                type: file
                src: test_input.txt
          generated:
            - output_data:
                type: file
                src: test_output.txt
        """
    ).strip()

    workflow_file = tmp_path / "test_workflow.yml"
    workflow_file.write_text(workflow_content)

    # Create a dummy input file referenced in the workflow
    input_file = tmp_path / "test_input.txt"
    input_file.write_text("test input")

    return workflow_file


class TestCLICommands:
    """Test the CLI commands."""

    def test_verify_command_success(self, runner, sample_workflow_file):
        """Test the verify command with a valid workflow file."""
        with patch('sirocco.parsing.ConfigWorkflow.from_config_file') as mock_parse:
            mock_parse.return_value = Mock()

            result = runner.invoke(app, ["verify", str(sample_workflow_file)])

            assert result.exit_code == 0
            assert "✅ Workflow definition is valid" in result.stdout
            mock_parse.assert_called_once_with(str(sample_workflow_file))

    def test_verify_command_failure(self, runner, sample_workflow_file):
        """Test the verify command with an invalid workflow file."""
        with patch('sirocco.parsing.ConfigWorkflow.from_config_file') as mock_parse:
            mock_parse.side_effect = ValueError("Invalid workflow")

            result = runner.invoke(app, ["verify", str(sample_workflow_file)])

            assert result.exit_code == 1
            assert "❌ Workflow validation failed" in result.stdout

    def test_verify_nonexistent_file(self, runner):
        """Test verify command with a nonexistent file."""
        result = runner.invoke(app, ["verify", "nonexistent.yml"])
        assert result.exit_code == 2  # File not found error from typer

    def test_visualize_command_default_output(self, runner, sample_workflow_file):
        """Test the visualize command with default output path."""
        with patch('sirocco.parsing.ConfigWorkflow.from_config_file') as mock_parse, \
             patch('sirocco.core.Workflow.from_config_workflow') as mock_core, \
             patch('sirocco.vizgraph.VizGraph.from_core_workflow') as mock_viz:

            mock_config = Mock()
            mock_core_wf = Mock()
            mock_core_wf.name = "test_workflow"
            mock_viz_instance = Mock()

            mock_parse.return_value = mock_config
            mock_core.return_value = mock_core_wf
            mock_viz.return_value = mock_viz_instance

            result = runner.invoke(app, ["visualize", str(sample_workflow_file)])

            assert result.exit_code == 0
            assert "✅ Visualization saved to" in result.stdout
            assert "test_workflow.svg" in result.stdout
            mock_viz_instance.draw.assert_called_once()

    def test_visualize_command_custom_output(self, runner, sample_workflow_file, tmp_path):
        """Test the visualize command with custom output path."""
        output_file = tmp_path / "custom_output.svg"

        with patch('sirocco.parsing.ConfigWorkflow.from_config_file') as mock_parse, \
             patch('sirocco.core.Workflow.from_config_workflow') as mock_core, \
             patch('sirocco.vizgraph.VizGraph.from_core_workflow') as mock_viz:

            mock_config = Mock()
            mock_core_wf = Mock()
            mock_core_wf.name = "test_workflow"
            mock_viz_instance = Mock()

            mock_parse.return_value = mock_config
            mock_core.return_value = mock_core_wf
            mock_viz.return_value = mock_viz_instance

            result = runner.invoke(app, ["visualize", str(sample_workflow_file), "--output", str(output_file)])

            assert result.exit_code == 0
            assert "✅ Visualization saved to" in result.stdout
            assert "custom_output.svg" in result.stdout

    def test_represent_command(self, runner, sample_workflow_file):
        """Test the represent command."""
        with patch('sirocco.parsing.ConfigWorkflow.from_config_file') as mock_parse, \
             patch('sirocco.core.Workflow.from_config_workflow') as mock_core, \
             patch('sirocco.pretty_print.PrettyPrinter') as mock_printer:

            mock_config = Mock()
            mock_core_wf = Mock()
            mock_printer_instance = Mock()
            mock_printer_instance.format.return_value = "formatted workflow output"

            mock_parse.return_value = mock_config
            mock_core.return_value = mock_core_wf
            mock_printer.return_value = mock_printer_instance

            result = runner.invoke(app, ["represent", str(sample_workflow_file)])

            assert result.exit_code == 0
            assert "formatted workflow output" in result.stdout

    @pytest.mark.usefixtures('aiida_localhost')
    def test_run_command(self, runner):
        """Test the run command."""
        # Use existing test case workflow
        workflow_path = "tests/cases/small/config/config.yml"

        # Mock the workgraph execution but allow real workflow parsing and validation
        with patch('sirocco.workgraph.AiidaWorkGraph.run') as mock_run:
            # Setup the computer that the test workflow references

            mock_run.return_value = {"result": "success"}

            result = runner.invoke(app, ["run", workflow_path])

            assert result.exit_code == 0
            assert "▶️ Running workflow" in result.stdout
            assert "✅ Workflow execution finished" in result.stdout

    def test_submit_command_basic(self, runner, tmp_path):
        """Test the submit command without waiting."""
        # Create a minimal workflow that doesn't require any computers
        minimal_workflow = textwrap.dedent(
            """
            name: minimal_submit_test
            cycles:
              - simple_cycle:
                  tasks:
                    - simple_task:
            tasks:
              - simple_task:
                  plugin: shell
                  command: "echo hello"
            data:
              available:
                - input_file:
                    type: file
                    src: input.txt
              generated:
                - output_file:
                    type: file
                    src: output.txt
            """
        ).strip()

        workflow_file = tmp_path / "minimal_workflow.yml"
        workflow_file.write_text(minimal_workflow)

        # Create the referenced input file
        input_file = tmp_path / "input.txt"
        input_file.write_text("test input")

        # Mock the entire submit process to avoid all validation
        with patch('sirocco.cli._prepare_aiida_workgraph') as mock_prepare:
            mock_aiida_wg = Mock()
            mock_aiida_wg._core_workflow.name = "minimal_submit_test"

            # Create a simple mock result that bypasses isinstance checks
            mock_result = "WorkChain<12345>"  # Simple string result
            mock_aiida_wg.submit.return_value = mock_result
            mock_prepare.return_value = mock_aiida_wg

            result = runner.invoke(app, ["submit", str(workflow_file)])

            assert result.exit_code == 0
            assert "🚀 Submitting workflow" in result.stdout
            # Check for the fallback message when isinstance check fails
            assert "✅ Submission initiated" in result.stdout

    def test_submit_command_with_wait(self, runner, tmp_path):
        """Test the submit command with wait option."""
        # Create a minimal workflow that doesn't require any computers
        minimal_workflow = textwrap.dedent(
            """
            name: minimal_submit_test_wait
            cycles:
              - simple_cycle:
                  tasks:
                    - simple_task:
            tasks:
              - simple_task:
                  plugin: shell
                  command: "echo hello"
            data:
              available:
                - input_file:
                    type: file
                    src: input.txt
              generated:
                - output_file:
                    type: file
                    src: output.txt
            """
        ).strip()

        workflow_file = tmp_path / "minimal_workflow.yml"
        workflow_file.write_text(minimal_workflow)

        # Create the referenced input file
        input_file = tmp_path / "input.txt"
        input_file.write_text("test input")

        # Mock the entire submit process to avoid all validation
        with patch('sirocco.cli._prepare_aiida_workgraph') as mock_prepare:
            mock_aiida_wg = Mock()
            mock_aiida_wg._core_workflow.name = "minimal_submit_test_wait"

            # Create a simple mock result that bypasses isinstance checks
            mock_result = "WorkChain<12345>"  # Simple string result
            mock_aiida_wg.submit.return_value = mock_result
            mock_prepare.return_value = mock_aiida_wg

            result = runner.invoke(app, ["submit", str(workflow_file), "--wait", "--timeout", "120"])

            assert result.exit_code == 0
            assert "🚀 Submitting workflow" in result.stdout
            # Check for the fallback message when isinstance check fails
            assert "✅ Submission initiated" in result.stdout

class TestCLIHelpers:
    """Test CLI helper functions."""

    def test_load_aiida_profile_success(self, aiida_profile):
        """Test successful AiiDA profile loading."""
        with patch('sirocco.cli.console') as mock_console:
            from sirocco.cli import load_aiida_profile
            load_aiida_profile(aiida_profile.name)
            # Should not raise an exception

    def test_load_aiida_profile_failure(self):
        """Test AiiDA profile loading failure."""
        with patch('aiida.load_profile') as mock_load, \
             patch('sirocco.cli.console') as mock_console:

            mock_load.side_effect = Exception("Profile not found")

            from sirocco.cli import load_aiida_profile
            with pytest.raises(typer.Exit):
                load_aiida_profile("nonexistent_profile")


class TestCLIIntegration:
    """Integration tests for the CLI using subprocess."""

    def test_cli_help(self):
        """Test that the CLI help works."""
        result = subprocess.run(["python", "-m", "sirocco.cli", "--help"],
                              capture_output=True, text=True)
        assert result.returncode == 0
        assert "Sirocco Weather and Climate Workflow Management Tool" in result.stdout

    def test_verify_help(self):
        """Test the verify command help."""
        result = subprocess.run(["python", "-m", "sirocco.cli", "verify", "--help"],
                              capture_output=True, text=True)
        assert result.returncode == 0
        assert "Validate the workflow definition file" in result.stdout

    def test_visualize_help(self):
        """Test the visualize command help."""
        result = subprocess.run(["python", "-m", "sirocco.cli", "visualize", "--help"],
                              capture_output=True, text=True)
        assert result.returncode == 0
        assert "Generate an interactive SVG visualization" in result.stdout


class TestCLIErrorHandling:
    """Test CLI error handling scenarios."""

    def test_workflow_preparation_failure(self, runner):
        """Test handling of workflow preparation failures."""
        workflow_path = "tests/cases/small/config/config.yml"

        # Mock _prepare_aiida_workgraph to raise an exception
        with patch('sirocco.cli._prepare_aiida_workgraph') as mock_prepare:
            mock_prepare.side_effect = Exception("Preparation failed")

            result = runner.invoke(app, ["run", workflow_path])

            assert result.exit_code == 1
            mock_prepare.assert_called_once()

    @pytest.mark.usefixtures('aiida_localhost')
    def test_run_execution_failure(self, runner):
        """Test handling of workflow execution failures."""
        workflow_path = "tests/cases/small/config/config.yml"

        with patch('sirocco.workgraph.AiidaWorkGraph.run') as mock_run:
            # Setup the computer that the test workflow references

            mock_run.side_effect = Exception("Execution failed")

            result = runner.invoke(app, ["run", workflow_path])

            assert result.exit_code == 1
            assert "❌ Workflow execution failed during run" in result.stdout

    @pytest.mark.usefixtures('aiida_localhost')
    def test_submit_execution_failure(self, runner):
        """Test handling of workflow submission failures."""
        workflow_path = "tests/cases/small/config/config.yml"

        with patch('sirocco.workgraph.AiidaWorkGraph.submit') as mock_submit:
            # Setup the computer that the test workflow references

            mock_submit.side_effect = Exception("Submission failed")

            result = runner.invoke(app, ["submit", workflow_path])

            assert result.exit_code == 1
            assert "❌ Workflow submission failed" in result.stdout
