"""Tests for the sirocco CLI interface.

These tests focus on the CLI layer, testing command parsing and basic integration
rather than the underlying functionality which should be tested elsewhere.
"""

import re
import subprocess
from unittest.mock import Mock, patch

import pytest
import typer.testing

from sirocco.cli import app


def strip_ansi(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


@pytest.fixture
def runner():
    """Create a typer test runner."""
    return typer.testing.CliRunner()


class TestCLICommands:
    """Test the CLI commands."""

    def test_cli_module_loads(self):
        """Test that the CLI module can be imported and shows expected commands."""
        result = subprocess.run(["python", "-m", "sirocco.cli", "--help"], capture_output=True, text=True, check=False)
        assert result.returncode == 0
        # Verify expected commands are listed
        assert "verify" in result.stdout
        assert "represent" in result.stdout
        assert "visualize" in result.stdout
        assert "run" in result.stdout
        assert "submit" in result.stdout

    @pytest.mark.parametrize("command", ["verify", "represent", "visualize", "run", "submit"])
    def test_command_with_nonexistent_workflow(self, runner, command):
        """Test commands with nonexistent workflow files."""
        result = runner.invoke(app, [command, "nonexistent.yml"])
        assert result.exit_code == 1

    @pytest.mark.parametrize("command", ["verify", "represent", "visualize", "run", "submit"])
    def test_command_empty_file(self, runner, command, tmp_path):
        """Test verify command with empty file."""
        empty_file = tmp_path / "empty.yml"
        empty_file.write_text("")

        result = runner.invoke(app, [command, str(empty_file)])
        assert result.exit_code == 1

    def test_verify_command_success(self, runner, sample_workflow_file):
        """Test the verify command with a valid workflow file."""

        result = runner.invoke(app, ["verify", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "✅ Workflow definition is valid" in result.stdout

    def test_verify_command_failure(self, runner, sample_workflow_file):
        """Test the verify command with an invalid workflow file."""

        # Mock failed workflow validation
        with patch("sirocco.parsing.ConfigWorkflow.from_config_file") as mock_parse:
            mock_parse.side_effect = ValueError("Invalid workflow")

            result = runner.invoke(app, ["verify", str(sample_workflow_file)])

            assert result.exit_code == 1
            assert "❌ Workflow validation failed" in result.stdout

    def test_visualize_command_default_output(self, runner, sample_workflow_file):
        """Test the visualize command with default output path."""

        result = runner.invoke(app, ["visualize", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        assert "test_workflow.svg" in result.stdout

    def test_visualize_command_custom_output(self, runner, sample_workflow_file, tmp_path):
        """Test the visualize command with custom output path."""
        output_file = tmp_path / "custom_output.svg"

        result = runner.invoke(app, ["visualize", str(sample_workflow_file), "--output", str(output_file)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        assert "custom_output.svg" in result.stdout

    def test_visualize_invalid_output_path(self, runner, sample_workflow_file):
        """Test visualize command with invalid output path."""
        # Try to write to a directory that doesn't exist
        result = runner.invoke(
            app, ["visualize", str(sample_workflow_file), "--output", "/nonexistent/path/output.svg"]
        )

        assert result.exit_code == 1

    def test_represent_command(self, runner, sample_workflow_file):
        """Test the represent command."""

        result = runner.invoke(app, ["represent", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "Representing workflow from" in result.stdout
        assert "cycles:" in result.stdout  # Should contain workflow structure
        assert "test_workflow" in result.stdout  # Should contain workflow name

    @pytest.mark.usefixtures("aiida_localhost")
    def test_run_command(self, runner, sample_workflow_file):
        """Test the run command."""
        result = runner.invoke(app, ["run", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "▶️ Running workflow" in result.stdout
        assert "✅ Workflow execution finished" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_run_execution_failure(self, runner, sample_workflow_file):
        """Test handling of workflow execution failures."""

        with patch("sirocco.workgraph.AiidaWorkGraph.run") as mock_run:
            mock_run.side_effect = Exception("Execution failed")
            result = runner.invoke(app, ["run", str(sample_workflow_file)])

            assert result.exit_code == 1
            assert "❌ Workflow execution failed during run" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_command_basic(self, runner, sample_workflow_file):
        """Test the submit command without waiting."""

        # Mock the entire submit process to avoid all validation
        with patch("sirocco.cli._prepare_aiida_workgraph") as mock_prepare:
            mock_aiida_wg = Mock()
            mock_aiida_wg._core_workflow.name = "minimal_submit_test"  # noqa: SLF001

            # Create a simple mock result that bypasses isinstance checks
            mock_result = Mock()  # Simple string result
            mock_result.pk = 12345
            mock_aiida_wg.submit.return_value = mock_result
            mock_prepare.return_value = mock_aiida_wg

            result = runner.invoke(app, ["submit", str(sample_workflow_file)])

            assert result.exit_code == 0
            assert "🚀 Submitting workflow" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_command_with_wait(self, runner, sample_workflow_file):
        """Test the submit command with wait option."""
        with patch("sirocco.cli._prepare_aiida_workgraph") as mock_prepare:
            mock_aiida_wg = Mock()
            mock_aiida_wg._core_workflow.name = "test_workflow"  # noqa: SLF001
            mock_result = Mock()
            mock_result.pk = 12345
            mock_aiida_wg.submit.return_value = mock_result
            mock_prepare.return_value = mock_aiida_wg

            result = runner.invoke(app, ["submit", str(sample_workflow_file), "--wait", "--timeout", "120"])

            # Verify the submit method was called with correct parameters
            mock_aiida_wg.submit.assert_called_once_with(inputs=None, wait=True, timeout=120)
            assert result.exit_code == 0

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_execution_failure(self, runner, sample_workflow_file):
        """Test handling of workflow submission failures."""

        with patch("sirocco.workgraph.AiidaWorkGraph.submit") as mock_submit:
            mock_submit.side_effect = Exception("Submission failed")
            result = runner.invoke(app, ["submit", str(sample_workflow_file)])

            assert result.exit_code == 1
            assert "❌ Workflow submission failed" in result.stdout


@pytest.mark.usefixtures("aiida_localhost")
def test_prepare_aiida_workgraph_success(sample_workflow_file, capsys):
    """Test successful workflow preparation."""
    from sirocco.cli import _prepare_aiida_workgraph

    # Should not raise an exception
    aiida_wg = _prepare_aiida_workgraph(str(sample_workflow_file))

    assert aiida_wg is not None
    assert hasattr(aiida_wg, "_core_workflow")

    # Capture and verify the console output
    captured = capsys.readouterr()
    assert "Workflow 'test_workflow' prepared for AiiDA execution" in strip_ansi(captured.out)


def test_prepare_aiida_workgraph_invalid_file(capsys):
    """Test workflow preparation with invalid config file."""
    from sirocco.cli import _prepare_aiida_workgraph

    with pytest.raises(typer.Exit):
        _prepare_aiida_workgraph("nonexistent.yml")

    captured = capsys.readouterr()
    assert "Failed to prepare AiiDA workflow" in captured.out


def test_prepare_aiida_workgraph_malformed_config(tmp_path, capsys):
    """Test workflow preparation with malformed config file."""
    from sirocco.cli import _prepare_aiida_workgraph

    # Create a malformed YAML file
    bad_config = tmp_path / "bad_config.yml"
    bad_config.write_text("invalid: yaml: content: [")

    with pytest.raises(typer.Exit):
        _prepare_aiida_workgraph(str(bad_config))

    captured = capsys.readouterr()
    assert "Failed to prepare AiiDA workflow" in captured.out
