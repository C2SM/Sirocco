"""Tests for the sirocco CLI interface.

These tests focus on the CLI layer, testing command parsing and basic integration
rather than the underlying functionality which should be tested elsewhere.
"""

import re
import subprocess
from unittest.mock import Mock

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
        # typers internal validation checks if the file exists, and if not, fails with exit code 2
        assert result.exit_code == 2

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

    def test_verify_command_failure(self, runner, sample_workflow_file, monkeypatch):
        """Test the verify command with an invalid workflow file."""

        # Mock failed workflow validation
        def mock_from_config_file():
            msg = "Invalid workflow"
            raise ValueError(msg)

        monkeypatch.setattr("sirocco.parsing.ConfigWorkflow.from_config_file", mock_from_config_file)

        result = runner.invoke(app, ["verify", str(sample_workflow_file)])

        assert result.exit_code == 1
        assert "❌ Workflow validation failed" in result.stdout

    def test_visualize_command_default_output(self, runner, sample_workflow_file):
        """Test the visualize command with default output path."""

        result = runner.invoke(app, ["visualize", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        # Can contain line breaks in this part of the output string
        assert "test_workflow.svg" in result.stdout.replace("\n", "")

    def test_visualize_command_custom_output(self, runner, sample_workflow_file, tmp_path):
        """Test the visualize command with custom output path."""
        output_file = tmp_path / "custom_output.svg"

        result = runner.invoke(app, ["visualize", str(sample_workflow_file), "--output", str(output_file)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        assert "custom_output.svg" in result.stdout.replace("\n", "")

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
    def test_run_execution_failure(self, runner, sample_workflow_file, monkeypatch):
        """Test handling of workflow execution failures."""

        def mock_run():
            msg = "Execution failed"
            raise Exception(msg)  # noqa: TRY002 | raise-vanilla-class

        monkeypatch.setattr("sirocco.workgraph.AiidaWorkGraph.run", mock_run)

        result = runner.invoke(app, ["run", str(sample_workflow_file)])

        assert result.exit_code == 1
        assert "❌ Workflow execution failed during run" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_command_basic(self, runner, sample_workflow_file, monkeypatch):
        """Test the submit command."""

        mock_aiida_wg = Mock()
        mock_aiida_wg._core_workflow.name = "test_workflow"  # noqa: SLF001
        mock_result = Mock()
        mock_result.pk = 12345
        mock_aiida_wg.submit.return_value = mock_result

        def mock_prepare_workgraph(_workflow_file):
            return mock_aiida_wg

        monkeypatch.setattr("sirocco.cli._prepare_aiida_workgraph", mock_prepare_workgraph)

        result = runner.invoke(app, ["submit", str(sample_workflow_file)])

        assert result.exit_code == 0
        assert "🚀 Submitting workflow" in result.stdout

    # @pytest.mark.usefixtures("aiida_localhost")
    # def test_submit_command_basic(self, runner, sample_workflow_file, monkeypatch):
    #     """Test the submit command."""

    #     # Create mock objects (you can still use Mock for object creation)
    #     mock_aiida_wg = Mock()
    #     mock_aiida_wg._core_workflow.name = "test_workflow"  # | private-member-access
    #     mock_result = Mock()
    #     mock_result.pk = 12345
    #     mock_aiida_wg.submit.return_value = mock_result

    #     # Use monkeypatch instead of patch context manager
    #     monkeypatch.setattr("sirocco.cli._prepare_aiida_workgraph", lambda: mock_aiida_wg)

    #     result = runner.invoke(app, ["submit", str(sample_workflow_file)])

    #     assert result.exit_code == 0
    #     assert "🚀 Submitting workflow" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_execution_failure(self, runner, sample_workflow_file, monkeypatch):
        """Test handling of workflow submission failures."""

        def mock_submit():
            msg = "Submission failed"
            raise Exception(msg)  # noqa: TRY002 | raise-vanilla-class

        monkeypatch.setattr("sirocco.workgraph.AiidaWorkGraph.submit", mock_submit)

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
