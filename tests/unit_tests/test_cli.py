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

    def test_verify_command_success(self, runner, minimal_config_path):
        """Test the verify command with a valid workflow file."""

        result = runner.invoke(app, ["verify", str(minimal_config_path)])

        assert result.exit_code == 0
        assert "✅ Workflow definition is valid" in result.stdout

    def test_verify_command_failure(self, runner, minimal_config_path, monkeypatch):
        """Test the verify command with an invalid workflow file."""

        # Mock failed workflow validation
        def mock_from_config_file():
            msg = "Invalid workflow"
            raise ValueError(msg)

        monkeypatch.setattr("sirocco.parsing.ConfigWorkflow.from_config_file", mock_from_config_file)

        result = runner.invoke(app, ["verify", str(minimal_config_path)])

        assert result.exit_code == 1
        assert "❌ Workflow validation failed" in result.stdout

    def test_visualize_command_default_output(self, runner, minimal_config_path):
        """Test the visualize command with default output path."""

        result = runner.invoke(app, ["visualize", str(minimal_config_path)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        # Can contain line breaks in this part of the output string
        assert "minimal.svg" in result.stdout.replace("\n", "")

    def test_visualize_command_custom_output(self, runner, minimal_config_path, tmp_path):
        """Test the visualize command with custom output path."""
        output_file = tmp_path / "custom_output.svg"

        result = runner.invoke(app, ["visualize", str(minimal_config_path), "--output", str(output_file)])

        assert result.exit_code == 0
        assert "✅ Visualization saved to" in result.stdout
        assert "custom_output.svg" in result.stdout.replace("\n", "")

    def test_visualize_invalid_output_path(self, runner, minimal_config_path):
        """Test visualize command with invalid output path."""
        # Try to write to a directory that doesn't exist
        result = runner.invoke(app, ["visualize", str(minimal_config_path), "--output", "/nonexistent/path/output.svg"])

        assert result.exit_code == 1

    def test_represent_command(self, runner, minimal_config_path):
        """Test the represent command."""

        result = runner.invoke(app, ["represent", str(minimal_config_path)])

        assert result.exit_code == 0
        assert "Representing workflow from" in result.stdout
        assert "cycles:" in result.stdout  # Should contain workflow structure
        assert "minimal" in result.stdout  # Should contain workflow name

    @pytest.mark.usefixtures("aiida_localhost")
    def test_run_command(self, runner, minimal_config_path, monkeypatch):
        """Test the run command."""
        from unittest.mock import Mock

        # Create a mock AiidaWorkGraph
        mock_aiida_wg = Mock()
        mock_aiida_wg._core_workflow.name = "minimal"  # noqa: SLF001
        mock_aiida_wg.run.return_value = None

        # Mock the _create_aiida_workflow function to return our mock
        def mock_create_aiida_workflow(_workflow_file):
            return mock_aiida_wg

        monkeypatch.setattr("sirocco.cli.create_aiida_workflow", mock_create_aiida_workflow)

        result = runner.invoke(app, ["run", str(minimal_config_path)])

        assert result.exit_code == 0
        assert "▶️ Running workflow" in result.stdout
        assert "✅ Workflow execution finished" in result.stdout
        # Verify the mock was called correctly
        mock_aiida_wg.run.assert_called_once_with(inputs=None)

    @pytest.mark.usefixtures("aiida_localhost")
    def test_run_execution_failure(self, runner, minimal_config_path, monkeypatch):
        """Test handling of workflow execution failures."""
        from unittest.mock import Mock

        # Create a mock AiidaWorkGraph
        mock_aiida_wg = Mock()
        mock_aiida_wg._core_workflow.name = "minimal"  # noqa: SLF001

        # Mock the execution to fail
        def mock_run():
            msg = "Execution failed"
            raise Exception(msg)  # noqa: TRY002 | raise-vanilla-class

        mock_aiida_wg.run = mock_run

        # Mock the workflow creation to return our mock (bypassing file issues)
        monkeypatch.setattr("sirocco.cli.create_aiida_workflow", lambda _: mock_aiida_wg)

        result = runner.invoke(app, ["run", str(minimal_config_path)])

        assert result.exit_code == 1
        assert "❌ Workflow execution failed during run" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_command_basic(self, runner, minimal_config_path, monkeypatch):
        """Test the submit command."""

        mock_aiida_wg = Mock()
        mock_aiida_wg._core_workflow.name = "minimal"  # noqa: SLF001
        mock_result = Mock()
        mock_result.pk = 12345
        mock_aiida_wg.submit.return_value = mock_result

        def mock_prepare_workgraph(_workflow_file):
            return mock_aiida_wg

        monkeypatch.setattr("sirocco.cli.create_aiida_workflow", mock_prepare_workgraph)

        result = runner.invoke(app, ["submit", str(minimal_config_path)])

        assert result.exit_code == 0
        assert "🚀 Submitting workflow" in result.stdout

    @pytest.mark.usefixtures("aiida_localhost")
    def test_submit_execution_failure(self, runner, minimal_config_path, monkeypatch):
        """Test handling of workflow submission failures."""
        from unittest.mock import Mock

        # Create a mock AiidaWorkGraph
        mock_aiida_wg = Mock()
        mock_aiida_wg._core_workflow.name = "minimal"  # noqa: SLF001

        # Mock the submission to fail
        def mock_submit():
            msg = "Submission failed"
            raise Exception(msg)  # noqa: TRY002 | raise-vanilla-class

        mock_aiida_wg.submit = mock_submit

        # Mock the workflow creation to return our mock (bypassing file issues)
        monkeypatch.setattr("sirocco.cli.create_aiida_workflow", lambda _: mock_aiida_wg)

        result = runner.invoke(app, ["submit", str(minimal_config_path)])

        assert result.exit_code == 1
        assert "❌ Workflow submission failed" in result.stdout


def test_create_aiida_workflow_invalid_file(capsys):
    """Test workflow preparation with invalid config file."""
    from sirocco.cli import create_aiida_workflow

    with pytest.raises(typer.Exit):
        create_aiida_workflow("nonexistent.yml")

    captured = capsys.readouterr()
    assert "Failed to prepare AiiDA workflow" in captured.out


def test_create_aiida_workflow_malformed_config(tmp_path, capsys):
    """Test workflow preparation with malformed config file."""
    from sirocco.cli import create_aiida_workflow

    # Create a malformed YAML file
    bad_config = tmp_path / "bad_config.yml"
    bad_config.write_text("invalid: yaml: content: [")

    with pytest.raises(typer.Exit):
        create_aiida_workflow(str(bad_config))

    captured = capsys.readouterr()
    assert "Failed to prepare AiiDA workflow" in captured.out
