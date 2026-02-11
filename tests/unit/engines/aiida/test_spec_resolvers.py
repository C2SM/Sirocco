"""Unit tests for sirocco.engines.aiida.spec_resolvers module."""

from unittest.mock import Mock, patch

import aiida.orm
import pytest
from rich.pretty import pprint

from sirocco.engines.aiida.models import (
    AiidaIconTaskSpec,
    AiidaMetadata,
    AiidaMetadataOptions,
    AiidaShellTaskSpec,
    DependencyInfo,
    ShellDependencyMappings,
)
from sirocco.engines.aiida.spec_resolvers import (
    IconTaskSpecResolver,
    ShellTaskSpecResolver,
)

# Reusable default metadata (no computer, empty options)
DEFAULT_METADATA = AiidaMetadata(options=AiidaMetadataOptions())


def _make_input_data_info(port, label, *, is_available=True, name="f", path="/x"):
    """Create a minimal input_data_info dict."""
    return {"port": port, "name": name, "coordinates": {}, "label": label, "is_available": is_available, "path": path}


def _make_shell_spec_dict(**overrides) -> dict:
    """Create a minimal valid AiidaShellTaskSpec dict for testing."""
    base = {
        "label": "test_task",
        "code_pk": 1,
        "node_pks": {},
        "metadata": DEFAULT_METADATA,
        "arguments_template": "echo hello",
        "filenames": {},
        "outputs": [],
        "input_data_info": [],
        "output_data_info": [],
        "output_port_mapping": {},
    }
    base.update(overrides)
    return base


def _make_icon_spec_dict(**overrides) -> dict:
    """Create a minimal valid AiidaIconTaskSpec dict for testing."""
    base = {
        "label": "test_icon",
        "code_pk": 1,
        "master_namelist_pk": 2,
        "model_namelist_pks": {},
        "metadata": DEFAULT_METADATA,
        "output_port_mapping": {},
    }
    base.update(overrides)
    return base


# --- Shell task tests ---


class TestShellTaskSpecResolverInit:
    """Test ShellTaskSpecResolver initialization and spec loading."""

    def test_load_spec(self):
        """Test that _load_spec creates a valid AiidaShellTaskSpec."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())

        print("\n=== Shell spec ===")
        pprint(resolver.spec)

        assert isinstance(resolver.spec, AiidaShellTaskSpec)
        assert resolver.spec.label == "test_task"
        assert resolver.spec.code_pk == 1

    def test_build_input_port_label_mapper_empty(self):
        """Test port label mapper with no input data."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())

        print(f"\n=== Mapper (empty) ===\n{resolver.input_port_label_mapper!r}")

        assert len(resolver.input_port_label_mapper) == 0

    def test_build_input_port_label_mapper_filters_available(self):
        """Test port label mapper only includes AvailableData (is_available=True)."""
        spec_dict = _make_shell_spec_dict(
            input_data_info=[
                _make_input_data_info("p1", "lbl1", is_available=True),
                _make_input_data_info("p2", "lbl2", is_available=False),
            ]
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        print(f"\n=== Mapper (filters available) ===\n{resolver.input_port_label_mapper!r}")

        assert len(resolver.input_port_label_mapper) == 1
        assert resolver.input_port_label_mapper.get_port_for_label("lbl1") == "p1"
        assert resolver.input_port_label_mapper.get_port_for_label("lbl2") is None

    def test_build_input_port_label_mapper_multiple_on_same_port(self):
        """Test port label mapper with multiple labels on one port."""
        spec_dict = _make_shell_spec_dict(
            input_data_info=[
                _make_input_data_info("p1", "lbl_a", name="a"),
                _make_input_data_info("p1", "lbl_b", name="b"),
            ]
        )
        resolver = ShellTaskSpecResolver(spec_dict)
        labels = resolver.input_port_label_mapper.get_labels_for_port("p1")

        print(f"\n=== Mapper (multi-label) ===\n{resolver.input_port_label_mapper!r}")
        print(f"Labels for p1: {labels}")

        assert len(resolver.input_port_label_mapper) == 2
        assert set(labels) == {"lbl_a", "lbl_b"}


class TestShellAddInputDataNodes:
    """Test _add_input_data_nodes method."""

    @pytest.mark.parametrize("input_data_nodes", [None, {}], ids=["none", "empty_dict"])
    def test_empty_input(self, input_data_nodes):
        """Test with None or empty dict input returns empty mappings."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        all_nodes: dict = {}
        result = resolver._add_input_data_nodes(all_nodes, input_data_nodes)

        print(f"\n=== _add_input_data_nodes({input_data_nodes}) ===")
        print(f"result: {result}")
        print(f"all_nodes: {all_nodes}")

        assert result == {}
        assert all_nodes == {}

    def test_resolves_port_to_label_via_mapper(self):
        """Test that nodes are keyed by data label (resolved via mapper), not port name."""
        spec_dict = _make_shell_spec_dict(input_data_info=[_make_input_data_info("input_port", "data_label")])
        resolver = ShellTaskSpecResolver(spec_dict)

        mock_node = Mock(spec=aiida.orm.Data)
        all_nodes: dict = {}
        result = resolver._add_input_data_nodes(all_nodes, {"input_port": mock_node})

        print("\n=== _add_input_data_nodes (port->label mapping) ===")
        print(f"result: {result}")
        print(f"all_nodes keys: {list(all_nodes.keys())}")

        assert "data_label" in all_nodes
        assert all_nodes["data_label"] is mock_node
        assert result == {"data_label": "data_label"}

    def test_falls_back_to_port_name_when_not_in_mapper(self):
        """Test fallback to port_name when port is not in mapper."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())

        mock_node = Mock(spec=aiida.orm.Data)
        all_nodes: dict = {}
        result = resolver._add_input_data_nodes(all_nodes, {"unknown_port": mock_node})

        print("\n=== _add_input_data_nodes (fallback to port name) ===")
        print(f"result: {result}")
        print(f"all_nodes keys: {list(all_nodes.keys())}")

        assert "unknown_port" in all_nodes
        assert result == {"unknown_port": "unknown_port"}

    def test_multiple_inputs(self):
        """Test adding multiple input data nodes preserves pre-existing nodes."""
        spec_dict = _make_shell_spec_dict(
            input_data_info=[
                _make_input_data_info("p1", "lbl_a", name="a"),
                _make_input_data_info("p2", "lbl_b", name="b"),
            ]
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        node_a = Mock(spec=aiida.orm.Data)
        node_b = Mock(spec=aiida.orm.Data)
        all_nodes: dict = {"existing": Mock()}
        result = resolver._add_input_data_nodes(all_nodes, {"p1": node_a, "p2": node_b})

        print("\n=== _add_input_data_nodes (multiple) ===")
        print(f"result: {result}")
        print(f"all_nodes keys: {list(all_nodes.keys())}")

        assert all_nodes["lbl_a"] is node_a
        assert all_nodes["lbl_b"] is node_b
        assert "existing" in all_nodes
        assert result == {"lbl_a": "lbl_a", "lbl_b": "lbl_b"}

    def test_multiple_labels_on_same_port_uses_first(self):
        """Test that when multiple labels map to one port, first label is used."""
        spec_dict = _make_shell_spec_dict(
            input_data_info=[
                _make_input_data_info("p1", "first_label", name="a"),
                _make_input_data_info("p1", "second_label", name="b"),
            ]
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        mock_node = Mock(spec=aiida.orm.Data)
        all_nodes: dict = {}
        resolver._add_input_data_nodes(all_nodes, {"p1": mock_node})

        first_label = resolver.input_port_label_mapper.get_labels_for_port("p1")[0]

        print("\n=== _add_input_data_nodes (multi-label same port) ===")
        print(f"first_label: {first_label}")
        print(f"all_nodes keys: {list(all_nodes.keys())}")

        assert first_label in all_nodes
        assert all_nodes[first_label] is mock_node


class TestShellLoadResources:
    """Test resource loading methods."""

    @patch("aiida.orm.load_code")
    def test_load_code(self, mock_load_code):
        """Test _load_code delegates to aiida.orm.load_code with correct PK."""
        mock_code = Mock(spec=aiida.orm.Code)
        mock_load_code.return_value = mock_code

        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(code_pk=42))
        result = resolver._load_code()

        print(f"\n=== _load_code ===\nresult: {result}")
        print(f"load_code called with: {mock_load_code.call_args}")

        mock_load_code.assert_called_once_with(pk=42)
        assert result is mock_code

    @patch("aiida.orm.load_node")
    def test_build_base_nodes(self, mock_load_node):
        """Test _build_base_nodes loads all stored nodes by PK."""
        mock_node_a = Mock(spec=aiida.orm.Data)
        mock_node_b = Mock(spec=aiida.orm.Data)
        mock_load_node.side_effect = [mock_node_a, mock_node_b]

        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(node_pks={"script": 10, "config": 20}))
        result = resolver._build_base_nodes()
        loaded_pks = {call.args[0] for call in mock_load_node.call_args_list}

        print("\n=== _build_base_nodes ===")
        print(f"result keys: {list(result.keys())}")
        print(f"loaded PKs: {loaded_pks}")

        assert result == {"script": mock_node_a, "config": mock_node_b}
        assert loaded_pks == {10, 20}

    @patch("aiida.orm.load_node")
    def test_build_base_nodes_empty(self, mock_load_node):
        """Test _build_base_nodes with no stored nodes."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        result = resolver._build_base_nodes()

        print(f"\n=== _build_base_nodes (empty) ===\nresult: {result}")

        assert result == {}
        mock_load_node.assert_not_called()


class TestShellResolveDependencies:
    """Test _resolve_dependencies method."""

    @pytest.mark.parametrize("task_folders", [None, {}], ids=["none", "empty_dict"])
    def test_no_dependencies(self, task_folders):
        """Test with None or empty task folders returns empty mappings."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        all_nodes: dict = {}
        placeholders, filenames = resolver._resolve_dependencies(all_nodes, task_folders)

        print(f"\n=== _resolve_dependencies({task_folders}) ===")
        print(f"placeholders: {placeholders}")
        print(f"filenames: {filenames}")

        assert placeholders == {}
        assert filenames == {}

    @patch("sirocco.engines.aiida.spec_resolvers.resolve_shell_dependency_mappings")
    def test_with_task_folders(self, mock_resolve):
        """Test dependency resolution with real DependencyInfo objects."""
        mock_remote = Mock(spec=aiida.orm.RemoteData)
        mock_resolve.return_value = ShellDependencyMappings(
            nodes={"parent_task_parent_output_remote": mock_remote},
            placeholders={"parent_output": "parent_task_parent_output_remote"},
            filenames={"parent_task_parent_output_remote": "output.txt"},
        )

        dep_info_dict = {"dep_label": "parent_task", "filename": "output.txt", "data_label": "parent_output"}
        spec_dict = _make_shell_spec_dict(
            port_dependency_mapping={"port1": [dep_info_dict]},
            filenames={"existing": "file.txt"},
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        all_nodes: dict = {}
        placeholders, filenames = resolver._resolve_dependencies(all_nodes, {"parent_task": 123})

        # Verify resolve_shell_dependency_mappings was called with DependencyInfo objects
        port_deps = mock_resolve.call_args[0][1]

        print("\n=== _resolve_dependencies (with task folders) ===")
        print(f"port_deps passed to resolver: {port_deps}")
        print(f"placeholders: {placeholders}")
        print(f"filenames: {filenames}")
        print(f"all_nodes keys: {list(all_nodes.keys())}")

        assert "port1" in port_deps
        assert isinstance(port_deps["port1"][0], DependencyInfo)
        assert port_deps["port1"][0].dep_label == "parent_task"

        assert placeholders == {"parent_output": "parent_task_parent_output_remote"}
        assert filenames == {"parent_task_parent_output_remote": "output.txt"}
        assert "parent_task_parent_output_remote" in all_nodes


class TestShellBuildMetadata:
    """Test _build_metadata method.

    Note: SLURM dependency injection is tested thoroughly in test_calcjob_builders.py.
    These tests verify the resolver's wiring (computer lookup, delegation).
    """

    def test_with_real_computer(self, aiida_localhost):
        """Test metadata building resolves computer from label."""
        spec_dict = _make_shell_spec_dict(
            metadata=AiidaMetadata(computer_label=aiida_localhost.label, options=AiidaMetadataOptions()),
        )
        resolver = ShellTaskSpecResolver(spec_dict)
        result = resolver._build_metadata(None)

        print("\n=== _build_metadata (with computer) ===")
        pprint(result)

        assert isinstance(result, AiidaMetadata)
        assert result.computer is not None
        assert result.computer.label == aiida_localhost.label

    @patch("sirocco.engines.aiida.spec_resolvers.add_slurm_dependencies_to_metadata")
    def test_without_computer_label(self, mock_add_slurm):
        """Test metadata building without computer label passes None."""
        mock_add_slurm.return_value = DEFAULT_METADATA

        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        resolver._build_metadata(None)

        print(f"\n=== _build_metadata (no computer) ===\ncalled with: {mock_add_slurm.call_args}")

        mock_add_slurm.assert_called_once_with(resolver.spec.metadata, None, None)


class TestShellBuildArguments:
    """Test _build_arguments method."""

    def test_substitutes_placeholders(self):
        """Test that placeholders in arguments_template are substituted."""
        resolver = ShellTaskSpecResolver(
            _make_shell_spec_dict(arguments_template="--input {data_label} --output {result}")
        )
        result = resolver._build_arguments({"data_label": "node_key", "result": "out_key"})

        print(f"\n=== _build_arguments (with placeholders) ===\nresult: {result}")

        assert isinstance(result, list)
        assert "--input" in result
        assert "{node_key}" in result
        assert "{out_key}" in result

    def test_no_placeholders(self):
        """Test arguments without any placeholders."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(arguments_template="--verbose --dry-run"))
        result = resolver._build_arguments({})

        print(f"\n=== _build_arguments (no placeholders) ===\nresult: {result}")

        assert result == ["--verbose", "--dry-run"]

    def test_empty_template(self):
        """Test with empty arguments template."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(arguments_template=""))
        result = resolver._build_arguments({})

        print(f"\n=== _build_arguments (empty) ===\nresult: {result}")

        assert result == []


class TestShellExecute:
    """Test ShellTaskSpecResolver.execute orchestration and _create_workgraph_task."""

    @pytest.mark.parametrize(
        "has_inputs",
        [
            pytest.param(True, id="all_inputs"),
            pytest.param(False, id="all_none"),
        ],
    )
    @patch.object(ShellTaskSpecResolver, "_create_workgraph_task")
    @patch.object(ShellTaskSpecResolver, "_build_arguments")
    @patch.object(ShellTaskSpecResolver, "_build_metadata")
    @patch.object(ShellTaskSpecResolver, "_resolve_dependencies")
    @patch.object(ShellTaskSpecResolver, "_add_input_data_nodes")
    @patch.object(ShellTaskSpecResolver, "_build_base_nodes")
    @patch.object(ShellTaskSpecResolver, "_load_code")
    def test_execute_calls_all_steps(
        self,
        mock_load_code,
        mock_build_nodes,
        mock_add_inputs,
        mock_resolve_deps,
        mock_build_metadata,
        mock_build_args,
        mock_create_task,
        has_inputs,
    ):
        """Test that execute calls all steps and returns _create_workgraph_task result."""
        mock_load_code.return_value = Mock(spec=aiida.orm.Code)
        mock_build_nodes.return_value = {"script": Mock()}
        mock_add_inputs.return_value = {"lbl": "lbl"}
        mock_resolve_deps.return_value = ({"dep_lbl": "dep_key"}, {"dep_key": "out.txt"})
        mock_build_metadata.return_value = Mock()
        mock_build_args.return_value = ["--input", "{lbl}"]
        expected_output = {"output": Mock()}
        mock_create_task.return_value = expected_output

        input_nodes = {"port": Mock(spec=aiida.orm.Data)} if has_inputs else None
        task_folders = {"parent": 123} if has_inputs else None
        task_job_ids = {"parent": 456} if has_inputs else None

        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        result = resolver.execute(
            input_data_nodes=input_nodes,
            task_folders=task_folders,
            task_job_ids=task_job_ids,
        )

        print(f"\n=== execute (has_inputs={has_inputs}) ===")
        print(f"_build_metadata called with: {mock_build_metadata.call_args}")
        print(f"_add_input_data_nodes called with: {mock_add_inputs.call_args}")
        print(f"_resolve_dependencies called with: {mock_resolve_deps.call_args}")
        print(f"result: {result}")

        mock_load_code.assert_called_once()
        mock_build_nodes.assert_called_once()
        mock_add_inputs.assert_called_once()
        mock_resolve_deps.assert_called_once()
        mock_build_metadata.assert_called_once_with(task_job_ids)
        mock_build_args.assert_called_once()
        mock_create_task.assert_called_once()
        assert result is expected_output

    @patch("sirocco.engines.aiida.spec_resolvers.get_current_graph")
    @patch("aiida_workgraph.tasks.shelljob_task._build_shelljob_TaskSpec")
    def test_create_workgraph_task(self, mock_build_spec, mock_get_graph):
        """Test that _create_workgraph_task adds a shell task to the current WorkGraph."""
        mock_spec = Mock()
        mock_build_spec.return_value = mock_spec

        mock_wg = Mock()
        mock_shell_task = Mock()
        mock_shell_task.outputs = {"remote_folder": Mock()}
        mock_wg.add_task.return_value = mock_shell_task
        mock_get_graph.return_value = mock_wg

        spec_dict = _make_shell_spec_dict(
            label="my_shell_task",
            outputs=["remote_folder"],
            output_data_info=[
                {"name": "out", "coordinates": {}, "label": "out", "path": "out.txt", "port": "p"},
            ],
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        result = resolver._create_workgraph_task(
            code=Mock(spec=aiida.orm.Code),
            all_nodes={"script": Mock()},
            filenames={"script": "script.sh"},
            metadata=DEFAULT_METADATA,
            arguments=["--flag"],
        )

        add_task_kwargs = mock_wg.add_task.call_args[1]

        print("\n=== _create_workgraph_task ===")
        print(f"_build_shelljob_TaskSpec called with: {mock_build_spec.call_args}")
        print(f"add_task kwargs: {add_task_kwargs}")
        print(f"result: {result}")

        assert mock_build_spec.call_args[1]["parser_outputs"] == ["out"]
        assert add_task_kwargs["name"] == "my_shell_task"
        assert add_task_kwargs["arguments"] == ["--flag"]
        assert add_task_kwargs["resolve_command"] is False
        assert result is mock_shell_task.outputs


# --- Icon task tests ---


class TestIconTaskSpecResolverInit:
    """Test IconTaskSpecResolver initialization and spec loading."""

    def test_load_spec(self):
        """Test that _load_spec creates a valid AiidaIconTaskSpec."""
        resolver = IconTaskSpecResolver(_make_icon_spec_dict())

        print("\n=== Icon spec ===")
        pprint(resolver.spec)

        assert isinstance(resolver.spec, AiidaIconTaskSpec)
        assert resolver.spec.label == "test_icon"
        assert resolver.spec.code_pk == 1
        assert resolver.spec.master_namelist_pk == 2


class TestIconResolveDependencies:
    """Test IconTaskSpecResolver._resolve_dependencies method."""

    @pytest.mark.parametrize("task_folders", [None, {}], ids=["none", "empty_dict"])
    def test_no_dependencies(self, task_folders):
        """Test _resolve_dependencies with None or empty task folders."""
        resolver = IconTaskSpecResolver(_make_icon_spec_dict())
        result = resolver._resolve_dependencies(task_folders)

        print(f"\n=== Icon _resolve_dependencies({task_folders}) ===\nresult: {result}")

        assert result == {}

    @patch("sirocco.engines.aiida.spec_resolvers.resolve_icon_dependency_mapping")
    def test_with_real_dep_info(self, mock_resolve):
        """Test _resolve_dependencies constructs DependencyInfo from serialized dicts."""
        mock_remote = Mock(spec=aiida.orm.RemoteData)
        mock_resolve.return_value = {"restart_file.atm": mock_remote}

        dep_info_dict = {"dep_label": "parent", "filename": None, "data_label": "restart"}
        spec_dict = _make_icon_spec_dict(
            port_dependency_mapping={"restart_file": [dep_info_dict]},
            master_namelist_pk=10,
            model_namelist_pks={"atm": 20},
        )
        resolver = IconTaskSpecResolver(spec_dict)
        result = resolver._resolve_dependencies({"parent": 123})

        call_args = mock_resolve.call_args[0]
        port_deps = call_args[1]

        print("\n=== Icon _resolve_dependencies (with deps) ===")
        print(f"port_deps: {port_deps}")
        print(f"master_namelist_pk: {call_args[2]}")
        print(f"model_namelist_pks: {call_args[3]}")
        print(f"result: {result}")

        assert isinstance(port_deps["restart_file"][0], DependencyInfo)
        assert port_deps["restart_file"][0].dep_label == "parent"
        assert port_deps["restart_file"][0].filename is None
        assert call_args[2] == 10  # master_namelist_pk
        assert call_args[3] == {"atm": 20}  # model_namelist_pks

        assert result == {"restart_file.atm": mock_remote}


class TestIconBuildMetadata:
    """Test IconTaskSpecResolver._build_metadata method.

    Note: SLURM dependency injection is tested thoroughly in test_calcjob_builders.py.
    These tests verify the resolver's wiring (computer lookup, label passthrough).
    """

    def test_with_real_computer(self, aiida_localhost):
        """Test _build_metadata resolves computer from label."""
        spec_dict = _make_icon_spec_dict(
            label="icon_run",
            metadata=AiidaMetadata(computer_label=aiida_localhost.label, options=AiidaMetadataOptions()),
        )
        resolver = IconTaskSpecResolver(spec_dict)
        result = resolver._build_metadata(None)

        print("\n=== Icon _build_metadata (with computer) ===")
        pprint(result)

        assert isinstance(result, AiidaMetadata)
        assert result.computer is not None
        assert result.computer.label == aiida_localhost.label

    @patch("sirocco.engines.aiida.spec_resolvers.add_slurm_dependencies_to_metadata")
    def test_without_computer(self, mock_add_slurm):
        """Test _build_metadata without computer label passes task label for ICON handling."""
        mock_add_slurm.return_value = DEFAULT_METADATA

        resolver = IconTaskSpecResolver(_make_icon_spec_dict())
        resolver._build_metadata(None)

        print(f"\n=== Icon _build_metadata (no computer) ===\ncalled with: {mock_add_slurm.call_args}")

        mock_add_slurm.assert_called_once_with(resolver.spec.metadata, None, None, "test_icon")


class TestIconExecute:
    """Test IconTaskSpecResolver.execute orchestration."""

    @pytest.mark.parametrize(
        "has_inputs",
        [
            pytest.param(True, id="all_inputs"),
            pytest.param(False, id="all_none"),
        ],
    )
    @patch("sirocco.engines.aiida.spec_resolvers.build_icon_calcjob_inputs")
    @patch("sirocco.engines.aiida.spec_resolvers.task")
    @patch.object(IconTaskSpecResolver, "_build_metadata")
    @patch.object(IconTaskSpecResolver, "_resolve_dependencies")
    def test_execute_orchestration(
        self,
        mock_resolve_deps,
        mock_build_metadata,
        mock_task_decorator,
        mock_build_inputs,
        has_inputs,
    ):
        """Test execute calls steps in correct order and returns task result."""
        input_nodes = {"port": Mock(spec=aiida.orm.Data)} if has_inputs else None
        task_folders = {"parent": 123} if has_inputs else None
        task_job_ids = {"parent": 456} if has_inputs else None

        mock_resolve_deps.return_value = {"dep": Mock(spec=aiida.orm.RemoteData)} if task_folders else {}
        mock_build_metadata.return_value = DEFAULT_METADATA
        mock_build_inputs.return_value = {"code": Mock()}

        expected_output = {"outputs": Mock()}
        mock_task_decorator.return_value = Mock(return_value=expected_output)

        resolver = IconTaskSpecResolver(_make_icon_spec_dict())
        result = resolver.execute(
            input_data_nodes=input_nodes,
            task_folders=task_folders,
            task_job_ids=task_job_ids,
        )

        # input_data_nodes should never be None when passed to build_icon_calcjob_inputs
        input_arg = mock_build_inputs.call_args[0][1]

        print(f"\n=== Icon execute (has_inputs={has_inputs}) ===")
        print(f"_resolve_dependencies called with: {mock_resolve_deps.call_args}")
        print(f"_build_metadata called with: {mock_build_metadata.call_args}")
        print(f"build_icon_calcjob_inputs input_data_nodes arg: {input_arg}")
        print(f"result: {result}")

        mock_resolve_deps.assert_called_once_with(task_folders)
        mock_build_metadata.assert_called_once_with(task_job_ids)
        mock_build_inputs.assert_called_once()
        assert result is expected_output
        assert isinstance(input_arg, dict)
