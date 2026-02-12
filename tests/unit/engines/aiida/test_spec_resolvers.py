"""Unit tests for sirocco.engines.aiida.spec_resolvers module."""

from unittest.mock import Mock, patch

import aiida.orm
import pytest
from rich.pretty import pprint

from sirocco.engines.aiida.models import (
    AiidaMetadata,
    AiidaMetadataOptions,
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

    def test_load_code(self, stored_shell_code):
        """Test _load_code loads code by PK."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(code_pk=stored_shell_code.pk))
        result = resolver._load_code()

        print(f"\n=== _load_code ===\nresult: {result}")
        print(f"result type: {type(result)}")
        print(f"result PK: {result.pk}")

        assert result.pk == stored_shell_code.pk
        assert result.is_stored
        assert isinstance(result, aiida.orm.Code)

    def test_build_base_nodes(self, stored_singlefile, stored_remote_data):
        """Test _build_base_nodes loads all stored nodes by PK."""
        node_pks = {"script": stored_singlefile.pk, "config": stored_remote_data.pk}
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(node_pks=node_pks))
        result = resolver._build_base_nodes()

        print("\n=== _build_base_nodes ===")
        print(f"result keys: {list(result.keys())}")
        print(f"result values: {[(k, v.pk, type(v).__name__) for k, v in result.items()]}")

        assert "script" in result
        assert "config" in result
        assert result["script"].pk == stored_singlefile.pk
        assert result["config"].pk == stored_remote_data.pk
        assert result["script"].is_stored
        assert result["config"].is_stored

    def test_build_base_nodes_empty(self):
        """Test _build_base_nodes with no stored nodes."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict())
        result = resolver._build_base_nodes()

        print(f"\n=== _build_base_nodes (empty) ===\nresult: {result}")

        assert result == {}


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
        """Test dependency resolution with DependencyInfo objects."""
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


class TestBuildMetadata:
    """Test _build_metadata method for both Shell and Icon resolvers.

    Note: SLURM dependency injection is tested thoroughly in test_calcjob_builders.py.
    These tests verify the resolver's wiring (computer lookup, delegation).
    """

    @pytest.mark.parametrize(
        ("resolver_class", "spec_factory"),
        [
            pytest.param(ShellTaskSpecResolver, _make_shell_spec_dict, id="shell"),
            pytest.param(IconTaskSpecResolver, _make_icon_spec_dict, id="icon"),
        ],
    )
    def test_with_real_computer(self, aiida_localhost, resolver_class, spec_factory):
        """Test metadata building resolves computer from label."""
        spec_dict = spec_factory(
            metadata=AiidaMetadata(computer_label=aiida_localhost.label, options=AiidaMetadataOptions()),
        )
        resolver = resolver_class(spec_dict)
        result = resolver._build_metadata(None)

        print(f"\n=== {resolver_class.__name__} _build_metadata (with computer) ===")
        pprint(result)

        assert isinstance(result, AiidaMetadata)
        assert result.computer is not None
        assert result.computer.label == aiida_localhost.label


class TestShellExecute:
    """Test ShellTaskSpecResolver execution and argument building."""

    def test_build_arguments_with_placeholders(self):
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

    def test_build_arguments_without_placeholders(self):
        """Test arguments without any placeholders."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(arguments_template="--verbose --dry-run"))
        result = resolver._build_arguments({})

        print(f"\n=== _build_arguments (no placeholders) ===\nresult: {result}")

        assert result == ["--verbose", "--dry-run"]

    def test_execute_with_invalid_code_pk(self):
        """Test execute raises NotExistent when code PK doesn't exist."""
        resolver = ShellTaskSpecResolver(_make_shell_spec_dict(code_pk=99999))

        with pytest.raises(aiida.common.NotExistent) as exc:
            resolver.execute(
                input_data_nodes=None,
                task_folders=None,
                task_job_ids=None,
            )

        assert "code" in str(exc.value).lower()

    def test_execute_with_real_code_and_inputs(self, aiida_localhost):
        """Test execute with code and input data nodes."""
        # Create real stored code
        code = aiida.orm.InstalledCode(
            computer=aiida_localhost,
            filepath_executable="/bin/echo",
        ).store()

        # Create real input node
        input_file = aiida.orm.SinglefileData.from_string("test content", filename="input.txt").store()

        spec_dict = _make_shell_spec_dict(
            code_pk=code.pk,
            arguments_template="cat {input_data}",
            input_data_info=[_make_input_data_info("input_port", "input_data")],
            metadata=AiidaMetadata(
                computer_label=aiida_localhost.label,
                options=AiidaMetadataOptions(),
            ),
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        # Mock only the WorkGraph interaction (unavoidable external dependency)
        with patch("sirocco.engines.aiida.spec_resolvers.get_current_graph") as mock_graph:
            mock_wg = Mock()
            mock_task = Mock()
            mock_task.outputs = {"remote_folder": Mock()}
            mock_wg.add_task.return_value = mock_task
            mock_graph.return_value = mock_wg

            result = resolver.execute(
                input_data_nodes={"input_port": input_file},
                task_folders=None,
                task_job_ids=None,
            )

            # Verify the task was added to workgraph
            mock_wg.add_task.assert_called_once()
            add_task_kwargs = mock_wg.add_task.call_args[1]

            print("\n=== execute ===")
            print(f"Task name: {add_task_kwargs['name']}")
            print(f"Arguments: {add_task_kwargs['arguments']}")
            print(f"Nodes keys: {list(add_task_kwargs['nodes'].keys())}")

            # Verify correct nodes and arguments were built
            assert add_task_kwargs["name"] == "test_task"
            assert "input_data" in add_task_kwargs["nodes"]
            assert add_task_kwargs["nodes"]["input_data"] is input_file
            assert "{input_data}" in " ".join(add_task_kwargs["arguments"])
            assert result == mock_task.outputs

    def test_execute_with_base_nodes(self, aiida_localhost):
        """Test execute loads and uses stored base nodes (scripts, configs)."""
        code = aiida.orm.InstalledCode(
            computer=aiida_localhost,
            filepath_executable="/bin/bash",
        ).store()

        script = aiida.orm.SinglefileData.from_string("#!/bin/bash\necho hello", filename="script.sh").store()

        spec_dict = _make_shell_spec_dict(
            code_pk=code.pk,
            node_pks={"script": script.pk},
            arguments_template="{script}",
            metadata=AiidaMetadata(
                computer_label=aiida_localhost.label,
                options=AiidaMetadataOptions(),
            ),
        )
        resolver = ShellTaskSpecResolver(spec_dict)

        with patch("sirocco.engines.aiida.spec_resolvers.get_current_graph") as mock_graph:
            mock_wg = Mock()
            mock_task = Mock()
            mock_task.outputs = {"remote_folder": Mock()}
            mock_wg.add_task.return_value = mock_task
            mock_graph.return_value = mock_wg

            result = resolver.execute(
                input_data_nodes=None,
                task_folders=None,
                task_job_ids=None,
            )

            add_task_kwargs = mock_wg.add_task.call_args[1]

            print("\n=== execute with base nodes ===")
            print(f"Nodes: {add_task_kwargs['nodes']}")
            print(f"Arguments: {add_task_kwargs['arguments']}")

            # Verify script node was loaded and included
            assert "script" in add_task_kwargs["nodes"]
            assert add_task_kwargs["nodes"]["script"].pk == script.pk
            # Verify the script placeholder appears in arguments
            assert "{script}" in " ".join(add_task_kwargs["arguments"])
            assert result == mock_task.outputs

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


class TestIconResolver:
    """Test IconTaskSpecResolver dependency resolution and execution."""

    @pytest.mark.parametrize("task_folders", [None, {}], ids=["none", "empty_dict"])
    def test_resolve_dependencies_empty(self, task_folders):
        """Test _resolve_dependencies with None or empty task folders."""
        resolver = IconTaskSpecResolver(_make_icon_spec_dict())
        result = resolver._resolve_dependencies(task_folders)

        print(f"\n=== Icon _resolve_dependencies({task_folders}) ===\nresult: {result}")

        assert result == {}

    @patch("sirocco.engines.aiida.spec_resolvers.resolve_icon_dependency_mapping")
    def test_resolve_dependencies_with_dep_info(self, mock_resolve):
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

    @patch("sirocco.engines.aiida.spec_resolvers.build_icon_calcjob_inputs")
    @patch("sirocco.engines.aiida.spec_resolvers.task")
    def test_execute_converts_none_to_empty_dict(self, mock_task, mock_build_inputs):
        """Test execute converts None input_data_nodes to empty dict before passing to builder.

        This is important because build_icon_calcjob_inputs expects a dict, not None.
        """
        mock_build_inputs.return_value = {"code": Mock()}
        mock_task.return_value = Mock(return_value={"outputs": Mock()})

        resolver = IconTaskSpecResolver(_make_icon_spec_dict())
        resolver.execute(
            input_data_nodes=None,  # Pass None
            task_folders=None,
            task_job_ids=None,
        )

        # Verify None was converted to empty dict
        input_arg = mock_build_inputs.call_args[0][1]

        print("\n=== Icon execute (None -> {}) ===")
        print(f"input_data_nodes arg type: {type(input_arg)}")
        print(f"input_data_nodes arg value: {input_arg}")

        assert isinstance(input_arg, dict)
        assert input_arg == {}

    def test_execute_with_invalid_code_pk(self):
        """Test execute fails when code PK doesn't exist."""
        resolver = IconTaskSpecResolver(_make_icon_spec_dict(code_pk=99999))

        with pytest.raises(aiida.common.NotExistent) as exc:
            resolver.execute(
                input_data_nodes=None,
                task_folders=None,
                task_job_ids=None,
            )

        assert "code" in str(exc.value).lower() or "99999" in str(exc.value)
