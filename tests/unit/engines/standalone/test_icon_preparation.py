"""Prepare ICON tasks with the standalone engine and compare the result to a reference.

The `icon-standalone` case covers ICON ports of every kind: available and generated
inputs, link only inputs, squashed images, restarts, output streams and finish status.
For each ICON task, the trace records the resolved output paths, the run script and the
run directory (symlinks with their targets, directories, content of generated files).
Files copied unchanged from the ICON runtime directory are only listed.

On mismatch, the new trace is written next to the reference with a `.new.txt` suffix.
Run pytest with `--reserialize` to regenerate the reference after an intended change.
"""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import pytest

from sirocco.core.tasks.icon_task import IconTask
from sirocco.engines.standalone import StandaloneEngine, handlers
from sirocco.engines.standalone.tasks.icon import RUNTIME_ROOT

if TYPE_CHECKING:
    from pathlib import Path

CASE = "icon-standalone"


def prepare(engine: StandaloneEngine, task: IconTask) -> Path:
    """Prepare `task` in a fresh run directory and generate its run script"""
    run_dir = engine.path_resolver.run_dir(task)
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True)
    handlers.prepare_for_submission(task, engine.path_resolver)
    engine.scheduler.generate_run_script(task, engine.path_resolver, "overwrite")
    return run_dir


def trace_run_dir(run_dir: Path, runtime_files: set[str]) -> list[str]:
    lines: list[str] = []
    for path in sorted(run_dir.rglob("*")):
        rel = path.relative_to(run_dir)
        if path.is_symlink():
            lines.append(f"{rel} -> {os.readlink(path)}")
        elif path.is_dir():
            lines.append(f"{rel}/")
        elif str(rel) in runtime_files:
            lines.append(f"{rel} (runtime file)")
        else:
            lines.append(f"--- {rel}\n{path.read_text().rstrip()}\n---")
    return lines


def icon_tasks(engine: StandaloneEngine) -> list[IconTask]:
    return sorted((task for task in engine.workflow.tasks if isinstance(task, IconTask)), key=lambda t: t.label)


@pytest.fixture
def engine(tmp_path, test_rootdir) -> StandaloneEngine:
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / f"tests/cases/{CASE}/config", config_dir)
    return StandaloneEngine.from_config_path(config_dir, template_context={"TESTS_ROOTDIR": str(test_rootdir)})


def test_icon_preparation(engine, tmp_path, test_rootdir, pytestconfig):
    runtime_files = {str(p.relative_to(RUNTIME_ROOT / "santis")) for p in (RUNTIME_ROOT / "santis").rglob("*")}
    trace: list[str] = []
    for task in icon_tasks(engine):
        run_dir = prepare(engine, task)
        trace.append(f"=== {task.label}")
        trace.append("--- resolved outputs")
        trace.extend(f"{data.label}: {engine.path_resolver.path(data)}" for data in task.output_data_nodes())
        trace.append("--- run directory")
        trace.extend(trace_run_dir(run_dir, runtime_files))
        trace.append("")
    text = "\n".join(trace).replace(str(tmp_path), "<ROOTDIR>").replace(str(test_rootdir), "<TESTS_ROOTDIR>")

    reference = test_rootdir / f"tests/cases/{CASE}/data/standalone_icon_preparation.txt"
    if pytestconfig.getoption("reserialize"):
        reference.parent.mkdir(exist_ok=True)
        reference.write_text(text)
    if not reference.exists() or reference.read_text() != text:
        new_path = reference.with_suffix(".new.txt")
        new_path.write_text(text)
        pytest.fail(f"ICON preparation differs from {reference}. New trace dumped to {new_path}.")


def test_icon_preparation_repeatable(engine):
    """Preparing a task again gives the same run directory, preparation does not alter the task"""
    for task in icon_tasks(engine):
        first = trace_run_dir(prepare(engine, task), set())
        second = trace_run_dir(prepare(engine, task), set())
        assert first == second
