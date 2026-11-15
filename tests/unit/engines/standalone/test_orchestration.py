"""Simulate standalone orchestration against a fake job scheduler.

Each sirocco command (start, continue, stop, restart) is run as in production: a fresh
`StandaloneEngine` is built from the config for every command, the only memory between
commands being the state file and the fake scheduler. Everything the engine does is
recorded in a trace and compared to a reference file:

- every submission (job id, parent job ids, dependency type, stdout/stderr mode),
- every cancellation,
- the state file after each command,
- the run script of each submission (only when it changed since the last submission of
  the same task, to keep the reference readable).

On mismatch, the new trace is written next to the reference with a `.new.txt` suffix.
Run pytest with `--reserialize` to regenerate the references after an intended change.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal

import pytest

from sirocco.engines.standalone import StandaloneEngine, handlers
from sirocco.engines.standalone.scheduler import DEPENDENCY_TYPE_T, STDOUTERR_MODE_T, Scheduler
from sirocco.engines.standalone.state import TaskStatus

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core import Task

ORCHESTRATION_CASES = ["parameters", "partial-dates"]
MAX_STEPS = 60  # Safety net, cases complete well before
# Steps at which events are injected, early enough for all cases to still be running
FAIL_STEP = 1
STOP_STEP = 0


@dataclass
class FakeCluster:
    """Job states and recorded events, shared by all scheduler instances of a test"""

    rootdir: Path
    status: dict[str, TaskStatus] = field(default_factory=dict)
    trace: list[str] = field(default_factory=list)
    scripts: dict[str, str] = field(default_factory=dict)
    next_jobid: int = 1001

    def record(self, line: str) -> None:
        self.trace.append(line.replace(str(self.rootdir), "<ROOTDIR>"))


@dataclass(kw_only=True)
class FakeScheduler(Scheduler):
    name: ClassVar[str] = "fake"
    cluster: ClassVar[FakeCluster]

    def header_lines(self, task: Task, stdouterr_mode: STDOUTERR_MODE_T = "overwrite") -> list[str]:
        stdouterr_filename = handlers.submission_settings(task).stdouterr_filename
        return [f"#FAKE --job-name={task.label} --output={stdouterr_filename} --mode={stdouterr_mode}"]

    def submit_to_scheduler(
        self,
        task: Task,
        run_dir: Path,
        parent_ids: list[str],
        env: dict[str, str],  # noqa: ARG002
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        jobid = str(self.cluster.next_jobid)
        self.cluster.next_jobid += 1
        self.cluster.status[jobid] = TaskStatus.WAITING
        self.cluster.record(f"submit {task.label} ({jobid}) after {parent_ids} [{dependency_type}]")
        script = (run_dir / handlers.submission_settings(task).submit_filename).read_text()
        if self.cluster.scripts.get(task.label) != script:
            self.cluster.scripts[task.label] = script
            self.cluster.record(f"--- run script {task.label}\n{script}\n---")
        return jobid

    def get_status(self, task: Task, jobid: str) -> TaskStatus:  # noqa: ARG002
        return self.cluster.status[jobid]

    def cancel(self, task: Task, jobid: str) -> None:
        self.cluster.record(f"cancel {task.label} ({jobid})")
        self.cluster.status[jobid] = TaskStatus.FAILED


class Simulation:
    def __init__(self, config_dir: Path, cluster: FakeCluster) -> None:
        self.config_dir = config_dir
        self.cluster = cluster

    def engine(self) -> StandaloneEngine:
        """Fresh engine, as built by each sirocco command"""
        return StandaloneEngine.from_config_path(self.config_dir)

    def command(self, name: Literal["start", "continue", "stop", "restart"]) -> StandaloneEngine:
        self.cluster.record(f"\n=== sirocco {name}")
        engine = self.engine()
        match name:
            case "start":
                engine.start(log_type="std")
            case "continue":
                engine.continue_wf(log_type="std")
            case "stop":
                engine.stop(mode="cool-down", log_type="std")
            case "restart":
                engine.restart(log_type="std")
        self.cluster.record(f"status: {engine.status.name}")
        self.cluster.record(f"--- state.yaml\n{engine.state_file.read_text()}---")
        return engine

    def loaded_engine(self) -> StandaloneEngine:
        """Engine with the current state loaded, to inspect the front"""
        engine = self.engine()
        engine.load_state()
        return engine

    def set_status(self, engine: StandaloneEngine, task: Task, status: TaskStatus) -> None:
        """Simulate the cluster changing the status of a submitted task"""
        self.cluster.record(f"# {task.label} -> {status.name}")
        self.cluster.status[engine.jobid(task)] = status


def run_scenario(sim: Simulation, scenario: Literal["complete", "fail"]) -> None:
    sim.command("start")
    for step in range(MAX_STEPS):
        engine = sim.loaded_engine()
        if not engine.front[0]:
            return
        if scenario == "fail" and step == FAIL_STEP:
            sim.set_status(engine, engine.front[0][0], TaskStatus.FAILED)
            sim.command("continue")
            return
        for task in engine.front[0]:
            sim.set_status(engine, task, TaskStatus.COMPLETED)
        sim.command("continue")
        if scenario == "complete" and step == STOP_STEP:
            # Stop in cool-down mode while one task is running, then restart
            engine = sim.loaded_engine()
            sim.set_status(engine, engine.front[0][0], TaskStatus.RUNNING)
            sim.command("stop")
            sim.command("restart")
    pytest.fail(f"workflow not completed after {MAX_STEPS} steps")


@pytest.mark.parametrize("scenario", ["complete", "fail"])
@pytest.mark.parametrize("case", ORCHESTRATION_CASES)
def test_orchestration(case, scenario, tmp_path, test_rootdir, monkeypatch, pytestconfig):
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / f"tests/cases/{case}/config", config_dir)
    cluster = FakeCluster(rootdir=config_dir)
    monkeypatch.setattr(FakeScheduler, "cluster", cluster, raising=False)
    monkeypatch.setitem(Scheduler.scheduler_classes, "slurm", FakeScheduler)
    # Avoid spawning a login shell, the fake scheduler doesn't use the environment
    monkeypatch.setattr(StandaloneEngine, "base_env", {"SIROCCO_TEST": "1"})

    run_scenario(Simulation(config_dir, cluster), scenario)

    trace = "\n".join(cluster.trace).lstrip() + "\n"
    reference = test_rootdir / f"tests/cases/{case}/data/standalone_{scenario}.txt"
    if pytestconfig.getoption("reserialize"):
        reference.write_text(trace)
    if not reference.exists() or reference.read_text() != trace:
        new_path = reference.with_suffix(".new.txt")
        new_path.write_text(trace)
        pytest.fail(f"Orchestration trace differs from {reference}. New trace dumped to {new_path}.")
