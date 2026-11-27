"""Simulate standalone orchestration against a fake job scheduler.

Each sirocco command (start, continue, stop, restart) is run as in production: in a
transaction holding the workflow lock, with a fresh `StandaloneEngine` built from the config
for every command, the only memory between commands being the state file and the fake
scheduler. `continue` runs as the continuation job named in the state. Everything the engine does is
recorded in a trace and compared to a reference file:

- every submission (job id, parent job ids, dependency type, stdout/stderr mode),
- every cancellation,
- the state file after each command, in a compact form showing the front generations
  (`compact_state`),
- the run script of each submission (only when it changed since the last submission of
  the same task, to keep the reference readable).

The cases cover front depths 3 (`parameters`), 2 (`partial-dates`) and, by overriding
the config, 1 (`partial-dates` again), each depth propagating the front differently.

On mismatch, the new output is written next to the reference with a `.new` suffix.
Run pytest with `--reserialize` to regenerate the references after an intended change.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal

import pytest
from ruamel.yaml import YAML

from sirocco.engines.standalone import (
    CommandOutcome,
    Outcome,
    StandaloneEngine,
    Transaction,
    WorkflowState,
    WorkflowStatus,
    clean_up,
    stop,
)
from sirocco.engines.standalone.scheduler import (
    DEPENDENCY_TYPE_T,
    JOBID_ENV_VAR,
    STDOUTERR_MODE_T,
    Scheduler,
    UnknownJobError,
)
from sirocco.engines.standalone.state import TaskStatus
from sirocco.parsing.yaml_data_models import ConfigWorkflow

if TYPE_CHECKING:
    from pathlib import Path

    from sirocco.core import Task
    from sirocco.engines.standalone.plugin_handlers import PluginHandler

LOGGER = logging.getLogger(__name__)

# Cases with the front depth to use instead of the one of the config, if any
ORCHESTRATION_CASES: list[tuple[str, int | None]] = [
    ("parameters", None),
    ("partial-dates", None),
    ("partial-dates", 1),
]
MAX_STEPS = 60  # Safety net, cases complete well before
# Steps at which events are injected, early enough for all cases to still be running
FAIL_STEP = 1
STOP_STEP = 0


@dataclass
class FakeCluster:
    """Job states and recorded events, shared by all scheduler instances of a test"""

    rootdir: Path
    status: dict[str, TaskStatus] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)  # task label of each job id
    trace: list[str] = field(default_factory=list)
    scripts: dict[str, str] = field(default_factory=dict)
    next_jobid: int = 1001

    def record(self, line: str) -> None:
        self.trace.append(line.replace(str(self.rootdir), "<ROOTDIR>"))


class FakeScheduler(Scheduler):
    """Not registered (no `name` at class creation), the tests put it in place of slurm"""

    jobid_variable = "FAKE_JOB_ID"
    fake_cluster: ClassVar[FakeCluster]

    def header_lines(self, task_handler: PluginHandler, stdouterr_mode: STDOUTERR_MODE_T = "overwrite") -> list[str]:
        return [
            f"#FAKE --job-name={task_handler.task.label} --output={task_handler.stdouterr_filename} --mode={stdouterr_mode}"
        ]

    def submit_to_scheduler(
        self,
        task_handler: PluginHandler,
        parent_ids: list[str],
        env: dict[str, str],  # noqa: ARG002
        dependency_type: DEPENDENCY_TYPE_T = "ALL_COMPLETED",
    ) -> str:
        task = task_handler.task
        jobid = str(self.fake_cluster.next_jobid)
        self.fake_cluster.next_jobid += 1
        self.fake_cluster.status[jobid] = TaskStatus.WAITING
        self.fake_cluster.labels[jobid] = task.label
        self.fake_cluster.record(f"submit {task.label} ({jobid}) after {parent_ids} [{dependency_type}]")
        script = task_handler.get_runscript_path().read_text()
        if self.fake_cluster.scripts.get(task.label) != script:
            self.fake_cluster.scripts[task.label] = script
            self.fake_cluster.record(f"--- run script {task.label}\n{script}\n---")
        return jobid

    def get_status(self, jobid: str) -> TaskStatus:
        if jobid not in self.fake_cluster.status:
            raise UnknownJobError(jobid)
        return self.fake_cluster.status[jobid]

    def cancel(self, jobid: str) -> None:
        self.fake_cluster.record(f"cancel {self.fake_cluster.labels[jobid]} ({jobid})")
        self.fake_cluster.status[jobid] = TaskStatus.FAILED


def compact_state(state_file: Path) -> str:
    """Content of the state file, one task per line, grouped by front generation"""
    state = YAML(typ="safe").load(state_file)

    def task(entry: dict) -> str:
        name, spec = next(iter(entry.items()))
        coordinates = ", ".join(f"{key}={value}" for key, value in spec["coordinates"].items())
        return f"  {name}[{coordinates}] ({spec['jobid']})" if coordinates else f"  {name} ({spec['jobid']})"

    lines = [f"--- state revision {state['revision']}, {state['status']} (continuation job {state['sirocco_jobid']})"]
    for k, generation in enumerate(state["front"]):
        lines.append(f"front[{k}]:")
        lines.extend(task(entry) for entry in generation)
    for key in ("cool_down_tasks", "completed_tasks"):
        lines.append(f"{key}:")
        lines.extend(task(entry) for entry in state[key])
    lines.append("---")
    return "\n".join(lines)


class Simulation:
    def __init__(
        self,
        config_dir: Path,
        cluster: FakeCluster,
        monkeypatch: pytest.MonkeyPatch,
        front_depth: int | None = None,
    ) -> None:
        self.config_dir = config_dir
        self.cluster = cluster
        self.monkeypatch = monkeypatch
        self.front_depth = front_depth

    def engine(self) -> StandaloneEngine:
        """Fresh engine, as built by each sirocco command"""
        config = ConfigWorkflow.from_config_path(self.config_dir)
        if self.front_depth is not None:
            config = config.model_copy(update={"front_depth": self.front_depth})
        return StandaloneEngine.from_config_workflow(config)

    @property
    def run_root(self) -> Path:
        return self.config_dir / StandaloneEngine.RUN_ROOT_NAME

    def run(self, name: Literal["start", "continue", "stop", "restart"]) -> WorkflowState:
        """Run command `name` in a transaction, `continue` as the continuation job of the state"""
        if name == "continue" and (state := WorkflowState.read(self.run_root / StandaloneEngine.STATE_FILE_NAME)):
            self.monkeypatch.setenv(JOBID_ENV_VAR, state.sirocco_jobid)
        with Transaction.begin(self.run_root, name, wait=0) as transaction:
            match name:
                case "start":
                    return self.engine().start(transaction, logger=LOGGER)
                case "continue":
                    return self.engine().continue_wf(transaction, logger=LOGGER)
                case "stop":
                    return stop(transaction, mode="cool-down", logger=LOGGER)
                case "restart":
                    return self.engine().restart(transaction, logger=LOGGER)

    def command(self, name: Literal["start", "continue", "stop", "restart"]) -> WorkflowStatus:
        self.cluster.record(f"\n=== sirocco {name}")
        state = self.run(name)
        self.cluster.record(f"status: {state.status.name}")
        self.cluster.record(compact_state(self.run_root / StandaloneEngine.STATE_FILE_NAME))
        return state.status

    def loaded_engine(self) -> StandaloneEngine:
        """Engine with the current state loaded, to inspect the front"""
        engine = self.engine()
        state = WorkflowState.read(engine.state_file)
        assert state is not None
        engine.load_state(state)
        return engine

    def set_status(self, engine: StandaloneEngine, task: Task, status: TaskStatus) -> None:
        """Simulate the cluster changing the status of a submitted task"""
        self.cluster.record(f"# {task.label} -> {status.name}")
        self.cluster.status[engine.jobids[task]] = status


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


def simulation(
    case: str,
    tmp_path: Path,
    test_rootdir: Path,
    monkeypatch: pytest.MonkeyPatch,
    front_depth: int | None = None,
) -> Simulation:
    """Simulation of `case`, copied to `tmp_path`, against a fresh fake cluster"""
    config_dir = tmp_path / "config"
    shutil.copytree(test_rootdir / f"tests/cases/{case}/config", config_dir)
    cluster = FakeCluster(rootdir=config_dir)
    monkeypatch.setattr(FakeScheduler, "fake_cluster", cluster, raising=False)
    monkeypatch.setitem(Scheduler.scheduler_classes, "slurm", FakeScheduler)
    monkeypatch.setattr(FakeScheduler, "name", "slurm")
    # Avoid spawning a login shell, the fake scheduler doesn't use the environment
    monkeypatch.setattr(StandaloneEngine, "base_env", {"SIROCCO_TEST": "1"})
    return Simulation(config_dir, cluster, monkeypatch, front_depth)


def check_reference(text: str, reference: Path, pytestconfig: pytest.Config) -> None:
    """Compare `text` to `reference`, regenerating it with `--reserialize`"""
    if pytestconfig.getoption("reserialize"):
        reference.write_text(text)
    if not reference.exists() or reference.read_text() != text:
        new_path = reference.with_name(f"{reference.stem}.new{reference.suffix}")
        new_path.write_text(text)
        pytest.fail(f"Output differs from {reference}. New output dumped to {new_path}.")


@pytest.mark.parametrize("scenario", ["complete", "fail"])
@pytest.mark.parametrize(("case", "front_depth"), ORCHESTRATION_CASES)
def test_orchestration(case, front_depth, scenario, tmp_path, test_rootdir, monkeypatch, pytestconfig):
    sim = simulation(case, tmp_path, test_rootdir, monkeypatch, front_depth)
    run_scenario(sim, scenario)
    trace = "\n".join(sim.cluster.trace).lstrip() + "\n"
    suffix = "" if front_depth is None else f"_front_depth_{front_depth}"
    check_reference(trace, test_rootdir / f"tests/cases/{case}/data/standalone_{scenario}{suffix}.txt", pytestconfig)


@pytest.mark.parametrize(
    ("command", "message"),
    [
        ("restart", "Workflow did not start, cannot restart"),
        ("stop", "Workflow did not start, cannot stop"),
        ("continue", "Workflow did not start, cannot continue"),
    ],
)
def test_command_before_start(command, message, tmp_path, test_rootdir, monkeypatch):
    sim = simulation("partial-dates", tmp_path, test_rootdir, monkeypatch)
    with pytest.raises(CommandOutcome, match=message) as outcome:
        sim.run(command)
    assert outcome.value.outcome == Outcome.REFUSED


def test_continue_fencing(tmp_path, test_rootdir, monkeypatch):
    """Only the continuation job named in the committed state continues the workflow"""
    sim = simulation("partial-dates", tmp_path, test_rootdir, monkeypatch)
    started = sim.run("start")
    with Transaction.begin(sim.run_root, "continue", wait=0) as transaction:
        with pytest.raises(CommandOutcome, match="only runs in the continuation job") as outcome:
            sim.engine().continue_wf(transaction, logger=LOGGER)
        assert outcome.value.outcome == Outcome.REFUSED
        monkeypatch.setenv(JOBID_ENV_VAR, "orphan")
        with pytest.raises(CommandOutcome, match="Continuation job orphan superseded") as outcome:
            sim.engine().continue_wf(transaction, logger=LOGGER)
        assert outcome.value.outcome == Outcome.NO_OP
    assert WorkflowState.read(sim.run_root / StandaloneEngine.STATE_FILE_NAME) == started
    assert sim.run("continue").revision == started.revision + 1


def test_start_twice(tmp_path, test_rootdir, monkeypatch):
    sim = simulation("partial-dates", tmp_path, test_rootdir, monkeypatch)
    sim.run("start")
    with pytest.raises(CommandOutcome, match="Workflow already exists") as outcome:
        sim.run("start")
    assert outcome.value.outcome == Outcome.REFUSED


def test_clean_up_running(tmp_path, test_rootdir, monkeypatch):
    sim = simulation("partial-dates", tmp_path, test_rootdir, monkeypatch)
    sim.run("start")
    with (
        pytest.raises(CommandOutcome, match="stop it before") as outcome,
        Transaction.begin(sim.run_root, "start --cleanup", wait=0) as transaction,
    ):
        clean_up(transaction)
    assert outcome.value.outcome == Outcome.REFUSED


def test_renamed_task(tmp_path, test_rootdir, monkeypatch):
    """Stop and cleanup work on the state file alone, restart needs the state to match the config"""
    sim = simulation("partial-dates", tmp_path, test_rootdir, monkeypatch)
    state = sim.run("start")
    state.front[0][0].name = "renamed"  # As if the task was renamed in the config since
    state.write(sim.run_root / StandaloneEngine.STATE_FILE_NAME)
    assert sim.run("stop").status == WorkflowStatus.STOPPED
    with pytest.raises(
        CommandOutcome, match="names task renamed__date_2026-01-01T00-00-00, which the config"
    ) as outcome:
        sim.run("restart")
    assert outcome.value.outcome == Outcome.REFUSED
    (sim.run_root / "leftover").mkdir()
    with Transaction.begin(sim.run_root, "start --cleanup", wait=0) as transaction:
        clean_up(transaction)
        restarted = sim.engine().start(transaction, logger=LOGGER)
    assert restarted.revision == 1
    assert restarted.run != state.run
    assert not (sim.run_root / "leftover").exists()
