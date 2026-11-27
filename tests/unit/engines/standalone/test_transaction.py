"""Transactions of the standalone engine: lock, holder liveness and commits."""

from __future__ import annotations

import json
import logging
import multiprocessing
import subprocess
import sys
from dataclasses import replace
from typing import TYPE_CHECKING, ClassVar

import pytest

from sirocco.engines.standalone import CommandOutcome, Outcome, Transaction, WorkflowState, WorkflowStatus
from sirocco.engines.standalone.scheduler import JOBID_ENV_VAR, SCHEDULER_ENV_VAR, Scheduler, UnknownJobError
from sirocco.engines.standalone.state import STATE_FILE_NAME, StateEntry, TaskStatus
from sirocco.engines.standalone.transaction import CONTROL_DIR_NAME, LOCK_FILE_NAME, HolderRecord, Lock

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event
    from pathlib import Path

    from sirocco.engines.standalone.plugin_handlers import PluginHandler


class StatusScheduler(Scheduler):
    """Scheduler only answering job status queries, from `statuses`, on cluster `current_cluster`"""

    jobid_variable = "STATUS_JOB_ID"
    statuses: ClassVar[dict[str, TaskStatus]] = {}
    current_cluster: ClassVar[str | None] = None

    def cluster(self) -> str | None:
        return self.current_cluster

    def get_status(self, jobid: str) -> TaskStatus:
        if jobid not in self.statuses:
            raise UnknownJobError(jobid)
        return self.statuses[jobid]

    def header_lines(self, task_handler: PluginHandler, stdouterr_mode="overwrite") -> list[str]:
        raise NotImplementedError

    def submit_to_scheduler(self, task_handler, parent_ids, env, dependency_type="ALL_COMPLETED") -> str:
        raise NotImplementedError

    def cancel(self, jobid: str) -> None:
        raise NotImplementedError


@pytest.fixture
def status_scheduler(monkeypatch: pytest.MonkeyPatch) -> type[StatusScheduler]:
    monkeypatch.setitem(Scheduler.scheduler_classes, "status", StatusScheduler)
    monkeypatch.setattr(StatusScheduler, "statuses", {})
    monkeypatch.setattr(StatusScheduler, "current_cluster", "here")
    return StatusScheduler


def lock_path(run_root: Path) -> Path:
    return run_root / CONTROL_DIR_NAME / LOCK_FILE_NAME


def place_lock(run_root: Path, record: HolderRecord) -> None:
    """Put a lock held by `record` in place, as if another process held it"""
    lock_path(run_root).parent.mkdir(parents=True, exist_ok=True)
    assert Lock(path=lock_path(run_root)).try_acquire(record)


def dead_pid() -> int:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait()
    return process.pid


def test_lock_holds_record(tmp_path):
    with Transaction.begin(tmp_path, "stop --cool-down", wait=0, transaction="request-1") as transaction:
        record = json.loads(lock_path(tmp_path).read_text())
        assert record["command"] == "stop --cool-down"
        assert record["transaction"] == "request-1"
        assert record["holder"] == transaction.record.holder
        assert record["jobid"] is None  # Not started by a Sirocco job script
        assert transaction.state is None
    assert not lock_path(tmp_path).exists()
    assert list(lock_path(tmp_path).parent.iterdir()) == []  # No temporary file left


def test_lock_released_on_error(tmp_path):
    with pytest.raises(RuntimeError), Transaction.begin(tmp_path, "start", wait=0):
        raise RuntimeError
    assert not lock_path(tmp_path).exists()


def test_busy_while_held(tmp_path, caplog):
    logger = logging.getLogger("test_busy_while_held")
    caplog.set_level(logging.INFO, logger=logger.name)
    with Transaction.begin(tmp_path, "restart", wait=0):
        with (
            pytest.raises(CommandOutcome, match="busy with `sirocco restart`") as outcome,
            Transaction.begin(tmp_path, "stop", wait=0.2, logger=logger),
        ):
            pass
        assert outcome.value.outcome == Outcome.BUSY
    assert "Waiting for `sirocco restart`" in caplog.text


def test_retries_sharing_a_transaction_id(tmp_path):
    """A retried remote request shares the transaction id of the first attempt, but not its lock"""
    with Transaction.begin(tmp_path, "stop", wait=0, transaction="request-1"):
        with (
            pytest.raises(CommandOutcome) as outcome,
            Transaction.begin(tmp_path, "stop", wait=0, transaction="request-1"),
        ):
            pass
        assert outcome.value.outcome == Outcome.BUSY


def test_lock_left_by_dead_process(tmp_path):
    record = replace(HolderRecord.current("continue"), pid=dead_pid(), start="other-boot:0")
    place_lock(tmp_path, record)
    with (
        pytest.raises(CommandOutcome, match="which no longer runs") as outcome,
        Transaction.begin(tmp_path, "stop", wait=10),
    ):
        pass
    assert outcome.value.outcome == Outcome.NEEDS_RECOVERY
    assert lock_path(tmp_path).exists()  # Never taken over automatically


def try_lock(path: Path, start: Event, results: multiprocessing.Queue) -> None:
    start.wait()
    results.put(Lock(path=path).try_acquire(HolderRecord.current("start")))


def test_lock_race(tmp_path):
    lock_path(tmp_path).parent.mkdir(parents=True)
    start = multiprocessing.Event()
    results: multiprocessing.Queue = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=try_lock, args=(lock_path(tmp_path), start, results)) for _ in range(8)]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join()
    assert sorted(results.get() for _ in processes) == [False] * 7 + [True]


@pytest.mark.parametrize(
    ("record_changes", "status", "liveness"),
    [
        ({"jobid": "12", "scheduler": "status", "cluster": "here"}, TaskStatus.RUNNING, "alive"),
        ({"jobid": "12", "scheduler": "status", "cluster": "here"}, TaskStatus.WAITING, "alive"),
        ({"jobid": "12", "scheduler": "status", "cluster": "here"}, TaskStatus.COMPLETED, "dead"),
        ({"jobid": "12", "scheduler": "status", "cluster": "here"}, TaskStatus.FAILED, "dead"),
        ({"jobid": "12", "scheduler": "status", "cluster": "here"}, None, "dead"),  # unknown to the scheduler
        ({"jobid": "12", "scheduler": "status", "cluster": "there"}, TaskStatus.COMPLETED, "unknown"),
        ({"jobid": "12", "scheduler": "unregistered", "cluster": "here"}, TaskStatus.COMPLETED, "unknown"),
        ({}, None, "unknown"),  # interactive command on another host
    ],
)
def test_holder_on_another_host(record_changes, status, liveness, status_scheduler):
    if status is not None:
        status_scheduler.statuses["12"] = status
    record = replace(HolderRecord.current("continue"), host="another-host", **record_changes)
    assert record.liveness() == liveness


def test_holder_on_this_host():
    record = HolderRecord.current("start")
    assert record.liveness() == "alive"
    assert replace(record, pid=dead_pid()).liveness() == "dead"
    if record.start is not None:  # Linux: a reused pid has another start
        assert replace(record, start="other-boot:0").liveness() == "dead"


@pytest.mark.usefixtures("status_scheduler")
def test_job_holder(monkeypatch):
    """Only a process started by a Sirocco job script records a job"""
    monkeypatch.setenv(JOBID_ENV_VAR, "12")
    monkeypatch.setenv(SCHEDULER_ENV_VAR, "status")
    record = HolderRecord.current("continue")
    assert (record.jobid, record.scheduler, record.cluster) == ("12", "status", "here")


def write_state(run_root: Path, **header) -> WorkflowState:
    state = WorkflowState(
        status=WorkflowStatus.RUNNING,
        sirocco_jobid="99",
        front=[[StateEntry(name="task", coordinates={"date": "2026-01-01T00:00:00"}, jobid="12")]],
        **header,
    )
    run_root.mkdir(parents=True, exist_ok=True)
    state.write(run_root / STATE_FILE_NAME)
    return state


def test_refused_on_another_cluster(tmp_path, status_scheduler):
    write_state(tmp_path, scheduler="status", cluster="there")
    with (
        pytest.raises(CommandOutcome, match="runs on cluster there, not on here") as outcome,
        Transaction.begin(tmp_path, "stop", wait=0),
    ):
        pass
    assert outcome.value.outcome == Outcome.REFUSED
    status_scheduler.current_cluster = "there"
    with Transaction.begin(tmp_path, "stop", wait=0):
        pass


def test_commits(tmp_path):
    with Transaction.begin(tmp_path, "start", wait=0, transaction="first") as transaction:
        first = transaction.commit(WorkflowState(status=WorkflowStatus.RUNNING))
    assert first.committed_by is not None
    assert (first.revision, first.committed_by["transaction"]) == (1, "first")
    assert first.run is not None
    with Transaction.begin(tmp_path, "stop", wait=0) as transaction:
        assert transaction.state == first
        second = transaction.commit(replace(transaction.state, status=WorkflowStatus.STOPPED))
    assert WorkflowState.read(tmp_path / STATE_FILE_NAME) == second
    assert second.committed_by is not None
    assert (second.run, second.revision, second.committed_by["command"]) == (first.run, 2, "stop")
    assert sorted(path.name for path in tmp_path.iterdir()) == [CONTROL_DIR_NAME, STATE_FILE_NAME]  # no tmp file


@pytest.mark.parametrize(
    ("sirocco_jobid", "front", "status"),
    [
        ("99", [[{"task": {"coordinates": {}, "jobid": "12"}}]], WorkflowStatus.RUNNING),
        ("_NO_ID_", [[{"task": {"coordinates": {}, "jobid": "12"}}]], WorkflowStatus.STOPPED),
        ("_NO_ID_", [[]], WorkflowStatus.COMPLETED),
    ],
)
def test_state_without_header(sirocco_jobid, front, status, tmp_path):
    """State files of earlier versions read as revision 0 of an unnamed run"""
    state_file = tmp_path / STATE_FILE_NAME
    state_file.write_text(
        json.dumps({"sirocco_jobid": sirocco_jobid, "completed_tasks": [], "cool_down_tasks": [], "front": front})
    )
    state = WorkflowState.read(state_file)
    assert state is not None
    assert (state.run, state.revision, state.status) == (None, 0, status)
    with Transaction.begin(tmp_path, "stop", wait=0) as transaction:
        committed = transaction.commit(replace(state, status=WorkflowStatus.STOPPED))
    assert committed.revision == 1
    assert committed.run is not None


def test_state_entry_label():
    entry = StateEntry(name="icon", coordinates={"date": "2026-01-01T00:00:00", "member": 2}, jobid="12")
    assert entry.label == "icon__date_2026-01-01T00-00-00__member_2"
