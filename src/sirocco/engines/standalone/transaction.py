"""Commands of the standalone engine run as transactions.

Every command that changes a workflow (`start`, `continue`, `restart`, `stop`) runs as one
transaction:

1. acquire the lock, waiting while another transaction holds it,
2. load the committed state and check the preconditions of the command,
3. submit and cancel jobs,
4. commit the new state atomically,
5. release the lock.

Read-only commands never take the lock: the state file is always complete.

The lock is the file `run/.sirocco/lock`. It is created with its content in one step, by hard
linking a fully written temporary file, so it is never seen empty or half written. It holds a
`HolderRecord`, from which other processes tell, without clocks, whether its holder is still
alive. Nothing takes over a lock automatically: a lock left by a dead holder is reported.
"""

from __future__ import annotations

import enum
import json
import os
import socket
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import sirocco
from sirocco.engines.standalone.scheduler import Scheduler, UnknownJobError
from sirocco.engines.standalone.state import STATE_FILE_NAME, TaskStatus, WorkflowState

if TYPE_CHECKING:
    import logging
    from collections.abc import Generator

CONTROL_DIR_NAME = ".sirocco"
LOCK_FILE_NAME = "lock"
MAX_WAIT_STEP = 30.0  # Longest pause between two attempts to acquire the lock, in seconds

type LIVENESS_T = Literal["alive", "dead", "unknown"]


class Outcome(enum.Enum):
    """How a command ended"""

    DONE = "done"  # committed
    NO_OP = "no-op"  # nothing to do
    REFUSED = "refused"  # precondition not met
    BUSY = "busy"  # lock still held by an alive or unknown holder after waiting
    NEEDS_RECOVERY = "needs recovery"  # lock left by a dead holder
    ABORTED = "aborted"  # failed

    @property
    def exit_code(self) -> int:
        """Exit code of the command, distinct for each outcome that is not a success

        2 is left out, as it is the exit code of command line usage errors.
        """
        return {
            Outcome.DONE: 0,
            Outcome.NO_OP: 0,
            Outcome.ABORTED: 1,
            Outcome.REFUSED: 3,
            Outcome.BUSY: 4,
            Outcome.NEEDS_RECOVERY: 5,
        }[self]


class CommandOutcome(Exception):  # noqa: N818 (not an error for all outcomes)
    """Ends a command early with `outcome`, e.g. a refused or no-op command"""

    def __init__(self, outcome: Outcome, message: str) -> None:
        super().__init__(message)
        self.outcome = outcome


def process_start(pid: int) -> str | None:
    """Start of process `pid`: boot id and start time in clock ticks since boot, `None` if unknown

    Both come from `/proc` (Linux only) and involve no wall clock, so that a clock change can't
    make a live process look dead.
    """
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # The process name (2nd field) is in parentheses and may contain spaces, the start time is the 22nd field
    start_ticks = stat.rpartition(")")[2].split()[19]
    return f"{boot_id}:{start_ticks}"


@dataclass(kw_only=True, frozen=True)
class HolderRecord:
    """Who holds the lock and why

    `jobid`, `scheduler` and `cluster` are only set for a process started by a Sirocco job script
    (see `Scheduler.current_jobid()`), whose life the scheduler can tell from anywhere.
    """

    holder: str  # unique id of this acquisition
    transaction: str  # transaction id, shared by retries of the same request: never decides who holds the lock
    command: str
    host: str
    pid: int
    start: str | None  # see `process_start`
    scheduler: str | None
    cluster: str | None
    jobid: str | None
    started_at: str  # UTC, for information only
    sirocco_version: str

    @classmethod
    def current(cls, command: str, transaction: str | None = None) -> Self:
        """Record of the current process, running `command` as `transaction` (new id if not given)"""
        jobid = Scheduler.current_jobid()
        scheduler = Scheduler.current_scheduler() if jobid is not None else None
        scheduler_class = Scheduler.scheduler_classes.get(scheduler) if scheduler is not None else None
        return cls(
            holder=uuid.uuid4().hex,
            transaction=transaction or uuid.uuid4().hex,
            command=command,
            host=socket.gethostname(),
            pid=os.getpid(),
            start=process_start(os.getpid()),
            scheduler=scheduler,
            cluster=scheduler_class().cluster() if scheduler_class is not None else None,
            jobid=jobid,
            started_at=datetime.now(UTC).isoformat(timespec="seconds"),
            sirocco_version=sirocco.__version__,
        )

    def describe(self) -> str:
        where = f"{self.host}, pid {self.pid}" + (f", job {self.jobid}" if self.jobid is not None else "")
        return f"`sirocco {self.command}` ({where}, since {self.started_at})"

    def liveness(self) -> LIVENESS_T:
        """Whether the holder still runs, decided without clocks

        - On the holder's host, its process tells.
        - Elsewhere, the scheduler tells for a holder running in a job of the current cluster. A job the
          scheduler doesn't know anymore is finished, a failed status query counts as alive.
        - Otherwise, e.g. for an interactive command on another login node, nobody can tell.
        """
        if self.host == socket.gethostname():
            if self.start is not None:
                return "alive" if process_start(self.pid) == self.start else "dead"
            try:
                os.kill(self.pid, 0)
            except ProcessLookupError:
                return "dead"
            except PermissionError:
                return "alive"  # Exists, but belongs to another user
            return "alive"
        if self.jobid is None or self.scheduler is None:
            return "unknown"
        if (scheduler_class := Scheduler.scheduler_classes.get(self.scheduler)) is None:
            return "unknown"
        scheduler = scheduler_class()
        if (cluster := scheduler.cluster()) is not None and self.cluster is not None and cluster != self.cluster:
            return "unknown"  # Job ids of another cluster mean nothing here
        try:
            status = scheduler.get_status(self.jobid)
        except UnknownJobError:
            return "dead"
        except Exception:  # noqa: BLE001 (any failure of the query)
            return "alive"
        return "alive" if status in (TaskStatus.WAITING, TaskStatus.RUNNING) else "dead"


@dataclass(kw_only=True, frozen=True)
class Lock:
    """Exclusive lock of a workflow, held by at most one transaction"""

    path: Path

    def try_acquire(self, record: HolderRecord) -> bool:
        """Create the lock holding `record`, `False` if it is held already"""
        tmp_path = self.path.with_name(f"{record.holder}.tmp")
        with tmp_path.open("w") as stream:
            json.dump(asdict(record), stream)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(tmp_path, self.path)
        except FileExistsError:
            # NFS can report a lost reply to a successful link as a failure: the link count tells
            return tmp_path.stat().st_nlink == 2  # noqa: PLR2004 (the temporary file and the lock)
        else:
            return True
        finally:
            tmp_path.unlink()

    def read(self) -> HolderRecord | None:
        """Record of the current holder, `None` if the lock is free"""
        try:
            return HolderRecord(**json.loads(self.path.read_text()))
        except FileNotFoundError:
            return None

    def release(self) -> None:
        self.path.unlink()


@dataclass(kw_only=True)
class Transaction:
    """One command changing a workflow, holding its lock

    Created by `begin`. `state` is the committed state, read under the lock (`None` before
    `start`), and `committed` the state committed by this transaction, if any.
    """

    run_root: Path
    record: HolderRecord
    state: WorkflowState | None
    committed: WorkflowState | None = None

    @property
    def state_file(self) -> Path:
        return self.run_root / STATE_FILE_NAME

    @classmethod
    @contextmanager
    def begin(
        cls,
        run_root: Path,
        command: str,
        *,
        wait: float,
        logger: logging.Logger | None = None,
        transaction: str | None = None,
    ) -> Generator[Self]:
        """Run `command` as a transaction on the workflow whose run directory is `run_root`

        Waits up to `wait` seconds while an alive or unknown holder has the lock, then ends as
        `busy`. Ends as `needs recovery` at once if the holder is dead.
        """
        check_cluster(WorkflowState.read(run_root / STATE_FILE_NAME))
        lock = Lock(path=run_root / CONTROL_DIR_NAME / LOCK_FILE_NAME)
        lock.path.parent.mkdir(parents=True, exist_ok=True)
        record = HolderRecord.current(command, transaction)
        deadline = time.monotonic() + wait
        pause = 1.0
        waiting_for: str | None = None
        while not lock.try_acquire(record):
            if (holder := lock.read()) is None:
                continue  # Released in the meantime
            if (liveness := holder.liveness()) == "dead":
                msg = (
                    f"The workflow lock {lock.path} was left by {holder.describe()}, which no longer runs. "
                    "Make sure none of the jobs it may have submitted is left, then remove the lock."
                )
                raise CommandOutcome(Outcome.NEEDS_RECOVERY, msg)
            if (remaining := deadline - time.monotonic()) <= 0:
                msg = f"The workflow is busy with {holder.describe()} ({liveness}), please retry later."
                raise CommandOutcome(Outcome.BUSY, msg)
            if logger is not None and waiting_for != holder.holder:
                logger.info(f"Waiting for {holder.describe()} to finish ...")  # noqa: G004
                waiting_for = holder.holder
            time.sleep(min(pause, remaining))
            pause = min(2 * pause, MAX_WAIT_STEP)
        try:
            # Read again under the lock: another transaction may have committed while waiting
            yield cls(run_root=run_root, record=record, state=WorkflowState.read(run_root / STATE_FILE_NAME))
        finally:
            lock.release()

    def commit(self, state: WorkflowState) -> WorkflowState:
        """Commit `state`, the next revision of the committed state, or the first one of a new run"""
        committed = replace(
            state,
            run=(self.state.run if self.state is not None else None) or uuid.uuid4().hex,
            revision=(self.state.revision if self.state is not None else 0) + 1,
            committed_by={
                "transaction": self.record.transaction,
                "command": self.record.command,
                "time": datetime.now(UTC).isoformat(timespec="seconds"),
            },
        )
        committed.write(self.state_file)
        self.state = self.committed = committed
        return committed

    def discard_state(self) -> None:
        """Delete the state file, the workflow then counts as not started"""
        self.state_file.unlink(missing_ok=True)
        self.state = None


def check_cluster(state: WorkflowState | None) -> None:
    """Refuse to act on a workflow whose jobs live on another cluster

    Job ids only mean something to their own scheduler, and the same file system can be reached
    from several clusters.
    """
    if state is None or state.scheduler is None or state.cluster is None:
        return
    if (scheduler_class := Scheduler.scheduler_classes.get(state.scheduler)) is None:
        return
    if (cluster := scheduler_class().cluster()) is not None and cluster != state.cluster:
        msg = f"The workflow runs on cluster {state.cluster}, not on {cluster}: run sirocco from {state.cluster}."
        raise CommandOutcome(Outcome.REFUSED, msg)
