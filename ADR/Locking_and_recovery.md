# Locking and Recovery

## Summary

[#297](https://github.com/C2SM/Sirocco/issues/297) asks for a race-free lock, a cleanup when Sirocco fails in the middle of a command, and `sirocco recover` / `sirocco restart --recover` to repair broken workflows. This document records the design. Every command that changes a workflow runs as a transaction: it holds an exclusive lock, journals each submission before making it, commits the state file atomically and rolls back when it fails. When the process can't roll back itself (killed, node failure), `sirocco recover` replays the same rollback from the journal.

It also records what this work prepares for remote control ([#253](https://github.com/C2SM/Sirocco/issues/253), [#283](https://github.com/C2SM/Sirocco/pull/283), [#288](https://github.com/C2SM/Sirocco/pull/288)), which runs the same commands in jobs submitted from a laptop.

Only the standalone engine is concerned: the AiiDA engine relies on the AiiDA daemon and database.

## Context

The standalone engine keeps the workflow state in `run/state.yaml`, while the scheduler holds the jobs. A command loads the state, submits or cancels jobs, then writes the state back. This currently goes wrong in several ways:

- **The lock is racy and partial.** `continue` and `stop` check that `run/.sirocco.lock` doesn't exist, then create it, so two processes can both take it. `start` and `restart` don't take it at all, and `stop` loads the state before taking it, so it may act on an outdated state.
- **A failure stops the workflow silently.** The lock isn't released when a command raises. `continue` reads any lock as "a stop is in progress" and exits without resubmitting itself, so nothing watches the workflow anymore and nobody is told. `stop` then answers "retry" forever, and `restart` refuses because the state still names the dead continuation job. The only way out is to delete the lock and edit the state by hand.
- **A failure leaves orphan jobs.** The state is written once, at the end of the command, so the jobs submitted before a failure are recorded nowhere. Later commands submit the same tasks again and wipe their run directories, possibly under a running orphan. An orphan continuation job can even start a second continuation chain, e.g. when a `restart` that failed after submitting it is run again.
- **The state file can be corrupted.** It is rewritten in place, so a crash while writing it leaves it truncated.

The design must hold under these constraints:

- Commands run interactively on login nodes (possibly two different ones at the same time) or in interactive allocations (`salloc`, JupyterHub sessions), in the continuation job on a compute node and, with remote control, in jobs submitted through FirecREST. Local and remote control act on the same workflow and must exclude each other.
- Workflows live on parallel file systems (Lustre, GPFS) or NFS, which several clusters may share, each with its own scheduler and job ids (e.g. the vClusters of Alps).
- Sirocco is not tied to Slurm, other schedulers will follow: nothing may rely on Slurm specific features.

## Decisions

### Commands are transactions

Every command that changes the state file or the jobs of a workflow (`start`, `continue`, `restart`, `stop`, `recover`, and the remote control commands to come) runs as one transaction:

1. Acquire the lock, waiting while another transaction holds it.
2. Load the committed state and check the preconditions of the command. Commands that move the front forward also parse the config. The state may have changed while waiting, so preconditions are only checked under the lock. A command whose effect is already there is a no-op, e.g. `stop` on a stopped workflow.
3. Act: submit and cancel jobs, journaling each submission ahead.
4. Commit: replace the state file atomically.
5. Delete the journal and release the lock.

If anything fails during steps 2 to 4, including on a termination signal, the transaction rolls back before releasing the lock (see [Rollback and recovery](#rollback-and-recovery)). Read-only commands (`stviz`, later `status` or `pull`) never take the lock: thanks to the atomic commit, the state file is always complete.

A `Transaction` (new module `engines/standalone/transaction.py`) owns the lock, the journal and the commit. The engine submits and commits through it. The files involved:

| Path | Role |
|---|---|
| `run/state.yaml` | committed state |
| `run/.sirocco/lock` | lock, holding the record of its holder |
| `run/.sirocco/journal` | submissions of the running, or of a failed, transaction |
| `run/.sirocco/<holder>.claim` | claim of a recovery on a stale lock |

A transaction rolls back to the last committed state rather than rolling forward from where it failed. Until the commit, everything a transaction changes lives in memory, except its effects on the scheduler, which the journal records. Undoing them costs at most one front of cancelled submissions, which the next transactions submit again. What the transaction observed (completed tasks) is observed again, since the scheduler still reports it.

Alternatives considered:

- **Committing after each submission** (rolling forward), the state file acting as the journal: every intermediate state would have to be a valid starting point for every command, a strong invariant to keep as the engine evolves, and the submission under way at the time of a crash would still have to be recorded ahead.
- **Locking only the critical commands**, as today: a single writer without the lock breaks the guarantees of all others.

### Lock

The lock is the file `run/.sirocco/lock`. Its path only depends on the config directory, so it can be taken before the config is parsed. It is created with its content in one step: the holder record is written to a temporary file with a unique name, flushed (`fsync`), then hard linked to the lock path. The link fails if the lock exists, atomically on local, parallel and network file systems, so the lock file is never seen empty or half written. NFS can report a lost reply to a successful link as a failure, so after a failed link the acquirer checks whether its temporary file got a second link (`st_nlink == 2`). Releasing the lock removes the file.

The holder record tells who holds the lock and why:

| Field | Content |
|---|---|
| `holder` | unique id of this acquisition, the name of the temporary file |
| `transaction` | transaction id, generated or given by the caller (a remote control request id). Retries of a request share it, so it never decides who holds the lock |
| `command` | command and options, e.g. `stop --cool-down` |
| `host`, `pid`, `start` | process holding the lock, with its start time in clock ticks since boot and the boot id, to rule out a reused pid |
| `scheduler`, `cluster`, `jobid` | for a process started by a Sirocco job script: scheduler, cluster and job id |
| `started_at` | UTC time, for information only |
| `sirocco_version` | version of the holder |
| `recovering` | for `recover`: holder record of the transaction being recovered |
| `abandoned` | set by a transaction whose rollback failed, right before it exits |

Only processes started by a job script that Sirocco generated count as job holders: the continuation job and, later, remote control jobs. These scripts export the job id as `SIROCCO_JOBID` (from `SLURM_JOB_ID` for Slurm), which `Scheduler.current_jobid()` reads, and start sirocco with `exec`, so that the process and the job end together. A command run by hand in an interactive allocation is a process holder, even though it inherits the allocation's job id: other commands may run in the same allocation, on other nodes, and the allocation may end before the command.

Whether a holder is alive is decided without clocks, by the first rule that applies:

1. An `abandoned` holder is dead.
2. On the holder's host, the holder is alive if its process still exists with the same start time and boot id, and dead otherwise.
3. A job holder of the current cluster is alive until the scheduler reports its job finished, or doesn't know it anymore. A failed status query counts as alive.
4. Any other holder is unknown, e.g. an interactive command on another login node.

A transaction that finds the lock held by an alive or unknown holder waits with backoff, up to a timeout, then ends as `busy` and reports the holder. Interactive commands wait briefly by default (`--wait` to change it). `continue` waits much longer, since giving up leaves the workflow without continuation. A transaction that finds a dead holder neither waits nor takes over: it ends as `needs recovery` and points to `sirocco recover`. Only `recover` breaks a lock.

Alternatives considered:

- **`flock` or `fcntl` locks**: depending on mount options, `flock` on Lustre fails or only excludes processes of the same node (`localflock`), without warning. Their release on crash wouldn't spare the recovery anyway: the journal still has to be replayed.
- **A lease renewed by a heartbeat thread** (first considered): it only helps with unknown holders, and it can't tell a dead process from a paused one (`Ctrl-Z`, a suspended job, a file system hang). Taking over an expired lease would then let two transactions run at once, unless each write checked the lease, which a file system can't do atomically. It also relies on synchronized clocks. As nothing takes over a lock automatically here, it would only spare a `--force` in a rare case.
- **Mutual exclusion through the scheduler** (Slurm `--dependency=singleton`): Slurm only, and doesn't cover commands run outside jobs.
- **`mkdir` or `O_EXCL` instead of `link`**: also atomic, but the holder record is written after the lock appears, so others can find a lock without holder. They remain the fallback for file systems without hard links.

### Journal

A transaction records its submissions in `run/.sirocco/journal`, an append-only file of JSON lines, each flushed (`fsync`) before going on:

- a header, before the first submission: the holder record, so that the journal still tells which transaction it belongs to if the lock was removed by hand, and the run and revision of the committed state the transaction started from, its base,
- an intent (task label), once the run directory and run script are ready, right before calling the scheduler,
- the outcome of the call: the job id, or the rejection reported by the scheduler.

Only submissions are journaled, as they are the only effects a rollback undoes. Cancelled jobs can't be brought back, and run directories are prepared again at the next submission. The journal is deleted right after the commit, so a journal found later belongs to a transaction that failed, or that crashed between its commit and the deletion. The state tells them apart: once it has moved past the journal's base (a later revision, or another run), the transaction or its rollback committed and nothing is left to undo. A truncated last line, from a crash while writing it, is ignored.

An intent without outcome is unresolved: the call failed in a way that may still have created a job (timeout, killed, unexpected error), and nobody knows its id. Only the scheduler can tell that a submission was rejected and created nothing (e.g. invalid account). A scheduler implementing the optional `find_jobs()` settles unresolved intents. Slurm can, by job name (the task label) and working directory (the task run directory), which together identify a task of a workflow. As a job can appear after its submission call timed out, `find_jobs()` is only trusted after a grace period from the intent. A match is not an orphan if it is the current job or a job the committed state names: all continuation jobs share a name and a working directory. Without `find_jobs()`, recovery names the tasks and their run directories, and the user settles them with `recover --force` after checking. Until an intent is settled, the journal and the lock stay. Since the intent is written after the run directory is prepared, the most common failure, an error while preparing a task, leaves no intent behind.

Alternatives considered:

- **Inferring the commit from journaled job ids found in the state** (first considered): ambiguous for a transaction that cancels its own submissions before committing (`restart`, a failing `continue`). The run and revision are unambiguous, and remote control needs them anyway.
- **Submitting jobs on hold, journaling their id, then releasing them**: closes the window without job id, but doubles the scheduler calls, needs holds from every scheduler, and a held job left by a crash still has to be found.
- **Tagging jobs** (e.g. Slurm `--comment`): scheduler specific, while the job name and working directory already identify a task.

### State file

The state is committed by writing it to a temporary file, flushing it (`fsync`) and renaming it over `run/state.yaml` (`os.replace`). The rename is atomic: readers see the previous state or the new one, never a mix. It is the commit point of a transaction.

The state file gets a header:

| Field | Content |
|---|---|
| `run` | id of the run, generated by `start` |
| `revision` | incremented by each commit, from 1 at `start` |
| `status` | `RUNNING`, `STOPPED`, `COMPLETED` or `FAILED` |
| `reason` | why the workflow stopped when not on user request, e.g. `continue aborted: <error>` |
| `scheduler`, `cluster` | where the jobs of the run live |
| `committed_by` | transaction id, command and time of the last commit |

Every transaction first checks, from the header and before taking the lock, that it runs on the cluster of the workflow, and refuses otherwise: job ids only mean something to their own scheduler, and the same file system can be reached from several clusters.

The state file is enough to stop and recover a workflow. `stop`, the checks of `start --cleanup`, rollback and recovery work on the header, task labels and job ids, without building the workflow graph. They therefore work even when the config is broken, or no longer matches the state (tasks renamed, dates changed). Only the commands that move the front forward (`start`, `continue`, `restart`) parse the config and resolve the state in the graph.

A missing state file is revision 0 of no run. A state file without header, written by an earlier version, reads as revision 0 of an unnamed run, with the status derived from the continuation job id as today, and the scheduler and cluster of the current config and machine. Earlier versions know neither the lock nor the [fencing](#fencing-the-continuation-chain): the Sirocco that runs a workflow, including the `venv` of its SIROCCO task, is only upgraded while the workflow is stopped. Also taking the lock file of earlier versions was considered: earlier `continue` and `stop` would respect it, but earlier `start` and `restart` wouldn't, and only for a one-time transition.

`WorkflowStatus` is split accordingly into this persisted status and the outcome of a command (below). Its values `INIT`, `CONTINUE`, `STOPPING` and `RESTART_FAILED` currently mix both.

### Command outcomes

| Outcome | Meaning |
|---|---|
| `done` | committed |
| `no-op` | nothing to do: already stopped, superseded continuation, nothing to recover |
| `refused` | precondition not met: `restart` of a running workflow, `start` of a started one, wrong cluster |
| `busy` | lock still held by an alive or unknown holder after waiting |
| `needs recovery` | stale lock or leftover journal found, or rollback incomplete |
| `aborted` | failed and rolled back, the error is reported |

`done` and `no-op` exit with 0, every other outcome with its own non-zero code, so that scripts and remote control jobs can tell them apart. Today a refused `restart` exits with 0.

### Fencing the continuation chain

`continue` proceeds only if the committed state names its own job, `Scheduler.current_jobid()`, as the continuation job. Otherwise it is a no-op: the workflow was stopped, or the job was submitted by a transaction that didn't commit. `continue` outside a Sirocco job is refused.

The lock is no longer a stop signal: `stop` commits a state without continuation job, and a continuation job still queued turns into a no-op. Above all, an orphan continuation job can't start a second chain, even when a rollback couldn't cancel it.

Alternatives considered:

- **A token passed on the command line of the continuation job and stored in the state**: works without `current_jobid()`, but the job id is needed anyway to tell whether job holders are alive.
- **The expected state revision as token**: a revision can be used again after a rollback, a job id can't.

### Cool-down mode keeps what started

Tasks in the front don't wait for the continuation job: each depends (`afterok`) on the jobs of its parents in the front only. When a rank 0 task completes, its children and the continuation job become eligible at the same time, and the children can start first, which they often do: they have been queued at least as long, and the continuation job waits for resources like any other job. Until the continuation job commits, the state still lists them at rank 1. The longer it waits, the more generations run ahead: a rank 2 task starts as soon as its rank 1 parents completed. Today, a cool-down stop in that window cancels these tasks although they run, and `restart` computes them again, as both only keep rank 0 tasks.

A task whose job runs or completed has all its parents completed, through these dependencies. Cool-down mode therefore keeps every such task, in any generation of the front, and cancels the others. `restart` keeps these tasks instead of resubmitting them, and the next continuations promote them, one generation each. Rollback relies on this when it stops a workflow (below), possibly long after the last commit, when deeper tasks have run or completed meanwhile.

### Rollback and recovery

Rollback is one procedure, run by the failing transaction itself or, when the process couldn't (killed, node failure, failed rollback), by `sirocco recover`:

1. Unless the state moved past the journal's base, cancel the journaled jobs.
2. Settle the intents without outcome (see [Journal](#journal)). If one can't be settled yet, stop here.
3. Bring the committed state in line with the queue:
   - An aborted `stop` is completed from the committed state, in its mode: a half done stop can't be undone, since cancelled jobs stay cancelled.
   - Otherwise, if the committed state names a continuation job that is finished, or that is the aborted transaction's own job, nothing watches the workflow anymore. It is stopped in [cool-down mode](#cool-down-mode-keeps-what-started), with status `STOPPED` and the reason.
4. Commit if the state changed, delete the journal, release the lock.

Every step can be repeated: cancelling a finished job is a no-op, and step 3 is recomputed from the committed state and the queue. So the journal and the lock are only removed once all steps succeeded. If the in-process rollback can't complete, e.g. because the scheduler doesn't answer or an intent is unresolved, the transaction leaves both, marks its holder record `abandoned` and ends as `needs recovery`. Rollback only needs the journal, the state file and the scheduler (see [State file](#state-file)).

| Aborted command | Left behind after rollback |
|---|---|
| `start` | its jobs cancelled, no state file: `start` can be run again |
| `continue` | its jobs cancelled, workflow stopped in cool-down mode, with the reason |
| `restart` | its jobs cancelled, workflow stopped as before the restart |
| `stop` | stop completed |
| `recover` | resumed by the next `recover` |

A failed `continue` stops the workflow in cool-down mode rather than failing it: tasks that started keep running, the reason is recorded, and `sirocco restart` resumes the workflow once the cause is fixed, without resubmitting the kept tasks. The continuation is not resubmitted automatically: a persistent error (broken config, missing file) would be retried forever, and without delay as soon as no task is left to wait for.

**Signals.** During a transaction, SIGTERM (cancelled job, walltime), SIGINT (`Ctrl-C`) and SIGHUP (lost terminal) raise an exception, so that the rollback runs. They are deferred during short critical sections, so that sirocco never cuts these in the middle itself: acquiring and releasing the lock, writing an intent, calling the scheduler and writing the outcome, the commit, and the rollback. Scheduler commands run in their own session, so that the signals of the terminal don't reach them, and with a timeout, which bounds the deferral. A scheduler command can still be killed, e.g. when the scheduler signals all processes of a job, which leaves its intent unresolved. After SIGHUP, output only goes to the log file, as writing to the lost terminal would fail.

**`sirocco recover [--force]`** handles, and reports what it did:

- a stale lock, if its holder is dead;
- a journal without lock, e.g. after the lock was removed by hand;
- a broken chain without journal: a running workflow whose continuation job is finished, e.g. because it failed before sirocco started (broken virtual environment). Step 3 applies.

`--force` accepts what recovery can't check, once the user did: an unknown holder is gone, a stale claim (see below) is abandoned, unresolved intents created no job or their jobs were cancelled by hand. When there is nothing to recover, `recover` is a no-op.

**`sirocco restart --recover`** recovers then restarts while holding the lock. The recovery commits first and the holder record is rewritten without `recovering`, so that the restart has its own journal and rollback.

**Breaking a stale lock** must not let two processes take it. `recover` first creates, with the same exclusive link as the lock, a claim file named after the stale holder id. It then checks that the lock still holds that record and replaces it in place (`os.replace`) with its own, which keeps the stale one under `recovering`. After a failed replace, as after a failed link, it checks whether the lock is now its temporary file (same inode). The lock never disappears during the takeover, so no other transaction can acquire it meanwhile, and only one recovery gets the claim. A recovery that dies leaves its own record in the lock, with the transaction it was recovering, and the next `recover` resumes it. Claim files are removed with the lock. A claim left by a recovery that died before replacing the lock needs `--force`.

### Commands

| Command | Under the lock | No-op when |
|---|---|---|
| `start` | refused if a committed state exists. With `--cleanup`: refused while jobs of the workflow may run (running workflow, kept cool-down tasks), then deletes the state file and everything else in `run/` except `run/.sirocco/`, and starts a new run | |
| `continue` | fencing, then as today | not the committed continuation job |
| `restart` | refused if running, or if the state names tasks the config no longer defines (`start --cleanup` is then the way) | |
| `stop` | stops a running workflow. In cancel mode, also cancels the tasks kept by an earlier cool-down stop | stopped and nothing left to cancel |
| `recover` | see above | nothing to recover |

`start`, `restart` and `stop` check for the state file rather than for `run/`, which now holds the lock. A `start` rolled back before its first commit can therefore be run again without `--cleanup`. `start --cleanup` deletes the old state before starting, so that its journal starts from no state and never refers to the previous run.

Letting `stop` cancel the tasks kept by a cool-down stop gives a way to get rid of them after a recovery, which keeps the tasks that started. Today they can only be cancelled through the scheduler.

### Scheduler interface

- `current_jobid()`: job id of the current Sirocco job, read from `SIROCCO_JOBID`, `None` elsewhere. Sirocco job scripts set it from the scheduler's own variable.
- `job_status()`: status from the job id alone, telling a finished job, a job the scheduler doesn't know anymore (counted as finished) and a failed query (counted as alive).
- `cluster()`, optional: name of the current cluster (Slurm `ClusterName`).
- `cancel()`: cancelling a job that is finished, or that the scheduler doesn't know anymore, is not an error. Rollback and recovery repeat cancellations.
- `submit_to_scheduler()`: tells a rejected submission, which created nothing, from any other failure.
- `find_jobs()`, optional: queued or running jobs of a task, found by name and working directory.

The Slurm implementation submits Sirocco jobs without requeue (`--no-requeue`): after a node failure, a requeued run would find its previous run holding the lock and wait for itself.

### Prepared for remote control

Remote control submits jobs running sirocco commands from the laptop. All of the following is part of this work, so that remote control builds on transactions without changing them:

- **One code path.** A remote request is a job running the same command as locally, so it is a transaction like any other, and local and remote control exclude each other through the lock only.
- **Transaction id given by the caller.** Remote control passes its request id. It is recorded in the holder record, the journal and `committed_by`, so that a retried request can be recognized. It never decides who holds the lock, since retries share it.
- **Job holders checked through the scheduler**, from the cluster as well as from the laptop: remote control jobs are Sirocco jobs.
- **Outcomes and exit codes** that a request result can report (`busy`, `needs recovery`, ...).
- **Config parsed under the lock.** A config push, run as a transaction under the same lock, can replace the config without a command reading half of it.
- **Lock-free reads.** The state file is always complete, so `sirocco pull` doesn't need to check the lock ([#292](https://github.com/C2SM/Sirocco/pull/292) currently refuses to download while it exists). Excluding `run/` from a push also excludes the lock.
- **Run and revision**, the base for conditional requests: a request can carry the run and revision it was prepared from, and be refused if the state moved on, even across `start --cleanup`.
- **`run/.sirocco/`**, the place for request results and pushed config archives.

Left to remote control: request result files, the `--request-id` option, conditional requests, the config hash in the state header and the config push transaction. The laptop never takes the lock nor writes the state.

## Consequences

- A command ends committed, rolled back to the last commit, or with a journal that `sirocco recover` resolves. Workflows no longer stop silently: a failed continuation stops the workflow in cool-down mode with the reason in the state, and `sirocco restart --recover` is the way back.
- A workflow can always be stopped, recovered and cleaned up, even when its config is broken or changed.
- Commands wait for each other instead of answering "retry", and refuse to run on another cluster than the workflow's.
- Deciding whether a lock holder is alive involves no clock. Only the grace period of unresolved intents uses time, at a scale of minutes.
- The state file gets a header, and `run/.sirocco/` holds the lock, the journal and the claim files.
- The Sirocco that runs a workflow, including the `venv` of its SIROCCO task, is only upgraded while the workflow is stopped.
- Each submission costs two flushed journal writes. The lock is also held while the config is parsed.
- The scheduler interface grows (see above), and `cancel()` must be idempotent.

## Implementation steps

Each step keeps the tests green:

1. **Lock and transactions**: lock with holder record and liveness, `SIROCCO_JOBID` and `exec` in Sirocco job scripts, waiting and outcomes, atomic commit with the state header and the cluster check, all commands as transactions with preconditions checked under the lock, `stop` on the state file alone, fencing of `continue`. Fixes the races and the silent stops.
2. **Journal and rollback**: journal, in-process rollback, cool-down keeping what started, signal handling, scheduler commands in their own session with a timeout, idempotent `cancel()`.
3. **Recovery**: lock takeover, `sirocco recover`, `restart --recover`, `stop` cancelling kept tasks, `find_jobs()` for Slurm.

Tests, besides the orchestration traces, which also show lock and journal events:

- two processes racing for the lock, two retries sharing a transaction id, two recoveries racing for a stale lock: one wins;
- a scheduler failing at the n-th submission: the rollback cancels the earlier ones;
- a submission call timing out: its intent stays unresolved until `find_jobs()` settles it;
- a command killed (SIGKILL) at the n-th submission, then `recover`: same trace as the in-process rollback;
- a crash between commit and journal deletion, also for `start --cleanup`: `recover` only cleans up;
- a continuation job not named in the committed state: no-op;
- `stop`, `recover` and `start --cleanup` after tasks were renamed in the config;
- a recovery long after a crash, with deeper tasks running or completed meanwhile: they are kept;
- SIGTERM during a transaction: the rollback runs.

## Open points

- Resubmitting the continuation after transient scheduler errors, with a retry budget, or recovering from the continuation job when it finds a stale lock whose holder is provably dead, if workflows turn out to stop often for such reasons.
- Telling the user that a workflow stopped after a failure, e.g. by a non-zero exit of the continuation job so that the scheduler sends its failure mail.
- A `status` command showing the workflow status, the lock holder and whether recovery is needed. Remote control needs it too.
- Default wait timeouts and grace period.
