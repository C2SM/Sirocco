# aiida-icon `link_dir_contents` Race Condition with SLURM Pre-submission

## Issue Summary

The `link_dir_contents` feature in aiida-icon is incompatible with SLURM pre-submission when using job dependencies. The IconCalculation fails during `prepare_for_submission` because it tries to list remote directory contents that don't exist yet (upstream jobs are still creating them).

## Root Cause

### How SLURM Pre-submission Works

1. Upstream job (e.g., `prepare_input`) is submitted to SLURM and returns `job_id` + `remote_folder` PK immediately
2. Downstream job (e.g., `icon`) gets this information via `get_job_data`
3. Downstream job's `prepare_for_submission` runs **locally** to prepare submission files
4. Job is submitted to SLURM with `--dependency=afterok:<upstream_job_id>`
5. SLURM holds the job until upstream finishes

### The Problem

aiida-icon's `prepare_for_submission` method (line 190 in `calculations.py`) calls `remotedata.listdir()` to enumerate files in `link_dir_contents`:

```python
if "link_dir_contents" in self.inputs:
    for remotedata in self.inputs.link_dir_contents.values():
        for subpath in remotedata.listdir():  # <-- FAILS HERE
            calcinfo.remote_symlink_list.append(...)
```

This happens **locally before SLURM submission**, but the remote directory doesn't exist yet because the upstream job is still running.

**SLURM dependencies prevent jobs from starting on compute nodes, but they don't prevent AiiDA from running local preparation steps.**

## Timeline Example (from DYAMOND workflow)

| Time | Event | Status |
|------|-------|--------|
| 06:36:54 | `prepare_input` job submitted to SLURM (job 545483) | Running on SLURM |
| 06:37:40 | `remote_folder` node created (PK 43155) | - |
| 06:37:50 | `get_job_data` returns job_id + remote_folder PK | Correct behavior |
| 06:37:52 | `icon` job's `prepare_for_submission` starts **locally** | - |
| 06:37:52-06:38:18 | `icon` calls `listdir()` on `icon_input/` directory | **FAILS** - directory doesn't exist |
| 06:38:18 | IconCalculation excepted with OSError | ❌ Failure |
| 06:39:43 | `prepare_input` finishes, creates `icon_input/` | Too late |

## Error Message

```
OSError: The required remote path /capstor/scratch/cscs/jgeiger/aiida/6a/5c/345a-8d48-44ca-a10d-6e1c0fb44d27/icon_input/.
on santis-async-ssh does not exist, is not a directory or has been deleted.
```

## Proposed Solutions

### Option 1: Skip Non-existing Directories (Simple)

Modify `aiida-icon/src/aiida_icon/calculations.py` line 188-197:

```python
if "link_dir_contents" in self.inputs:
    for remotedata in self.inputs.link_dir_contents.values():
        try:
            subpaths = remotedata.listdir()
        except OSError as e:
            # Directory doesn't exist yet - skip for now
            # It will be created by upstream job before this job starts (SLURM dependency)
            self.logger.warning(
                f"Directory {remotedata.get_remote_path()} does not exist yet, "
                f"skipping link_dir_contents enumeration. Will be created by upstream job."
            )
            continue

        for subpath in subpaths:
            calcinfo.remote_symlink_list.append(
                (
                    remotedata.computer.uuid,
                    str(pathlib.Path(remotedata.get_remote_path()) / subpath),
                    subpath,
                )
            )
```

**Pros:**
- Simple fix
- Maintains SLURM dependency semantics
- Job will still fail if directory truly doesn't exist when it starts running

**Cons:**
- Can't validate directory contents during submission
- Symlinks won't be enumerated in advance (but they'll be created via different mechanism?)

### Option 2: Defer Symlink Creation to Compute Node

Instead of enumerating files during `prepare_for_submission`, create a wrapper script that:
1. Waits for upstream jobs (handled by SLURM)
2. Creates symlinks from the remote directory contents on the compute node
3. Runs ICON

This would require more extensive changes to the workflow.

### Option 3: Wait for Directory in prepare_for_submission (Not Recommended)

Poll until the directory exists before calling `listdir()`.

**Problems:**
- Defeats the purpose of SLURM pre-submission
- Introduces arbitrary delays in workflow submission
- Could cause deadlocks if upstream job fails

## Recommendation

**Option 1** is the best approach:
- Skip non-existing directories with a warning
- Trust SLURM dependencies to ensure directory exists when job starts
- Simple, minimal change to aiida-icon
- Preserves the benefits of SLURM pre-submission

## Context

This issue was discovered in the DYAMOND workflow where:
- `prepare_input` task creates an `icon_input/` directory with symlinks
- `icon` task uses `link_dir_contents` to reference this directory
- With SLURM pre-submission enabled, the race condition manifests

The workflow configuration:
```yaml
- icon:
    inputs:
      - icon_link_input:
          port: link_dir_contents
```

Where `icon_link_input` is a GeneratedData output from `prepare_input`.
