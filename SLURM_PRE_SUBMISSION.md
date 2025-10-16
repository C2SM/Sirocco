# SLURM Pre-Submission Feature

This document explains how to use the SLURM pre-submission feature in Sirocco to submit chains of dependent tasks ahead of time, with dependencies managed by SLURM rather than WorkGraph.

## Overview

By default, Sirocco workflows submit tasks sequentially - each task waits for its dependencies to complete before being submitted to SLURM. The pre-submission feature allows you to submit multiple dependent tasks at once, with SLURM managing the execution order using job dependencies (`--dependency=afterok:jobid`).

## Benefits

- **Reduced latency**: Tasks are queued in SLURM before their dependencies complete
- **Better resource utilization**: SLURM can optimize job scheduling across the entire dependency chain
- **Faster workflows**: Eliminates the overhead of waiting for WorkGraph to submit each task

## Usage

The `pre_submission_depth` parameter controls how many dependency levels ahead to pre-submit:

```python
from sirocco.workgraph import AiidaWorkGraph
from sirocco.core import Workflow

# Load your workflow
workflow = Workflow.from_config_file("workflow.yaml")

# Option 0: Fully sequential - no SLURM dependencies (WorkGraph control)
wg = AiidaWorkGraph(workflow, pre_submission_depth=0)
wg.submit()

# Option 1: Default behavior - one step ahead (direct dependencies use SLURM)
wg = AiidaWorkGraph(workflow, pre_submission_depth=1)
wg.submit()

# Option 2: Pre-submit chains where dependencies are within 3 levels
wg = AiidaWorkGraph(workflow, pre_submission_depth=3)
wg.submit()

# Option 3: Submit entire workflow to SLURM at once
wg = AiidaWorkGraph(workflow, pre_submission_depth=9999)
wg.submit()
```

### Command-line Usage

```bash
# Fully sequential (no SLURM dependencies)
sirocco submit workflow.yaml --pre-submission-depth 0

# One step ahead (default)
sirocco submit workflow.yaml --pre-submission-depth 1
# or simply
sirocco submit workflow.yaml

# Submit chains 3 levels deep
sirocco submit workflow.yaml -d 3

# Submit entire workflow
sirocco submit workflow.yaml -d 9999
```

## How It Works

### Dependency Depth Calculation

The system calculates the dependency depth for each task:
- Root tasks (no dependencies) have depth 0
- Tasks depending on depth-0 tasks have depth 1
- Tasks depending on depth-1 tasks have depth 2
- And so on...

### Pre-Submission Logic

For tasks where the dependency depth difference is ≤ `pre_submission_depth`:

1. **Job ID Monitoring**: A `get_job_id` task is created to poll for the SLURM job ID of each dependency
2. **SLURM Dependency Injection**: The job IDs are collected and injected into the task's SLURM script as:
   ```bash
   #SBATCH --dependency=afterok:job_id_1:job_id_2:...
   ```
3. **WorkGraph Dependency Removal**: The WorkGraph waiting dependency is removed (SLURM handles it instead)

### Example Workflow

Consider this dependency chain:
```
Task A (depth 0)
  └─> Task B (depth 1)
      └─> Task C (depth 2)
          └─> Task D (depth 3)
```

**With `pre_submission_depth=0`**:
- A is submitted
- B waits for A to complete (WorkGraph control), then is submitted
- C waits for B to complete (WorkGraph control), then is submitted
- D waits for C to complete (WorkGraph control), then is submitted
- **Result: Fully sequential, no SLURM dependencies, traditional WorkGraph behavior**

**With `pre_submission_depth=1` (default)**:
- A is submitted
- B is submitted immediately with SLURM dependency on A's job ID (depth diff: 1-0=1, satisfies ≤1)
- C is submitted immediately with SLURM dependency on B's job ID (depth diff: 2-1=1, satisfies ≤1)
- D is submitted immediately with SLURM dependency on C's job ID (depth diff: 3-2=1, satisfies ≤1)
- **Result: All directly-dependent tasks use SLURM dependencies (one step ahead)**

**With `pre_submission_depth=2`**:
- Same as depth=1 for this linear chain
- In a more complex DAG, tasks could have SLURM dependencies on ancestors up to 2 levels away
- **Result: Dependencies up to 2 levels apart use SLURM dependencies**

**With `pre_submission_depth=9999`**:
- All tasks are submitted immediately with SLURM dependencies
- For complex DAGs, even distant ancestors can use SLURM dependencies
- **Result: Entire workflow submitted at once, SLURM manages all dependencies**

### Key Insight

In the simple linear chain A→B→C→D, `pre_submission_depth=1` already enables SLURM dependencies for all tasks because each task only depends on the one immediately before it (depth difference = 1). Higher values of `pre_submission_depth` become relevant in more complex DAGs where tasks might depend on multiple ancestors at different depth levels.

## Implementation Details

The feature is implemented in `/home/geiger_j/aiida_projects/swiss-twins/git-repos/Sirocco/src/sirocco/workgraph.py`:

1. `get_job_id(workgraph_name, task_name)`: Async task that polls for a task's SLURM job ID
2. `_calculate_task_depths()`: Calculates dependency depth for each task
3. `_apply_slurm_dependencies()`: Applies SLURM dependencies based on depth
4. `_wrap_task_with_slurm_dependency()`: Injects SLURM dependency commands into tasks

## Limitations and Notes

- **SLURM scheduler required**: This feature only works with SLURM-based HPC systems
- **Metadata injection timing**: The current implementation includes infrastructure for runtime metadata injection, but the exact mechanism may need refinement based on WorkGraph's capabilities
- **Job ID polling**: The `get_job_id` task polls the AiiDA database every 2 seconds (configurable) with a 600-second timeout

## Based On

This feature is based on the approach demonstrated in `xing-wg-pre-submission.py`, which shows how to use WorkGraph's `@task.graph` decorator and dynamic inputs to handle SLURM job dependencies.
