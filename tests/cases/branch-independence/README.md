# Branch Independence Test Case

This test case demonstrates the dynamic task level feature in Sirocco's window-size implementation.

## Overview

This workflow has two parallel branches with different execution times:
- **Fast branch**: root (1s) → fast_1 (1s) → fast_2 (1s) → fast_3 (1s) = ~4s total
- **Slow branch**: root (1s) → slow_1 (8s) → slow_2 (8s) → slow_3 (8s) = ~25s total

## What This Demonstrates

With **dynamic levels**, the fast branch can complete independently without waiting for the slow branch. This is particularly important when using `window_size=1`, which limits how many topological levels ahead tasks can be submitted.

### Key Behavior:
- `fast_2` starts immediately after `fast_1` completes (~2s)
- `fast_3` starts immediately after `fast_2` completes (~3s)
- Fast branch completes at ~4s
- Slow branch continues independently

Without dynamic levels, tasks at the same topological depth would wait for each other unnecessarily.

## Workflow Structure

```
       root (1s)
      /         \
  fast_1 (1s)   slow_1 (8s)
     |              |
  fast_2 (1s)   slow_2 (8s)
     |              |
  fast_3 (1s)   slow_3 (8s)
```

## Environment Variable Configuration

This test case uses environment variables to support both pytest integration testing and manual CLI execution without code duplication:

- **`SIROCCO_COMPUTER`**: The AiiDA computer to use (default: `localhost`)
- **`SIROCCO_SCRIPTS_DIR`**: Path to the scripts directory (default: `scripts`)

The config file (`config/config.yml`) uses `${VAR:-default}` syntax for variable substitution.

## Running as Pytest Integration Test

```bash
# Run the integration test (uses 'remote' computer via pytest fixtures)
pytest tests/unit_tests/test_workgraph.py::test_branch_independence_execution -v -m slow

# The pytest fixture automatically sets:
# - SIROCCO_COMPUTER=remote
# - SIROCCO_SCRIPTS_DIR=<tmp_path>/tests/cases/branch-independence/config/scripts
```

## Running Manually via CLI

### Option 1: Use the wrapper script (recommended)

```bash
# Run with default settings (localhost)
./tests/cases/branch-independence/run.sh

# Override computer if needed
SIROCCO_COMPUTER=my-remote-computer ./tests/cases/branch-independence/run.sh
```

### Option 2: Set environment variables manually

```bash
# Set variables
export SIROCCO_COMPUTER=localhost
export SIROCCO_SCRIPTS_DIR="$(pwd)/tests/cases/branch-independence/config/scripts"

# Submit
sirocco submit tests/cases/branch-independence/config/config.yml --window-size 1

# Monitor
verdi process list
verdi process report <PK>
```

## Expected Timeline

```
Time: 0s  - root starts
Time: 1s  - root finishes, fast_1 and slow_1 start
Time: 2s  - fast_1 finishes, fast_2 starts immediately ✅
Time: 3s  - fast_2 finishes, fast_3 starts immediately ✅
Time: 4s  - fast_3 finishes ✅ Fast branch complete!
Time: 9s  - slow_1 finishes, slow_2 starts
Time: 17s - slow_2 finishes, slow_3 starts
Time: 25s - slow_3 finishes ✅ Workflow complete!
```

## Test Coverage

### 1. Configuration Test (`test_branch_independence_config`)
Verifies that:
- The workflow builds correctly with `window_size=1`
- Window config contains `task_dependencies` (not static `task_levels`)
- All expected tasks are present
- Dependencies are correctly structured

### 2. Integration Test with Actual Execution (`test_branch_independence_execution`)
**This is the main test that validates the dynamic level feature!**

Executes the full workflow with real shell scripts and verifies:
- The workflow completes successfully
- **Key validation**: Fast branch completes before slow branch
- At least 2 fast tasks complete while `slow_1` is still running
- Total execution time is reasonable (~25-30s)

### 3. Unit Test for Dynamic Level Computation (`test_dynamic_levels_branch_independence`)
Simulates task completion sequence to verify the algorithm mathematically.

## Files

```
tests/cases/branch-independence/
├── config/
│   ├── config.yml              # Workflow definition (uses env vars)
│   └── scripts/
│       ├── fast_task.sh        # Fast task script (sleeps 1s)
│       └── slow_task.sh        # Slow task script (sleeps 8s)
├── data/
│   └── config.txt              # Serialized workflow representation
├── run.sh                      # Wrapper script for manual execution
└── README.md                   # This file
```

## Viewing Results

After running the workflow, check the process report to see dynamic level updates:

```bash
# Get the PK from the output or from process list
verdi process list

# View the report
verdi process report <PK>
```

You should see:
- Window level updates: `"Window: levels 0-1 (max dynamic level: 3)"` → ... → `"(max dynamic level: 0)"`
- Tasks being submitted as dependencies complete
- Fast branch completing before slow branch

## Persistent Logging

If running via pytest, the test creates a `branch_independence_test.log` file with detailed execution information including:
- Workflow build details
- Execution timing
- Node discovery results
- All test assertions

## Comparing with Static Levels

**Without dynamic levels** (old behavior):
- `fast_2` would wait for `slow_1` to finish (both at level 2)
- `fast_3` would wait for `slow_2` to finish (both at level 3)
- Unnecessary waiting causes delays

**With dynamic levels** (new behavior):
- Levels are recomputed after each task completion
- Only unfinished tasks are considered
- Fast branch advances independently
- No unnecessary waiting

## Design Rationale

This test case demonstrates a key design principle in Sirocco: **test cases and examples share the same configuration** through environment variable substitution. Benefits:

- ✅ No code duplication
- ✅ Test cases use pytest fixtures to set variables
- ✅ Manual runs use environment variables or wrapper scripts
- ✅ Single source of truth for workflow configuration
- ✅ Easy to maintain and update

## Troubleshooting

If tasks time out with "Timeout waiting for job_id":
- Check that the timeout is set to 600s (should be default now)
- Ensure the computer is properly configured in AiiDA
- Check that scripts have execute permissions: `chmod +x config/scripts/*.sh`
- Verify environment variables are set correctly
