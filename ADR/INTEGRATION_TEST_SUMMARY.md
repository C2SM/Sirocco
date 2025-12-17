# Branch Independence Integration Test

## Overview

I've created a comprehensive integration test that actually executes a workflow to validate the dynamic task level feature. This test uses real shell scripts with different execution times to demonstrate that faster branches can advance independently of slower branches.

## What Was Created

### 1. Test Case Configuration
**Location**: `tests/cases/branch-independence/`

- **config.yml**: Workflow with diamond DAG structure
  ```
         root (1s)
        /         \
    fast_1 (1s)   slow_1 (8s)
       |              |
    fast_2 (1s)   slow_2 (8s)
       |              |
    fast_3 (1s)   slow_3 (8s)
  ```

- **scripts/fast_task.sh**: Shell script that sleeps for 1 second
- **scripts/slow_task.sh**: Shell script that sleeps for 8 seconds
- **README.md**: Comprehensive documentation

### 2. Test Implementation
**Location**: `tests/unit_tests/test_workgraph.py`

#### Test 1: Configuration Test (`test_branch_independence_config`)
- Fast test that builds the WorkGraph
- Verifies window config structure
- Validates dependencies are correct
- Checks that `task_dependencies` (not `task_levels`) is stored

#### Test 2: Integration Test (`test_branch_independence_execution`) ⭐
**This is the main validation test!**

- Marked as `@pytest.mark.slow` (~30 seconds execution)
- Actually runs the workflow with `workgraph.run()`
- Uses real shell scripts with sleep times
- **Key validations**:
  1. Fast branch completes before slow branch
  2. At least 2 fast tasks complete while `slow_1` is still running
  3. Total execution time is reasonable (~25-30s)

**What it proves**:
- With static levels, fast_2 would wait for slow_1 to finish (same level 2)
- With dynamic levels, fast_2 runs immediately after fast_1 completes
- Window_size=1 works correctly with dynamic level updates

#### Test 3: Unit Test (`test_dynamic_levels_branch_independence`)
- Fast algorithmic test
- Simulates task state changes
- Validates the dynamic level computation logic mathematically
- No actual execution, just algorithm verification

## How to Run

```bash
# Quick tests (configuration and algorithm)
pytest tests/unit_tests/test_workgraph.py::test_branch_independence_config -v
pytest tests/unit_tests/test_workgraph.py::test_dynamic_levels_branch_independence -v

# Integration test (actually runs workflow, ~30s)
pytest tests/unit_tests/test_workgraph.py::test_branch_independence_execution -v -m slow

# Run all branch independence tests
pytest tests/unit_tests/test_workgraph.py -k branch_independence -v
```

## Expected Behavior

### With Static Levels (Old)
```
Time: 0s  - root starts
Time: 1s  - root finishes, fast_1 and slow_1 start (both level 1)
Time: 2s  - fast_1 finishes
Time: 9s  - slow_1 finishes
Time: 9s  - fast_2 and slow_2 start (both level 2) ❌ fast_2 waited!
Time: 10s - fast_2 finishes
Time: 17s - slow_2 finishes
...
Total: ~25-30s
```

### With Dynamic Levels (New)
```
Time: 0s  - root starts
Time: 1s  - root finishes, fast_1 and slow_1 start (both level 0)
Time: 2s  - fast_1 finishes, fast_2 starts immediately (level 0) ✅
Time: 3s  - fast_2 finishes, fast_3 starts immediately (level 0) ✅
Time: 4s  - fast_3 finishes ✅ Fast branch done!
Time: 9s  - slow_1 finishes, slow_2 starts (level 0)
Time: 17s - slow_2 finishes, slow_3 starts (level 0)
Time: 25s - slow_3 finishes
Total: ~25s (no unnecessary waiting)
```

## Test Assertions

The integration test makes the following key assertions:

1. **Workflow completes successfully**: No failures or errors
2. **Fast branch finishes first**: `last_fast_time < last_slow_time`
3. **Branch independence**: At least 2 fast tasks complete before `slow_1` finishes
4. **Total time reasonable**: < 40 seconds (allows for AiiDA overhead)

## Why This Test is Important

This integration test:

✅ **Validates real-world behavior**: Uses actual shell scripts and timing
✅ **Proves branch independence**: Demonstrates fast branch doesn't wait
✅ **Tests end-to-end**: Full workflow execution, not just mocked
✅ **Measurable results**: Uses timestamps to verify behavior
✅ **Regression protection**: Will catch if dynamic levels stop working

## Files Created

```
tests/cases/branch-independence/
├── config/
│   ├── config.yml                    # Workflow definition
│   └── scripts/
│       ├── fast_task.sh              # Fast execution (1s)
│       └── slow_task.sh              # Slow execution (8s)
└── README.md                         # Test case documentation

tests/unit_tests/test_workgraph.py
├── test_branch_independence_config()      # Configuration test
├── test_branch_independence_execution()   # Integration test (NEW!)
└── test_dynamic_levels_branch_independence() # Unit test

INTEGRATION_TEST_SUMMARY.md               # This file
```

## Next Steps

To run this test in CI/CD:

```bash
# In your CI pipeline
pytest tests/unit_tests/test_workgraph.py -v -m slow
```

To manually verify the dynamic level behavior:

```bash
# Submit the workflow manually
sirocco submit tests/cases/branch-independence/config/config.yml --window-size 1

# Monitor the execution
verdi process list

# Check the reports for window level updates
verdi process report <PK>
```

## Summary

The integration test successfully validates that:
- Dynamic levels are computed at runtime
- Tasks advance as soon as their dependencies complete
- Faster branches don't wait for slower branches
- The window-size feature works correctly with dynamic updates

This provides strong evidence that the dynamic level implementation is working as designed! 🎉
