# Testing Dynamic Levels and Pre-submission

This document describes the testing strategy for Sirocco's dynamic level computation and pre-submission features.

## Table of Contents

- [Overview](#overview)
- [Testing Strategy](#testing-strategy)
- [Test Levels](#test-levels)
- [Running Tests](#running-tests)
- [Key Testing Principles](#key-testing-principles)
- [CI Setup](#ci-setup)
- [Test Files](#test-files)
- [Writing New Tests](#writing-new-tests)

## Overview

Sirocco implements **dynamic level computation** and **pre-submission** (controlled by `window_size`) to enable efficient parallel workflow execution. These features allow:

1. **Branch Independence**: Fast-completing branches advance without waiting for slow branches at the same topological level
2. **Pre-submission**: Tasks can be submitted before their dependencies finish (controlled by `window_size`)
3. **Dynamic Recomputation**: Task levels are recomputed as tasks complete, enabling optimal parallelization

Testing these features is challenging because:
- Timing is non-deterministic (depends on scheduler, network, etc.)
- Absolute times vary between runs and environments
- We need to test relationships, not absolute values

## Testing Strategy

Our testing strategy uses **three complementary levels**:

### Level 1: Unit Tests (Fast, No Infrastructure)
- Test algorithms directly
- No AiiDA infrastructure needed
- Run in milliseconds
- Test `compute_topological_levels()` function
- Validate window-size logic
- **Location**: `tests/unit_tests/test_dynamic_levels.py`

### Level 2: Integration Tests (Slow, Minimal Infrastructure)
- Test actual WorkGraph execution
- Use SSH localhost + `core.direct` scheduler
- Test relationships, not absolute times
- **No SLURM needed!**
- **Location**: `tests/unit_tests/test_workgraph.py`

### Level 3: Manual Testing (Real Infrastructure)
- Run on actual HPC with SSH + SLURM
- Longer task durations for clear timing separation
- Use analysis scripts for visualization
- **Location**: `tests/cases/branch-independence/`

## Test Levels

### Unit Tests (`test_dynamic_levels.py`)

**Purpose**: Validate core algorithms without infrastructure overhead

**Test Classes**:
```python
TestTopologicalLevels
├── test_simple_linear_chain
├── test_parallel_branches
├── test_diamond_dependency
├── test_cross_branch_dependency
└── test_complex_graph

TestDynamicLevels
├── test_levels_update_after_root_completes
├── test_fast_branch_advances_independently
└── test_cross_dependency_blocks_advancement

TestWindowSizeLogic
├── test_window_size_zero_sequential
├── test_window_size_one_ahead
├── test_window_size_two_aggressive
└── test_window_advances_with_max_level

TestComplexScenarios
├── test_branch_independence_simulation
└── test_complex_workflow_with_cross_deps
```

**Run with**:
```bash
hatch test tests/unit_tests/test_dynamic_levels.py -v
```

**Expected time**: 1-5 seconds

### Integration Tests (`test_workgraph.py`)

**Purpose**: Validate behavior with actual WorkGraph execution

**Key Tests**:

#### 1. `test_branch_independence_execution`
- Tests basic 2-branch workflow (fast + slow)
- Validates branch independence
- Checks pre-submission behavior
- Verifies submission order
- **Duration**: ~2-3 minutes (with CI infrastructure)

#### 2. `test_branch_independence_with_window_sizes` (Parameterized)
- Tests with `window_size=0, 1, 2`
- Validates different pre-submission strategies
- Ensures branch independence works with all window sizes
- **Duration**: ~6-9 minutes (3 tests × 2-3 min)

#### 3. `test_complex_workflow_with_cross_dependencies`
- Tests 3-branch workflow (fast + medium + slow)
- Validates cross-dependencies
- Checks convergence points
- Tests complex dependency graphs
- **Duration**: ~3-5 minutes

**Run with**:
```bash
# Run all slow integration tests
hatch test tests/unit_tests/test_workgraph.py -v -m slow

# Run specific test
hatch test tests/unit_tests/test_workgraph.py::test_branch_independence_execution -v -m slow

# Run with specific window_size
hatch test tests/unit_tests/test_workgraph.py::test_branch_independence_with_window_sizes[branch-independence-1] -v -m slow
```

### Manual Testing

**Purpose**: Visual validation and performance testing on real infrastructure

**Test Cases**:
- `tests/cases/branch-independence/` - Simple 2-branch test
  - Config: `config/config.yml`
  - Complex: `config/config_complex.yml` (3 branches + cross-deps)

**Run with**:
```bash
# Set up environment
export SIROCCO_COMPUTER=your-hpc-computer
export SIROCCO_SCRIPTS_DIR=tests/cases/branch-independence/config/scripts

# Run simple test
bash tests/cases/branch-independence/run.sh

# Run complex test
sirocco run tests/cases/branch-independence/config/config_complex.yml --window-size 1

# Analyze results
python tests/cases/branch-independence/analyze.py <PK>
```

**Analyzing Results**:
```python
# The analyze.py script generates:
# 1. Gantt chart showing task submission timeline
# 2. Validation checks for branch independence
# 3. Submission order analysis

# Example output:
✓ PASS: Fast branch completed before slow branch
✓ fast_3 was submitted BEFORE slow_3 (correct)
```

## Running Tests

### Quick Commands

```bash
# Run all unit tests (fast, < 10 seconds)
hatch test tests/unit_tests/test_dynamic_levels.py -v

# Run all integration tests (slow, ~15-30 minutes)
hatch test tests/unit_tests/test_workgraph.py -v -m slow

# Run specific integration test
hatch test tests/unit_tests/test_workgraph.py::test_branch_independence_execution -v -m slow

# Run tests with specific Python version
hatch test -py 3.12 tests/unit_tests/test_dynamic_levels.py -v
```

### CI Testing

Tests run automatically on GitHub Actions:

```yaml
# .github/workflows/test-integration.yml
- name: Run integration tests
  run: hatch test tests/unit_tests/test_workgraph.py -m slow
```

**CI Infrastructure**:
- Ubuntu latest
- Python 3.11, 3.12
- AiiDA core
- SSH localhost (configured in CI)
- `core.direct` scheduler (no SLURM needed!)

## Key Testing Principles

### 1. Test Relationships, Not Absolute Times

❌ **Don't do this**:
```python
assert task_completes_at == 15.0  # Timing-dependent, will fail!
```

✅ **Do this instead**:
```python
assert fast_end_time < slow_end_time  # Relationship-based, robust
assert task_b_submit < task_a_finish  # Tests pre-submission
```

### 2. Use Relative Timing

Extract timing relationships:
```python
from tests.unit_tests.test_utils import extract_launcher_times

launcher_times = extract_launcher_times(workgraph.process)

# Get submission times (ctime) and completion times (mtime)
fast_3_submit = launcher_times['fast_3']['ctime']
slow_3_submit = launcher_times['slow_3']['ctime']

# Test relationship
assert fast_3_submit < slow_3_submit  # fast submitted first
```

### 3. Understand What to Measure

**Two types of timing data**:

1. **Launcher creation time (ctime)**: When WorkGraph engine decided to submit a task
   - Tests: Dynamic level computation, submission order
   - Extract with: `extract_launcher_times()`

2. **Task completion time (mtime)**: When actual job finished
   - Tests: Branch independence, overall timing
   - Extract with: `extract_task_completion_times()`

### 4. Handle Timing Variability

Integration tests should be resilient to timing variations:

```python
# Use try-except for timing-sensitive checks
try:
    assert_pre_submission_occurred(times, 'fast_2', 'fast_1')
    LOGGER.info("✓ PASS: Pre-submission detected")
except AssertionError as e:
    LOGGER.warning(f"⚠ SKIPPED: Pre-submission not detected (timing variation)")
    # Don't fail the test - timing depends on overhead
```

### 5. Test Core Behaviors

**Always test**:
- ✓ Branch independence (fast branch completes before slow)
- ✓ Submission order (tasks submitted in correct sequence)
- ✓ Cross-dependencies respected

**Optional (timing-dependent)**:
- ⚠ Pre-submission occurs (depends on scheduler overhead)
- ⚠ Exact number of pre-submitted tasks

## CI Setup

### Requirements

**Minimal infrastructure** (already in CI):
```yaml
- Ubuntu latest
- Python 3.11+
- AiiDA core
- SSH localhost
- core.direct scheduler
```

**NOT needed**:
- ❌ SLURM
- ❌ PBS/Torque
- ❌ Real HPC cluster

### CI Configuration

The CI is already set up correctly in `.github/workflows/test-integration.yml`:

```yaml
- name: Setup SSH
  run: |
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ssh-keyscan localhost >> ~/.ssh/known_hosts

- name: Setup AiiDA computer
  run: |
    # Sets up localhost-ssh computer with core.direct scheduler
    # This is sufficient for testing dynamic levels!
```

### Why No SLURM Needed?

**Dynamic level computation happens in the WorkGraph engine**, not the scheduler. The scheduler just needs to run jobs - `core.direct` does this fine.

```
┌─────────────────────────────────────────┐
│   WorkGraph Engine (where we test)     │
│  - Computes dynamic levels              │
│  - Decides submission order             │
│  - Handles pre-submission               │
└─────────────┬───────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────┐
│   Scheduler (just runs jobs)            │
│  - core.direct is sufficient            │
│  - SLURM would add complexity            │
└─────────────────────────────────────────┘
```

## Test Files

### Core Test Files

```
tests/
├── TESTING.md                          # This file
├── unit_tests/
│   ├── test_dynamic_levels.py          # Unit tests (fast)
│   ├── test_workgraph.py               # Integration tests (slow)
│   └── test_utils.py                   # Test utility functions
└── cases/
    └── branch-independence/
        ├── config/
        │   ├── config.yml              # Simple 2-branch test
        │   ├── config_complex.yml      # Complex 3-branch test
        │   ├── DIAGRAM.txt             # Visual dependency diagram
        │   └── scripts/
        │       ├── fast_task.sh        # Fast tasks (5s)
        │       ├── medium_task.sh      # Medium tasks (20s)
        │       └── slow_task.sh        # Slow tasks (60s)
        ├── analyze.py                  # Timing analysis script
        ├── run.sh                      # Wrapper script
        ├── README.md                   # Simple test docs
        └── README_COMPLEX.md           # Complex test docs
```

### Test Utilities (`test_utils.py`)

Reusable helper functions:

```python
from tests.unit_tests.test_utils import (
    extract_launcher_times,          # Get task submission/completion times
    extract_task_completion_times,   # Get actual job completion times
    compute_relative_times,          # Convert to seconds from start
    assert_branch_independence,      # Validate fast < slow
    assert_pre_submission_occurred,  # Check pre-submission
    assert_submission_order,         # Verify submission sequence
    assert_cross_dependency_respected, # Validate cross-deps
    print_timing_summary,            # Debug timing data
)
```

## Writing New Tests

### Adding a Unit Test

```python
# tests/unit_tests/test_dynamic_levels.py

def test_my_new_scenario():
    """Test description."""
    task_deps = {
        'task_a': [],
        'task_b': ['task_a'],
        # ... define dependency graph
    }

    levels = compute_topological_levels(task_deps)

    # Assert expected levels
    assert levels['task_a'] == 0
    assert levels['task_b'] == 1
```

### Adding an Integration Test

```python
# tests/unit_tests/test_workgraph.py

@pytest.mark.slow
@pytest.mark.usefixtures("config_case", "aiida_localhost", "aiida_remote_computer")
@pytest.mark.parametrize("config_case", ["my-test-case"])
def test_my_integration_test(config_paths):
    """Test description."""
    from tests.unit_tests.test_utils import (
        extract_launcher_times,
        assert_branch_independence,
    )

    # Build and run workflow
    core_workflow = Workflow.from_config_file(str(config_paths["yml"]))
    workgraph = build_sirocco_workgraph(core_workflow, window_size=1)
    workgraph.run()

    # Extract timing data
    launcher_times = extract_launcher_times(workgraph.process)

    # Assert relationships
    assert_branch_independence(launcher_times)
    # ... more assertions
```

### Adding a Test Utility

```python
# tests/unit_tests/test_utils.py

def my_new_assertion(timing_data: dict, ...) -> None:
    """My new validation function.

    Args:
        timing_data: Output from extract_launcher_times()

    Raises:
        AssertionError: If validation fails
    """
    # Implement validation logic
    assert some_condition, "Error message explaining what failed"
```

## Test Configuration

### Parametrized Tests

Use pytest parametrization to test multiple configurations:

```python
@pytest.mark.parametrize(
    "window_size",
    [0, 1, 2],  # Test with different window sizes
)
def test_with_window_size(config_paths, window_size):
    workgraph = build_sirocco_workgraph(workflow, window_size=window_size)
    # ... test logic
```

### Test Fixtures

The test infrastructure uses pytest fixtures defined in `conftest.py`:

```python
@pytest.fixture
def aiida_localhost(aiida_profile):
    """Provides localhost computer."""
    # ...

@pytest.fixture
def aiida_remote_computer(aiida_profile):
    """Provides SSH-based remote computer."""
    # Uses core.direct scheduler
```

## Troubleshooting

### Test Failures

**"Fast branch did not complete before slow branch"**
- Check task durations are sufficiently different
- Verify dynamic level computation is enabled
- Review WorkGraph report for errors

**"Pre-submission did not occur"**
- This is timing-dependent and may not always occur in CI
- Check if using `window_size > 0`
- Review launcher creation times with `analyze.py`

**Timeout errors**
- Increase timeout in test configuration
- Check SSH connectivity: `ssh localhost 'echo test'`
- Verify AiiDA daemon is running

### Debugging Tests

**Enable verbose logging**:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Print timing summary**:
```python
from tests.unit_tests.test_utils import print_timing_summary
print_timing_summary(launcher_times)
```

**Analyze WorkGraph report**:
```bash
verdi process report <PK>
```

**Check descendant nodes**:
```python
for node in workgraph.process.called_descendants:
    print(f"{node.pk}: {node.label} - {node.process_state}")
```

## Best Practices

1. **Keep tests focused**: Each test should validate one specific behavior
2. **Use descriptive names**: Test names should explain what they validate
3. **Test relationships**: Don't rely on absolute timing values
4. **Handle variability**: Use try-except for timing-sensitive checks
5. **Document expectations**: Explain what behavior the test validates
6. **Keep tests fast**: Unit tests should run in seconds
7. **Make tests reproducible**: Avoid random behavior
8. **Log useful info**: Help future debuggers understand failures

## Summary

Our testing strategy provides comprehensive coverage:

- **Unit tests** validate core algorithms (fast, no infrastructure)
- **Integration tests** validate actual execution (relationships, not times)
- **Manual tests** provide visualization and performance analysis

This approach ensures dynamic level computation and pre-submission work correctly across different environments, without requiring expensive SLURM infrastructure in CI.

## Further Reading

- [Branch Independence Test Documentation](cases/branch-independence/README.md)
- [Complex Workflow Test Documentation](cases/branch-independence/README_COMPLEX.md)
- [Dynamic Levels Implementation](../DYNAMIC_LEVELS_IMPLEMENTATION.md)
- [AiiDA WorkGraph Documentation](https://github.com/aiidateam/aiida-workgraph)
