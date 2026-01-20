# Complex Multi-Branch Test Case

This test case extends the basic branch-independence test with:
- **3 branches** with different execution speeds
- **Cross-dependencies** between branches
- **Multiple cycles** (3 monthly iterations)
- **Convergence points** where all branches synchronize

## Test Structure

### Branch Characteristics

| Branch | Task Duration | Purpose |
|--------|--------------|---------|
| **Fast** | 5s | Quick processing, advances independently |
| **Medium** | 20s | Moderate processing, depends on fast branch |
| **Slow** | 60s | Heavy processing, depends on medium branch |

### Dependency Graph (Per Cycle)

```
setup (init, 5s)
  ↓
  ├──→ fast_1 (5s) → fast_2 (5s) → fast_3 (5s) ──┐
  │                      ↓                        │
  ├──→ medium_1 (20s) → medium_2 (20s) → medium_3 (20s) ──┤
  │                          ↓                              │
  └──→ slow_1 (60s) → slow_2 (60s) → slow_3 (60s) ────────┤
                                                            ↓
                                                      finalize (5s)
                                                            ↓
                                                      prepare_next (5s)
```

### Cross-Dependencies

1. **medium_2** depends on:
   - `medium_1` (same branch)
   - `fast_2` ← **Cross-dependency from fast branch**

2. **slow_2** depends on:
   - `slow_1` (same branch)
   - `medium_2` ← **Cross-dependency from medium branch**

3. **finalize** depends on:
   - `fast_3` (fast branch end)
   - `medium_3` (medium branch end)
   - `slow_3` (slow branch end)
   - **Convergence point** where all branches must sync

### Multi-Cycle Behavior

The workflow runs for **3 monthly cycles** (2026-01-01, 2026-02-01, 2026-03-01):

```
Cycle 1 (2026-01-01):
  setup → [fast + medium + slow branches] → finalize → prepare_next

Cycle 2 (2026-02-01):
  [depends on Cycle 1's prepare_next]
  → [fast + medium + slow branches] → finalize → prepare_next

Cycle 3 (2026-03-01):
  [depends on Cycle 2's prepare_next]
  → [fast + medium + slow branches] → finalize → prepare_next
```

## What This Tests

### 1. Dynamic Level Computation with Cross-Dependencies

**Without dynamic levels:**
- fast_2, medium_2, slow_2 would all wait for each other (same topological level)
- Unnecessary blocking despite no direct dependencies

**With dynamic levels:**
- fast_2 can complete while slow_1 is still running
- medium_2 starts as soon as BOTH medium_1 AND fast_2 finish
- Each branch advances independently until cross-dependencies require synchronization

### 2. Pre-submission with Different Window Sizes

Test with various `front_depth` values:

#### `front_depth=0` (Sequential)
```
Level 0 completes → Submit Level 1
Level 1 completes → Submit Level 2
(etc.)
```
- Most conservative
- No pre-submission

#### `front_depth=1` (Default)
```
Level 0 running → Can submit Level 0 + Level 1
```
- Tasks submitted before dependencies finish
- Optimal for most workflows

#### `front_depth=2` (Aggressive)
```
Level 0 running → Can submit Level 0 + Level 1 + Level 2
```
- Very aggressive pre-submission
- Good for high-throughput scenarios

### 3. Branch Convergence

At the **finalize** task:
- Fast branch will complete first (~15s)
- Medium branch completes next (~60s)
- Slow branch completes last (~180s)
- finalize must wait for ALL three

This tests:
- Dynamic levels correctly handle convergence
- No premature task submission
- Proper synchronization at merge points

### 4. Inter-Cycle Dependencies

The `prepare_next` task:
- Depends on current cycle's finalize
- Referenced by next cycle's prepare_next
- Tests cyclic workflow patterns

## Running the Test

### Option 1: CLI with different front depths

```bash
# Test with front_depth=0 (sequential)
export SIROCCO_COMPUTER=localhost
export SIROCCO_SCRIPTS_DIR=tests/cases/branch-independence/config/scripts
sirocco run tests/cases/branch-independence/config/config_complex.yml --front-depth 0

# Test with front_depth=1 (default, one level ahead)
sirocco run tests/cases/branch-independence/config/config_complex.yml --front-depth 1

# Test with front_depth=2 (aggressive, two levels ahead)
sirocco run tests/cases/branch-independence/config/config_complex.yml --front-depth 2
```

### Option 2: Wrapper Script

```bash
#!/bin/bash
export SIROCCO_COMPUTER=localhost
export SIROCCO_SCRIPTS_DIR=tests/cases/branch-independence/config/scripts

WINDOW_SIZE=${1:-1}
sirocco run tests/cases/branch-independence/config/config_complex.yml \
    --front-depth $WINDOW_SIZE
```

### Option 3: Python Test

```python
from sirocco.core import Workflow
from sirocco.workgraph import build_sirocco_workgraph

# Load workflow
wf = Workflow.from_config_file('config_complex.yml')

# Test with different front depths
for front_depth in [0, 1, 2]:
    print(f"Testing with front_depth={front_depth}")
    wg = build_sirocco_workgraph(wf, front_depth=front_depth)
    wg.submit()
```

## Analyzing Results

Use the analyze.py script:

```bash
# After workflow completes, get its PK
verdi process list

# Analyze timing
python tests/cases/branch-independence/analyze.py <PK>
```

### Expected Behavior

**With front_depth=1 and dynamic levels:**

1. **Fast branch independence:**
   - fast_1, fast_2, fast_3 complete in ~15s total
   - Not blocked by medium or slow branches

2. **Cross-dependency synchronization:**
   - medium_2 waits for fast_2 (cross-dep)
   - slow_2 waits for medium_2 (cross-dep)
   - Proper synchronization without unnecessary blocking

3. **Pre-submission:**
   - Tasks submitted before dependencies finish
   - Example: medium_2 submitted while medium_1 still running

4. **Convergence:**
   - finalize waits for all three branches
   - Only starts after slow_3 completes (~180s)

### Validation Metrics

The analyze script will show:
- ✓ Fast branch completes before medium and slow
- ✓ Medium branch completes before slow
- ✓ Cross-dependencies respected (medium_2 after fast_2)
- ✓ Finalize is last to start (after all branches)

## Comparison with Simple Test

| Feature | Simple (config.yml) | Complex (config_complex.yml) |
|---------|---------------------|------------------------------|
| Branches | 2 (fast, slow) | 3 (fast, medium, slow) |
| Cross-deps | None | Yes (medium←fast, slow←medium) |
| Cycles | 1 | 3 (monthly) |
| Convergence | No | Yes (finalize) |
| Inter-cycle | No | Yes (prepare_next) |
| Complexity | Minimal | Moderate |

The complex test provides a more realistic scenario while remaining simpler than the full `large` test case.

## Troubleshooting

### Tasks not advancing independently
- Check that `front_depth > 0`
- Verify dynamic level computation is enabled
- Review WorkGraph report: `verdi process report <PK>`

### Unexpected task ordering
- Examine launcher creation times with analyze.py
- Check for missing cross-dependencies in config
- Validate that scripts are executable: `ls -l scripts/*.sh`

### Performance issues
- Large overhead is expected (launcher creation, job submission)
- For better visibility, increase task durations:
  - fast: 10s → 30s
  - medium: 20s → 60s
  - slow: 60s → 180s
