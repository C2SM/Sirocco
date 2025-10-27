# WorkGraph Refactoring Summary

## Overview
Refactored the AiiDA WorkGraph implementation to properly handle SLURM job dependencies by passing all task data through serializable formats (PKs, labels, primitives) instead of complex Python objects.

## Core Problem
The original implementation tried to pass complex AiiDA objects (Codes, Builders, Data nodes, Computer objects) and Python objects (task instances with datetime coordinates) through WorkGraph `@task.graph` decorated functions. These functions require JSON-serializable inputs because WorkGraph needs to pickle/serialize the data.

## Key Constraint
**ALL data passed to `@task.graph` functions must be JSON-serializable**:
- ✅ Integers, strings, dicts, lists, booleans
- ❌ AiiDA ORM objects (Code, SinglefileData, Computer, Builder)
- ❌ Python datetime objects
- ❌ Custom Python class instances

## Major Changes

### 1. Serialize Datetime Objects (`workgraph.py:30-41`)
**Problem**: `coordinates` dict contained datetime objects (cycle dates)

**Solution**: Created `serialize_coordinates()` helper function
```python
def serialize_coordinates(coordinates: dict) -> dict:
    """Convert datetime objects to ISO format strings"""
    for key, value in coordinates.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
```

### 2. Shell Tasks: Pass PKs Instead of Objects

#### A. Code Objects (`workgraph.py:587-623`)
**Problem**: `ShellCode` objects aren't serializable

**Solution**:
- Remove UUID from code labels (was creating duplicate codes)
- Try to load existing code first: `load_code(f"{cmd}@{computer.label}")`
- Only create if doesn't exist
- Store `code_pk` in task_spec instead of code object
- Load code from PK at runtime: `aiida.orm.load_node(task_spec["code_pk"])`

#### B. Script Nodes (`workgraph.py:607-612`)
**Problem**: `SinglefileData` nodes aren't serializable

**Solution**:
- Store script nodes and get PKs: `script_node.store(); node_pks[label] = script_node.pk`
- Pass `node_pks` dict instead of `nodes` dict
- Load nodes at runtime: `{key: aiida.orm.load_node(pk) for key, pk in node_pks.items()}`

#### C. Computer in Metadata (`workgraph.py:556-574`)
**Problem**: `Computer` objects aren't serializable

**Solution**:
- Store `computer_label` string in metadata
- At runtime, pop label and load computer: `computer = Computer.collection.get(label=metadata.pop("computer_label"))`
- Set in metadata: `metadata["computer"] = computer`

#### D. Pre-compute Input/Output Information (`workgraph.py:625-649`)
**Problem**: Task objects with methods aren't serializable

**Solution**:
- Pre-compute all input data info as serializable dicts:
  ```python
  input_info = {
      "port": port_name,
      "name": input_.name,
      "coordinates": serialize_coordinates(input_.coordinates),
      "label": ...,
      "is_available": ...,
      "path": str(input_.path) if input_.path else None,
  }
  ```
- Pre-resolve arguments template BEFORE passing through WorkGraph
- Build filenames dict upfront
- Build outputs list upfront

### 3. ICON Tasks: Serialize Builder Components

#### A. Decompose Builder into PKs (`workgraph.py:742-796`)
**Problem**: `IconCalculation` builder contains AiiDA nodes and can't be pickled

**Solution**: Instead of passing builder, pass component PKs:
```python
return {
    "code_pk": icon_code.pk,
    "master_namelist_pk": master_namelist_node.pk,
    "model_namelist_pks": {model_name: node.pk, ...},
    "wrapper_script_pk": wrapper_script_pk,
    "metadata": metadata,  # with computer_label
    "output_port_mapping": {...},
}
```

#### B. Reconstruct Builder at Runtime (`workgraph.py:223-255`)
**Solution**:
```python
builder = IconCalculation.get_builder()
builder.code = aiida.orm.load_node(task_spec["code_pk"])
builder.master_namelist = aiida.orm.load_node(task_spec["master_namelist_pk"])
for model_name, model_pk in task_spec["model_namelist_pks"].items():
    setattr(builder.models, model_name, aiida.orm.load_node(model_pk))
# ... load computer, set metadata, etc.
```

#### C. Output Port Mapping (`workgraph.py:789-796`)
**Problem**: ICON output port names (e.g., `latest_restart_file`) differ from data names (e.g., `restart`)

**Solution**:
- Pre-compute mapping: `data.name -> ICON port_name`
- Use this mapping to return outputs with correct keys:
  ```python
  for data_name, icon_port_name in output_port_mapping.items():
      outputs[data_name] = getattr(icon_task.outputs, icon_port_name)
  ```

### 4. Metadata Options

#### Base Metadata (all tasks) (`workgraph.py:558-579`)
- `account` (generic)
- `additional_retrieve_list` (generic CalcJob option)
- Scheduler options (resources, walltime, memory)
- `computer_label` (converted to Computer at runtime)

#### Shell-Specific (`workgraph.py:604-605`)
- `use_symlinks = True` (aiida-shell specific, not in CalcJob spec)

#### Important Notes
- ❌ `use_symlinks` is NOT a generic CalcJob option (aiida-shell only)
- ✅ `additional_retrieve_list` IS generic (not `additional_retrieve`)
- ICON tasks should NOT get `use_symlinks`

### 5. Dynamic Output Namespaces (`workgraph.py:78, 202`)
**Problem**: WorkGraph didn't know about dynamic output keys like `icon_output`, `restart`

**Solution**: Add return type annotation to `@task.graph` functions:
```python
) -> Annotated[dict, dynamic(aiida.orm.Data)]:
```

### 6. Optional Ports Handling (`workgraph.py:629-638`)
**Problem**: Command templates reference ports (e.g., `{PORT::restart}`) that aren't connected yet

**Solution**: Pre-scan command for all ports and add missing ones with empty lists:
```python
for port_match in task.port_pattern.finditer(task.command):
    port_name = port_match.group(2)
    if port_name and port_name not in input_labels:
        input_labels[port_name] = []
```

## Architecture Pattern

### Before
```
_build_shell_task_spec() returns:
  - code: ShellCode object ❌
  - nodes: {name: SinglefileData} ❌
  - metadata: {computer: Computer} ❌
  - task: ShellTask object ❌

@task.graph function receives non-serializable objects → FAILS
```

### After
```
_build_shell_task_spec() returns:
  - code_pk: int ✅
  - node_pks: {name: int} ✅
  - metadata: {computer_label: str} ✅
  - input_data_info: [dict, ...] ✅
  - output_data_info: [dict, ...] ✅

@task.graph function:
  1. Load objects from PKs
  2. Reconstruct complex objects
  3. Use them (objects never cross WorkGraph boundary)
```

## Issues Fixed

### 1. Output Connection Issue (FIXED)
**Problem**: Tasks were receiving wrong output types (Dict instead of RemoteData for restart_file)

**Root Cause**: When @task.graph functions returned custom dicts, WorkGraph wrapped them in TaskSocketNamespace but individual keys weren't accessible as attributes.

**Solution** (`workgraph.py:87-290`):
- Return task.outputs directly instead of custom dicts
- Added output_port_mapping to both shell and ICON task specs
- Updated connection logic to use output_port_mapping for translating data names to port names
- Made input_data_nodes optional with default=None

**Result**: ✅ All ICON tasks now start successfully with correct output connections

### 2. Missing SLURM Job Dependencies (IN PROGRESS)
**Problem**: Tasks that depend on generated data weren't getting SLURM job dependencies

**Root Cause**: Only collecting job IDs from `task.wait_on`, not from tasks that produce input data (GeneratedData).

**Solution** (`workgraph.py:328-355`):
- When collecting `dep_job_ids`, also add job IDs from tasks that generate input data
- Ensures second ICON task waits for first ICON task's SLURM job to complete

### 3. Metadata Setting Issue (IN PROGRESS)
**Problem**: custom_scheduler_commands might be overwritten when setting builder.metadata

**Solution** (`workgraph.py:238-262`):
- Set builder.metadata.computer and builder.metadata.options individually
- Avoid replacing entire metadata dict which might lose options
- Added debug logging for custom_scheduler_commands

## Testing Status
- Output mapping: ✅ Working
- Job dependencies: 🔄 Testing in progress

## Files Modified
- `src/sirocco/workgraph.py` - Main refactoring
- All changes preserve the existing workflow logic, just change how data is passed through WorkGraph boundaries

## Testing Notes
- Shell tasks: Working correctly
- ICON tasks: First task works, fails when connecting to second task (restart_file type mismatch)
- Need to fix: Sequential execution and output connection logic

## Key Takeaways
1. **Always pass PKs/labels through WorkGraph boundaries, never ORM objects**
2. **Pre-compute everything that requires complex objects before @task.graph**
3. **Reconstruct objects from PKs inside @task.graph functions**
4. **Test serialization**: If it can't be JSON-encoded, it can't cross the boundary
5. **Shell-specific options** (like `use_symlinks`) must not be applied to ICON tasks
