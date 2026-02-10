# AiiDA Engine Architecture

## Summary

This document describes the architecture of Sirocco's AiiDA-based execution engine, which translates Sirocco workflows into WorkGraph structures for submission with AiiDA. The engine handles dependency resolution, SLURM job chaining, and rolling window execution.

This design was introduced in the [PR #234](https://github.com/C2SM/Sirocco/pull/234) and implements a complete execution engine that bridges Sirocco's domain model with AiiDA's workflow infrastructure.

## Architecture

```mermaid
graph TB
    A[YAML Workflow] --> B[core.Workflow]
    B --> C[WorkGraphBuilder]

    C --> D[AiidaAdapter<br/>Domain Translation]
    C --> E[TaskSpecBuilder<br/>Pre-Runtime Specs]
    C --> F[DependencyResolver<br/>Build Mapping]

    E --> G1[ShellTaskSpecBuilder]
    E --> G2[IconTaskSpecBuilder]

    F --> H[DependencyMapping]

    C --> I[TaskPairContext<br/>Create Pairs]
    I --> J1[Launcher Task]
    I --> J2[Monitor Task]

    J1 --> K[TaskSpecResolver<br/>Runtime]
    K --> L1[ShellTaskSpecResolver]
    K --> L2[IconTaskSpecResolver]

    J2 --> M[get_job_data<br/>Async Polling]
    M --> N[job_id + remote_folder]

    N -.feeds into.-> J1

    I --> O[AiiDA WorkGraph]

    style A fill:#e1f5ff
    style C fill:#ffe1e1
    style D fill:#e1ffe1
    style E fill:#f0e1ff
    style F fill:#ffe1f0
    style I fill:#fff0e1
    style K fill:#e1f0ff
    style M fill:#f0ffe1
    style O fill:#ffe1e1
```

## Module Structure

The engine is organized into modules by execution phase and responsibility:

```
src/sirocco/engines/aiida/
├── execute.py                      Public API
├── builder.py                      Top-level WorkGraph builder
├── adapter.py                      Core ↔ AiiDA translation
├── spec_builders.py                Pre-runtime spec building
├── spec_resolvers.py               Runtime spec resolution
├── task_pairs.py                   WG task pair creation
├── monitoring.py                   Job monitoring for pre-submission
├── dependency_resolvers.py         Runtime dependency resolution
├── calcjob_builders.py             CalcJob input builders (for IconCalculation)
├── models.py                       AiiDA-specific data models
├── types.py                        Type aliases, Protocols & TypedDicts
├── code_factory.py                 AiiDA Code creation
├── utils.py                        AiiDA-specific helper utilities
├── topology.py                     Topological level computation
└── patches/
    ├── firecrest_symlink.py        Handle dangling symlinks in pre-submission (aiida-firecrest)
    ├── slurm_dependencies.py       Handle SLURM dependency race conditions (aiida-core)
    └── workgraph_window.py         Rolling window submission control (aiida-workgraph)
```

## Core Design

### Two-Phase Execution Model

**Phase 1: Pre-Runtime (Spec Building)**
```python
# Build immutable, serializable specifications
ShellTaskSpecBuilder(task).build_spec()
└─> AiidaShellTaskSpec (Pydantic model with code_pk, metadata, arguments_template, ...)
```

**Phase 2: Runtime (Execution)**
```python
# Execute with runtime dependencies
ShellTaskSpecResolver(spec).execute(input_data_nodes, task_folders, task_job_ids)
└─> Load code → Resolve deps → Build metadata → Add to WorkGraph
```

### Launcher + Monitor Task Pairs

Each Sirocco task becomes **two** WorkGraph tasks:

```mermaid
graph LR
    A[Launcher Task<br/>build_shell_task_with_dependencies]
    A --> B[Submit CalcJob to HPC]
    B --> C[Monitor Task<br/>get_job_data]
    C --> D[Async Poll]
    D --> E[Return job_id + remote_folder]
    E -.feeds.-> F[Next Launcher Task]
    C -.chain.-> G[Next Monitor Task]

    style A fill:#e1f0ff
    style C fill:#f0ffe1
```

**Why?**
- **Separation**: Launcher handles submission, Monitor tracks completion
- **Chaining**: Monitors chain serially (`monitor_a >> monitor_b`)
- **Data flow**: `remote_folder` → `task_folders`, `job_id` → `task_job_ids`

### Dependency Resolution

**Pre-Runtime: Build Mapping**
```python
build_dependency_mapping(task, workflow) -> DependencyMapping
├─ port_mapping: {port_name: [DependencyInfo(dep_label, filename, data_label)]}
├─ task_folders: {dep_label: remote_folder_socket}
└─ task_job_ids: {dep_label: job_id_socket}
```

**Runtime: Resolve to Nodes**

Shell tasks:
```python
resolve_shell_dependency_mappings(task_folders, port_dependencies)
└─> ShellDependencyMappings(nodes, placeholders, filenames)
```

ICON tasks:
```python
resolve_icon_dependency_mapping(task_folders, port_dependencies, namelists)
└─> {port_name: RemoteData}  # Includes restart file resolution
```

### Rolling Window Execution

Controls submission for large workflows. Configuration is read from `workflow.front_depth` (set in YAML config):

```python
window_config = {
    "enabled": True,
    "front_depth": 2,  # Max active topological levels (from workflow.front_depth)
    "task_dependencies": {...}
}
```

- `front_depth=1`: Sequential (one level at a time)
- `front_depth=2`: Moderate parallelism (2 levels active)
- Higher: More aggressive pre-submission

## Data Models

Models to encapsulate AiiDA-specific data structures, keeping the core domain model engine-agnostic:

**Why separate models?**
- **Separation of concerns**: Core domain models (in `sirocco.core`) contain no business logic or AiiDA-specific data
- **Engine abstraction**: Different execution engines can define their own models or DTOs
- **Type safety**: Pydantic models provide validation and serialization for WorkGraph
- **Immutability**: Specs are built once and remain immutable during execution

**Key model categories:**

1. **Task Specifications** (`AiidaShellTaskSpec`, `AiidaIconTaskSpec`)
   - Pre-built, serializable specifications containing code PKs, metadata, arguments templates
   - Store everything needed for runtime execution (no re-computation)

2. **Dependency Models** (`DependencyInfo`, `DependencyMapping`, `ShellDependencyMappings`)
   - Map task dependencies to AiiDA RemoteData nodes and SLURM job IDs
   - Bridge core workflow structure to AiiDA execution graph
   - `ShellDependencyMappings`: Dataclass grouping nodes, placeholders, and filenames for shell tasks

3. **Metadata Models** (`AiidaMetadata`, `AiidaMetadataOptions`, `AiidaResources`)
   - Translate HPC configuration (cores, walltime, queue) to AiiDA scheduler options
   - Handle computer assignments and custom scheduler directives

4. **Data Info Models** (`InputDataInfo`, `OutputDataInfo`)
   - Track metadata about input/output files (paths, labels, ports, coordinates)
   - Enable proper file staging and output retrieval

5. **Serialized TypedDicts** (in `types.py`)
   - `SerializedInputDataInfo`, `SerializedOutputDataInfo`, `SerializedDependencyInfo`
   - Define the shape of serialized data passed through WorkGraph
   - Imported outside `TYPE_CHECKING` in models.py for Pydantic runtime validation

## Complete Data Flow

```mermaid
sequenceDiagram
    participant User
    participant API
    participant Builder
    participant TaskSpecBuilder
    participant TaskSpecResolver
    participant Monitor
    participant HPC

    User->>API: build_sirocco_workgraph(workflow)
    API->>Builder: WorkGraphBuilder(workflow)

    Builder->>Builder: validate & prepare data nodes

    loop For each task
        Builder->>TaskSpecBuilder: build_spec()
        TaskSpecBuilder->>TaskSpecBuilder: create code, build input/output info
        TaskSpecBuilder-->>Builder: AiidaTaskSpec
    end

    Builder->>Builder: _create_workgraph()

    loop For each task
        Builder->>Builder: build_dependency_mapping()
        Builder->>Builder: TaskPairContext.add_task_pair()
        Note over Builder: Creates Launcher + Monitor
    end

    Builder-->>API: WorkGraph
    API->>HPC: submit()

    loop For each task pair
        HPC->>TaskSpecResolver: launcher.execute(...)
        TaskSpecResolver->>TaskSpecResolver: resolve deps, build metadata
        TaskSpecResolver->>HPC: submit CalcJob

        loop Async
            Monitor->>HPC: poll for job data
        end

        Monitor-->>HPC: job_id + remote_folder
        Note over HPC: Feeds to next task
    end
```

## Design Decisions

### Decision 1: Launcher + Monitor Task Pairs

**Context**: Need SLURM job IDs for dependency chaining while jobs are still running.

**Decision**: Create two tasks per Sirocco task:
- Launcher: Submits the CalcJob
- Monitor: Polls for job_id and remote_folder asynchronously

**Rationale**:
- Decouples submission from completion tracking
- Enables pre-submission workflows (SLURM dependencies)
- Allows rolling window control via monitor chaining

**Alternatives considered**:
- Single task: Can't get job_id until completion
- Direct AiiDA queries: Would bypass WorkGraph's dependency tracking

### Decision 2: Adapter Pattern for Domain Translation

**Context**: Core domain models should remain engine-agnostic.

**Decision**: Create `AiidaAdapter` class with static methods for all core → AiiDA translations.

**Rationale**:
- Keeps core domain clean of execution-engine concerns
- Centralizes all translation logic
- Makes it easier to support multiple engines

**Alternatives considered**:
- Methods on core classes: Would pollute core with engine-specific logic
- Free functions: Less organized, harder to test

### Decision 3: Pydantic Models for Task Specs

**Context**: Task specifications need validation and must be serializable for WorkGraph.

**Decision**: Use Pydantic models for all task specifications and metadata.

**Rationale**:
- Built-in validation ensures correctness
- Automatic serialization/deserialization
- Type safety and IDE support
- Immutability via frozen models

**Alternatives considered**:
- TypedDicts: No validation, mutable
- Dataclasses: No validation, harder to serialize
- Plain dicts: No type safety or validation
