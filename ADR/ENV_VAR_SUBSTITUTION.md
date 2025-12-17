# Environment Variable Substitution in Sirocco Configs

## Overview

Sirocco now supports environment variable substitution in workflow configuration files using the `${VAR:-default}` syntax. This eliminates code duplication between test cases and examples by allowing the same configuration file to be used in different contexts.

## Syntax

```yaml
tasks:
  - my_task:
      computer: ${SIROCCO_COMPUTER:-localhost}
      path: ${SIROCCO_SCRIPTS_DIR:-scripts}/my_script.sh
```

### Supported Patterns

- **`${VAR}`**: Replace with environment variable `VAR`. If not set, keeps the literal string `${VAR}`.
- **`${VAR:-default}`**: Replace with environment variable `VAR`. If not set, use `default` value.

### Important: Path Validation

**Sirocco requires paths to be relative to the config directory**. When using environment variables for paths, ensure they expand to relative paths, not absolute paths:

```bash
# ✅ Correct - relative path
export SIROCCO_SCRIPTS_DIR="scripts"

# ❌ Wrong - absolute path (will fail validation)
export SIROCCO_SCRIPTS_DIR="/absolute/path/to/scripts"
```

## Implementation

Environment variable expansion is implemented in `src/sirocco/parsing/yaml_data_models.py` in the `ConfigWorkflow.from_config_file()` method:

```python
def expand_env_vars(text: str) -> str:
    """Expand environment variables in text with ${VAR} or ${VAR:-default} syntax."""
    def replace_var(match):
        var_expr = match.group(1)
        if ":-" in var_expr:
            var_name, default = var_expr.split(":-", 1)
            return os.environ.get(var_name, default)
        else:
            return os.environ.get(var_expr, match.group(0))
    return re.sub(r'\$\{([^}]+)\}', replace_var, text)
```

The substitution happens after reading the file content but before YAML parsing, ensuring all variable references are resolved before validation.

## Usage in Tests

### Test Fixture Setup

In `tests/conftest.py`, the `config_paths` fixture automatically sets environment variables for test cases that use them:

```python
if config_case == "branch-independence":
    import os
    scripts_dir = str(tmp_path / f"tests/cases/{config_case}/config/scripts")
    os.environ['SIROCCO_COMPUTER'] = 'remote'
    os.environ['SIROCCO_SCRIPTS_DIR'] = scripts_dir

    # Clean up after test
    def cleanup():
        os.environ.pop('SIROCCO_COMPUTER', None)
        os.environ.pop('SIROCCO_SCRIPTS_DIR', None)
    request.addfinalizer(cleanup)
```

### Running Tests

```bash
# Environment variables are automatically set by pytest fixtures
pytest tests/unit_tests/test_workgraph.py::test_branch_independence_execution -v -m slow
```

## Usage in Manual Runs

### Option 1: Wrapper Script

Create a `run.sh` script that sets variables and calls `sirocco submit`:

```bash
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-localhost}"
export SIROCCO_SCRIPTS_DIR="${SCRIPT_DIR}/config/scripts"

sirocco submit "${SCRIPT_DIR}/config/config.yml" --window-size 1 "$@"
```

Usage:
```bash
./tests/cases/branch-independence/run.sh

# Or override computer
SIROCCO_COMPUTER=my-remote ./tests/cases/branch-independence/run.sh
```

### Option 2: Manual Environment Variables

```bash
export SIROCCO_COMPUTER=localhost
export SIROCCO_SCRIPTS_DIR=/absolute/path/to/scripts
sirocco submit config.yml --window-size 1
```

## Example: Branch Independence Test Case

The `branch-independence` test case demonstrates this feature:

**Config file** (`tests/cases/branch-independence/config/config.yml`):
```yaml
tasks:
  - root:
      plugin: shell
      computer: ${SIROCCO_COMPUTER:-localhost}
      path: ${SIROCCO_SCRIPTS_DIR:-scripts}/fast_task.sh
      command: "bash fast_task.sh root 1"
```

**For pytest**: Fixture sets `SIROCCO_COMPUTER=remote` and `SIROCCO_SCRIPTS_DIR=scripts` (relative path)

**For manual runs**: Wrapper script sets `SIROCCO_COMPUTER=localhost` and `SIROCCO_SCRIPTS_DIR=scripts` (relative path)

## Benefits

### Before (with duplication):
```
examples/branch-independence/
├── config.yml          # Hardcoded localhost, absolute paths
└── scripts/...

tests/cases/branch-independence/
├── config.yml          # Hardcoded remote, TEST_ROOTDIR placeholders
└── scripts/...
```

### After (no duplication):
```
tests/cases/branch-independence/
├── config/
│   ├── config.yml      # Uses ${SIROCCO_COMPUTER:-localhost}
│   └── scripts/...
├── run.sh              # Wrapper for manual execution
└── README.md
```

**Benefits:**
- ✅ Single source of truth for workflow configuration
- ✅ No code duplication
- ✅ Tests use pytest fixtures to configure
- ✅ Manual runs use environment variables
- ✅ Easy to maintain and update
- ✅ Consistent behavior between test and manual execution

## Adding Variable Substitution to New Test Cases

1. **Update config file** to use `${VAR:-default}` syntax for environment-specific values
2. **Add fixture logic** in `tests/conftest.py` to set variables for your test case
3. **Create run.sh script** for manual execution
4. **Document** the variables in the test case README

## Limitations

- Only string substitution is supported (not nested structures or expressions)
- Variables must use `${...}` syntax (not `$VAR`)
- No arithmetic or string manipulation within variables
- Substitution happens at file read time, not runtime

## Future Enhancements

Potential future improvements:
- Support for nested variable references: `${DIR}/${SUBDIR}`
- Config-specific variable files: `config.env`
- CLI parameter passing: `sirocco submit config.yml --set VAR=value`
- Variable validation and required variables
