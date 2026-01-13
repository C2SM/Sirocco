#!/bin/bash
# Wrapper script to run the dynamic-deps-complex workflow manually
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
# Note: Paths must be relative to the config file location for Sirocco validation
export SIROCCO_COMPUTER="eiger-firecrest"
export SIROCCO_SCRIPTS_DIR="../scripts"  # Relative to config file
export SLURM_ACCOUNT="mr32"

# Set time multiplication factor (1 for quick testing, 10 for production plots)
export SIROCCO_TIME_FACTOR="1"

# Calculate actual task durations (base: fast=10s, medium=30s, slow=60s)
export FAST_TIME=$((10 * SIROCCO_TIME_FACTOR))
export MEDIUM_TIME=$((30 * SIROCCO_TIME_FACTOR))
export SLOW_TIME=$((60 * SIROCCO_TIME_FACTOR))

echo "Running dynamic-deps-complex workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Scripts directory: $SIROCCO_SCRIPTS_DIR (relative to config file)"
echo "  Time factor: $SIROCCO_TIME_FACTOR"
echo "  SLURM account: $SLURM_ACCOUNT"
echo ""

# Run sirocco with the config
sirocco run "${SCRIPT_DIR}/config/config.yml" "$@"
