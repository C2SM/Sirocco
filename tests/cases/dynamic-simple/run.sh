#!/bin/bash
# Wrapper script to run the dynamic-deps-simple workflow manually
# This sets the required environment variables and runs sirocco submit

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
# Note: Paths must be relative to the config file location for Sirocco validation
export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-'santis-ssh'}"
export SIROCCO_SCRIPTS_DIR="../scripts"  # Relative to config file

echo "Running dynamic-deps-simple workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Scripts directory: $SIROCCO_SCRIPTS_DIR (relative to config file)"
echo ""

# Run sirocco with the config
sirocco run "${SCRIPT_DIR}/config.yml" "$@"
