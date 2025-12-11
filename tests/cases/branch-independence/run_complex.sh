#!/bin/bash
# Wrapper script to run the complex branch-independence workflow manually
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
# Note: Paths must be relative to the config directory for Sirocco validation
export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-'santis-async-ssh'}"
export SIROCCO_SCRIPTS_DIR="scripts"  # Relative to config directory

echo "Running branch-independence workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Scripts directory: $SIROCCO_SCRIPTS_DIR (relative to config dir)"
echo ""

# Run sirocco submit with the config
sirocco run "${SCRIPT_DIR}/config/config_complex.yml" --window-size 1 "$@"
