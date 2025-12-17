#!/bin/bash
# Wrapper script to run the DYAMOND_cwd01 workflow via CLI
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-'santis-ssh'}"

echo "Running DYAMOND_cwd01 workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Config: ${SCRIPT_DIR}/config.yaml"
echo ""

# Run sirocco with the config
sirocco run "${SCRIPT_DIR}/config.yaml" "$@"
