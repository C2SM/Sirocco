#!/bin/bash
# Wrapper script to run the small-icon workflow via CLI
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-remote}"

echo "Running small-icon workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo ""

# Run sirocco run with the config
sirocco run "${SCRIPT_DIR}/config/config.yml" "$@"
