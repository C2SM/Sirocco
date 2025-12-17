#!/bin/bash
# Wrapper script to run the small-shell workflow via CLI
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
export SIROCCO_COMPUTER="${SIROCCO_COMPUTER:-remote}"
export SIROCCO_DATA_COMPUTER="${SIROCCO_DATA_COMPUTER:-localhost}"

echo "Running small-shell workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Data computer: $SIROCCO_DATA_COMPUTER"
echo ""

# Run sirocco run with the config
sirocco run "${SCRIPT_DIR}/config/config.yml" "$@"
