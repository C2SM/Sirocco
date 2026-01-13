#!/bin/bash
# Wrapper script to run the DYAMOND_cwd01 workflow via CLI
# This sets the required environment variables and runs sirocco run

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set environment variables
# Note: Paths must be relative to the config file location for Sirocco validation
export SIROCCO_COMPUTER="santis-ssh"
export SIROCCO_SCRIPTS_DIR="../scripts"  # Relative to config file
export SLURM_ACCOUNT="cwd01"

echo "Running DYAMOND workflow with:"
echo "  Computer: $SIROCCO_COMPUTER"
echo "  Config: ${SCRIPT_DIR}/config/config.yaml"
echo "  SLURM account: $SLURM_ACCOUNT"
echo ""

# Run sirocco with the config
sirocco run "${SCRIPT_DIR}/config/config.yaml" "$@"
