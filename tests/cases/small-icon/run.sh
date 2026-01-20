#!/bin/bash
# Wrapper script to run the small-icon workflow
# Variables are loaded from config/vars.yml and can be overridden by environment variables

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Running small-icon workflow with Jinja2 templating"
echo "Variables loaded from:"
echo "  1. config/vars.yml (base configuration)"
echo "  2. Environment variables (runtime overrides, if set)"
echo ""
echo "Content of the \`vars.yml\` file:"
echo "*********************************"
cat "${SCRIPT_DIR}/config/vars.yml"
echo "*********************************"

# Run sirocco with the config
# The config uses Jinja2 syntax ({{ VAR }}) and gets values from vars.yml
sirocco run "${SCRIPT_DIR}/config/config.yml" "$@"
