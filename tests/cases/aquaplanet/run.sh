#!/bin/bash
# Wrapper script to run the aquaplanet workflow
# Variables are loaded from config/vars.yml and can be overridden by environment variables

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Running aquaplanet workflow with Jinja2 templating"
echo "Content of the \`vars.yml\` file:"
echo "*********************************"
cat "${SCRIPT_DIR}/config/vars.yml"
echo "*********************************"

# Run sirocco with the config
# The config uses Jinja2 syntax ({{ VAR }}) and gets values from vars.yml
sirocco run "${SCRIPT_DIR}/config/config.yml" "$@"
