#!/bin/bash
# Slow task script for branch independence testing
# Usage: slow_task.sh <task_name> <sleep_seconds>

TASK_NAME="${1:-slow_task}"
SLEEP_TIME="${2:-8}"

echo "[$TASK_NAME] Starting at $(date +%H:%M:%S)"
echo "[$TASK_NAME] Sleeping for ${SLEEP_TIME} seconds..."

sleep "$SLEEP_TIME"

echo "[$TASK_NAME] Creating output file..."
mkdir -p "${TASK_NAME}_output"
echo "Slow task $TASK_NAME completed at $(date +%H:%M:%S)" > "${TASK_NAME}_output/result.txt"

echo "[$TASK_NAME] Completed at $(date +%H:%M:%S)"
