#!/bin/bash
# Slow task script for branch independence testing
# Usage: slow_task.sh <task_name> <sleep_seconds>

TASK_NAME="${1:-slow_task}"
SLEEP_TIME="${2:-8}"

echo "[$TASK_NAME] Starting at $(date +%H:%M:%S)"

# Read input values if they exist (for dependency verification)
RESULT=0
shopt -s nullglob  # Make non-matching globs expand to nothing
INPUT_FILES=(*_output/value.txt)
shopt -u nullglob
if [ ${#INPUT_FILES[@]} -gt 0 ] && [ -f "${INPUT_FILES[0]}" ]; then
    # Sum all input values
    for input_file in *_output/value.txt; do
        if [ -f "$input_file" ]; then
            value=$(cat "$input_file")
            RESULT=$(echo "$RESULT + $value" | bc)
            echo "[$TASK_NAME] Read input: $value from $input_file"
        fi
    done
    # Multiply by 3 for slow tasks
    RESULT=$(echo "$RESULT * 3" | bc)
    echo "[$TASK_NAME] Computed: inputs * 3 = $RESULT"
else
    # No inputs - initialize with 3
    RESULT=3
    echo "[$TASK_NAME] No inputs, initializing with value: $RESULT"
fi

echo "[$TASK_NAME] Sleeping for ${SLEEP_TIME} seconds..."
sleep "$SLEEP_TIME"

echo "[$TASK_NAME] Creating output file..."
mkdir -p "${TASK_NAME}_output"
echo "$RESULT" > "${TASK_NAME}_output/value.txt"
echo "Slow task $TASK_NAME completed at $(date +%H:%M:%S)" > "${TASK_NAME}_output/result.txt"

echo "[$TASK_NAME] Wrote result: $RESULT"
echo "[$TASK_NAME] Completed at $(date +%H:%M:%S)"
