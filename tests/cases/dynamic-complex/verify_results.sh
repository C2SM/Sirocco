#!/bin/bash
# Verify mathematical results from workflow execution
# Usage: ./verify_results.sh <workflow_pk>

if [ -z "$1" ]; then
    echo "Usage: $0 <workflow_pk>"
    exit 1
fi

WORKFLOW_PK=$1

echo "Verifying results for workflow PK=$WORKFLOW_PK"
echo "================================================"
echo ""

# Expected values per cycle
declare -A EXPECTED=(
    ["setup"]=1
    ["fast_1"]=2
    ["medium_1"]=2
    ["slow_1"]=3
    ["fast_2"]=3
    ["medium_2"]=10
    ["slow_2"]=39
    ["fast_3"]=4
    ["medium_3"]=20
    ["slow_3"]=117
    ["finalize"]=142
)

# Dates for the cycles
DATES=("2026_01_01" "2026_02_01")

# Function to check a value
check_value() {
    local task=$1
    local date=$2
    local expected=$3
    local workdir=$4

    local value_file="${workdir}/${task}_output/value.txt"

    if [ ! -f "$value_file" ]; then
        echo "  ❌ MISSING: $value_file"
        return 1
    fi

    local actual=$(cat "$value_file")

    if [ "$actual" == "$expected" ]; then
        echo "  ✓ ${task} (${date}): $actual (correct)"
        return 0
    else
        echo "  ❌ ${task} (${date}): $actual (expected $expected)"
        return 1
    fi
}

# Find all CalcJob work directories
WORKDIRS=$(verdi process list -a -p $WORKFLOW_PK | grep CalcJobNode | awk '{print $1}' | xargs -I {} verdi calcjob show {} | grep "remote_workdir" | awk '{print $2}')

# Group by cycle date
for date in "${DATES[@]}"; do
    echo "Cycle: ${date//_/-}"
    echo "-------------------"

    # Find work directories for this cycle
    cycle_workdirs=$(echo "$WORKDIRS" | grep "date_${date}")

    if [ -z "$cycle_workdirs" ]; then
        echo "  No work directories found for cycle $date"
        continue
    fi

    # Check each task
    for task in setup fast_1 medium_1 slow_1 fast_2 medium_2 slow_2 fast_3 medium_3 slow_3 finalize; do
        expected=${EXPECTED[$task]}

        # Find work directory for this task
        workdir=$(echo "$cycle_workdirs" | grep "${task}_date_${date}" | head -1)

        if [ -z "$workdir" ]; then
            # Try without date suffix for setup
            workdir=$(echo "$WORKDIRS" | grep "/${task}$" | head -1)
        fi

        if [ -n "$workdir" ]; then
            check_value "$task" "$date" "$expected" "$workdir"
        fi
    done

    # Check prepare_next (has special logic)
    if [ "$date" == "2026_01_01" ]; then
        expected_prepare=143
    else
        expected_prepare=285
    fi

    workdir=$(echo "$cycle_workdirs" | grep "prepare_next_date_${date}" | head -1)
    if [ -n "$workdir" ]; then
        check_value "prepare_next" "$date" "$expected_prepare" "$workdir"
    fi

    echo ""
done

echo "================================================"
echo "Verification complete!"
