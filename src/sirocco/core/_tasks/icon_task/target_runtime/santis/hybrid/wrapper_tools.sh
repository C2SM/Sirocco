#!/usr/local/bin/bash -l

# Number of sockets per node (could be manually set to 4 ...)
# --------------------------
N_SOCKETS=$(nvidia-smi --list-gpus | wc -l)

# Check that all required variables are defined
# ---------------------------------------------
check_required_vars(){
    local COMPULSORY_VARIABLE_NAMES=( "IO_RANKS" "GPU_COMPUTE_TASKS_PER_NODE" "CPU_COMPUTE_TASKS_PER_NODE" "N_SOCKETS")
    for VAR_NAME in ${COMPULSORY_VARIABLE_NAMES[@]}; do
        if [ -z "${!VAR_NAME}" ]; then
            echo "ERROR: ${VAR_NAME} not defined"
            exit 1
        fi
        if (( GPU_COMPUTE_TASKS_PER_NODE != N_SOCKETS )); then
            echo "ERROR: number of GPU tasks per node must be the same as the number of sockets ${N_SOCKETS}, got ${GPU_COMPUTE_TASKS_PER_NODE}"
            exit 1
        fi
        if (( CPU_COMPUTE_TASKS_PER_NODE % N_SOCKETS != 0 )); then
            echo "ERROR: number of CPU tasks per node must be divisible by the number of sockets ${N_SOCKETS}, got ${CPU_COMPUTE_TASKS_PER_NODE}"
            exit 1
        fi
    done
}

# NUMA node computation
# ---------------------
get_numa_node(){
    local mode="${1}"
    case "${mode}" in
        "gpu" | "io")
            NUMA_NODE="$(( SLURM_LOCALID % N_SOCKETS ))"
            ;;
        "cpu")
            (( LOCAL_RANK = SLURM_LOCALID - GPU_COMPUTE_TASKS_PER_NODE ))
            (( CPU_COMPUTE_TASKS_PER_SOCKET = CPU_COMPUTE_TASKS_PER_NODE / N_SOCKETS ))
            NUMA_NODE=$(( LOCAL_RANK / CPU_COMPUTE_TASKS_PER_SOCKET ))
            if (( NUMA_NODE >= N_SOCKETS )); then
                echo "ERROR: wrong NUMA_NODE computation. Got NUMA_NODE=${NUMA_NODE} but should get a value < ${N_SOCKETS}"
                exit 1
            fi
            ;;
    esac
}

# Check if rank corresponds to an IO task
# ---------------------------------------
is_compute(){
    for rank in ${IO_RANKS[@]}; do
        if [ "$rank" == "$1" ]; then
            echo "false"
            return
        fi
    done
    echo "true"
}
