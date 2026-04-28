#!/usr/local/bin/bash -l

# ==================================================================
# This wrapper requires the availability of the following environment variables:
#   - IO_RANKS
#   - GPU_COMPUTE_TASKS_PER_NODE
#   - CPU_COMPUTE_TASKS_PER_NODE
# This wrapper assumes the following order of rank allocation:
#   1- GPU compute ranks
#   2- CPU compute ranks
#   3- IO ranks (from GPU or CPU binary doesn't matter)
# ==================================================================

# tools
source wrapper_tools.sh

# Load environment 
source santis_common_run_environment.sh

# Check if all compulsory environment variables are defined
check_required_vars

# Compute NUMA_NODE and set some env vars for compute ranks
if [ "$(is_compute ${SLURM_PROCID})" == "true" ]; then
    get_numa_node "gpu"
    source santis_gpu_run_environment.sh
    export CUDA_VISIBLE_DEVICES=$NUMA_NODE
else  # => IO rank
    get_numa_node "io"
fi

# Launch executable
numactl --cpunodebind=$NUMA_NODE --membind=$NUMA_NODE bash -c "$@"
