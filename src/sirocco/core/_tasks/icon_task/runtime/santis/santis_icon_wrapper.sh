#!/usr/local/bin/bash -l
set -e

# Parse annotated hostfile
# ------------------------
if [ ! -f ${SLURM_HOSTFILE_ANNOTATED} ]; then
    echo "ERROR: Annotated hostfile ${SLURM_HOSTFILE_ANNOTATED} not found."
    exit 1
fi
# read line corresponding to SLURM_PROCID
rank_info=($(sed -n $((SLURM_PROCID+1))p ${SLURM_HOSTFILE_ANNOTATED}))
# Parse line
NID=${rank_info[0]}  # node id
NUMA_NODE=${rank_info[1]}  # numa node 
PE_TYPE=${rank_info[2]}  # "compute", "io"  or "hiopy"
TARGET=${rank_info[3]}  # "cpu", "gpu" or "hiopy"
MODEL=${rank_info[4]}  # icon master model name or "hiopy"

# Set up environment
# ------------------
source santis_environments.sh
santis_common_environment
if [ "${PE_TYPE}" == "compute" ]; then
    if [ "${TARGET}" == "cpu" ]; then
        santis_compute_cpu_environment
    elif [ "${TARGET}" == "gpu" ]; then
        santis_compute_gpu_environment
        [ -n "${ICON4PY_VENV}" ] && santis_icon4py_environment
    fi
elif [ "${PE_TYPE}" == "io" ]; then
    santis_io_environment
fi

# Launch executable
# -----------------
numactl --cpunodebind=$NUMA_NODE --membind=$NUMA_NODE bash -c "$@"
