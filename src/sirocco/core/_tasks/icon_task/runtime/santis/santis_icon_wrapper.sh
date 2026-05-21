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
source santis_common_run_environment.sh
if [ "${PE_TYPE}" == "compute" ]; then
    if [ "${TARGET}" == "cpu" ]; then
        export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
        export ICON_THREADS=${SLURM_CPUS_PER_TASK}  # NOTE: is that used anywhere??
    elif [ "${TARGET}" == "gpu" ]; then
        source santis_gpu_run_environment.sh
        [ -n "${ICON4PY_VENV}" ] && source santis_icon4py_environment.sh
        export CUDA_VISIBLE_DEVICES=$NUMA_NODE
    fi
fi

# Launch executable
# -----------------
numactl --cpunodebind=$NUMA_NODE --membind=$NUMA_NODE bash -c "$@"
