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
nid=${rank_info[0]}  # node id
numa_node=${rank_info[1]}  # numa node
pe_type=${rank_info[2]}  # "compute", "io"  or "hiopy"
target=${rank_info[3]}  # "cpu", "gpu" or "hiopy"
model=${rank_info[4]}  # icon master model name or "hiopy"

# Set up environment
# ------------------
source santis_environments.sh
santis_common_environment
if [ "${pe_type}" == "compute" ]; then
    if [ "${target}" == "cpu" ]; then
        santis_compute_cpu_environment
    elif [ "${target}" == "gpu" ]; then
        santis_compute_gpu_environment
        [ -n "${ICON4PY_VENV}" ] && santis_icon4py_environment
    fi
elif [ "${pe_type}" == "io" ]; then
    santis_io_environment
fi

# Launch executable
# -----------------
numactl --cpunodebind=$numa_node --membind=$numa_node bash -c "$@"
