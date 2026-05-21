#!/usr/local/bin/bash -l

# Dump SLURM environment variables
set | grep SLURM

# Generate SLURM hostfile for arbitrary task distribution
SIROCCO_HOSTFILE="./hostfile-sirocco"
export SLURM_HOSTFILE="./hostfile-${SLURM_JOB_ID}"
export SLURM_HOSTFILE_ANNOTATED="./hostfile-${SLURM_JOB_ID}_annotated"
./generate_hostfile.sh ${SIROCCO_HOSTFILE} ${SLURM_HOSTFILE} ${SLURM_HOSTFILE_ANNOTATED}

# build srun command
srun_cmd="srun --ntasks=${N_PROCS} --hint=nomultithread --distribution=arbitrary"
[ -n "${CORES_PER_PROC}" ] && srun_cmd+=" --cpus-per-task=${CORES_PER_PROC}"
[ -n "${SIROCCO_UENV}" ] && srun_cmd+=" --uenv=${SIROCCO_UENV}"
[ -n "${SIROCCO_VIEW}" ] && srun_cmd+=" --view=${SIROCCO_VIEW}"
if [ "${SIROCCO_TARGET}" == "cpu" ]; then
    srun_cmd+=" ./santis_icon_wrapper.sh ./icon_cpu"
elif [ "${SIROCCO_TARGET}" == "gpu" ]; then
    srun_cmd+=" ./santis_icon_wrapper.sh ./icon_gpu"
elif [ "${SIROCCO_TARGET}" == "hybrid" ]; then
    srun_cmd+=" --multi-prog multi-prog.conf"
else
    echo "ERROR: unrecognized SIROCCO_TARGET, got ${SIROCCO_TARGET}"
    exit 1
fi

# launch
echo "running ICON with ${srun_cmd}"
${srun_cmd}
